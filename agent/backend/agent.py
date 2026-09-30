
import json
import os
from pathlib import Path
from openai import OpenAI, BadRequestError
from database import get_db_schema, execute_sql_query, retrieve_memory, save_memory

# OpenAI-compatible client — DeepSeek by default. Override LLM_API_KEY/LLM_BASE_URL/
# LLM_MODEL (e.g. to CometAPI's https://api.cometapi.com/v1) to swap providers/models
# for comparison without touching the agent logic below.
LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("DEEPSEEK_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL") or "https://api.deepseek.com/v1"
LLM_MODEL = os.getenv("LLM_MODEL") or "deepseek-chat"

client = OpenAI(
    api_key=LLM_API_KEY,
    base_url=LLM_BASE_URL
)

# Define the tool metadata for the LLM
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "execute_sql_query",
            "description": "Executes a clean, read-only ClickHouse SELECT query and returns the rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The complete ClickHouse SQL query to run."}
                },
                "required": ["query"]
            }
        }
    }
]

PROMPT_PATH = Path(__file__).parent / "prompts" / "system_instructions.md"
SYSTEM_INSTRUCTIONS = PROMPT_PATH.read_text().replace("{db_schema}", get_db_schema())

MAX_TOOL_ITERATIONS = 10


def run_bike_agent(user_question: str, history: list[dict] = None) -> dict:
    """Runs the agent loop and returns {"answer": str, "queries": list[str]}."""
    history = history or []
    
    # Retrieve long-term memory for few-shot examples
    memories = retrieve_memory(user_question)
    memory_context = ""
    if memories:
        memory_context = "\n\nCRITICAL: Here are some proven successful SQL queries for similar past questions. Use them as reference if helpful:\n"
        for mem in memories:
            memory_context += f"- Question: {mem['question']}\n  Query: {mem['query']}\n"

    messages = [
        {"role": "system", "content": SYSTEM_INSTRUCTIONS + memory_context}
    ]
    
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})
        
    messages.append({"role": "user", "content": user_question})

    executed_queries = []
    last_successful_query = None

    # The orchestration loop: keep calling the model — with `tools` present on every
    # turn — until it stops requesting tool calls and returns a final answer.
    for iteration in range(MAX_TOOL_ITERATIONS):
        # Forced on the very first turn: the model must query before it's allowed
        # to say anything, so a final answer can never skip the database and be
        # fabricated from training-data guesses instead of this turn's actual data.
        tool_choice = "required" if iteration == 0 else "auto"

        try:
            response = client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                tools=TOOLS,
                tool_choice=tool_choice
            )
        except BadRequestError as e:
            # Some models/providers reject tool_choice="required" outright (e.g.
            # Qwen's "thinking mode" via CometAPI). Degrade to "auto" for this
            # call instead of hard-failing — the GROUNDING RULES in the system
            # prompt still apply, just without the hard guarantee "required" gives.
            if tool_choice == "required" and "tool_choice" in str(e).lower():
                response = client.chat.completions.create(
                    model=LLM_MODEL,
                    messages=messages,
                    tools=TOOLS,
                    tool_choice="auto"
                )
            else:
                raise

        response_message = response.choices[0].message

        if not response_message.tool_calls:
            if last_successful_query:
                save_memory(user_question, last_successful_query)
            return {"answer": response_message.content, "queries": executed_queries}

        messages.append(response_message)

        for tool_call in response_message.tool_calls:
            if tool_call.function.name == "execute_sql_query":
                arguments = json.loads(tool_call.function.arguments)
                generated_sql = arguments.get("query")

                print(f"\n[Agent Generated SQL]:\n{generated_sql}\n")
                executed_queries.append(generated_sql)

                query_result = execute_sql_query(generated_sql)
                
                # Track the last successful query to save if the agent resolves the question
                if not str(query_result).startswith("Error") and not str(query_result).startswith("Database Error"):
                    last_successful_query = generated_sql
            else:
                query_result = f"Error: unknown tool '{tool_call.function.name}'"

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "name": tool_call.function.name,
                "content": query_result
            })

    return {
        "answer": "I couldn't reach a final answer within the allowed number of query attempts.",
        "queries": executed_queries,
    }