
import json
import os
from openai import OpenAI
from database import get_db_schema, execute_sql_query, retrieve_memory, save_memory

# DeepSeek client (OpenAI-compatible API, pointed at DeepSeek's base_url)
client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com/v1"
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

SYSTEM_INSTRUCTIONS = f"""
You are an expert data analyst for a bike-share system. 
You answer user questions by writing and executing ClickHouse SQL queries.

CRITICAL JOIN RULES:
- Always start from the central fact table: `gold.fact_trips` (alias as `t`).
- To filter by dates/seasons/weekends, JOIN `gold.dim_date` (dd) ON `t.date_key = dd.date_key`.
- To filter by rider type (Member vs Casual), JOIN `gold.dim_rider_type` (dr) ON `t.rider_type_key = dr.rider_type_key`.
- To filter by bike types, JOIN `gold.dim_bike_type` (db) ON `t.bike_type_key = db.bike_type_key`.
- To get station names, JOIN `gold.dim_station` (ds) ON `t.start_station_key = ds.station_key`.

Here is your database schema:
{get_db_schema()}

Always double-check that your query only contains valid columns listed above. DO NOT hallucinate columns like `humidity` that do not exist in the schema.
If a user specifies a month and day without a year, default to the most recent available year according to the data coverage dates.
ClickHouse string comparisons are case-sensitive. Unless you are certain of a column's exact stored
casing (see the schema notes below), filter with lower(column) = lower('value') instead of a bare '=',
so a wrong guess about casing returns the right rows instead of zero.
Keep formatting simple: plain sentences and "- " bullet lists with **bold** for key numbers only. No headers, no tables.
"""

MAX_TOOL_ITERATIONS = 10


def run_bike_agent(user_question: str, history: list[dict] = None) -> dict:
    """Runs the agent loop and returns {"answer": str, "queries": list[str]}."""
    history = history or []
    
    # 1. Retrieve long-term memory for few-shot examples
    memories = retrieve_memory(user_question)
    memory_context = ""
    if memories:
        memory_context = "\n\nCRITICAL: Here are some proven successful SQL queries for similar past questions. Use them as reference if helpful:\n"
        for mem in memories:
            # We indent the query for readability in the prompt
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
    for _ in range(MAX_TOOL_ITERATIONS):
        response = client.chat.completions.create(
            model="deepseek-chat",  # or deepseek-reasoner for advanced chains
            messages=messages,
            tools=TOOLS,
            tool_choice="auto"
        )

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