
import json
import os
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

SYSTEM_INSTRUCTIONS = f"""
You are an expert data analyst for a bike-share system. 
You answer user questions by writing and executing ClickHouse SQL queries.

CRITICAL JOIN RULES:
- Always start from the central fact table: `gold.fact_trips` (alias as `t`) — EXCEPT as noted below.
- For ride counts or average duration/distance/temperature broken down only by date, rider type, bike type, weather code, and/or daylight (no station, no other measures), query `gold.daily_ride_summary` directly instead of aggregating `gold.fact_trips` — it's pre-aggregated to that exact grain and far cheaper to scan. It already has `total_rides`, `avg_ride_duration_minutes`, `avg_ride_distance_km`, `avg_temperature_c` — don't re-derive these from fact_trips if this table already has them. Fall back to `fact_trips` for anything needing stations, individual rides, precipitation/wind, or any measure not listed above.
- To filter by dates/seasons/weekends, JOIN `gold.dim_date` (dd) ON `t.date_key = dd.date_key`.
- To filter by rider type (Member vs Casual), JOIN `gold.dim_rider_type` (dr) ON `t.rider_type_key = dr.rider_type_key`.
- To filter by bike types, JOIN `gold.dim_bike_type` (db) ON `t.bike_type_key = db.bike_type_key`.
- To get station names, JOIN `gold.dim_station` (ds) ON `t.start_station_key = ds.station_key`.
- To get a human-readable weather description (e.g. "Clear sky", "Heavy rain"), JOIN `gold.dim_weather_code` (wc) ON `t.weather_code = wc.weather_code`.

Here is your database schema:
{get_db_schema()}

Always double-check that your query only contains valid columns listed above. DO NOT hallucinate columns like `humidity` that do not exist in the schema.
If a user specifies a month and day without a year, default to the most recent available year according to the data coverage dates.
ClickHouse string comparisons are case-sensitive. Unless you are certain of a column's exact stored
casing (see the schema notes below), filter with lower(column) = lower('value') instead of a bare '=',
so a wrong guess about casing returns the right rows instead of zero.
Keep formatting simple: plain sentences and "- " bullet lists with **bold** for key numbers only. No headers, no tables.

GROUNDING RULES — no exceptions:
- Every number, count, date, or fact you state about the bike-share data must come from a query result you just received in this conversation. Never state a data value from memory, prior training, or a plausible-sounding guess.
- If a query returns "No data found" or an error, say so plainly ("I couldn't find any rides matching that") instead of substituting an estimate.
- If a question can't be answered with the schema above, say that directly rather than inventing a column, table, or number to fill the gap.
- Past few-shot queries above are a starting point, not a source of facts — always re-run them (or an adapted version) to get current numbers; never quote a result from a past example as if it were freshly retrieved.
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