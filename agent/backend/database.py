import os
import re

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = "ai_agent"
CLICKHOUSE_PASSWORD = os.getenv("AI_AGENT_PASSWORD", "")

FORBIDDEN_KEYWORDS = [
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "ATTACH",
    "DETACH",
    "RENAME",
    "OPTIMIZE",
    "SYSTEM",
    "KILL",
    "SET",
]

MAX_ROWS_RETURNED = 50

# Low-cardinality dimension columns that string filters commonly target. Their raw
# values often differ in casing from their *_desc display counterparts (e.g.
# rider_type='casual' vs rider_type_desc='Casual'), which is a common source of
# zero-result queries if the model guesses. Probed live so the agent never has to guess,
# and so it stays correct if a new bike/rider type is ever added.
ENUM_COLUMNS = [
    ("dim_rider_type", "rider_type"),
    ("dim_rider_type", "rider_type_desc"),
    ("dim_bike_type", "bike_type"),
    ("dim_bike_type", "bike_type_desc"),
]


def _get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def get_db_schema() -> str:
    """Introspects the live gold.* schema plus known enum values, so this never
    drifts from the actual dbt models. Called once at process startup."""
    client = _get_client()

    columns = client.query("""
        SELECT table, name, type, comment
        FROM system.columns
        WHERE database = 'gold'
        ORDER BY table, position
        """).result_rows

    tables: dict[str, list[tuple[str, str, str]]] = {}
    for table, name, col_type, comment in columns:
        tables.setdefault(table, []).append((name, col_type, comment))

    lines = []
    for table, cols in tables.items():
        col_strings = []
        for name, col_type, comment in cols:
            col_str = f"{name} {col_type}"
            if comment:
                col_str += f" COMMENT '{comment}'"
            col_strings.append(col_str)
        lines.append(f"TABLE gold.{table} (\n  " + ",\n  ".join(col_strings) + "\n);")

    enum_notes = []
    for table, column in ENUM_COLUMNS:
        values = client.query(f"SELECT DISTINCT {column} FROM gold.{table}").result_rows
        values_list = ", ".join(repr(v[0]) for v in values)
        enum_notes.append(f"gold.{table}.{column} actual values: {values_list}")

    return "\n".join(lines) + "\n\n" + "\n".join(enum_notes)


def execute_sql_query(query: str) -> str:
    """Validates the SQL and runs it against ClickHouse using the read-only ai_agent user."""

    statements = [s for s in query.strip().split(";") if s.strip()]
    if len(statements) != 1:
        return "Error: Only a single SELECT statement is allowed per call."

    clean_query = statements[0].strip()
    upper_query = clean_query.upper()

    if not (upper_query.startswith("SELECT") or upper_query.startswith("WITH")):
        return "Error: Security block. Only SELECT (or WITH ... SELECT) queries are allowed."

    if any(re.search(rf"\b{word}\b", upper_query) for word in FORBIDDEN_KEYWORDS):
        return "Error: Security block. Only read-only SELECT queries are allowed."

    try:
        client = _get_client()
        # Defense in depth: ai_agent is also created with SETTINGS readonly = 2 server-side,
        # which blocks all writes at the ClickHouse engine level regardless of this app-side check.
        result = client.query(clean_query, settings={"max_execution_time": 30})

        if not result.result_rows:
            return "No data found for this query."

        rows = result.result_rows[:MAX_ROWS_RETURNED]
        output = f"Columns: {result.column_names}\nRows: {rows}"
        if len(result.result_rows) > MAX_ROWS_RETURNED:
            output += f"\n(truncated to first {MAX_ROWS_RETURNED} of {len(result.result_rows)} rows)"
        return output
    except Exception as e:
        return f"Database Error: {str(e)}"


def save_memory(question: str, query: str) -> None:
    """Saves a successful question-query pair into long-term vector memory."""
    from memory import (
        embed_text,
    )  # Local import to avoid initialization overhead if not needed

    try:
        embedding = embed_text(question)
        client = _get_client()

        # ClickHouse connect insert syntax: client.insert('table', data, column_names)
        # where data is a list of rows, and each row is a list/tuple of values.
        client.insert(
            "agent.memory",
            [[question, query, embedding]],
            column_names=["question", "query", "embedding"],
        )
    except Exception as e:
        print(f"Warning: Failed to save memory to ClickHouse: {e}")


def retrieve_memory(question: str, limit: int = 3) -> list[dict]:
    """Retrieves the most similar past question-query pairs from memory."""
    from memory import embed_text

    try:
        embedding = embed_text(question)
        client = _get_client()

        # Find similar queries using cosineDistance
        result = client.query(
            f"""
            SELECT question, query, cosineDistance(embedding, {{vector:Array(Float32)}}) as dist
            FROM agent.memory
            ORDER BY dist ASC
            LIMIT {limit}
            """,
            parameters={"vector": embedding},
        )

        return [
            {"question": row[0], "query": row[1], "distance": row[2]}
            for row in result.result_rows
        ]
    except Exception as e:
        print(f"Warning: Failed to retrieve memory from ClickHouse: {e}")
        return []
