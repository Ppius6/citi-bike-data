import hashlib
import logging
import os
from dataclasses import dataclass
from threading import Lock
from time import monotonic

import clickhouse_connect
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

from contracts import QueryResult, SQLArguments

logger = logging.getLogger(__name__)
MAX_ROWS_RETURNED = 50
SCHEMA_CACHE_SECONDS = 300
MEMORY_MAX_DISTANCE = 0.25

# Unknown functions fail closed, including external I/O and dictionary lookups.
ALLOWED_FUNCTIONS = set("""
    ABS AND ANY ANY_VALUE ARG_MAX ARG_MIN ARRAY ARRAY_AGG AVG AVG_WEIGHTED
    CASE CAST CEIL COALESCE CONCAT COUNT COUNT_IF DATE DATE_ADD DATE_DIFF
    DATE_TRUNC TIMESTAMP_TRUNC TIME_TRUNC DATE_SUB TIMESTAMP_SUB
    DAY DAY_OF_MONTH DAY_OF_WEEK DENSE_RANK EXTRACT FLOOR GREATEST
    IF IFNULL ILIKE LAG LEAD LEAST LENGTH LIKE LOWER MAX MIN MONTH NULLIF
    POW QUANTILE QUANTILES RANK REPLACE ROUND ROW_NUMBER SAFE_DIVIDE SQRT
    STDDEV STDDEV_POP STDDEV_SAMP STR_POSITION SUBSTRING SUM SUM_IF
    TIME_TO_STR TIMESTAMP_ADD TIMESTAMP_DIFF TO_CHAR TRIM TRUNC UPPER VARIANCE
    WEEK YEAR
    avgWeighted countIf sumIf quantileExact quantileExactLow quantileExactHigh
    quantileTDigest quantilesTDigest uniq uniqExact uniqIf uniqExactIf
    toDate toDateTime toStartOfDay toStartOfWeek toStartOfMonth toStartOfYear
    toStartOfQuarter toQuarter toISOWeek toDayOfYear toMonday dateTrunc addHours
    toYYYYMM toYYYYMMDD toYear toMonth toDayOfMonth toDayOfWeek toHour
    dateDiff formatDateTime addDays addMonths subtractDays subtractMonths
    today now lowerUTF8 positionCaseInsensitive multiIf toFloat64 toUInt32
    isNull isNotNull ifNull nullIf roundBankers
""".upper().split())

ENUM_COLUMNS = [
    ("dim_rider_type", "rider_type"),
    ("dim_rider_type", "rider_type_desc"),
    ("dim_bike_type", "bike_type"),
    ("dim_bike_type", "bike_type_desc"),
]


def _get_client(*, memory=False):
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username="ai_memory" if memory else "ai_agent",
        password=os.getenv("AI_MEMORY_PASSWORD" if memory else "AI_AGENT_PASSWORD", ""),
        connect_timeout=3,
        send_receive_timeout=35 if not memory else 5,
    )


@dataclass(frozen=True)
class SchemaContext:
    text: str
    version: str


_schema_context = None
_schema_expires_at = 0.0
_schema_lock = Lock()


def get_schema_context() -> SchemaContext:
    """Refresh schema, enums and fact-table coverage every five minutes.

    A failed refresh is surfaced instead of silently using outdated metadata.
    The version covers structure/semantics, not changing coverage dates.
    """
    global _schema_context, _schema_expires_at
    with _schema_lock:
        if _schema_context is not None and monotonic() < _schema_expires_at:
            return _schema_context
        client = _get_client()
        try:
            columns = client.query("""
                SELECT database, table, name, type, comment FROM system.columns
                WHERE database IN ('gold', 'marts')
                ORDER BY database, table, position
            """, settings={"max_execution_time": 5}).result_rows
            if not columns:
                raise RuntimeError("The analytics schema is not ready.")
            tables = {}
            for database, table, name, col_type, comment in columns:
                tables.setdefault((database, table), []).append(
                    f"{name} {col_type}" + (f" COMMENT {comment!r}" if comment else "")
                )
            lines = [f"TABLE {db}.{table} (\n  " + ",\n  ".join(cols) + "\n);"
                     for (db, table), cols in tables.items()]
            for table, column in ENUM_COLUMNS:
                values = client.query(
                    f"SELECT DISTINCT {column} FROM gold.{table} ORDER BY {column}",
                    settings={"max_execution_time": 5},
                ).result_rows
                lines.append(f"gold.{table}.{column} actual values: " +
                             ", ".join(repr(row[0]) for row in values))
            version = hashlib.sha256(("analytics-v2\n" + "\n".join(lines)).encode()).hexdigest()
            bounds = client.query("""
                SELECT min(toDate(started_at)), max(toDate(started_at)), count()
                FROM gold.fact_trips
            """, settings={"max_execution_time": 5}).result_rows[0]
            lines.append(f"Trip start-date coverage: {bounds[0]} to {bounds[1]}."
                         if bounds[2] else "Trip start-date coverage: no rides loaded.")
            _schema_context = SchemaContext("\n".join(lines), version)
            _schema_expires_at = monotonic() + SCHEMA_CACHE_SECONDS
            return _schema_context
        finally:
            client.close()


def validate_sql(query: str) -> tuple[str, list[str]]:
    """Resolve CTEs and allow only SELECTs over explicit gold/marts tables."""
    SQLArguments(query=query)
    statements = [stmt for stmt in sqlglot.parse(query, read="clickhouse") if stmt is not None]
    if len(statements) != 1:
        raise ValueError("Only a single SELECT statement is allowed per call.")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        raise ValueError("Only SELECT queries are allowed.")
    for node in tree.walk():
        if isinstance(node, (exp.DDL, exp.DML, exp.Command, exp.Into)):
            raise ValueError("Only read-only SELECT queries are allowed.")
        if node.args.get("settings") or node.args.get("format") or node.args.get("into"):
            raise ValueError("Query settings, output formats and INTO are not allowed.")
        if isinstance(node, exp.Func):
            name = node.name if isinstance(node, exp.Anonymous) else node.sql_name()
            if name.upper() not in ALLOWED_FUNCTIONS:
                raise ValueError(f"Function {name} is not approved for analytics.")
    sources = set()
    for scope in traverse_scope(tree):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                if (not isinstance(source.this, exp.Identifier) or source.catalog
                        or source.db not in {"gold", "marts"}):
                    raise ValueError("Only explicit gold.* and marts.* tables are allowed; no table functions.")
                sources.add(f"{source.db}.{source.name}")
    # Execute the validated AST, never unparsed trailing syntax.
    return tree.sql(dialect="clickhouse"), sorted(sources)


def execute_sql_query(query: str, *, timeout: float = 30) -> QueryResult:
    try:
        clean_query, sources = validate_sql(query)
    except (ValueError, sqlglot.errors.SqlglotError) as error:
        return QueryResult(status="error", error=str(error))
    client = None
    try:
        client = _get_client()
        # Limit output, never the input of an aggregate. An extra row detects
        # truncation without downloading the complete result into Python.
        result = client.query(
            f"SELECT * FROM ({clean_query}) AS agent_result LIMIT {MAX_ROWS_RETURNED + 1}",
            settings={
                "max_execution_time": min(30, max(1, int(timeout))),
                "max_result_rows": MAX_ROWS_RETURNED + 1,
                "max_result_bytes": 1000000,
                "result_overflow_mode": "throw",
                "max_memory_usage": 536870912,
                "max_rows_to_read": 20000000,
                "read_overflow_mode": "throw",
            },
        )
        return QueryResult(
            status="ok" if result.result_rows else "empty",
            columns=list(result.column_names),
            rows=[list(row) for row in result.result_rows[:MAX_ROWS_RETURNED]],
            source_tables=sources,
            truncated=len(result.result_rows) > MAX_ROWS_RETURNED,
        )
    except Exception:
        logger.exception("Analytics query failed")
        return QueryResult(status="error", error="Database query failed or exceeded its resource limits.")
    finally:
        if client is not None:
            client.close()


def save_memory(question: str, query: str, schema_version: str) -> bool:
    """Persist an explicitly approved example, never automatic success."""
    if not os.getenv("AI_MEMORY_PASSWORD"):
        return False
    client = None
    try:
        from memory import embed_text

        validate_sql(query)
        client = _get_client(memory=True)
        params = {"question": question, "query": query, "version": schema_version}
        exists = client.query("""
            SELECT count() FROM agent.memory
            WHERE question = {question:String} AND query = {query:String}
              AND schema_version = {version:String} AND validated = 1
        """, parameters=params, settings={"max_execution_time": 3}).result_rows[0][0]
        if not exists:
            client.insert("agent.memory", [[question, query, embed_text(question), schema_version, 1]],
                          column_names=["question", "query", "embedding", "schema_version", "validated"])
        return True
    except Exception:
        logger.exception("Memory save failed")
        return False
    finally:
        if client is not None:
            client.close()


def retrieve_memory(question: str, schema_version: str, limit: int = 3) -> list[dict]:
    if not os.getenv("AI_MEMORY_PASSWORD"):
        return []
    client = None
    try:
        from memory import embed_text

        client = _get_client(memory=True)
        result = client.query("""
            SELECT question, query, cosineDistance(any(embedding), {vector:Array(Float32)}) AS dist
            FROM agent.memory
            WHERE validated = 1 AND schema_version = {version:String}
            GROUP BY question, query
            HAVING dist <= {max_distance:Float64}
            ORDER BY dist ASC, question, query
            LIMIT {limit:UInt32}
        """, parameters={"vector": embed_text(question), "version": schema_version,
                          "max_distance": MEMORY_MAX_DISTANCE, "limit": min(3, max(1, limit))},
                              settings={"max_execution_time": 3, "max_memory_usage": 134217728})
        return [{"question": row[0], "query": row[1], "distance": row[2]}
                for row in result.result_rows if row[2] <= MEMORY_MAX_DISTANCE]
    except Exception:
        logger.exception("Memory retrieval failed; continuing without examples")
        return []
    finally:
        if client is not None:
            client.close()
