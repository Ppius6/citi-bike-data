# Agent reliability changes

## 1. Validate conversation input

`contracts.py` is shared by the HTTP API and CLI agent loop. History accepts only
`user` and `assistant` messages, rejects extra fields, and is limited to 20 messages,
4,000 characters per message and 24,000 characters in total (including the current
question). Blank questions are rejected. The frontend keeps recent history within
these limits and bounds the fetch timeout. This prevents callers from injecting
system/developer/tool roles and keeps conversation costs bounded. Client-supplied
assistant text is still untrusted context, not evidence for numerical answers.

## 2. Preserve additive statistics in summary marts

`daily_ride_summary` now includes unrounded sums and non-null counts for duration,
distance and temperature. `mart_city_flow` includes the same duration and distance
fields. Existing display averages remain available. To combine groups, the agent
uses `sum(ride_distance_sum) / nullIf(sum(ride_distance_count), 0)`, and the analogous
formula for each metric. Counting actual observations matters: a ride with unknown
distance contributes to ride counts but must not contribute to the distance mean's
denominator. The prompt falls back to `fact_trips` until the upgraded columns exist.

The `summary_metrics_reconcile` dbt test compares counts and sums in both marts with
the fact table. It runs in the pipeline and documentation CI along with the existing
tests. Analytical fixtures deliberately have unequal group sizes and null metrics,
so averaging averages or using total rides as every denominator fails the tests.

## 3. Require approval before learning examples

Successful SQL execution no longer automatically creates long-term memory. Only a
standalone question answered with one successful, non-truncated query can produce
a short-lived feedback candidate. Selecting **Yes** in the UI saves that example;
**No** discards it. The API accepts an opaque candidate ID, never caller-supplied
SQL to insert into memory. Candidates expire after 15 minutes, are single-use, and
are capped at 512 per process. Failed saves can be retried until expiration.

Follow-ups and multi-query analyses are deliberately excluded because a single
query may not represent the full question. Retrieval uses only approved examples
with the current schema/semantic version, deduplicates question/query pairs and
requires cosine distance <= 0.25. This threshold is a starting value to tune with
real evaluations. Retrieved examples are presented as untrusted reference data,
not system instructions or facts. Legacy memory rows default to unapproved.

Memory is optional. A missing credential, model or memory database does not block
chat. The embedding model is loaded lazily from the local cache; chat requests
never download it. Approval is a user feedback signal, not proof of analytical
correctness. Candidates are process-local: a restart loses pending feedback, and
multiple API workers require sticky routing or a shared candidate store.

## 4. Separate permissions and bound SQL execution

The analytics account `ai_agent` can read gold/marts and schema metadata. The
separate `ai_memory` account can read/insert only `agent.memory`. The idempotent
upgrade revokes legacy memory permissions from `ai_agent` and constrains its
read-only setting and resource limits on the server.

SQLGlot parses ClickHouse SQL and resolves CTE scopes. The tool permits one SELECT
(or SELECT set operation), requires explicit gold/marts table references, blocks
external table functions, and permits only approved scalar/aggregate functions.
It rejects query-level settings, output formats, writes and INTO clauses. It
executes the serialized validated syntax tree. This handles quoted semicolons,
comments and legitimate identifiers without the old keyword false positives.
New analytical functions must be reviewed and added to the allowlist explicitly.

Results are wrapped with an output LIMIT of 51: at most 50 rows reach the model,
with an explicit truncation flag. Aggregation inputs are not limited. Query limits
include 30 seconds, 512 MiB memory, 1 MB of result data and 20 million rows read.
Exceeding a resource limit throws instead of silently returning a partial result.
These values are intentionally conservative and can be adjusted after profiling.

## 5. Make tool failures recoverable and visible

Tool arguments are validated with Pydantic. Malformed JSON, missing queries and
unknown tools return structured errors for the model to repair. Every tool result
contains `status`, `columns`, `rows`, `source_tables`, `truncated` and `error`.
Database failures are distinguished from empty results; internal database error
text is logged rather than sent to users or providers.

The loop enforces a total of 10 tool calls, including parallel calls, and reserves
a final synthesis turn. LLM calls have explicit timeouts and no hidden SDK retries.
A 120-second request budget is checked between operations and bounds subsequent
LLM/query timeouts; synchronous setup, model loading and transport cleanup are not
a hard wall-clock cancellation guarantee. The first turn requires a tool call, and
the agent cannot return unsupported numbers after no query, failed queries, empty
results or SELECTs of constants. This gate is a minimum
evidence check; it does not prove that every sentence correctly interprets SQL.

Responses and logs include LLM call count, tool calls, query attempts/errors,
provider-reported prompt/completion tokens and elapsed milliseconds. The HTTP chat
response retains `answer` and `queries`, and adds `metrics` and optional
`memory_candidate_id`. `POST /api/feedback` takes `{candidate_id, approved}`.

## 6. Evaluate behavior and analytical results

Run unit, API, memory and offline analytical tests without credentials:

```bash
pip install -r agent/backend/requirements-dev.txt
pytest agent/backend/tests -q
python agent/backend/evaluation.py
```

The evaluator renders the actual four mart models over a tiny synthetic warehouse.
Its eight cases check weighted duration, nullable distance/temperature, city means,
SCD2 station joins, quiet hours, relative weekends and contextual follow-ups.
Unit tests cover unavailable columns, role injection, malformed tools, ungrounded
answers, provider timeouts, truncation, memory approval and budgets.

To compare a configured provider/model on the same fixtures, opt into API usage:

```bash
python agent/backend/evaluation.py --live
```

Live mode reads the project's `.env` and uses the existing LLM overrides. It reports
query-result correctness and per-request metrics, plus the answer for manual
review. It compares final result values, not exact SQL. It does not score prose
faithfulness, and DuckDB transpilation is not a substitute for native compatibility.

CI also starts a disposable ClickHouse **24.3** service, applies the permission
migration twice, renders the four marts, runs all eight analytical cases and the
reconciliation test, and checks database permissions without the application
parser. To run these native tests locally, create a fresh container and set
`CLICKHOUSE_TEST_PORT` and `CLICKHOUSE_TEST_CONTAINER`. The fixture deliberately
fails if the databases already exist; never point it at your real warehouse.

## 7. Refresh schema and define dates

Schema, enum values and actual fact-table coverage are cached for five minutes,
then refreshed on the next request. There is no database access at module import,
so a missing warehouse at startup no longer prevents the API from starting. A
failed refresh produces a controlled unavailable response rather than silently
serving stale metadata. Schema changes invalidate approved memory examples.

Relative dates use the current **America/New_York** date. “Last weekend” means the
most recent fully completed Saturday–Sunday; on Sunday it means the prior weekend.
The agent states the dates and reports unavailable historical coverage instead of
silently shifting to older data. A month/day without a year uses its latest
occurrence inside coverage and states the assumption. CLI and web requests both
receive the same date context.

## Upgrade an existing Docker installation

The source changes do not automatically migrate or restart running services.
Keep existing data volumes. Add a distinct `AI_MEMORY_PASSWORD` to `.env` only if
you want vector memory; leaving it empty disables memory while preserving chat.

Recreate the ClickHouse service to mount the new migration script and environment,
then apply the idempotent upgrade (existing volumes do not re-run entrypoint SQL):

```bash
docker compose up -d clickhouse
docker compose exec clickhouse bash /opt/citibike/agent-access.sh
```

Rebuild the summary marts and refresh the serving view using the existing worker:

```bash
docker compose exec prefect-worker uv run dbt run --project-dir dbt --profiles-dir dbt --target clickhouse --select daily_ride_summary mart_city_flow
docker compose exec prefect-worker uv run dbt run-operation refresh_materialized_view --project-dir dbt --profiles-dir dbt --target clickhouse --args '{view_name: marts.daily_ride_summary}'
docker compose exec prefect-worker uv run dbt test --project-dir dbt --profiles-dir dbt --target clickhouse --select summary_metrics_reconcile
docker compose up -d --build agent-api web
```

If enabling memory on a fresh cache, explicitly download the embedding model once:

```bash
docker compose run --rm --no-deps agent-api python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
```

The separate memory password is provisioned when the user is first created. To
rotate an existing memory user's password, change it in ClickHouse as well as
`.env`; the idempotent migration intentionally preserves existing passwords.
