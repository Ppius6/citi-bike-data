#!/bin/bash
set -euo pipefail

CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:-9009}"

# Re-run safely against existing volumes after upgrading the agent.
# Idempotent upgrade for existing installations: legacy automatically saved
# examples remain unvalidated and cannot enter new prompts.
clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --query "ALTER TABLE agent.memory ADD COLUMN IF NOT EXISTS schema_version String DEFAULT ''"
clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --query "ALTER TABLE agent.memory ADD COLUMN IF NOT EXISTS validated UInt8 DEFAULT 0"
clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --query "REVOKE SELECT, INSERT ON agent.memory FROM ai_agent"

# readonly=2 allows the driver to set bounded query limits. CONST prevents
# callers from disabling it; the account has no table write permissions.
clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --query "
ALTER USER ai_agent SETTINGS
    readonly = 2 CONST,
    max_execution_time = 30 MIN 1 MAX 30,
    max_memory_usage = 536870912 MIN 1 MAX 536870912,
    max_result_rows = 10000 MIN 1 MAX 10000,
    max_result_bytes = 1000000 MIN 1 MAX 1000000,
    result_overflow_mode = 'throw' CONST,
    max_rows_to_read = 20000000 MIN 1 MAX 20000000,
    read_overflow_mode = 'throw' CONST
"

# Optional memory uses separate credentials; absence disables memory, not chat.
if [ -n "${AI_MEMORY_PASSWORD:-}" ]; then
    clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --param_memory_password="$AI_MEMORY_PASSWORD" --query \
        "CREATE USER IF NOT EXISTS ai_memory IDENTIFIED BY {memory_password:String}"
    clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --query "GRANT SELECT, INSERT ON agent.memory TO ai_memory"
    clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --query "ALTER USER ai_memory SETTINGS max_execution_time = 5 MAX 5, max_memory_usage = 134217728 MAX 134217728"
fi

