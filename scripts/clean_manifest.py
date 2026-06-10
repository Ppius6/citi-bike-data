"""
Strip dbt 1.11 fields from manifest.json / catalog.json for OpenMetadata 1.5.x.

OM 1.5 only supports dbt manifest v9 and rejects many dbt 1.11 keys.
Produces separate artifacts for Postgres vs ClickHouse dbt ingestion.

Usage:
    python scripts/clean_manifest.py
"""
import copy
import json
import sys
from pathlib import Path

MANIFEST_PATH = Path("dbt/target/manifest.json")
CATALOG_PATH = Path("dbt/target/catalog.json")
ARTIFACTS_DIR = Path("openmetadata/dbt-artifacts")

DBT_MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v9.json"
DBT_CATALOG_SCHEMA = "https://schemas.getdbt.com/dbt/catalog/v1.json"

POSTGRES_DB = "citi-bike"
# OpenMetadata models ClickHouse as database="default", schema=<actual db> (e.g. "gold")
CLICKHOUSE_DB = "default"

POSTGRES_SCHEMAS = {"bronze", "silver", "snapshots"}
CLICKHOUSE_SCHEMAS = {"gold"}

# Top-level keys introduced in dbt 1.11 — remove entirely (empty dict still fails)
TOP_LEVEL_REMOVE = {
    "saved_queries",
    "semantic_models",
    "unit_tests",
    "functions",
}

# OM 1.5 requires these keys but accepts empty dicts
TOP_LEVEL_EMPTY = {
    "groups",
    "group_map",
}

METADATA_KEEP = {
    "dbt_schema_version",
    "dbt_version",
    "generated_at",
}

# Node fields OM 1.5 does not understand — do NOT include checksum or compiled_code
NODE_STRIP = {
    "access",
    "attached_node",
    "column_name",
    "compiled",
    "compiled_path",
    "constraints",
    "contract",
    "created_at",
    "deprecation_date",
    "doc_blocks",
    "extra_ctes",
    "extra_ctes_injected",
    "file_key_name",
    "functions",
    "group",
    "language",
    "latest_version",
    "metrics",
    "primary_key",
    "refs",
    "sources",
    "test_metadata",
    "time_spine",
    "unrendered_config",
    "index",
}

CONFIG_STRIP = {
    "access",
    "batch_size",
    "begin",
    "column_types",
    "concurrent_batches",
    "contract",
    "event_time",
    "freshness",
    "grants",
    "group",
    "lookback",
    "on_configuration_change",
    "packages",
    "quoting",
}

SOURCE_STRIP = {
    "created_at",
    "doc_blocks",
    "loaded_at_query",
    "unrendered_config",
    "unrendered_database",
    "unrendered_schema",
}

PACKAGES_TO_EXCLUDE = {"elementary", "dbt_utils"}
KEEP_RESOURCE_TYPES = {"model", "snapshot"}


def _clean_node(node: dict, database_name: str) -> None:
    for field in NODE_STRIP:
        node.pop(field, None)

    if node.get("version") is None:
        node.pop("version", None)

    config = node.get("config")
    if isinstance(config, dict):
        for field in CONFIG_STRIP:
            config.pop(field, None)

    node["database"] = database_name


def _clean_source(source: dict, database_name: str) -> None:
    for field in SOURCE_STRIP:
        source.pop(field, None)
    source["database"] = database_name


def _filter_manifest(manifest: dict, schemas: set[str], database_name: str) -> dict:
    cleaned = copy.deepcopy(manifest)

    for key in TOP_LEVEL_REMOVE:
        cleaned.pop(key, None)

    for key in TOP_LEVEL_EMPTY:
        cleaned[key] = {}

    metadata = cleaned.get("metadata", {})
    cleaned["metadata"] = {
        key: metadata[key]
        for key in METADATA_KEEP
        if key in metadata
    }
    cleaned["metadata"]["dbt_schema_version"] = DBT_MANIFEST_SCHEMA

    nodes = cleaned.get("nodes", {})
    remove_keys = []
    for key, node in nodes.items():
        if node.get("package_name") in PACKAGES_TO_EXCLUDE:
            remove_keys.append(key)
            continue
        if node.get("resource_type") not in KEEP_RESOURCE_TYPES:
            remove_keys.append(key)
            continue
        if node.get("schema") not in schemas:
            remove_keys.append(key)
            continue

    for key in remove_keys:
        del nodes[key]

    for node in nodes.values():
        _clean_node(node, database_name)

    sources = cleaned.get("sources", {})
    remove_sources = [
        key
        for key, source in sources.items()
        if source.get("schema") not in schemas
        and source.get("package_name") not in PACKAGES_TO_EXCLUDE
    ]
    for key in remove_sources:
        del sources[key]

    for source in sources.values():
        if source.get("package_name") not in PACKAGES_TO_EXCLUDE:
            _clean_source(source, database_name)

    for section in ("exposures", "metrics", "macros"):
        for item in cleaned.get(section, {}).values():
            item.pop("created_at", None)
            item.pop("doc_blocks", None)

    return cleaned


def clean_catalog(catalog_path: Path, output_path: Path) -> None:
    with open(catalog_path) as f:
        catalog = json.load(f)

    metadata = catalog.get("metadata", {})
    metadata.pop("invocation_started_at", None)
    metadata.pop("env", None)
    metadata["dbt_schema_version"] = DBT_CATALOG_SCHEMA
    catalog["metadata"] = metadata

    with open(output_path, "w") as f:
        json.dump(catalog, f)


def write_target_artifacts(manifest: dict) -> None:
    targets = {
        "postgres": (POSTGRES_SCHEMAS, POSTGRES_DB),
        "clickhouse": (CLICKHOUSE_SCHEMAS, CLICKHOUSE_DB),
    }

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    for target, (schemas, database_name) in targets.items():
        target_dir = ARTIFACTS_DIR / target
        target_dir.mkdir(parents=True, exist_ok=True)

        filtered = _filter_manifest(manifest, schemas, database_name)
        node_count = len(filtered.get("nodes", {}))
        source_count = len(filtered.get("sources", {}))

        manifest_out = target_dir / "manifest.json"
        with open(manifest_out, "w") as f:
            json.dump(filtered, f)

        print(
            f"✓ {target}: {node_count} nodes, {source_count} sources "
            f"→ {manifest_out}"
        )


def main() -> None:
    if not MANIFEST_PATH.exists():
        print(f"ERROR: {MANIFEST_PATH} not found. Run 'dbt docs generate' first.")
        sys.exit(1)
    if not CATALOG_PATH.exists():
        print(f"ERROR: {CATALOG_PATH} not found. Run 'dbt docs generate' first.")
        sys.exit(1)

    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    write_target_artifacts(manifest)

    catalog_out = ARTIFACTS_DIR / "catalog.json"
    clean_catalog(CATALOG_PATH, catalog_out)
    print(f"✓ catalog → {catalog_out}")


if __name__ == "__main__":
    main()
