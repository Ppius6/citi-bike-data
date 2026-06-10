"""
Strip dbt 1.11 + Elementary fields from manifest.json and catalog.json
that OpenMetadata 1.5.x rejects.
Run before OpenMetadata dbt ingestion:
    python scripts/clean_manifest.py
"""
import json
import sys
from pathlib import Path

MANIFEST_PATH = Path("dbt/target/manifest.json")
CLEANED_MANIFEST = Path("dbt/target/manifest_clean.json")
CATALOG_PATH    = Path("dbt/target/catalog.json")
CLEANED_CATALOG = Path("dbt/target/catalog_clean.json")

NODE_STRIP = {
    "access", "attached_node", "checksum", "column_name",
    "compiled", "compiled_code", "compiled_path", "constraints",
    "contract", "created_at", "deprecation_date", "doc_blocks",
    "extra_ctes", "extra_ctes_injected", "file_key_name",
    "functions", "group", "language", "latest_version",
    "metrics", "primary_key", "refs", "sources", "test_metadata",
    "time_spine", "unrendered_config", "index",
}

SOURCE_STRIP = {
    "created_at", "doc_blocks", "loaded_at_query",
    "unrendered_config", "unrendered_database", "unrendered_schema",
}

# OM 1.5.x expects these keys to exist but as empty dicts — don't remove, set to {}
TOP_LEVEL_EMPTY = {
    "groups", "group_map", "saved_queries", "semantic_models", "unit_tests",
}

CATALOG_METADATA_STRIP = {
    "invocation_started_at",
    "env",
}

# Remove nodes from these packages entirely — they confuse OM's parser
PACKAGES_TO_EXCLUDE = {"elementary", "dbt_utils"}

# Remove nodes with these resource_type prefixes
NODE_PREFIXES_TO_REMOVE = {"test.", "operation."}


def clean_manifest(manifest_path: Path, output_path: Path) -> None:
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Set top-level sections to empty dicts instead of removing them
    for key in TOP_LEVEL_EMPTY:
        manifest[key] = {}

    # Remove unwanted nodes
    nodes = manifest.get("nodes", {})
    remove_keys = [
        k for k in nodes
        if any(k.startswith(prefix) for prefix in NODE_PREFIXES_TO_REMOVE)
        or nodes[k].get("package_name") in PACKAGES_TO_EXCLUDE
    ]
    for k in remove_keys:
        del nodes[k]

    # Clean remaining nodes
    node_count = 0
    for node in nodes.values():
        for field in NODE_STRIP:
            node.pop(field, None)
        if "depends_on" in node and isinstance(node["depends_on"], dict):
            node["depends_on"].pop("nodes", None)
        node_count += 1

    # Clean sources
    source_count = 0
    for source in manifest.get("sources", {}).values():
        for field in SOURCE_STRIP:
            source.pop(field, None)
        source_count += 1

    # Clean other sections
    for section in ("exposures", "metrics", "macros"):
        for item in manifest.get(section, {}).values():
            item.pop("created_at", None)
            item.pop("doc_blocks", None)

    with open(output_path, "w") as f:
        json.dump(manifest, f)

    print(f"✓ Manifest: removed {len(remove_keys)} unwanted nodes, kept {node_count} model nodes, {source_count} sources")
    print(f"✓ Top-level sections set to empty dicts: {TOP_LEVEL_EMPTY}")
    print(f"✓ Written to: {output_path}")


def clean_catalog(catalog_path: Path, output_path: Path) -> None:
    with open(catalog_path) as f:
        catalog = json.load(f)

    metadata = catalog.get("metadata", {})
    for field in CATALOG_METADATA_STRIP:
        metadata.pop(field, None)

    with open(output_path, "w") as f:
        json.dump(catalog, f)

    print(f"✓ Catalog: stripped metadata fields {CATALOG_METADATA_STRIP}")
    print(f"✓ Written to: {output_path}")


if __name__ == "__main__":
    if not MANIFEST_PATH.exists():
        print(f"ERROR: {MANIFEST_PATH} not found. Run 'dbt docs generate' first.")
        sys.exit(1)
    if not CATALOG_PATH.exists():
        print(f"ERROR: {CATALOG_PATH} not found. Run 'dbt docs generate' first.")
        sys.exit(1)

    clean_manifest(MANIFEST_PATH, CLEANED_MANIFEST)
    clean_catalog(CATALOG_PATH, CLEANED_CATALOG)
