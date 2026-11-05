"""
Build the static pages that sit alongside dbt docs on GitHub Pages, for people
reading the Power BI report:

  /columns/  Column definitions for the reporting layer (gold + marts), from
             dbt's manifest.json (descriptions) and catalog.json (types).
  /model/    The Power BI model catalog (tables, measures, relationships),
             rendered from docs/powerbi/model-catalog.md.

Both pages support deep links that a report button can point at:
    .../columns/#gold.fact_trips.ride_duration_minutes
    .../model/#rides

Usage:
    python scripts/docs/build_report_docs.py dbt/target
"""

import json
import re
import sys
from datetime import datetime, timezone
from html import escape, unescape
from pathlib import Path

import markdown

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_CATALOG = REPO_ROOT / "docs" / "powerbi" / "model-catalog.md"

# Schemas a report consumer actually queries, in display order.
REPORT_SCHEMAS = {
    "gold": "Gold — star schema (facts and dimensions)",
    "marts": "Marts — pre-aggregated summaries",
}

SITE_LINKS = [
    ("columns", "Column definitions", "../columns/"),
    ("model", "Power BI model", "../model/"),
    ("dbt", "dbt docs", "../"),
]

CSS = """
  :root {
    --bg: #f7f8fa; --surface: #ffffff; --text: #1b1f24; --muted: #5b6470;
    --border: #e2e5ea; --accent: #0b6bcb; --code-bg: #eef1f5; --hit: #fff4c2;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #111418; --surface: #1a1e24; --text: #e6e9ee; --muted: #9aa3ae;
      --border: #2b313a; --accent: #6cb3ff; --code-bg: #252b33; --hit: #3d3410;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
         font: 15px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  code { background: var(--code-bg); padding: 1px 5px; border-radius: 4px; font-size: 0.9em;
         font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .topbar { border-bottom: 1px solid var(--border); background: var(--surface); }
  .topbar-inner { max-width: 1200px; margin: 0 auto; padding: 12px 16px; display: flex;
                  gap: 6px 20px; flex-wrap: wrap; align-items: center; }
  .brand { font-weight: 600; margin-right: auto; }
  .topbar a.site { color: var(--muted); font-size: 14px; }
  .topbar a.site.current { color: var(--text); font-weight: 600; }
  .layout { display: grid; grid-template-columns: 240px minmax(0, 1fr); max-width: 1200px; margin: 0 auto; }
  nav.side { position: sticky; top: 0; align-self: start; max-height: 100vh; overflow-y: auto;
             padding: 28px 16px; border-right: 1px solid var(--border); }
  nav.side ul { list-style: none; margin: 0 0 18px; padding: 0; }
  nav.side li a { display: block; padding: 3px 8px; border-radius: 6px; color: var(--text); font-size: 14px; }
  nav.side li a:hover { background: var(--code-bg); text-decoration: none; }
  nav.side li.sub a { padding-left: 20px; color: var(--muted); font-size: 13px; }
  .nav-group { margin: 0 0 6px 8px; font-size: 12px; font-weight: 600; letter-spacing: .06em;
               text-transform: uppercase; color: var(--muted); }
  main { padding: 28px 32px 80px; }
  header h1, .doc h1 { margin: 0 0 6px; font-size: 26px; }
  header p { margin: 0; color: var(--muted); }
  .scroll { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th { text-align: left; font-weight: 600; color: var(--muted); padding: 8px 10px;
       border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
  tr:last-child td { border-bottom: 0; }
  :target { scroll-margin-top: 90px; }
  tr:target td, .table:target, .doc h4:target { background: var(--hit); }

  /* Column definitions */
  .search { position: sticky; top: 0; z-index: 1; background: var(--bg); padding: 16px 0 12px; }
  .search input { width: 100%; padding: 10px 14px; font: inherit; color: var(--text);
                  background: var(--surface); border: 1px solid var(--border); border-radius: 8px; }
  .search input:focus { outline: 2px solid var(--accent); outline-offset: -1px; }
  .layer { margin: 32px 0 4px; font-size: 13px; font-weight: 600; letter-spacing: .06em;
           text-transform: uppercase; color: var(--muted); }
  .table { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
           padding: 18px 20px; margin: 14px 0; }
  .table h3 { margin: 0 0 4px; font-size: 18px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .table h3 a { color: var(--text); }
  .badge { margin-left: 8px; padding: 2px 8px; border-radius: 999px; background: var(--code-bg);
           color: var(--muted); font: 500 12px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           vertical-align: middle; }
  .table-desc { margin: 0 0 12px; color: var(--muted); }
  .col-name, .col-type { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; white-space: nowrap; }
  .col-name a { color: var(--text); }
  .col-type { color: var(--muted); font-size: 13px; }
  .missing { color: var(--muted); font-style: italic; }
  .empty { display: none; color: var(--muted); padding: 24px 0; }

  /* Power BI model catalog (rendered markdown) */
  .doc { max-width: 920px; }
  .doc h2 { margin: 40px 0 10px; padding-top: 8px; font-size: 22px; border-top: 1px solid var(--border); }
  .doc h3 { margin: 28px 0 8px; font-size: 18px; }
  .doc h4 { margin: 24px -8px 6px; padding: 2px 8px; border-radius: 6px; font-size: 16px; }
  .doc hr { display: none; }
  .doc table { display: block; overflow-x: auto; margin: 12px 0; background: var(--surface);
               border: 1px solid var(--border); border-radius: 8px; }
  .doc pre { background: var(--code-bg); border-radius: 8px; padding: 12px 14px; overflow-x: auto;
             font-size: 13px; line-height: 1.5; }
  .doc pre code { background: none; padding: 0; font-size: inherit; }
  .doc blockquote { margin: 12px 0; padding: 8px 14px; border-left: 3px solid var(--accent);
                    background: var(--surface); color: var(--muted); }
  .doc blockquote p { margin: 0; }

  @media (max-width: 760px) {
    .layout { grid-template-columns: 1fr; }
    nav.side { display: none; }
    main { padding: 20px 16px 60px; }
  }
"""


def render_shell(title: str, current: str, nav: str, main: str, script: str = "") -> str:
    links = "".join(
        f'<a class="site{" current" if key == current else ""}" href="{href}">{label}</a>'
        for key, label, href in SITE_LINKS
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<style>{CSS}</style>
</head>
<body>
<div class="topbar"><div class="topbar-inner"><span class="brand">Citi Bike Analysis</span>{links}</div></div>
<div class="layout">
  <nav class="side" aria-label="On this page">{nav}</nav>
  <main>{main}</main>
</div>
{script}
</body>
</html>
"""


# --- Column definitions (from dbt) -----------------------------------------

def render_text(text: str) -> str:
    """Escape a dbt description and turn `backticks` into <code>."""
    html = escape(" ".join((text or "").split()))
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", html)


def load_tables(target_dir: Path) -> list[dict]:
    manifest = json.loads((target_dir / "manifest.json").read_text())
    catalog_path = target_dir / "catalog.json"
    catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else {"nodes": {}}

    tables = []
    for unique_id, node in manifest["nodes"].items():
        if node["resource_type"] != "model" or node["schema"] not in REPORT_SCHEMAS:
            continue

        documented = {name.lower(): col for name, col in node["columns"].items()}
        catalogued = catalog["nodes"].get(unique_id, {}).get("columns", {})

        # Catalog order is the real column order; fall back to the schema.yml
        # order when the catalog was generated without this target.
        names = [col["name"] for col in sorted(catalogued.values(), key=lambda c: c["index"])]
        names += [col["name"] for col in node["columns"].values()
                  if col["name"].lower() not in {n.lower() for n in names}]

        columns = []
        for name in names:
            doc = documented.get(name.lower(), {})
            cat = next((c for c in catalogued.values() if c["name"].lower() == name.lower()), {})
            columns.append({
                "name": name,
                "type": cat.get("type") or doc.get("data_type") or "",
                "description": doc.get("description", ""),
            })

        tables.append({
            "schema": node["schema"],
            "name": node["name"],
            "description": node.get("description", ""),
            "materialized": node["config"].get("materialized", ""),
            "columns": columns,
        })

    schema_order = list(REPORT_SCHEMAS)
    tables.sort(key=lambda t: (schema_order.index(t["schema"]),
                               not t["name"].startswith("fact"), t["name"]))
    return tables


def render_table(table: dict) -> str:
    table_id = f"{table['schema']}.{table['name']}"
    rows = []
    for col in table["columns"]:
        col_id = f"{table_id}.{col['name']}"
        description = (render_text(col["description"]) if col["description"]
                       else '<span class="missing">Not documented yet</span>')
        rows.append(
            f'<tr id="{escape(col_id)}" data-search="{escape((col["name"] + " " + col["description"]).lower())}">'
            f'<td class="col-name"><a href="#{escape(col_id)}">{escape(col["name"])}</a></td>'
            f'<td class="col-type">{escape(col["type"]) or "—"}</td>'
            f"<td>{description}</td></tr>"
        )
    return f"""
    <section class="table" id="{escape(table_id)}">
      <h3><a href="#{escape(table_id)}">{escape(table_id)}</a>
        <span class="badge">{escape(table["materialized"].replace("_", " "))}</span></h3>
      <p class="table-desc">{render_text(table["description"])}</p>
      <div class="scroll">
        <table>
          <thead><tr><th>Column</th><th>Type</th><th>Definition</th></tr></thead>
          <tbody>{"".join(rows)}</tbody>
        </table>
      </div>
    </section>"""


FILTER_SCRIPT = """<script>
  const input = document.getElementById("filter");
  input.addEventListener("input", () => {
    const term = input.value.trim().toLowerCase();
    let anyVisible = false;
    document.querySelectorAll("section.table").forEach((section) => {
      const tableMatch = section.id.toLowerCase().includes(term);
      let visibleRows = 0;
      section.querySelectorAll("tbody tr").forEach((row) => {
        const show = !term || tableMatch || row.dataset.search.includes(term);
        row.hidden = !show;
        if (show) visibleRows++;
      });
      section.hidden = visibleRows === 0;
      anyVisible = anyVisible || visibleRows > 0;
    });
    document.querySelectorAll(".layer").forEach((heading) => {
      let next = heading.nextElementSibling, visible = false;
      while (next && next.matches("section.table")) { visible = visible || !next.hidden; next = next.nextElementSibling; }
      heading.hidden = !visible;
    });
    document.getElementById("empty").style.display = anyVisible ? "none" : "block";
  });
</script>"""


def render_columns_page(tables: list[dict]) -> str:
    generated = datetime.now(timezone.utc).strftime("%d %b %Y")
    column_count = sum(len(t["columns"]) for t in tables)

    nav, sections = [], []
    for schema, heading in REPORT_SCHEMAS.items():
        group = [t for t in tables if t["schema"] == schema]
        if not group:
            continue
        nav.append(f'<p class="nav-group">{escape(schema)}</p><ul>' + "".join(
            f'<li><a href="#{escape(t["schema"])}.{escape(t["name"])}">{escape(t["name"])}</a></li>'
            for t in group) + "</ul>")
        sections.append(f'<h2 class="layer">{escape(heading)}</h2>' + "".join(map(render_table, group)))

    main = f"""
    <header>
      <h1>Column definitions</h1>
      <p>{len(tables)} tables, {column_count} columns in the Citi Bike reporting layer.
         Generated from the dbt project on {generated}. For measures and relationships, see the
         <a href="../model/">Power BI model</a>.</p>
    </header>
    <div class="search">
      <input type="search" id="filter" placeholder="Filter by column name or definition…"
             aria-label="Filter columns" autocomplete="off">
    </div>
    {"".join(sections)}
    <p class="empty" id="empty">No columns match that filter.</p>"""
    return render_shell("Column Definitions", "columns", "".join(nav), main, FILTER_SCRIPT)


# --- Power BI model catalog (from markdown) --------------------------------

def github_slug(value: str, separator: str = "-") -> str:
    """Match GitHub's heading anchors, so the catalog's own #links keep working."""
    return re.sub(r"[^\w\- ]", "", value.strip().lower()).replace(" ", separator)


def render_model_page(catalog_path: Path) -> str:
    md = markdown.Markdown(extensions=["tables", "fenced_code", "toc"],
                           extension_configs={"toc": {"slugify": github_slug}})
    # Table cells escape pipes as \| (needed on GitHub too); Python-Markdown
    # keeps the backslash inside code spans, so drop it after rendering.
    body = md.convert(catalog_path.read_text()).replace("\\|", "|")

    def flatten(tokens):
        for token in tokens:
            yield token
            yield from flatten(token["children"])

    nav_items = []
    for token in flatten(md.toc_tokens):
        if token["level"] in (2, 3):
            css = ' class="sub"' if token["level"] == 3 else ""
            name = escape(unescape(token["name"]))
            nav_items.append(f'<li{css}><a href="#{token["id"]}">{name}</a></li>')
    nav = "<ul>" + "".join(nav_items) + "</ul>"
    return render_shell("Power BI Model", "model", nav, f'<div class="doc">{body}</div>')


def main(target_dir: str) -> None:
    target = Path(target_dir)
    tables = load_tables(target)
    if not tables:
        raise SystemExit(f"No gold/marts models found in {target / 'manifest.json'}.")

    pages = {
        "columns": render_columns_page(tables),
        "model": render_model_page(MODEL_CATALOG),
    }
    for folder, html in pages.items():
        out = target / folder / "index.html"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html)
        print(f"Wrote {out}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "dbt/target")
