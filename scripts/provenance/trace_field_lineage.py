"""Automatic field-level provenance extraction using sqlglot.

Parses compiled dbt SQL from the manifest and traces each output column
in curated models back to its base/staging model sources using sqlglot's
column-level lineage API.

The lineage trace stops at the base/staging model boundary, producing
human-readable source references like "base_games.home_club_goals" rather
than raw file column names.

Outputs:
  - A _field_provenance table in the DuckDB database
  - source() and sources() DuckDB table macros for easy querying

Usage:
    python scripts/provenance/trace_field_lineage.py [--db dbt/duck.db] [--manifest dbt/target/manifest.json]

After running, query provenance in DuckDB:

    SELECT * FROM source('games', 'home_club_name');
    SELECT * FROM sources('games');
"""

import argparse
import json
import re
import sys

import duckdb
import sqlglot
from sqlglot.lineage import lineage

# Curated models to trace provenance for
CURATED_MODELS = ["competitions", "clubs", "games"]

# Map base/staging model prefixes to their source system
SOURCE_SYSTEM_PREFIXES = {
    "stg_openfootball": "openfootball",
    "base_": "transfermarkt",
}


def load_manifest(manifest_path: str) -> dict:
    with open(manifest_path) as f:
        return json.load(f)


def build_sources(manifest: dict) -> tuple[dict, dict]:
    """Build sources dict and model-to-source-system mapping from the manifest.

    Returns:
        sources: {fully_qualified_name: compiled_sql}
        model_source_map: {model_alias: source_system} for base/staging models
    """
    sources = {}
    model_source_map = {}

    for _k, v in manifest["nodes"].items():
        if v["resource_type"] != "model" or not v.get("compiled_code"):
            continue

        alias = v["alias"]
        db = v.get("database", "duck")
        schema_name = v.get("schema", "dev")
        fq_name = f'"{db}"."{schema_name}"."{alias}"'
        sources[fq_name] = v["compiled_code"]

        # Classify base/staging models by source system
        for prefix, system in SOURCE_SYSTEM_PREFIXES.items():
            if alias.startswith(prefix):
                model_source_map[alias] = system
                break

    return sources, model_source_map


def get_output_columns(compiled_sql: str) -> list[str]:
    """Extract output column names from a curated model's compiled SQL.

    For UNION ALL models, reads columns from the explicit 'of_unmatched' CTE.
    Falls back to the outermost SELECT for non-union models.
    """
    parsed = sqlglot.parse_one(compiled_sql, dialect="duckdb")

    # Try the of_unmatched CTE first (union models with explicit columns)
    for cte in parsed.find_all(sqlglot.exp.CTE):
        if cte.alias == "of_unmatched":
            select = cte.find(sqlglot.exp.Select)
            return _extract_select_columns(select)

    # Fallback: outermost SELECT
    select = parsed.find(sqlglot.exp.Select)
    return _extract_select_columns(select) if select else []


def _extract_select_columns(select) -> list[str]:
    cols = []
    for expr in select.expressions:
        if hasattr(expr, "alias") and expr.alias:
            cols.append(expr.alias)
        elif isinstance(expr, sqlglot.exp.Column):
            cols.append(expr.name)
    return cols


def _fq_to_alias(fq_name: str) -> str:
    """Convert 'duck.dev.base_clubs' to 'base_clubs'."""
    parts = fq_name.split(".")
    return parts[-1] if parts else fq_name


def collect_at_boundary(node, model_source_map: dict) -> list[dict]:
    """Walk lineage tree and collect nodes at the base/staging model boundary.

    Stops descending when source_name matches a known base/staging model,
    giving us clean references like base_games.home_club_goals.
    """
    base_models = set(model_source_map.keys())
    results = []
    _walk_boundary(node, results, base_models, model_source_map, entered_base=False)
    return results


def _walk_boundary(n, results, base_models, model_source_map, entered_base):
    src = n.source_name
    src_alias = _fq_to_alias(src) if src else None

    # Check if we've just crossed into a base model
    if src_alias and src_alias in base_models and not entered_base:
        source_system = model_source_map[src_alias]
        col_name = n.name
        # Clean: "duck.dev.base_clubs.name" -> "base_clubs.name"
        if "." in col_name:
            parts = col_name.split(".")
            # Keep model.column format
            col_name = parts[-1]
        results.append({
            "source_model": src_alias,
            "source_column": col_name,
            "source_system": source_system,
        })
        return  # Don't descend further

    if not n.downstream:
        # Leaf with no base model match -> literal/static value
        results.append({
            "source_model": "_literal",
            "source_column": n.name,
            "source_system": "_literal",
        })
        return

    for d in n.downstream:
        new_entered = entered_base or (src_alias in base_models if src_alias else False)
        _walk_boundary(d, results, base_models, model_source_map, new_entered)


def classify_derivation(sources_found: list[dict]) -> str:
    """Classify derivation type from collected source records."""
    systems = {s["source_system"] for s in sources_found if s["source_system"] != "_literal"}
    all_literal = all(s["source_system"] == "_literal" for s in sources_found)

    if all_literal:
        return "static"
    if len(systems) > 1:
        return "coalesce"
    if len(sources_found) > 1 and len(systems) == 1:
        # Multiple columns from the same source system -> computed
        unique_cols = {(s["source_model"], s["source_column"]) for s in sources_found
                       if s["source_system"] != "_literal"}
        if len(unique_cols) > 1:
            return "computed"
    return "direct"


def trace_column(model_name: str, column: str, compiled_sql: str,
                 sources: dict, model_source_map: dict) -> list[dict]:
    """Trace a single column's lineage and return provenance records."""
    try:
        node = lineage(
            column=column,
            sql=compiled_sql,
            sources=sources,
            dialect="duckdb",
        )
    except Exception as e:
        print(f"  WARNING: cannot trace {model_name}.{column}: {e}", file=sys.stderr)
        return [{
            "model": model_name,
            "field": column,
            "source_model": "_unknown",
            "source_column": "",
            "source_system": "_unknown",
            "derivation": "unknown",
        }]

    raw_sources = collect_at_boundary(node, model_source_map)
    derivation = classify_derivation(raw_sources)

    # Deduplicate by (source_model, source_column)
    seen = set()
    unique = []
    for s in raw_sources:
        key = (s["source_system"], s["source_model"], s["source_column"])
        if key not in seen:
            seen.add(key)
            unique.append(s)

    # Filter out literal entries for non-static fields to reduce noise
    if derivation != "static":
        non_literal = [s for s in unique if s["source_system"] != "_literal"]
        if non_literal:
            unique = non_literal

    return [{
        "model": model_name,
        "field": column,
        "source_model": s["source_model"],
        "source_column": s["source_column"],
        "source_system": s["source_system"],
        "derivation": derivation,
    } for s in unique]


def trace_model(model_name: str, sources: dict, model_source_map: dict) -> list[dict]:
    """Trace all columns in a curated model."""
    fq_name = f'"duck"."dev"."{model_name}"'
    if fq_name not in sources:
        print(f"WARNING: {model_name} not found in manifest", file=sys.stderr)
        return []

    compiled_sql = sources[fq_name]
    columns = get_output_columns(compiled_sql)
    print(f"Tracing {model_name}: {len(columns)} columns")

    records = []
    for col in columns:
        col_records = trace_column(model_name, col, compiled_sql, sources, model_source_map)
        records.extend(col_records)
        src_count = len(col_records)
        derivation = col_records[0]["derivation"] if col_records else "?"
        print(f"  {col:40s} -> {derivation:10s} ({src_count} source(s))")

    return records


def write_to_duckdb(records: list[dict], db_path: str):
    """Write provenance records to the _field_provenance table in DuckDB."""
    con = duckdb.connect(db_path)

    con.execute("CREATE SCHEMA IF NOT EXISTS dev")

    con.execute("DROP TABLE IF EXISTS dev._field_provenance")
    con.execute("""
        CREATE TABLE dev._field_provenance (
            model           VARCHAR NOT NULL,
            field           VARCHAR NOT NULL,
            source_system   VARCHAR NOT NULL,
            source_model    VARCHAR NOT NULL,
            source_column   VARCHAR NOT NULL,
            derivation      VARCHAR NOT NULL
        )
    """)

    con.executemany(
        "INSERT INTO dev._field_provenance VALUES (?, ?, ?, ?, ?, ?)",
        [
            (r["model"], r["field"], r["source_system"],
             r["source_model"], r["source_column"], r["derivation"])
            for r in records
        ],
    )

    # source(model, field) -> all provenance records for that field
    con.execute("""
        CREATE OR REPLACE MACRO source(model_name, field_name) AS TABLE
            SELECT
                model,
                field,
                source_system,
                source_model,
                source_column,
                derivation
            FROM dev._field_provenance
            WHERE model = model_name
              AND field = field_name
    """)

    # sources(model) -> all provenance records for all fields in a model
    con.execute("""
        CREATE OR REPLACE MACRO sources(model_name) AS TABLE
            SELECT
                model,
                field,
                source_system,
                source_model,
                source_column,
                derivation
            FROM dev._field_provenance
            WHERE model = model_name
            ORDER BY field, source_system
    """)

    row_count = con.execute("SELECT COUNT(*) FROM dev._field_provenance").fetchone()[0]
    model_count = con.execute(
        "SELECT COUNT(DISTINCT model) FROM dev._field_provenance"
    ).fetchone()[0]
    field_count = con.execute(
        "SELECT COUNT(DISTINCT model || '.' || field) FROM dev._field_provenance"
    ).fetchone()[0]

    print(f"\nWrote {row_count} provenance records ({model_count} models, {field_count} fields) to {db_path}")
    print("\nQuery examples:")
    print("  SELECT * FROM source('games', 'home_club_name');")
    print("  SELECT * FROM sources('games');")
    print("  SELECT * FROM dev._field_provenance;")

    con.close()


def main():
    parser = argparse.ArgumentParser(description="Trace field-level provenance using sqlglot")
    parser.add_argument("--db", default="dbt/duck.db", help="Path to DuckDB database")
    parser.add_argument("--manifest", default="dbt/target/manifest.json", help="Path to dbt manifest")
    parser.add_argument("--models", nargs="*", default=CURATED_MODELS, help="Curated models to trace")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    sources, model_source_map = build_sources(manifest)

    all_records = []
    for model_name in args.models:
        records = trace_model(model_name, sources, model_source_map)
        all_records.extend(records)

    if all_records:
        write_to_duckdb(all_records, args.db)
    else:
        print("No provenance records generated.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
