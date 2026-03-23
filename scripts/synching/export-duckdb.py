"""
Export curated tables from dbt/duck.db into a clean DuckDB file.

Produces a standalone DuckDB database with only the 12 curated tables
in the default (main) schema, plus a version table with the commit hash.

Usage:
    python scripts/synching/export-duckdb.py --schema dev
    python scripts/synching/export-duckdb.py --schema prod --commit $GITHUB_SHA
"""

import argparse
import os
import subprocess

import duckdb

CURATED_TABLES = [
    "appearances",
    "club_games",
    "clubs",
    "competitions",
    "countries",
    "game_events",
    "game_lineups",
    "games",
    "national_teams",
    "player_valuations",
    "players",
    "transfers",
]


def get_git_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def export_duckdb(source_db, output_db, source_schema, commit):
    if os.path.exists(output_db):
        os.remove(output_db)

    conn = duckdb.connect(output_db)
    conn.execute(f"ATTACH '{source_db}' AS source (READ_ONLY)")

    for table in CURATED_TABLES:
        print(f"  {source_schema}.{table} -> main.{table}")
        conn.execute(
            f"CREATE TABLE main.{table} AS SELECT * FROM source.{source_schema}.{table}"
        )

    conn.execute(
        "CREATE TABLE main.version AS SELECT ? AS commit_hash",
        [commit],
    )
    print(f"  version.commit_hash = {commit}")

    conn.execute("DETACH source")
    conn.close()

    size_mb = os.path.getsize(output_db) / (1024 * 1024)
    print(f"  Exported {len(CURATED_TABLES)} tables to {output_db} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export curated DuckDB tables")
    parser.add_argument("--source-db", default="dbt/duck.db")
    parser.add_argument("--output-db", default="data/prep/transfermarkt-datasets.duckdb")
    parser.add_argument("--schema", default="prod")
    parser.add_argument("--commit", default=None)
    args = parser.parse_args()

    commit = args.commit or get_git_commit()

    print("--> Export clean DuckDB file")
    export_duckdb(args.source_db, args.output_db, args.schema, commit)
    print("Done")
