"""Automated dynamic profiling and drift detection for curated models.

Computes profiling baselines (row count, null ratio, cardinality, value ranges)
per source/model, compares against stored baselines, and flags statistically
significant deviations.

Usage:
    python scripts/profiling/profile_models.py --db dbt/duck.db --schema dev
    python scripts/profiling/profile_models.py --db dbt/duck.db --schema dev --update-baseline
"""

import argparse
import json
import pathlib
import sys
from dataclasses import dataclass, asdict
from typing import Optional

import duckdb

BASELINE_PATH = pathlib.Path("scripts/profiling/baselines.json")

# Models to profile (Phase 0 in-scope)
PROFILED_MODELS = ["competitions", "clubs", "games"]

# Key columns to profile per model
MODEL_COLUMNS = {
    "competitions": ["competition_id", "name", "source_system"],
    "clubs": ["club_id", "name", "source_system"],
    "games": ["game_id", "date", "home_club_name", "away_club_name", "source_system"],
}

# Thresholds for drift detection
DRIFT_THRESHOLDS = {
    "row_count_decrease_pct": 0.10,   # >10% row decrease is flagged
    "row_count_increase_pct": 0.50,   # >50% row increase is flagged
    "null_ratio_increase": 0.05,      # >5pp null ratio increase is flagged
    "cardinality_decrease_pct": 0.10, # >10% cardinality decrease is flagged
}


@dataclass
class ColumnProfile:
    column: str
    null_count: int
    null_ratio: float
    distinct_count: int
    min_value: Optional[str]
    max_value: Optional[str]


@dataclass
class ModelProfile:
    model: str
    row_count: int
    source_counts: dict  # source_system -> count
    columns: list  # list of ColumnProfile dicts


def profile_model(conn: duckdb.DuckDBPyConnection, schema: str, model: str) -> ModelProfile:
    """Profile a single model."""
    table = f"{schema}.{model}"

    # Row count
    row_count = conn.sql(f"SELECT count(*) FROM {table}").fetchone()[0]

    # Source system distribution
    source_counts = {}
    try:
        rows = conn.sql(
            f"SELECT source_system, count(*) as cnt FROM {table} GROUP BY source_system"
        ).fetchall()
        source_counts = {row[0]: row[1] for row in rows}
    except duckdb.CatalogException:
        source_counts = {"unknown": row_count}

    # Column profiles
    columns = []
    for col in MODEL_COLUMNS.get(model, []):
        try:
            result = conn.sql(f"""
                SELECT
                    count(*) filter (where "{col}" is null) as null_count,
                    count(*) filter (where "{col}" is null) * 1.0 / count(*) as null_ratio,
                    count(distinct "{col}") as distinct_count,
                    min("{col}")::varchar as min_value,
                    max("{col}")::varchar as max_value
                FROM {table}
            """).fetchone()

            columns.append(asdict(ColumnProfile(
                column=col,
                null_count=result[0],
                null_ratio=round(result[1], 4),
                distinct_count=result[2],
                min_value=result[3],
                max_value=result[4],
            )))
        except duckdb.CatalogException:
            pass

    return ModelProfile(
        model=model,
        row_count=row_count,
        source_counts=source_counts,
        columns=columns,
    )


def load_baselines() -> dict:
    """Load stored baselines from disk."""
    if BASELINE_PATH.exists():
        with open(BASELINE_PATH) as f:
            return json.load(f)
    return {}


def save_baselines(profiles: dict):
    """Save profiles as new baselines."""
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BASELINE_PATH, "w") as f:
        json.dump(profiles, f, indent=2, default=str)


def check_drift(current: ModelProfile, baseline: dict) -> list:
    """Compare current profile against baseline, return list of drift alerts."""
    alerts = []
    model = current.model

    # Row count drift
    baseline_rows = baseline.get("row_count", 0)
    if baseline_rows > 0:
        pct_change = (current.row_count - baseline_rows) / baseline_rows

        if pct_change < -DRIFT_THRESHOLDS["row_count_decrease_pct"]:
            alerts.append(
                f"[{model}] Row count decreased by {abs(pct_change)*100:.1f}% "
                f"({baseline_rows} -> {current.row_count})"
            )
        elif pct_change > DRIFT_THRESHOLDS["row_count_increase_pct"]:
            alerts.append(
                f"[{model}] Row count increased by {pct_change*100:.1f}% "
                f"({baseline_rows} -> {current.row_count})"
            )

    # Column-level drift
    baseline_cols = {c["column"]: c for c in baseline.get("columns", [])}
    for col_profile in current.columns:
        col = col_profile["column"]
        if col not in baseline_cols:
            continue

        bcol = baseline_cols[col]

        # Null ratio increase
        null_delta = col_profile["null_ratio"] - bcol.get("null_ratio", 0)
        if null_delta > DRIFT_THRESHOLDS["null_ratio_increase"]:
            alerts.append(
                f"[{model}.{col}] Null ratio increased by {null_delta*100:.1f}pp "
                f"({bcol['null_ratio']*100:.1f}% -> {col_profile['null_ratio']*100:.1f}%)"
            )

        # Cardinality decrease
        b_distinct = bcol.get("distinct_count", 0)
        if b_distinct > 0:
            card_change = (col_profile["distinct_count"] - b_distinct) / b_distinct
            if card_change < -DRIFT_THRESHOLDS["cardinality_decrease_pct"]:
                alerts.append(
                    f"[{model}.{col}] Cardinality decreased by {abs(card_change)*100:.1f}% "
                    f"({b_distinct} -> {col_profile['distinct_count']})"
                )

    return alerts


def main():
    parser = argparse.ArgumentParser(description="Profile dbt models and detect drift")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--schema", default="dev", help="Schema name")
    parser.add_argument(
        "--update-baseline", action="store_true",
        help="Save current profiles as new baselines"
    )
    args = parser.parse_args()

    conn = duckdb.connect(args.db, read_only=True)

    # Profile all models
    profiles = {}
    for model in PROFILED_MODELS:
        try:
            profile = profile_model(conn, args.schema, model)
            profiles[model] = asdict(profile)
            print(f"Profiled {model}: {profile.row_count} rows, sources={profile.source_counts}")
        except Exception as e:
            print(f"WARNING: Could not profile {model}: {e}", file=sys.stderr)

    conn.close()

    # Update baselines if requested
    if args.update_baseline:
        save_baselines(profiles)
        print(f"\nBaselines updated at {BASELINE_PATH}")
        return

    # Check for drift against baselines
    baselines = load_baselines()
    if not baselines:
        print("\nNo baselines found. Run with --update-baseline to create initial baselines.")
        return

    all_alerts = []
    for model, profile_dict in profiles.items():
        if model in baselines:
            profile = ModelProfile(**{
                k: v for k, v in profile_dict.items()
            })
            alerts = check_drift(profile, baselines[model])
            all_alerts.extend(alerts)

    if all_alerts:
        print("\n=== DRIFT ALERTS ===")
        for alert in all_alerts:
            print(f"  DRIFT: {alert}")
        print(f"\n{len(all_alerts)} drift alert(s) detected.")
        sys.exit(1)
    else:
        print("\nNo drift detected. All models within expected ranges.")


if __name__ == "__main__":
    main()
