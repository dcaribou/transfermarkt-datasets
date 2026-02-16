"""Acquire raw data from openfootball/football.json GitHub repository.

Downloads JSON match data for specified seasons and competition codes from
https://github.com/openfootball/football.json

Usage:
    python openfootball.py --seasons 2024
    python openfootball.py --seasons 2022-2024
    python openfootball.py --seasons 2024 --codes en.1,de.1,es.1

Raw output follows the source-agnostic convention:
    data/raw/openfootball/<season>/<code>.json
"""

import argparse
import json
import logging
import logging.config
import pathlib
import urllib.request
import urllib.error
from typing import List, Optional

from transfermarkt_datasets.core.utils import (
    read_config,
    seasons_list,
    raw_data_path,
)

SOURCE_NAME = "openfootball"

REPO_BASE_URL = "https://raw.githubusercontent.com/openfootball/football.json/master"
TREE_API_URL = "https://api.github.com/repos/openfootball/football.json/git/trees/master?recursive=1"

# Default competition codes to acquire (top-tier European leagues + Champions League)
DEFAULT_CODES = [
    "en.1", "en.2",
    "de.1", "de.2",
    "es.1", "es.2",
    "it.1", "it.2",
    "fr.1", "fr.2",
    "nl.1",
    "pt.1",
    "be.1",
    "tr.1",
    "gr.1",
    "sco.1",
    "at.1",
    "ch.1",
    "ru.1",
    "uefa.cl",
]

acquire_config = read_config()["acquire"]
logging.config.dictConfig(acquire_config["logging"])


def season_to_openfootball(season_year: int) -> str:
    """Convert a season start year to the openfootball folder name.

    European leagues use '2024-25' style; calendar-year leagues use '2025'.
    We target European leagues so we use the hyphenated form.
    """
    next_year = (season_year + 1) % 100
    return f"{season_year}-{next_year:02d}"


def discover_available_files() -> dict:
    """Query the GitHub tree API to find all available season/code files.

    Returns:
        dict mapping 'season_folder/code' to the download path.
    """
    logging.info("Discovering available files from GitHub tree API")
    req = urllib.request.Request(
        TREE_API_URL,
        headers={"User-Agent": "transfermarkt-datasets/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            tree = json.loads(resp.read().decode())
    except urllib.error.URLError as e:
        logging.warning(f"Could not fetch tree API: {e}. Will attempt direct downloads.")
        return {}

    available = {}
    for item in tree.get("tree", []):
        path = item["path"]
        if path.endswith(".json") and "/" in path:
            available[path] = path
    logging.info(f"Found {len(available)} JSON files in repo")
    return available


def download_file(url: str, dest: pathlib.Path) -> bool:
    """Download a single file from URL to dest path.

    Returns True on success, False on failure.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "transfermarkt-datasets/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return True
    except urllib.error.URLError as e:
        logging.warning(f"Failed to download {url}: {e}")
        return False


def acquire_season(season_year: int, codes: List[str], available: Optional[dict] = None) -> dict:
    """Acquire all competition files for a single season.

    Args:
        season_year: The season start year (e.g. 2024 for 2024-25).
        codes: List of competition codes to download.
        available: Optional pre-fetched map of available files.

    Returns:
        dict with counts of successful and failed downloads.
    """
    season_folder = season_to_openfootball(season_year)
    stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    for code in codes:
        relative_path = f"{season_folder}/{code}.json"

        # If we have the tree, check availability first
        if available and relative_path not in available:
            logging.debug(f"Not available in repo: {relative_path}")
            stats["skipped"] += 1
            continue

        url = f"{REPO_BASE_URL}/{relative_path}"
        dest = raw_data_path(SOURCE_NAME, season_year, code, ext="json")

        logging.info(f"Downloading {url} -> {dest}")
        if download_file(url, dest):
            stats["downloaded"] += 1
        else:
            stats["failed"] += 1

    return stats


def validate_season(season_year: int) -> bool:
    """Check that at least one non-empty file was downloaded for a season."""
    season_dir = raw_data_path(SOURCE_NAME, season_year)
    if not season_dir.exists():
        return False
    files = list(season_dir.glob("*.json"))
    return len(files) > 0


def main():
    parser = argparse.ArgumentParser(description="Acquire OpenFootball data")
    parser.add_argument(
        "--seasons",
        help="Season(s) to acquire, e.g. '2024' or '2022-2024'",
        default="2024",
        type=str,
    )
    parser.add_argument(
        "--codes",
        help="Comma-separated competition codes (default: top European leagues)",
        default=None,
        type=str,
    )
    args = parser.parse_args()

    expanded_seasons = seasons_list(args.seasons)
    codes = args.codes.split(",") if args.codes else DEFAULT_CODES

    logging.info(f"Acquiring OpenFootball data for seasons={expanded_seasons}, codes={codes}")

    # Discover available files once
    available = discover_available_files()

    total_stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    for season in expanded_seasons:
        stats = acquire_season(season, codes, available)
        for k in total_stats:
            total_stats[k] += stats[k]

        if not validate_season(season):
            logging.warning(f"No files downloaded for season {season}")

    logging.info(
        f"Acquisition complete: {total_stats['downloaded']} downloaded, "
        f"{total_stats['skipped']} skipped, {total_stats['failed']} failed"
    )

    if total_stats["downloaded"] == 0:
        raise RuntimeError("No files were downloaded. Check seasons and codes.")


if __name__ == "__main__":
    main()
