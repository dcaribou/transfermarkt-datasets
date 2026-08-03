"""Collect market value and transfer history data from transfermarkt's REST API
https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{player_id}
https://www.transfermarkt.co.uk/ceapi/transferHistory/list/{player_id}

Usage:
    python transfermarkt-api.py --seasons=<seasons> [--players=<ids>] [--clubs=<ids>] [--competitions=<ids>]

Note that the will look for the players asset from the transfermarkt-scraper acquirer under
    data/raw/transfermarkt-scraper/{season}/players.json.gz
"""

import pathlib
from typing import List
import json
import gzip
import argparse

import aiohttp
import asyncio

from transfermarkt_datasets.core.utils import (
  read_config,
  seasons_list
)

import logging
import logging.config

acquire_config = read_config()["acquire"]

logging.config.dictConfig(
  acquire_config["logging"]
)

MARKET_VALUES_API = "https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/"
TRANSFERS_API = "https://www.transfermarkt.co.uk/ceapi/transferHistory/list/"
USER_AGENT = "transfermarkt-datasets/1.0 (https://github.com/dcaribou/transfermarkt-datasets)"

# how many times to re-request the players that came back with a null response
MAX_BATCH_RETRIES = 2

# a run with a higher share of null responses than this is treated as failed,
# and is never written over the raw data already on disk
MAX_NULL_RESPONSE_RATE = 0.2


# get the player ids from the players asset from transfermarkt-scraper source
def get_player_ids(season: int, player_filter=None, club_filter=None, competition_filter=None) -> List[int]:
    """Get the player ids from the players asset from transfermarkt-scraper source.

    Args:
        season: The season year.
        player_filter: Optional set of player IDs to include (as strings).
        club_filter: Optional set of club IDs to include (as strings).
        competition_filter: Optional set of competition IDs to include (as strings).

    Returns:
        List[int]: List of player ids
    """

    players_asset_path = f"data/raw/transfermarkt-scraper/{season}/players.json.gz"

    # read lines from a zipped file
    with gzip.open(players_asset_path, mode="r") as z:
        players = [json.loads(line) for line in z.readlines()]

    if player_filter:
        players = [p for p in players if p["href"].split("/")[-1] in player_filter]
    elif club_filter:
        players = [p for p in players
                   if p.get("parent", {}).get("href", "").split("/")[-1] in club_filter]
    elif competition_filter:
        # Resolve competition IDs → club IDs from the clubs file, then filter players by club
        clubs_path = f"data/raw/transfermarkt-scraper/{season}/clubs.json.gz"
        with gzip.open(clubs_path, mode="r") as z:
            clubs = [json.loads(line) for line in z.readlines()]
        club_ids = {
            c["href"].split("/")[-1] for c in clubs
            if c.get("parent", {}).get("href", "").rstrip("/").split("/")[-1] in competition_filter
        }
        logging.info(f"Competition filter resolved to {len(club_ids)} club IDs")
        players = [p for p in players
                   if p.get("parent", {}).get("href", "").split("/")[-1] in club_ids]

    player_ids = [
        int(player["href"].split("/")[-1])
        for player in players
    ]
    logging.info(f"Fetched {len(player_ids)} player ids from {players_asset_path}")

    return player_ids

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1  # seconds
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)

# helper function to fetch data from API
async def fetch_data(session, url, player_id):
    """Fetch data from the API for a given URL and player ID.
    Retries up to MAX_RETRIES times with exponential backoff on transient errors.

    Args:
        session (aiohttp.ClientSession): The aiohttp session
        url (str): The API URL
        player_id (int): The player ID

    Returns:
        dict: The API response and player ID
    """
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT
    }

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url=url, headers=headers, ssl=False) as response:
                if 400 <= response.status < 500:
                    logging.warning(f"HTTP {response.status} for player {player_id}, not retrying")
                    return {"response": None, "player_id": player_id}
                if response.status >= 500:
                    logging.warning(f"HTTP {response.status} for player {player_id}, attempt {attempt + 1}/{MAX_RETRIES}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                        continue
                    return {"response": None, "player_id": player_id}
                try:
                    body = await response.json()
                except aiohttp.ContentTypeError as e:
                    logging.error(f"Failed to parse response for player {player_id}: {e}")
                    body = None
                if body is None and attempt < MAX_RETRIES - 1:
                    logging.warning(f"Null response for player {player_id}, attempt {attempt + 1}/{MAX_RETRIES}, retrying")
                    await asyncio.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                    continue
                return {"response": body, "player_id": player_id}
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Request error for player {player_id}, attempt {attempt + 1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
            else:
                return {"response": None, "player_id": player_id}

# for each player id, get the market value data from the API
async def get_market_values(player_ids: List[int]) -> List[dict]:
    """Get the market value data from the API for each player id.

    Args:
        player_ids (List[int]): List of player ids

    Returns:
        List[dict]: List of dicts with market value data
    """

    logging.info(f"Requesting market values for {len(player_ids)} players")

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        tasks = [fetch_data(session, MARKET_VALUES_API + str(player_id), player_id) for player_id in player_ids]

        # Use asyncio.gather to execute the tasks concurrently
        responses = await asyncio.gather(*tasks)

    return responses

# for each player id, get the transfer history data from the API
async def get_transfers(player_ids: List[int]) -> List[dict]:
    """Get the transfer history data from the API for each player id.

    Args:
        player_ids (List[int]): List of player ids

    Returns:
        List[dict]: List of dicts with transfer history data
    """

    logging.info(f"Requesting transfer history for {len(player_ids)} players")

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        tasks = [fetch_data(session, TRANSFERS_API + str(player_id), player_id) for player_id in player_ids]

        # Use asyncio.gather to execute the tasks concurrently
        responses = await asyncio.gather(*tasks)

    return responses

def fetch_with_retries(fetch_all, player_ids: List, label: str) -> List[dict]:
    """Fetch responses for `player_ids`, re-requesting the ones that come back null.

    The API intermittently returns null for individual players, and rejects
    whole runs when it decides to block us. Retrying only the failed players
    keeps a partial failure from turning into a lost season.

    Args:
        fetch_all: Coroutine function taking a list of player ids
        player_ids (List): The players to request
        label (str): Name of the asset, used for logging

    Returns:
        List[dict]: One response per requested player, in the original order
    """
    results = asyncio.run(fetch_all(player_ids))

    for attempt in range(MAX_BATCH_RETRIES):
        null_ids = [item["player_id"] for item in results if item["response"] is None]
        if not null_ids:
            break
        logging.warning(
            f"Batch retry {attempt + 1}/{MAX_BATCH_RETRIES}: "
            f"{len(null_ids)} players with null {label} responses"
        )
        retry_lookup = {item["player_id"]: item for item in asyncio.run(fetch_all(null_ids))}
        results = [
            retry_lookup.get(item["player_id"], item) if item["response"] is None else item
            for item in results
        ]

    null_count = sum(1 for item in results if item["response"] is None)
    logging.info(
        f"{label} complete: {len(results)} total, {null_count} null responses remaining"
    )

    return results

def validate_responses(data: List[dict], path: str, label: str) -> None:
    """Check that an acquisition result is good enough to overwrite raw data.

    A blocked run still returns a well-formed response per player, just with a
    null payload, so without this check it overwrites good raw data with
    nothing. That is what happened on 2026-07-11: 22,324 null responses were
    written over season 2025, erasing 242MB of transfer history and removing
    players from the published dataset.

    Args:
        data (List[dict]): List of dicts with data to persist
        path (str): Path the data would be written to, used in error messages
        label (str): Name of the asset, used in log and error messages

    Raises:
        RuntimeError: If the result is empty or too many responses are null.
    """
    if not data:
        raise RuntimeError(
            f"{label} acquisition returned no records; refusing to overwrite {path}"
        )

    null_count = sum(1 for item in data if item["response"] is None)
    null_rate = null_count / len(data)

    if null_rate > MAX_NULL_RESPONSE_RATE:
        raise RuntimeError(
            f"{label} acquisition returned {null_count}/{len(data)} "
            f"({null_rate:.1%}) null responses, above the "
            f"{MAX_NULL_RESPONSE_RATE:.0%} threshold; refusing to overwrite {path}. "
            "This usually means the API blocked the run."
        )

    if null_count:
        logging.warning(
            f"Persisting {label} with {null_count}/{len(data)} null responses "
            f"({null_rate:.1%})"
        )

def persist_data(data: List[dict], path: str, label: str) -> None:
    """Persist the data to a file, unless the run looks like it failed.

    Args:
        data (List[dict]): List of dicts with data to persist
        path (str): Path where to store the data
        label (str): Name of the asset, used in log and error messages

    Raises:
        RuntimeError: If the result would not pass validate_responses.
    """
    validate_responses(data, path, label)

    with open(path, "w") as f:
        f.writelines(json.dumps(item) + "\n" for item in data)

def run_for_season(season: int, player_filter=None, club_filter=None, competition_filter=None) -> None:
    """Run all steps for a given season.

    Args:
        season (int): The season to process
        player_filter: Optional set of player IDs to filter.
        club_filter: Optional set of club IDs to filter.
        competition_filter: Optional set of competition IDs to filter.
    """
    target_market_values_path = f"data/raw/transfermarkt-api/{season}/market_values.json"
    target_transfers_path = f"data/raw/transfermarkt-api/{season}/transfers.json"

    logging.info(f"Starting player data acquisition for season {season}")

    # create target directories if they do not exist
    pathlib.Path(target_market_values_path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(target_transfers_path).parent.mkdir(parents=True, exist_ok=True)

    # get player IDs for the season
    player_ids = get_player_ids(season, player_filter=player_filter,
                                club_filter=club_filter, competition_filter=competition_filter)

    # collect market values and transfers for players in SEASON
    market_values = fetch_with_retries(get_market_values, player_ids, "market values")

    transfers = fetch_with_retries(get_transfers, player_ids, "transfers")

    # filter out player ids in responses that are not in the original list
    transfers = [item for item in transfers if item["player_id"] in player_ids]

    logging.info(f"Persisting market values and transfers for season {season}")

    # check both before writing either, so a failed run cannot leave one file
    # updated and the other stale
    validate_responses(market_values, target_market_values_path, "market values")
    validate_responses(transfers, target_transfers_path, "transfers")

    # persist market values and transfers to files
    persist_data(market_values, target_market_values_path, "market values")
    persist_data(transfers, target_transfers_path, "transfers")

def main():
    """Parse arguments and run the acquisition for every requested season."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--seasons',
      help="Season to be acquired. This is passed to the scraper as the SEASON argument",
      default="2024",
      type=str
    )
    parser.add_argument(
      '--competitions',
      help="Comma-separated competition IDs to filter (e.g., GB1,ES1). Only fetches data for players in these competitions.",
      default=None
    )
    parser.add_argument(
      '--clubs',
      help="Comma-separated club IDs to filter (e.g., 131,583). Only fetches data for players in these clubs.",
      default=None
    )
    parser.add_argument(
      '--players',
      help="Comma-separated player IDs to filter (e.g., 28003,1122196). Only fetches data for these players.",
      default=None
    )

    parsed = parser.parse_args()

    # Validate mutual exclusivity
    active_filters = sum(1 for f in [parsed.competitions, parsed.clubs, parsed.players] if f is not None)
    if active_filters > 1:
        parser.error("Only one filter (--competitions, --clubs, or --players) can be used at a time")

    player_filter = set(parsed.players.split(',')) if parsed.players else None
    club_filter = set(parsed.clubs.split(',')) if parsed.clubs else None
    competition_filter = set(parsed.competitions.split(',')) if parsed.competitions else None

    expanded_seasons = seasons_list(parsed.seasons)

    for season in expanded_seasons:
        run_for_season(season, player_filter=player_filter, club_filter=club_filter,
                       competition_filter=competition_filter)


if __name__ == "__main__":
    main()
