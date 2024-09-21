"""Collect market value and transfer history data from transfermarkt's REST API
https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{player_id}
https://www.transfermarkt.co.uk/ceapi/transferHistory/list/{player_id}

Usage:
    python transfermarkt-api.py --seasons=<seasons>

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


# get the player ids from the players asset from transfermarkt-scraper source
def get_player_ids(season: int) -> List[int]:
    """Get the player ids from the players asset from transfermarkt-scraper source.

    Returns:
        List[int]: List of player ids
    """

    players_asset_path = f"data/raw/transfermarkt-scraper/{season}/players.json.gz"

    # read lines from a zipped file
    with gzip.open(players_asset_path, mode="r") as z:
        players = [json.loads(line) for line in z.readlines()]

    player_ids = [
        int(player["href"].split("/")[-1])
        for player in players
    ]
    logging.info(f"Fetched {len(player_ids)} player ids from {players_asset_path}")

    return player_ids

# helper function to fetch data from API
async def fetch_data(session, url, player_id):
    """Fetch data from the API for a given URL and player ID.

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

    async with session.get(url=url, headers=headers, ssl=False) as response:
        try:
            json = await response.json()
            return {"response": json, "player_id": player_id}
        except aiohttp.ContentTypeError as e:
            logging.error(f"Failed to fetch data for player {player_id}: {e}")
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

    async with aiohttp.ClientSession() as session:
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

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, TRANSFERS_API + str(player_id), player_id) for player_id in player_ids]

        # Use asyncio.gather to execute the tasks concurrently
        responses = await asyncio.gather(*tasks)

    return responses

def persist_data(data: List[dict], path: str) -> None:
    """Persist the data to a file.

    Args:
        data (List[dict]): List of dicts with data to persist
        path (str): Path where to store the data
    """
    with open(path, "w") as f:
        f.writelines(json.dumps(item) + "\n" for item in data)

def run_for_season(season: int) -> None:
    """Run all steps for a given season.

    Args:
        season (int): The season to process
    """
    target_market_values_path = f"data/raw/transfermarkt-api/{season}/market_values.json"
    target_transfers_path = f"data/raw/transfermarkt-api/{season}/transfers.json"

    logging.info(f"Starting player data acquisition for season {season}")

    # create target directories if they do not exist
    pathlib.Path(target_market_values_path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(target_transfers_path).parent.mkdir(parents=True, exist_ok=True)

    # get player IDs for the season
    player_ids = get_player_ids(season)

    # collect market values and transfers for players in SEASON
    market_values = asyncio.run(get_market_values(player_ids))
    transfers = asyncio.run(get_transfers(player_ids))

    # filter out player ids in responses that are not in the original list
    transfers = [item for item in transfers if item["player_id"] in player_ids]

    logging.info(f"Persisting market values and transfers for season {season}")

    # persist market values and transfers to files
    persist_data(market_values, target_market_values_path)
    persist_data(transfers, target_transfers_path)

parser = argparse.ArgumentParser()
parser.add_argument(
  '--seasons',
  help="Season to be acquired. This is passed to the scraper as the SEASON argument",
  default="2024",
  type=str
)

parsed = parser.parse_args()

expanded_seasons = seasons_list(parsed.seasons)

for season in expanded_seasons:
    run_for_season(season)
