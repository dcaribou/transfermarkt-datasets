"""Collect market value data from transfermarkt's REST API

https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{player_id}
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

# for each player id, get the market value data from the API
async def get_market_values(player_ids: List[int]) -> List[dict]:
    """Get the market value data from the API for each player id.

    Args:
        player_ids (List[int]): List of player ids

    Returns:
        List[dict]: List of dicts with market value data
    """

    logging.info(f"Requesting market values for {len(player_ids)} players")

    async def fetch_data(session, player_id):

        headers = {
            'Content-Type': 'application/json',
            'User-Agent': USER_AGENT
        }

        async with session.get(
            url=(MARKET_VALUES_API + str(player_id)),
            headers=headers) as response:

            json = await response.json()
            return {"response": json, "player_id": player_id}

    async with aiohttp.ClientSession() as session:
            tasks = [fetch_data(session, player_id) for player_id in player_ids]

            # Use asyncio.gather to execute the tasks concurrently
            responses = await asyncio.gather(*tasks)

    return responses

def persist_market_values(market_values: List[dict], path: str) -> None:
    """Persist the market value data to a file.

    Args:
        market_values (List[dict]): List of dicts with market value data
        path (str): Path where to store the market value data
    """
    with open(path, "w") as f:
        f.writelines(json.dumps(market_value) + "\n" for market_value in market_values)

def run_for_season(season: int) -> None:
    """Run all steps for a given season.

    :param season: _description_
    :type season: int
    """
    players_asset_path = f"data/raw/transfermarkt-scraper/{season}/players.json.gz"
    target_path = f"data/raw/transfermarkt-api/{season}/market_values.json"

    logging.info(f"Starting player market value acquisition for season {season}")

    # create target directory if it does not exist
    pathlib.Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    # collect market values for players in SEASON
    market_values = asyncio.run(get_market_values(get_player_ids(season)))

    logging.info(f"Persisting market values for season {season}")

    # persist market values to file
    persist_market_values(market_values, target_path)

parser = argparse.ArgumentParser()
parser.add_argument(
  '--seasons',
  help="Season to be acquired. This is passed to the scraper as the SEASON argument",
  default="2023",
  type=str
)

parsed = parser.parse_args()

expanded_seasons = seasons_list(parsed.seasons)

for season in expanded_seasons:
    run_for_season(season)
