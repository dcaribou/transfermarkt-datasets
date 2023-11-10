"""Collect market value data from transfermarkt's REST API

https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{player_id}
"""

import pathlib
from typing import List
import json
import gzip

import aiohttp
import asyncio

from transfermarkt_datasets.core.utils import read_config

import logging
import logging.config

acquire_config = read_config()["acquire"]

logging.config.dictConfig(
  acquire_config["logging"]
)

SEASON = 2023
PLAYERS_ASSET_PATH = f"data/raw/transfermarkt-scraper/{SEASON}/players.json.gz"
TARGET_PATH = f"data/raw/transfermarkt-api/{SEASON}/market_values.json"

MARKET_VALUES_API = "https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/"
USER_AGENT = "transfermarkt-datasets/1.0 (https://github.com/dcaribou/transfermarkt-datasets)"


# get the player ids from the players asset from transfermarkt-scraper source
def get_player_ids() -> List[int]:
    """Get the player ids from the players asset from transfermarkt-scraper source.

    Returns:
        List[int]: List of player ids
    """
    # read lines from a zipped file
    with gzip.open(PLAYERS_ASSET_PATH, mode="r") as z:
        players = [json.loads(line) for line in z.readlines()]

    player_ids = [
        int(player["href"].split("/")[-1]) 
        for player in players
    ]
    logging.info(f"Fetched {len(player_ids)} player ids from {PLAYERS_ASSET_PATH}")

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

def persist_market_values(market_values: List[dict]) -> None:
    """Persist the market value data to a file.

    Args:
        market_values (List[dict]): List of dicts with market value data
    """
    with open(TARGET_PATH, "w") as f:
        f.writelines(json.dumps(market_value) + "\n" for market_value in market_values)


logging.info("Starting player market value acquisition")

# create target directory if it does not exist
pathlib.Path(TARGET_PATH).parent.mkdir(parents=True, exist_ok=True)

# collect market values for players in SEASON
market_values = asyncio.run(get_market_values(get_player_ids()))

logging.info("Persisting market values")

# persist market values to file
persist_market_values(market_values)
