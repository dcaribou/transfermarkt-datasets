from unicodedata import name
import requests
import csv
from dagster import job, op, get_dagster_logger
from transfermarkt_datasets.assets.games import GamesAsset


@op
def hello_cereal():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereals = [row for row in csv.DictReader(lines)]
    get_dagster_logger().info(f"Found {len(cereals)} cereals")

@op
def build_games():
    games = GamesAsset(name="games",seasons=[2021])
    games.build()
