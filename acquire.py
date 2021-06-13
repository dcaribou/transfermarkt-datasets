"""Acquire raw data from transfermark website using a dockerized version of transfermark-scraper
"""
import sys
import os

import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument(
  '--asset',
  help="Name of the asset to be acquired",
  choices=['clubs', 'players', 'games', 'appearances', 'all'],
  required=True
)
parser.add_argument(
  '--scrapy-cache',
  help="Scrapy cache location. If set, this cache will be used during the acquiring process",
  default=os.environ.get('SCRAPY_CACHE')
)

arguments = parser.parse_args()

SEASON = 2020
IMAGE = 'dcaribou/transfermarkt-scraper'
IMAGE_TAG = 'main'
ASSET_NAME = arguments.asset
SCRAPY_CACHE = arguments.scrapy_cache
DRY_RUN = os.environ.get('DRY_RUN')

class Asset():
  asset_parents = {
      'leagues' : None,
      'games': 'leagues',
      'clubs': 'leagues',
      'players': 'clubs',
      'appearances': 'players'
    }
  
  def __init__(self, name, season) -> None:
    import pathlib

    self.name = name
    self.season = season
    self.path = pathlib.Path(f"data/raw/{season}/{name}.json")

  def parent(self):
    return Asset(
      self.asset_parents[self.name],
      self.season
    )

  def file_full_path(self):
    return str(self.path.absolute())

  def file_name(self):
    return self.path.name

  @classmethod
  def all(self, season):
    return [Asset(name, season) for name in self.asset_parents if name != 'leagues']


def acquire_asset(asset, scrapy_cache):
  import docker
  import pathlib
  
  parent_asset = asset.parent()
  
  docker_client = docker.from_env()

  volumes = {}
  volumes[parent_asset.file_full_path()] = {'bind': f"/app/parents/{parent_asset.file_name()}", 'mode': 'ro'}
  if scrapy_cache is not None:
    path = pathlib.Path(scrapy_cache)
    volumes[path.absolute()] = {'bind': '/app/.scrapy', 'mode': 'rw'}


  docker_client.images.pull(
    repository=IMAGE,
    tag=IMAGE_TAG
  )

  acquired_data = docker_client.containers.run(
    image=f"{IMAGE}:{IMAGE_TAG}",
    command=f"""
      scrapy crawl {asset.name} \
        -a parents=parents/{parent_asset.file_name()} \
        -s SEASON={asset.season} \
    """,
    volumes=volumes
  )

  acquired_data_decoded = acquired_data.decode("utf-8")
  # [json.loads(line) for line in acquired_data_decoded.splitlines()] # this will raise an exception if acquired data is not in the form of json lines
  return acquired_data_decoded

if ASSET_NAME == 'all':
  assets = Asset.all(SEASON)
else:
  assets = [
    Asset(
      name=ASSET_NAME,
      season=SEASON
    )
  ]

if SCRAPY_CACHE is not None:
  print(f"Scrapy cache in {SCRAPY_CACHE} will be used")

for asset in assets:
  print(f"--> Acquiring {asset.name}")
  if DRY_RUN != "1":
    acquired_data = acquire_asset(
      asset,
      SCRAPY_CACHE
    )

    with open(asset.file_full_path(), mode='w+') as asset_file:
      asset_file.write(acquired_data)
