"""Acquire raw data from transfermark website using a dockerized version of transfermark-scraper.

Usage:
 > python 1_acquire.py --asset [clubs, player, games, etc] [--scrapy-cache .scrapy] --season <the season to be acquired>

The environment variable SCRAPY_CACHE can be used as well to tell the script to run do the acquiring on
a local scrapy cache. The command line argument '--scrapy-cache' takes precedence.
"""
import sys
import os
import pathlib

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
parser.add_argument(
  '--season',
  help="Season to be acquired. This is passed to the scraper as the SEASON argument",
  default=2020
)
parser.add_argument(
  '--cat',
  default=False,
  action='store_const',
  help="Pipe acquired data to the stdout instead of saving to a file",
  const=True
)

arguments = parser.parse_args()

SEASON = arguments.season
IMAGE = 'dcaribou/transfermarkt-scraper'
IMAGE_TAG = 'main'
ASSET_NAME = arguments.asset
SCRAPY_CACHE = arguments.scrapy_cache
DRY_RUN = os.environ.get('DRY_RUN')
# identify this scraping jobs accordinly by setting a nice user agent
USER_AGENT = 'transfermarkt-datasets/1.0 (https://github.com/dcaribou/transfermarkt-datasets)'
CAT = arguments.cat

class Asset():
  """A wrapper for the asset to be acquired.
  It contains some useful methods for manipulating the assets, such as path and parents rendering.
  """
  asset_parents = {
      'competitions' : None,
      'games': 'competitions',
      'clubs': 'competitions',
      'players': 'clubs',
      'appearances': 'players'
    }
  
  def __init__(self, name, season) -> None:
    import pathlib

    self.name = name
    self.season = season
    if name == 'competitions':
      self.path = pathlib.Path(f"data/competitions.json")
    else:
      self.path = pathlib.Path(f"data/raw/{season}/{name}.json")

  def parent(self):
    """Get the parent of this asset as a new Asset"""

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
    """Get an ordered list of assets to be acquired.
    Asset acquisition have dependecies between each other. This list returns the right order for asset
    acquisition steps to run.
    """
    return [Asset(name, season) for name in self.asset_parents if name != 'competitions']


def acquire_asset(asset, scrapy_cache):
  """Orchestrate asset acquisition steps on a Docker server and collect the results as strings"""
  
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

  command = f"""
    scrapy crawl {asset.name} \
      -a parents=parents/{parent_asset.file_name()} \
      -s SEASON={asset.season} \
      -s USER_AGENT='{USER_AGENT}'"""

  acquired_data = docker_client.containers.run(
    image=f"{IMAGE}:{IMAGE_TAG}",
    command=command,
    volumes=volumes,
    tty=True
  )

  acquired_data_decoded = acquired_data.decode("utf-8")
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

season_path = pathlib.Path(f"data/raw/{SEASON}")
if not season_path.exists():
  season_path.mkdir()

for asset in assets:
  print(f"--> Acquiring {asset.name}")
  acquired_data = acquire_asset(
    asset,
    SCRAPY_CACHE
  )

  if CAT:
    print(acquired_data)
  else:
    with open(asset.file_full_path(), mode='w+') as asset_file:
      asset_file.write(acquired_data)
