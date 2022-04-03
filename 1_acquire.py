"""
Acquire raw data from transfermark website using a dockerized version of transfermark-scraper.

usage: 1_acquire.py [-h] {local,cloud} ...

positional arguments:
  {local,cloud}
    local        Run the acquiring step locally
    cloud        Run the acquiring step in the cloud

optional arguments:
  -h, --help     show this help message and exit
"""

import os
import sys
import pathlib

import argparse

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from cloud_lib import submit_batch_job_and_wait

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

    self.file_full_path = str(self.path.absolute())
    self.parent = None

  def set_parent(self):
    """Get the parent of this asset as a new Asset"""

    self.parent = Asset(
      self.asset_parents[self.name],
      self.season
    )

  def file_name(self):
    return self.path.name

  @classmethod
  def all(self, season):
    """Get an ordered list of assets to be acquired.
    Asset acquisition have dependecies between each other. This list returns the right order for asset
    acquisition steps to run.
    """
    assets = [Asset(name, season) for name in self.asset_parents if name != 'competitions']
    for asset in assets:
      asset.set_parent()
    return assets


def acquire_on_local(asset, season, func):

  # identify this scraping jobs accordinly by setting a nice user agent
  USER_AGENT = 'transfermarkt-datasets/1.0 (https://github.com/dcaribou/transfermarkt-datasets)'

  if asset == 'all':
    assets = Asset.all(season)
  else:
    asset_obj = Asset(
        name=asset,
        season=season
      )
    asset_obj.set_parent()
    assets = [asset_obj]

  season_path = pathlib.Path(f"data/raw/{season}")
  if not season_path.exists():
    season_path.mkdir()


  os.chdir("transfermarkt-scraper")

  settings = get_project_settings()

  settings.set("USER_AGENT", USER_AGENT)
  settings.set("SEASON", season)
  settings.set("FEED_URI", f"../data/raw/{season}/%(name)s.json" )

  configure_logging(settings)

  process = CrawlerProcess(settings)

  for asset_obj in assets:
    print(f"Schedule {asset_obj.name}")
    process.crawl(asset_obj.name, parents=asset_obj.parent.file_full_path)
  
  process.start()

def acquire_on_cloud(job_name, job_queue, job_definition, branch, args, func):

  submit_batch_job_and_wait(
    job_name=job_name,
    job_queue=job_queue,
    job_definition=job_definition,
    branch=branch,
    script="1_acquire.py",
    args=[
      "--asset", "all",
      "--season", "2021"
    ],
    vcpus=0.5,
    memory=1024
  )

# main

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers()

local_parser = subparsers.add_parser('local', help='Run the acquiring step locally')
local_parser.add_argument(
  '--asset',
  help="Name of the asset to be acquired",
  choices=['clubs', 'players', 'games', 'appearances', 'all'],
  required=True
)
local_parser.add_argument(
  '--season',
  help="Season to be acquired. This is passed to the scraper as the SEASON argument",
  default=2020
)
local_parser.set_defaults(func=acquire_on_local)

cloud_parser = subparsers.add_parser('cloud', help='Run the acquiring step in the cloud')
cloud_parser.add_argument(
  '--job-name',
  default="on-cli"
)
cloud_parser.add_argument(
  '--job-queue',
  default="transfermarkt-datasets-batch-compute-job-queue"
)
cloud_parser.add_argument(
  '--job-definition',
  default="transfermarkt-datasets-batch-job-definition-dev"
)
cloud_parser.add_argument(
  '--branch',
  required=True
)
cloud_parser.add_argument(
  "args",
  default=["--asset", "games", "--season", "2021"],
  nargs="*"
)
cloud_parser.set_defaults(func=acquire_on_cloud)

arguments = parser.parse_args()
arguments.func(**vars(arguments))
