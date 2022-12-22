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
import pathlib

import argparse
from typing import List

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from transfermarkt_datasets.core.utils import submit_batch_job_and_wait

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
  
  def __init__(self, name) -> None:

    self.name = name
    self.parent = None

  def set_parent(self):
    """Get the parent of this asset as a new Asset"""

    self.parent = Asset(self.asset_parents[self.name])
  
  def file_path(self, season):
    if self.name == 'competitions':
      return pathlib.Path(f"../data/competitions.json")
    else:
      return pathlib.Path(f"../data/raw/{season}/{self.name}.json")
  
  def file_full_path(self, season):
    return str(self.file_path(season).absolute())

  @classmethod
  def all(self):
    """Get an ordered list of assets to be acquired.
    Asset acquisition have dependecies between each other. This list returns the right order for asset
    acquisition steps to run.
    """
    assets = [Asset(name) for name in self.asset_parents if name != 'competitions']
    for asset in assets:
      asset.set_parent()
    return assets


def acquire_on_local(asset, seasons, func):

  def seasons_list(seasons: str) -> List[str]:
    """Generate a list of seasons to acquire based on the "seasons" string. For example,
    for "2012-2014", it should return [2012, 2013, 2014].

    Args:
        seasons (str): A string representing a date or range of dates to acquire.

    Returns:
        List[str]: The expanded list of seasons to acquire.
    """
    parts = seasons.split("-")
    
    if len(parts) == 0:
      raise Exception("Empty string provided for seasons")

    elif len(parts) == 1: # single season string
      return [int(seasons)]

    elif len(parts) == 2: # range of seasons
      start, end = parts
      season_range = list(range(int(start), int(end) + 1))

      if len(season_range) > 20:
        raise Exception("The range is too high")
      else:
        return season_range

    else:
      raise Exception(f"Invalid string: {seasons}")

  def assets_list(assets: str) -> List[Asset]:
    """Generate the ordered list of Assets to be scraped based on the provided string.

    Args:
        assets (str): A string representing the assets to be scraped.

    Returns:
        List[Asset]: The ordered list of assets to be scraped.
    """

    if asset == 'all':
      assets = Asset.all()
    else:
      asset_obj = Asset(name=asset)
      asset_obj.set_parent()
      assets = [asset_obj]
    
    return assets

  def issue_crawlers_and_wait(assets, seasons, settings):
    """Create and submit scrapy crawlers to the reactor, and block until they've completed.

    Args:
        assets (List[Asset]): List of assets to be scraped.
        seasons (List[int]): List of season to be scraped.
        settings (dict): Crawler setting.
    """

    # https://docs.scrapy.org/en/latest/topics/practices.html#running-multiple-spiders-in-the-same-process

    runner = CrawlerRunner(settings)

    # https://twistedmatrix.com/documents/13.2.0/api/twisted.internet.defer.inlineCallbacks.html
    @defer.inlineCallbacks
    def crawl():
      for season in seasons:
        # if there's no path created yet for this season create one
        season_path = pathlib.Path(f"../data/raw/{season}")
        if not season_path.exists():
          season_path.mkdir()

        for asset_obj in assets:
          # TODO: ideally, let transfermark-scraper handle destination file truncation via a setting instead of doing it here
          # checkout https://foroayuda.es/scrapy-sobrescribe-los-archivos-json-en-lugar-de-agregar-el-archivo/
          file_path = asset_obj.file_path(season)
          if file_path.exists():
            os.remove(str(file_path))
          print(f"Schedule {asset_obj.name} for season {season}")
          yield runner.crawl(
            asset_obj.name,
            parents=asset_obj.parent.file_full_path(season),
            season=season
          )

      reactor.stop()
    
    crawl()
    reactor.run()
  
  # identify this scraping jobs accordinly by setting a nice user agent
  USER_AGENT = 'transfermarkt-datasets/1.0 (https://github.com/dcaribou/transfermarkt-datasets)'

  # get seasons and assets list
  expanded_seasons = seasons_list(seasons)
  expanded_assets = assets_list(asset)

  os.chdir("transfermarkt-scraper")

  # define crawler settings
  settings = get_project_settings()
  
  settings.set("USER_AGENT", USER_AGENT)  
  settings.set("FEED_URI", None)
  settings.set("FEED_URI_PARAMS", "tfmkt.utils.uri_params")
  settings.set("FEEDS",{
    "../data/raw/%(season)s/%(name)s.json": {
      "format": "jsonlines"
    }
  })
  settings.set("LOG_LEVEL", "INFO")

  # create crawlers and wait until they complete
  issue_crawlers_and_wait(expanded_assets, expanded_seasons, settings)

  os.chdir("..")

def acquire_on_cloud(job_name, job_queue, job_definition, branch, message, args, func):

  submit_batch_job_and_wait(
    job_name=job_name,
    job_queue=job_queue,
    job_definition=job_definition,
    cmd=[
      branch,
      "make",
      "dvc_pull",
      "acquire_local",
      "stash_and_commit",
      args
    ],
    vcpus=1.0,
    memory=3072,
    timeout=5
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
  '--seasons',
  help="Season to be acquired. This is passed to the scraper as the SEASON argument",
  default="2020",
  type=str
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
  '--message',
  default="🤖 updated raw dataset files"
)
cloud_parser.add_argument(
  "args"
)
cloud_parser.set_defaults(func=acquire_on_cloud)

arguments = parser.parse_args()
arguments.func(**vars(arguments))
