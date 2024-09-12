"""
Acquire raw data from transfermark website using transfermark-scraper.

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
import sys
from typing import List

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner

from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings

from transfermarkt_datasets.core.utils import (
  read_config,
  submit_batch_job_and_wait,
  seasons_list
)

import logging

acquire_config = read_config()["acquire"]

logging.config.dictConfig(
  acquire_config["logging"]
)

def scrapy_config() -> Settings:
  """Instantiate scrappy settings for the acquiring run.

  Returns:
      Settings: The default scrapy settings instance + overrides in config.yml
  """
  # https://github.com/scrapy/scrapy/blob/master/scrapy/utils/project.py#L61
  default_settings = get_project_settings()
  overrides = acquire_config["scrapy_config"]

  default_settings.setdict(overrides)
    
  return default_settings

class Asset():
  """A wrapper for the asset to be acquired.
  It contains some useful methods for manipulating the assets, such as path and parents rendering.
  """
  asset_parents = {
      'competitions' : None,
      'games': 'competitions',
      'clubs': 'competitions',
      'players': 'clubs',
      'appearances': 'players',
      'game_lineups': 'games'
    }
  
  def __init__(self, name) -> None:

    self.name = name
    self.parent = None

  def set_parent(self):
    """Get the parent of this asset as a new Asset"""

    self.parent = Asset(self.asset_parents[self.name])
  
  def file_path(self, season):
    if self.name == 'competitions':
      return pathlib.Path(f"data/competitions.json")
    else:
      return pathlib.Path(f"data/raw/transfermarkt-scraper/{season}/{self.name}.json.gz")
  
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

def acquire_on_local(asset, seasons):

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
        season_path = pathlib.Path(f"data/raw/transfermarkt-scraper/{season}")
        if not season_path.exists():
          season_path.mkdir(parents=True)

        for asset_obj in assets:
          # TODO: ideally, let transfermark-scraper handle destination file truncation via a setting instead of doing it here
          # checkout https://foroayuda.es/scrapy-sobrescribe-los-archivos-json-en-lugar-de-agregar-el-archivo/
          file_path = asset_obj.file_path(season)
          if file_path.exists():
            os.remove(str(file_path))
          logging.info(
            f"Schedule {asset_obj.name} for season {season}"
          )
          yield runner.crawl(
            asset_obj.name,
            parents=asset_obj.parent.file_full_path(season),
            season=season
          )

      reactor.stop()
    
    crawl()
    reactor.run()
  
  # get seasons and assets list
  expanded_seasons = seasons_list(seasons)
  expanded_assets = assets_list(asset)

  # define crawler settings
  settings = scrapy_config()
  
  # create crawlers and wait until they complete
  issue_crawlers_and_wait(expanded_assets, expanded_seasons, settings)

parser = argparse.ArgumentParser()

parser.add_argument(
  '--asset',
  help="Name of the asset to be acquired",
  choices=['clubs', 'players', 'games', 'game_lineups', 'appearances', 'all'],
  required=True
)
parser.add_argument(
  '--seasons',
  help="Season to be acquired. This is passed to the scraper as the SEASON argument",
  default="2024",
  type=str
)

arguments = parser.parse_args()
acquire_on_local(**vars(arguments))
