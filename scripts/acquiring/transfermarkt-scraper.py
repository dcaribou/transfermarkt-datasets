"""
Acquire raw data from transfermark website using transfermark-scraper.

usage: transfermarkt-scraper.py [-h] --asset ASSET --seasons SEASONS

optional arguments:
  -h, --help       show this help message and exit
  --asset ASSET    Name of the asset to be acquired
  --seasons SEASONS Season to be acquired
"""

import gzip
import os
import pathlib
import argparse
import subprocess
from typing import List

from transfermarkt_datasets.core.utils import (
  read_config,
  seasons_list,
  raw_data_path
)

import logging
import logging.config

SOURCE_NAME = "transfermarkt-scraper"

acquire_config = read_config()["acquire"]

logging.config.dictConfig(
  acquire_config["logging"]
)

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
      return raw_data_path(SOURCE_NAME, season, self.name)

  def file_full_path(self, season):
    return str(self.file_path(season).absolute())

  @classmethod
  def all(cls):
    """Get an ordered list of assets to be acquired.
    Asset acquisition have dependecies between each other. This list returns the right order for asset
    acquisition steps to run.
    """
    assets = [Asset(name) for name in cls.asset_parents if name != 'competitions']
    for asset in assets:
      asset.set_parent()
    return assets

def run_tfmkt(crawler, season=None, parents_file=None):
  """Run a tfmkt CLI command and return its stdout output.

  Args:
      crawler (str): The crawler to run (e.g. 'clubs', 'players', 'confederations').
      season (int, optional): The season year.
      parents_file (str, optional): Path to the parents JSONL file.

  Returns:
      str: The stdout output (JSONL).
  """
  cmd = ["tfmkt", crawler]
  if season is not None:
    cmd.extend(["-s", str(season)])
  if parents_file is not None:
    cmd.extend(["-p", str(parents_file)])

  logging.info(f"Running: {' '.join(cmd)}")
  result = subprocess.run(cmd, capture_output=True, text=True)

  if result.returncode != 0:
    logging.error(f"tfmkt failed with return code {result.returncode}")
    if result.stderr:
      logging.error(f"stderr: {result.stderr}")
    raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)

  return result.stdout

def acquire_asset(asset, season):
  """Acquire a single asset for a given season using the tfmkt CLI."""
  season_dir = raw_data_path(SOURCE_NAME, season)
  season_dir.mkdir(parents=True, exist_ok=True)

  parent_file = asset.parent.file_full_path(season) if asset.parent else None
  output_file = asset.file_path(season)

  logging.info(f"Acquiring {asset.name} for season {season}")
  output = run_tfmkt(asset.name, season=season, parents_file=parent_file)

  # Remove existing file if present
  if output_file.exists():
    os.remove(str(output_file))

  # Gzip and write output
  with gzip.open(str(output_file), 'wt') as f:
    f.write(output)
  logging.info(f"Wrote {output_file}")

def acquire_on_local(asset, seasons):

  def assets_list(asset: str) -> List[Asset]:
    if asset == 'all':
      assets = Asset.all()
    else:
      asset_obj = Asset(name=asset)
      asset_obj.set_parent()
      assets = [asset_obj]

    return assets

  expanded_seasons = seasons_list(seasons)
  expanded_assets = assets_list(asset)

  for season in expanded_seasons:
    for asset_obj in expanded_assets:
      acquire_asset(asset_obj, season)

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
