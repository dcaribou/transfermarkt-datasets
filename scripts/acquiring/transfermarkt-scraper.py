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
import tempfile
from typing import List

import duckdb

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
      'game_lineups': 'games',
      'countries': None,
      'national_teams': 'countries',
      'national_team_players': 'national_teams',
    }

  # Some assets reuse another asset's crawler and output file.
  # Maps asset name -> (crawler_name, output_asset_name)
  asset_aliases = {
      'national_team_players': ('players', 'players'),
  }

  def __init__(self, name) -> None:

    self.name = name
    self.parent = None

  def set_parent(self):
    """Get the parent of this asset as a new Asset"""

    self.parent = Asset(self.asset_parents[self.name])

  def crawler_name(self):
    """The tfmkt CLI crawler to invoke (may differ from asset name for aliases)."""
    return self.asset_aliases.get(self.name, (self.name,))[0]

  def output_name(self):
    """The output file asset name (may differ for aliases that share a file)."""
    return self.asset_aliases.get(self.name, (None, self.name))[1]

  def file_path(self, season):
    if self.name in ('competitions', 'countries'):
      return pathlib.Path(f"data/{self.name}.json")
    else:
      return pathlib.Path(f"data/raw/transfermarkt-scraper/{season}/{self.output_name()}.json.gz")

  def file_full_path(self, season):
    return str(self.file_path(season).absolute())

  @classmethod
  def all(cls):
    """Get an ordered list of assets to be acquired.
    Asset acquisition have dependecies between each other. This list returns the right order for asset
    acquisition steps to run.
    """
    assets = [Asset(name) for name in cls.asset_parents if name not in ('competitions', 'countries')]
    for asset in assets:
      asset.set_parent()
    return assets

# Scalar fields that DuckDB infers as JSON when null but are actually VARCHAR.
# Explicit casts prevent type-mismatch errors in UNION ALL BY NAME when
# an existing file has all-null values and a new scrape has actual strings.
VARCHAR_CASTS = {
  'clubs': ['coach_name', 'total_market_value', 'league_position'],
  'players': ['day_of_last_contract_extension', 'current_market_value', 'highest_market_value'],
  'national_team_players': ['day_of_last_contract_extension', 'current_market_value', 'highest_market_value'],
}

# Struct/complex fields whose internal schema may change between scrapes (e.g. because
# a nested field like parent.coach_name changed from JSON to VARCHAR in a child asset).
# Casting to JSON normalises the type and avoids UNION ALL BY NAME conflicts.
# Safe to do because base models only ever access these via json_extract_string().
JSON_CASTS = {
  'players': ['parent'],
  'games': ['parent'],
  'game_lineups': ['parent'],
  'appearances': ['parent'],
  'national_team_players': ['parent'],
}

KEY_EXPRS = {
  'clubs': 'href',
  'players': 'href',
  'games': 'href',
  'game_lineups': 'href',
  'appearances': "(parent.href || '|' || competition_code || '|' || matchday || '|' || date)",
  'national_teams': 'href',
  'national_team_players': 'href',
}


def run_tfmkt(crawler, output_file, season=None, parents_file=None):
  """Run tfmkt, piping stdout directly to a gzipped file.
  Returns (record_count, returncode). Partial output is preserved
  even on non-zero exit, since tfmkt flushes records as they're scraped.
  """
  cmd = ["tfmkt", crawler]
  if season is not None:
    cmd.extend(["-s", str(season)])
  if parents_file is not None:
    cmd.extend(["-p", str(parents_file)])

  # Pipe: tfmkt ... | grep JSON lines only | gzip > output_file
  # Use pipefail so we get tfmkt's exit code, not gzip's.
  # grep for lines starting with '{' to filter out any non-JSON stdout noise from tfmkt.
  shell_cmd = f"set -o pipefail; {' '.join(cmd)} | grep '^{{' | gzip > '{output_file}'"
  logging.info(f"Running: {' '.join(cmd)} | gzip > '{output_file}'")
  result = subprocess.run(shell_cmd, shell=True, executable='/bin/bash', capture_output=False, stderr=subprocess.PIPE, text=True)

  if result.returncode != 0:
    logging.warning(f"tfmkt exited with code {result.returncode}")
    if result.stderr:
      logging.warning(f"stderr (tail): ...{result.stderr[-500:]}")

  # Count records from the written file
  record_count = 0
  if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
    try:
      with gzip.open(output_file, 'rt') as f:
        record_count = sum(1 for _ in f)
    except gzip.BadGzipFile:
      pass

  return record_count, result.returncode


def merge_output(existing_file, new_file, asset_name):
  """Merge new gzipped JSONL file into existing gzipped JSONL file using DuckDB.
  New records take precedence over existing ones (fresher data).
  Returns the total number of records after merge.
  """
  key_expr = KEY_EXPRS[asset_name]
  varchar_fields = VARCHAR_CASTS.get(asset_name, [])
  json_fields = JSON_CASTS.get(asset_name, [])

  conn = duckdb.connect()
  try:
    def read_expr(filepath):
      """SELECT expression for one file, with explicit type casts for known fields."""
      cols = {row[0] for row in conn.sql(
        f"DESCRIBE SELECT * FROM read_json_auto('{filepath}', sample_size=10)"
      ).fetchall()}
      casts = (
        [f"{c}::VARCHAR AS {c}" for c in varchar_fields if c in cols] +
        [f"to_json({c}) AS {c}" for c in json_fields if c in cols]
      )
      replace = f" REPLACE ({', '.join(casts)})" if casts else ""
      return f"SELECT *{replace} FROM read_json_auto('{filepath}', sample_size=-1, union_by_name=true)"

    existing_has_data = existing_file.exists() and conn.sql(
      f"SELECT count(*) FROM read_json_auto('{str(existing_file)}', sample_size=1)"
    ).fetchone()[0] > 0

    if existing_has_data:
      merge_query = f"""
        WITH all_records AS (
          SELECT *, {key_expr} AS _key, 1 AS _priority FROM ({read_expr(str(new_file))})
          UNION ALL BY NAME
          SELECT *, {key_expr} AS _key, 0 AS _priority FROM ({read_expr(str(existing_file))})
        ),
        deduped AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY _key ORDER BY _priority DESC) AS _rn
          FROM all_records
        )
        SELECT * EXCLUDE (_key, _priority, _rn) FROM deduped WHERE _rn = 1
      """
    else:
      merge_query = read_expr(str(new_file))

    conn.sql(f"CREATE TEMP TABLE merged AS {merge_query}")
    record_count = conn.sql("SELECT count(*) FROM merged").fetchone()[0]
    conn.sql(f"COPY merged TO '{str(existing_file)}' (FORMAT JSON, ARRAY false, COMPRESSION gzip)")
    return record_count
  finally:
    conn.close()

def acquire_countries():
  """Acquire the countries asset (non-seasonal, plain JSONL, no gzip).
  Requires confederation parents discovered from tfmkt confederations.
  """
  output_file = pathlib.Path("data/countries.json")

  # Step 1: get confederations as parents for the countries crawler
  logging.info("Running: tfmkt confederations")
  conf_result = subprocess.run(["tfmkt", "confederations"], capture_output=True, text=True)
  if conf_result.returncode != 0:
    logging.warning(f"tfmkt confederations exited with {conf_result.returncode}")
  conf_lines = [l for l in conf_result.stdout.splitlines() if l.startswith('{')]
  if not conf_lines:
    raise RuntimeError("tfmkt confederations produced no output")
  logging.info(f"Found {len(conf_lines)} confederations")

  # Step 2: write confederations to a temp file and run countries
  with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
    tmp.write('\n'.join(conf_lines))
    tmp_path = tmp.name

  try:
    cmd = ["tfmkt", "countries", "-p", tmp_path]
    logging.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
      logging.warning(f"tfmkt countries exited with code {result.returncode}")
      if result.stderr:
        logging.warning(f"stderr (tail): ...{result.stderr[-500:]}")

    lines = [l for l in result.stdout.splitlines() if l.startswith('{')]
    if not lines and result.returncode != 0:
      raise RuntimeError("tfmkt countries failed with no output")

    with open(str(output_file), 'w') as f:
      f.write('\n'.join(lines))
    logging.info(f"Wrote {len(lines)} countries to {output_file}")
  finally:
    os.unlink(tmp_path)


def acquire_asset(asset, season):
  """Acquire a single asset for a given season, merging with existing data."""
  season_dir = pathlib.Path(f"data/raw/transfermarkt-scraper/{season}")
  season_dir.mkdir(parents=True, exist_ok=True)

  parent_file = asset.parent.file_full_path(season) if asset.parent else None
  output_file = asset.file_path(season)

  # Scrape into a temp file, then merge into the output file
  with tempfile.NamedTemporaryFile(suffix='.jsonl.gz', delete=False) as tmp:
    tmp_path = tmp.name

  try:
    logging.info(f"Acquiring {asset.name} for season {season}")
    new_count, returncode = run_tfmkt(asset.crawler_name(), tmp_path, season=season, parents_file=parent_file)
    logging.info(f"Scraped {new_count} new records for {asset.name}")

    if new_count == 0 and returncode != 0:
      raise RuntimeError(f"tfmkt failed with no output for {asset.name}")

    merged_count = merge_output(output_file, tmp_path, asset.name)
    logging.info(f"Merged result: {merged_count} total records")
  finally:
    if os.path.exists(tmp_path):
      os.unlink(tmp_path)

  if returncode != 0:
    logging.warning(f"Completed with partial failures")

def acquire_on_local(asset, seasons):

  def assets_list(asset: str) -> List[Asset]:
    if asset == 'all':
      assets = Asset.all()
    else:
      asset_obj = Asset(name=asset)
      asset_obj.set_parent()
      assets = [asset_obj]

    return assets

  # countries is non-seasonal: acquire once regardless of seasons arg
  if asset == 'countries':
    acquire_countries()
    return

  expanded_seasons = seasons_list(seasons)
  expanded_assets = assets_list(asset)

  for season in expanded_seasons:
    for asset_obj in expanded_assets:
      acquire_asset(asset_obj, season)

parser = argparse.ArgumentParser()

parser.add_argument(
  '--asset',
  help="Name of the asset to be acquired",
  choices=['clubs', 'players', 'games', 'game_lineups', 'appearances', 'countries', 'national_teams', 'national_team_players', 'all'],
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
