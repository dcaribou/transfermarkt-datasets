"""
Acquire raw data from transfermark website using transfermark-scraper.

usage: transfermarkt-scraper.py [-h] --asset ASSET --seasons SEASONS

optional arguments:
  -h, --help       show this help message and exit
  --asset ASSET    Name of the asset to be acquired
  --seasons SEASONS Season to be acquired
"""

import gzip
import json
import os
import pathlib
import argparse
import subprocess
import tempfile
from typing import List, Optional

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
  'games': ['home_club_position', 'away_club_position'],
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


# --- Partial acquisition helpers ---

def _competition_id_from_record(record):
  """Extract competition ID from a competition record's href."""
  return record.get('href', '').rstrip('/').split('/')[-1]

def _club_id_from_record(record):
  """Extract numeric club ID from a club record's href."""
  return record.get('href', '').rstrip('/').split('/')[-1]

def _player_id_from_record(record):
  """Extract numeric player ID from a player record's href."""
  return record.get('href', '').rstrip('/').split('/')[-1]

def _read_jsonl(filepath):
  """Read JSONL file (gzipped or plain) and return list of parsed records."""
  path = pathlib.Path(filepath)
  if path.suffix == '.gz':
    with gzip.open(str(path), 'rt') as f:
      return [json.loads(line) for line in f if line.strip()]
  else:
    with open(str(path)) as f:
      return [json.loads(line) for line in f if line.strip()]

def _write_jsonl(records, filepath):
  """Write records as JSONL to a file (gzipped if .gz suffix)."""
  path = pathlib.Path(filepath)
  if path.suffix == '.gz':
    with gzip.open(str(path), 'wt') as f:
      for record in records:
        f.write(json.dumps(record) + '\n')
  else:
    with open(str(path), 'w') as f:
      for record in records:
        f.write(json.dumps(record) + '\n')

def filter_parent_file(source_file, filter_ids, id_extractor):
  """Filter a JSONL parent file to only include records matching filter_ids.
  Returns path to a temp file containing the filtered records.
  Caller is responsible for cleaning up the temp file.
  """
  records = _read_jsonl(source_file)
  filtered = [r for r in records if id_extractor(r) in filter_ids]
  logging.info(f"Filtered {source_file}: {len(filtered)}/{len(records)} records match filter")
  if not filtered:
    raise RuntimeError(f"Filter matched 0 records in {source_file}")

  suffix = '.json.gz' if str(source_file).endswith('.gz') else '.json'
  tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
  tmp.close()
  _write_jsonl(filtered, tmp.name)
  return tmp.name

# --- Filter level definitions ---
# Maps filter name -> (id_extractor, assets to run with --asset all, parent asset name)

FILTER_ASSETS = {
  'competitions': ['clubs', 'players', 'appearances', 'games', 'game_lineups'],
  'clubs': ['players', 'appearances'],
  'players': ['appearances'],
}

FILTER_ID_EXTRACTORS = {
  'competitions': _competition_id_from_record,
  'clubs': _club_id_from_record,
  'players': _player_id_from_record,
}

# Which parent asset each filter level targets
FILTER_PARENT_ASSET = {
  'competitions': 'competitions',
  'clubs': 'clubs',
  'players': 'players',
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

def _get_confederations():
  """Discover confederations from tfmkt. Returns list of JSON lines."""
  logging.info("Running: tfmkt confederations")
  conf_result = subprocess.run(["tfmkt", "confederations"], capture_output=True, text=True)
  if conf_result.returncode != 0:
    logging.warning(f"tfmkt confederations exited with {conf_result.returncode}")
  conf_lines = [l for l in conf_result.stdout.splitlines() if l.startswith('{')]
  if not conf_lines:
    raise RuntimeError("tfmkt confederations produced no output")
  logging.info(f"Found {len(conf_lines)} confederations")
  return conf_lines


def _competition_id_from_href(href):
  """Extract competition ID from a transfermarkt href path."""
  parts = href.rstrip('/').split('/')
  return parts[-1] if parts else None


def acquire_competitions():
  """Acquire the competitions asset (non-seasonal, plain JSONL, no gzip).
  Requires confederation parents discovered from tfmkt confederations.
  Only keeps competitions whose ID is listed in config.yml competition_ids.
  """
  import json

  output_file = pathlib.Path("data/competitions.json")

  config = read_config()
  allowed_ids = set(config["defintions"]["competition_ids"])
  logging.info(f"Filtering to {len(allowed_ids)} competition IDs from config.yml")

  conf_lines = _get_confederations()

  with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
    tmp.write('\n'.join(conf_lines))
    tmp_path = tmp.name

  try:
    cmd = ["tfmkt", "competitions", "-p", tmp_path]
    logging.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
      logging.warning(f"tfmkt competitions exited with code {result.returncode}")
      if result.stderr:
        logging.warning(f"stderr (tail): ...{result.stderr[-500:]}")

    lines = [l for l in result.stdout.splitlines() if l.startswith('{')]
    if not lines and result.returncode != 0:
      raise RuntimeError("tfmkt competitions failed with no output")

    # Filter to only competitions listed in config.yml
    filtered = []
    for line in lines:
      record = json.loads(line)
      comp_id = _competition_id_from_href(record.get('href', ''))
      if comp_id in allowed_ids:
        filtered.append(line)

    logging.info(f"Scraped {len(lines)} competitions, kept {len(filtered)} matching existing config")

    with open(str(output_file), 'w') as f:
      f.write('\n'.join(filtered))
    logging.info(f"Wrote {len(filtered)} competitions to {output_file}")
  finally:
    os.unlink(tmp_path)


def acquire_countries():
  """Acquire the countries asset (non-seasonal, plain JSONL, no gzip).
  Requires confederation parents discovered from tfmkt confederations.
  """
  output_file = pathlib.Path("data/countries.json")

  conf_lines = _get_confederations()

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


def acquire_asset(asset, season, parent_override=None):
  """Acquire a single asset for a given season, merging with existing data.

  Args:
    asset: The Asset to acquire.
    season: The season year.
    parent_override: Optional path to a filtered parent file (for partial runs).

  Returns:
    Path to the raw scrape output (before merge), for use as downstream parent
    in filtered chains. Caller is responsible for cleanup. Returns None if
    no scrape output was produced.
  """
  season_dir = pathlib.Path(f"data/raw/transfermarkt-scraper/{season}")
  season_dir.mkdir(parents=True, exist_ok=True)

  parent_file = parent_override or (asset.parent.file_full_path(season) if asset.parent else None)
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
  except Exception:
    if os.path.exists(tmp_path):
      os.unlink(tmp_path)
    raise

  if returncode != 0:
    logging.warning(f"Completed with partial failures")

  return tmp_path

def acquire_tournament_games():
  """Acquire games for each tournament edition listed in data/tournament_editions.json."""
  import json

  editions_file = pathlib.Path("data/tournament_editions.json")
  with open(str(editions_file)) as f:
    editions = [json.loads(line) for line in f if line.strip()]

  for edition in editions:
    season = edition['season']
    season_dir = pathlib.Path(f"data/raw/transfermarkt-scraper/{season}")
    season_dir.mkdir(parents=True, exist_ok=True)

    output_file = pathlib.Path(f"data/raw/transfermarkt-scraper/{season}/games.json.gz")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_edition:
      tmp_edition.write(json.dumps(edition))
      tmp_edition_path = tmp_edition.name

    with tempfile.NamedTemporaryFile(suffix='.jsonl.gz', delete=False) as tmp_out:
      tmp_out_path = tmp_out.name

    try:
      logging.info(f"Acquiring tournament games for season {season} (year {edition['year']})")
      new_count, returncode = run_tfmkt('games', tmp_out_path, season=season, parents_file=tmp_edition_path)
      logging.info(f"Scraped {new_count} new records for tournament games season {season}")

      if new_count == 0 and returncode != 0:
        raise RuntimeError(f"tfmkt failed with no output for tournament games season {season}")

      merged_count = merge_output(output_file, tmp_out_path, 'games')
      logging.info(f"Merged result: {merged_count} total records in {output_file}")
    finally:
      if os.path.exists(tmp_edition_path):
        os.unlink(tmp_edition_path)
      if os.path.exists(tmp_out_path):
        os.unlink(tmp_out_path)


def _resolve_filter(competitions, clubs, players):
  """Parse filter arguments. Returns (filter_level, filter_ids) or (None, None)."""
  active = [(name, val) for name, val in
            [('competitions', competitions), ('clubs', clubs), ('players', players)]
            if val is not None]
  if len(active) > 1:
    raise ValueError("Only one filter (--competitions, --clubs, or --players) can be used at a time")
  if not active:
    return None, None
  name, val = active[0]
  return name, set(val.split(','))


def _acquire_filtered_chain(asset_names, season, filter_level, filter_ids):
  """Run a chain of assets for one season, using filtered parents.

  The first asset in the chain gets a filtered parent file. Each subsequent
  asset uses the raw scrape output of the previous asset as its parent,
  ensuring the filter scope propagates through the chain.
  """
  # Build the ordered chain respecting dependencies
  # Two independent sub-chains from competitions: clubs→players→appearances and games→game_lineups
  chains = [
    ['clubs', 'players', 'appearances'],
    ['games', 'game_lineups'],
  ]

  # For club-level filter, only the clubs→players→appearances chain applies
  if filter_level == 'clubs':
    chains = [['players', 'appearances']]
  elif filter_level == 'players':
    chains = [['appearances']]

  # Track temp files for cleanup
  temp_files = []

  try:
    for chain in chains:
      # Filter the relevant assets from this chain
      chain_assets = [name for name in chain if name in asset_names]
      if not chain_assets:
        continue

      # Create filtered parent for the first asset in the chain
      first_asset = Asset(chain_assets[0])
      first_asset.set_parent()
      parent_asset_name = first_asset.parent.name if first_asset.parent else None

      # Determine the parent file to filter
      if parent_asset_name:
        parent_path = first_asset.parent.file_full_path(season)
      else:
        continue

      id_extractor = FILTER_ID_EXTRACTORS[filter_level]

      # The filter level always matches the first asset's parent in our chain definitions:
      # - competitions filter → chains start with clubs (parent=competitions) or games (parent=competitions)
      # - clubs filter → chain starts with players (parent=clubs)
      # - players filter → chain starts with appearances (parent=players)
      filtered_parent = filter_parent_file(parent_path, filter_ids, id_extractor)
      temp_files.append(filtered_parent)

      # Run each asset in the chain, passing scrape output as next parent
      current_parent = filtered_parent
      for asset_name in chain_assets:
        asset_obj = Asset(asset_name)
        asset_obj.set_parent()
        scrape_output = acquire_asset(asset_obj, season, parent_override=current_parent)
        if scrape_output:
          temp_files.append(scrape_output)
          current_parent = scrape_output
  finally:
    for tmp in temp_files:
      if os.path.exists(tmp):
        os.unlink(tmp)


def acquire_on_local(asset, seasons, competitions=None, clubs=None, players=None):

  filter_level, filter_ids = _resolve_filter(competitions, clubs, players)

  def assets_list(asset: str) -> List[Asset]:
    if asset == 'all':
      if filter_level:
        # When filtering, only run the assets relevant to the filter level
        return [Asset(name) for name in FILTER_ASSETS[filter_level]]
      assets = Asset.all()
    else:
      asset_obj = Asset(name=asset)
      asset_obj.set_parent()
      assets = [asset_obj]

    return assets

  # competitions is non-seasonal: acquire once regardless of seasons arg
  if asset == 'competitions':
    acquire_competitions()
    return

  # countries is non-seasonal: acquire once regardless of seasons arg
  if asset == 'countries':
    acquire_countries()
    return

  # tournament_games is non-seasonal: driven by data/tournament_editions.json
  if asset == 'tournament_games':
    acquire_tournament_games()
    return

  expanded_seasons = seasons_list(seasons)
  expanded_assets = assets_list(asset)

  if filter_level:
    asset_names = {a.name for a in expanded_assets}
    logging.info(f"Partial acquisition: {filter_level}={filter_ids}, assets={asset_names}")
    for season in expanded_seasons:
      if asset == 'all':
        _acquire_filtered_chain(asset_names, season, filter_level, filter_ids)
      else:
        # Single asset with filter: filter its parent directly
        asset_obj = expanded_assets[0]
        parent_asset_name = Asset.asset_parents.get(asset_obj.name)
        if parent_asset_name and parent_asset_name == FILTER_PARENT_ASSET.get(filter_level):
          parent_path = asset_obj.parent.file_full_path(season)
          id_extractor = FILTER_ID_EXTRACTORS[filter_level]
          filtered_parent = filter_parent_file(parent_path, filter_ids, id_extractor)
          try:
            scrape_output = acquire_asset(asset_obj, season, parent_override=filtered_parent)
            if scrape_output and os.path.exists(scrape_output):
              os.unlink(scrape_output)
          finally:
            if os.path.exists(filtered_parent):
              os.unlink(filtered_parent)
        else:
          logging.warning(f"Filter --{filter_level} does not directly apply to asset '{asset_obj.name}' "
                          f"(parent is '{parent_asset_name}'). Running unfiltered.")
          scrape_output = acquire_asset(asset_obj, season)
          if scrape_output and os.path.exists(scrape_output):
            os.unlink(scrape_output)
  else:
    for season in expanded_seasons:
      for asset_obj in expanded_assets:
        scrape_output = acquire_asset(asset_obj, season)
        if scrape_output and os.path.exists(scrape_output):
          os.unlink(scrape_output)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument(
    '--asset',
    help="Name of the asset to be acquired",
    choices=['competitions', 'clubs', 'players', 'games', 'game_lineups', 'appearances', 'countries', 'national_teams', 'national_team_players', 'tournament_games', 'all'],
    required=True
  )
  parser.add_argument(
    '--seasons',
    help="Season to be acquired. This is passed to the scraper as the SEASON argument",
    default="2024",
    type=str
  )
  parser.add_argument(
    '--competitions',
    help="Comma-separated competition IDs to filter (e.g., GB1,ES1). Only scrapes data for these competitions.",
    default=None
  )
  parser.add_argument(
    '--clubs',
    help="Comma-separated club IDs to filter (e.g., 131,583). Only scrapes data for these clubs.",
    default=None
  )
  parser.add_argument(
    '--players',
    help="Comma-separated player IDs to filter (e.g., 28003,1122196). Only scrapes data for these players.",
    default=None
  )

  arguments = parser.parse_args()
  acquire_on_local(**vars(arguments))
