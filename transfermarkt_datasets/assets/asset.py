from typing import List
from frictionless import Detector
from frictionless.resource import Resource
import pandas
import logging
import logging.config

# import tracemalloc

class Asset:
  description = None

  def __init__(
    self,
    name: str,
    seasons: List[str],
    source_path = "data/raw",
    target_path = "stage",
    source_files_name: str = None,
    settings: dict = None) -> None:

      self.name = name

      self.raw_files_path = source_path
      self.seasons = seasons
      self.prep_file_path = target_path

      self.prep_df = None
      self.validations = None
      self.validation_report = None
      self.errors_tolerance = 0

      if not source_files_name:
        self.raw_files_name = name + ".json"
      else:
        self.raw_files_name = source_files_name
      
      self.settings = settings
      self.checks = []

      self.log = logging.getLogger("main")

  def get_stacked_data(self) -> pandas.DataFrame:

    raw_dfs = []

    if self.name == 'competitions':
        df = pandas.read_json(
          f"data/competitions.json",
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        raw_dfs.append(df)
    else:
      for season in self.seasons:

        # snapshot = tracemalloc.take_snapshot()
        # top_stats = snapshot.statistics('lineno')
        # print("[ Top 10 ]")
        # for stat in top_stats[:10]:
        #     print(stat)

        season_file = f"{self.raw_files_path}/{season}/{self.raw_files_name}"

        self.log.debug("Reading raw data from %s", season_file)
        df = pandas.read_json(
          season_file,
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        df["season"] = season
        df["season_file"] = season_file
        if len(df) > 0:
          raw_dfs.append(df)

    return pandas.concat(raw_dfs, axis=0)

  def __str__(self) -> str:
      return f'Asset(name={self.name},season={self.min_season}..{self.max_season})'
  
  @property
  def min_season(self) -> int:
    return min(self.seasons)

  @property
  def max_season(self) -> int:
    return max(self.seasons)

  def build(self):
    pass

  def drop_duplicates(self):

    self.log.info("Stared processing asset %s", self.name)

    if self.schema.primary_key:
      self.prep_df = self.prep_df.drop_duplicates(subset=self.schema.primary_key, keep='last')
    else:
      self.prep_df = self.prep_df.drop_duplicates(keep='last')

    self.log.info("Finished processing asset %s\n%s", self.name, self.output_summary())
  
  def url_unquote(self, url_series):
    from urllib.parse import unquote
    return url_series.apply(unquote)

  def url_prepend(self, partial_url_series):
    return 'https://www.transfermarkt.co.uk' + partial_url_series

  def get_columns(self):
    pass

  def output_summary(self):
    from tabulate import tabulate # https://github.com/astanin/python-tabulate
    summary = self.prep_df.describe()
    summary.insert(0, 'metric', summary.index)
    table = summary.values.tolist()
    return tabulate(table, headers=summary.columns, floatfmt=".2f")

  def export(self):
  
    self.prep_df.to_csv(
      self.prep_file_path,
      index=False
    )

  def get_resource(self, basepath):
    detector = Detector(schema_sync=True)
    resource = Resource(
      title=self.name,
      path=self.prep_file_path.split('/')[-1],
      trusted=True,
      detector=detector,
      description=self.description,
      basepath=basepath
    )
    resource.schema = self.schema
    return resource

  def is_valid(self):
    self.log.debug("Checking validation results for %s", self.name)
    results = self.validation_report['stats']['errors']
    self.log.debug("Failed validations are %i, tolerance is %i", results, self.errors_tolerance)
    if results > self.errors_tolerance:
      self.log.error("Invalid asset\n%s", self.validation_report['tasks'][0]['errors'][0])
      return False
    else:
      return True


