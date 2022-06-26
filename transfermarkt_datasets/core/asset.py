from typing import List
from frictionless import Detector
from frictionless.resource import Resource
import pandas
import logging
import logging.config

from transfermarkt_datasets.dagster.io_managers import PrepIOManager

class Asset:
  description = None
  name = "generic"

  def __init__(
    self,
    settings: dict = None) -> None:

      self.prep_df = None
      self.validations = None
      self.validation_report = None
      self.errors_tolerance = 0
      
      self.settings = settings
      self.checks = []

      self.log = logging.getLogger("main")

  def __str__(self) -> str:
      return f'Asset(name={self.name})'
  
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

  def load_from_stage(self):
    io = PrepIOManager()
    data = io.load_input(context=None, asset_name=self.name)

    self.prep_df = data
  
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


