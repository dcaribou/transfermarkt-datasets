from typing import Dict, List
from dagster import DependencyDefinition, InputDefinition, OpDefinition, Output, OutputDefinition
from frictionless import Detector
from frictionless.resource import Resource
import pandas as pd
import logging
import logging.config

from frictionless.package import Package
from frictionless import validate

import json
import inspect

class FailedAssetValidation(Exception):
  pass

class Asset:
  description = None
  name = "generic"
  file_name = None

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

      self.stage_location = "transfermarkt_datasets/stage"
      self.datapackage_descriptor_path = f"{self.stage_location}/dataset-metadata.json"

      if not self.file_name:
        file_name = self.name.replace("base_", "")
        self.file_name = file_name + ".csv"

  def __str__(self) -> str:
      return f'Asset(name={self.name})'
  
  @property
  def min_season(self) -> int:
    return min(self.seasons)

  @property
  def max_season(self) -> int:
    return max(self.seasons)

  @property
  def file_name(self) -> str:
    return self.name + ".csv"

  @property
  def stage_path(self) -> str:
    return f"{self.stage_location}/{self.file_name}"

  @property
  def dagster_task_name(self) -> str:
    return f"build_{self.name}"

  def build(self) -> None:
    pass

  def as_dagster_deps(self) -> Dict[str, DependencyDefinition]:

    s = inspect.signature(self.build)
    deps = {}

    for param in  s.parameters.values():
      deps[param.name] = DependencyDefinition(param.annotation().dagster_task_name)

    return deps

  def drop_duplicates(self):
    """Drop duplicates from prep_df
    """

    self.log.info("Stared processing asset %s", self.name)

    if self.schema.primary_key:
      self.prep_df = self.prep_df.drop_duplicates(subset=self.schema.primary_key, keep='last')
    else:
      self.prep_df = self.prep_df.drop_duplicates(keep='last')

    self.log.info("Finished processing asset %s\n%s", self.name, self.output_summary())

  def load_from_stage(self):
    self.prep_df = pd.read_csv(
      filepath_or_buffer=self.stage_path
    )

  def save_to_stage(self):
    self.prep_df.to_csv(
      self.stage_path,
      index=False
    )

  def validate(self):
    package = Package(self.datapackage_descriptor_path)

    self.log.info("Datapackage resource validation")
    for resource in package.resources:
      if resource.name == self.name:
        validation_report = validate(resource, limit_memory=20000, checks=self.checks)
        self.validation_report = validation_report
        with open(f"transfermarkt_datasets/datapackage_resource_{resource.name}_validation.json", 'w+') as file:
          file.write(
            json.dumps(validation_report, indent=4, sort_keys=True)
          )

    return self.is_valid()
  
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

  def as_frictionless_resource(self, basepath) -> Resource:
    detector = Detector(schema_sync=True)
    resource = Resource(
      title=self.name,
      path=self.file_name,
      trusted=True,
      detector=detector,
      description=self.description,
      basepath=basepath
    )
    resource.schema = self.schema
    return resource

  def as_dagster_op(self) -> OpDefinition:

    def build_base_fn(context, inputs):
      self.build(**inputs)
      yield Output(self)

    deps = self.as_dagster_deps()

    input_defs = []
    for dep, dep_def in deps.items():
      input_defs.append(InputDefinition(
          name=dep,
          dagster_type=Asset,
          root_manager_key="asset_io_manager"
        )
      )

    op = OpDefinition(
        name=f"build_{self.name}",
        input_defs=input_defs,
        output_defs=[OutputDefinition(dagster_type=Asset, io_manager_key="asset_io_manager")],
        compute_fn=build_base_fn,
        # required_resource_keys={"settings"},

    )

    return op

  def is_valid(self):
    self.log.debug("Checking validation results for %s", self.name)
    results = self.validation_report['stats']['errors']
    self.log.debug("Failed validations are %i, tolerance is %i", results, self.errors_tolerance)
    if results > self.errors_tolerance:
      self.log.error("Invalid asset\n%s", self.validation_report['tasks'][0]['errors'][0])
      raise FailedAssetValidation()


class RawAsset(Asset):

  raw_file_name = None

  def __init__(self, settings: dict = None) -> None:
    super().__init__(settings)

    self.raw_df = None
    self.raw_files_path = "data/raw"

    if not self.raw_file_name:
      file_name = self.name.replace("base_", "")
      self.raw_file_name = file_name + ".json"

  def load_raw_from_stage(self):

    if "competitions" in self.raw_file_name:
        df = pd.read_json(
          f"data/competitions.json",
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
    else:
      season = 2021

      season_file = f"{self.raw_files_path}/{season}/{self.raw_file_name}"

      self.log.debug("Reading raw data from %s", season_file)
      df = pd.read_json(
        season_file,
        lines=True,
        convert_dates=True,
        orient={'index', 'date'}
      )
      df["season"] = season
      df["season_file"] = season_file

    self.raw_df = df
