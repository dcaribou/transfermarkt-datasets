from typing import Dict, List
from dagster import DependencyDefinition, InputDefinition, OpDefinition, Output, OutputDefinition
from frictionless import Detector
from frictionless.resource import Resource
import pandas as pd
import logging
import logging.config

from frictionless.package import Package
from frictionless import validate
from frictionless.schema import Schema

import json
import inspect

import public

from transfermarkt_datasets.core.utils import read_config

class FailedAssetValidation(Exception):
  pass

class Asset:

  description = None
  name = "generic"
  file_name = None
  public = True

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
      self.prep_location = "data/prep"

      self.datapackage_descriptor_path = f"{self.stage_location}/dataset-metadata.json"

      if not self.file_name:
        file_name = self.name.replace("base_", "")
        self.file_name = file_name + ".csv"

      self.schema = Schema()

  def __str__(self) -> str:
      return f'Asset(name={self.name})'

  @property
  def file_name(self) -> str:
    return self.name + ".csv"

  @property
  def stage_path(self) -> str:
    return f"{self.stage_location}/{self.file_name}"
  
  @property
  def prep_path(self) -> str:
    return f"{self.prep_location}/{self.file_name}"

  @property
  def dagster_build_task_name(self) -> str:
    return f"build_{self.name}"

  @property
  def dagster_validate_task_name(self) -> str:
    return f"validate_{self.name}"

  @property
  def frictionless_resource_name(self) -> str:
    return self.file_name.replace(".csv", "")

  def build(self) -> None:
    pass

  def as_dagster_deps(self) -> Dict[str, DependencyDefinition]:

    s = inspect.signature(self.build)
    deps = {}

    for param in  s.parameters.values():
      deps[param.name] = DependencyDefinition(param.annotation().dagster_build_task_name)

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

  def load_from_prep(self):
    """Load prepared dataset from the local to a pandas dataframe.
    """
    self.prep_df = pd.read_csv(
      filepath_or_buffer=self.prep_path
    )

  def load_from_stage(self):
    self.prep_df = pd.read_csv(
      filepath_or_buffer=self.stage_path
    )

  def save_to_stage(self):
    self.prep_df.to_csv(
      self.stage_path,
      index=False
    )
  
  def url_unquote(self, url_series):
    from urllib.parse import unquote
    return url_series.apply(unquote)

  def url_prepend(self, partial_url_series):
    return 'https://www.transfermarkt.co.uk' + partial_url_series

  def output_summary(self):
    from tabulate import tabulate # https://github.com/astanin/python-tabulate
    summary = self.prep_df.describe()
    summary.insert(0, 'metric', summary.index)
    table = summary.values.tolist()
    return tabulate(table, headers=summary.columns, floatfmt=".2f")

  def schema_as_dataframe(self) -> pd.DataFrame:
    """Render the asset schema as a pandas dataframe.

    Returns:
        pd.DataFrame: A pandas dataframe representing the asset schema.
    """

    fields = [field.name for field in  self.schema["fields"]]
    types = [field.type for field in  self.schema["fields"]]
    descriptions = [field.description for field in  self.schema["fields"]]

    df = pd.DataFrame(
      data=dict(
        description=descriptions,
        type=types
      ),
      index=fields
    )
    
    return df

  def as_frictionless_resource(self) -> Resource:
    detector = Detector(schema_sync=True)
    resource = Resource(
      title=self.frictionless_resource_name,
      path=self.file_name,
      trusted=True,
      detector=detector,
      description=self.description,
      basepath="transfermarkt_datasets/stage"
    )
    resource.schema = self.schema
    return resource

  def as_build_dagster_op(self) -> OpDefinition:

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
        name=self.dagster_build_task_name,
        input_defs=input_defs,
        output_defs=[OutputDefinition(dagster_type=Asset, io_manager_key="asset_io_manager")],
        compute_fn=build_base_fn
    )

    return op

  def as_validate_dagster_op(self) -> OpDefinition:

    if not self.as_frictionless_resource():
      return None
    
    def validate_fn(context, inputs):
      self.validate()

    input_defs = [
      InputDefinition(name="asset", dagster_type=Asset)
    ]
    op = OpDefinition(
        name=f"validate_{self.name}",
        input_defs=input_defs,
        output_defs=[],
        compute_fn=validate_fn
    )

    return op


  def validate(self, asset=None):

    resource = self.as_frictionless_resource()
    validation_report = validate(resource, limit_memory=30000, checks=self.checks)
    with open(f"transfermarkt_datasets/datapackage_resource_{resource.name}_validation.json", 'w+') as file:
      file.write(
        json.dumps(validation_report, indent=4, sort_keys=True)
      )
    self.validation_report = validation_report

    return self.is_valid()

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

  def load_raw(self):

    raw_dfs = []

    if "competitions" in self.raw_file_name:
        df = pd.read_json(
          f"data/competitions.json",
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        raw_dfs.append(df)
    else:
      seasons = read_config()["seasons"]
      for season in seasons:

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
        if len(df) > 0:
          raw_dfs.append(df)

    self.raw_df = pd.concat(raw_dfs, axis=0)
