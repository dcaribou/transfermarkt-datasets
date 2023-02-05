import pathlib
from typing import Dict, List

from dagster import DependencyDefinition, GraphDefinition, JobDefinition, OpDefinition

from frictionless.package import Package
from frictionless import validate

import importlib
import inflection
import os
import logging.config

from transfermarkt_datasets.core.utils import read_config

class AssetNotFound(Exception):
  """Exception to be raised when attempting to load an asset that is not defined.
  """
  def __init__(self, asset_name, message=None) -> None:
      self.message = asset_name
      self.asset_name = asset_name
      super().__init__(self.message)

class InvalidStagingLocation(Exception):
  pass

class Dataset:
  def __init__(
    self,
    config=None,
    config_file="config.yml",
    assets_root=".",
    assets_relative_path="transfermarkt_datasets/assets",

    ) -> None:

      self.assets_root = assets_root
      self.assets_relative_path = assets_relative_path

      self.config = config or read_config(config_file)["prepare"]

      self.prep_folder_path = 'transfermarkt_datasets/stage'
      self.assets = {}
      self.validation_report = None

      logging.config.dictConfig(
        self.config["logging"]
      )

      self.log = logging.getLogger()

      self.run_result = None

  @property
  def assets_module(self):
    return self.assets_relative_path.replace("/", ".")

  @property
  def asset_names(self):
    """Return the names of the asset in the dataset.

    Returns:
        list(str): The list of asset names.
    """
    return list(self.assets.keys())

  def discover_assets(self):
    for file in pathlib.Path(os.path.join(self.assets_root, self.assets_relative_path)).glob("**/*.py"):
      filename = file.name
      class_ = self.get_asset_def(filename.split(".")[0])
      asset = class_()
      self.assets[asset.name] = asset

  def load_assets(self):
    """Load all assets in the dataset from local.
    """
    for asset_name, asset in self.assets.items():
      if asset.public:
        asset.load_from_prep()

  def get_asset_def(self, asset_name):
    class_name = inflection.camelize(asset_name) + "Asset"
    module = importlib.import_module(f"{self.assets_module}.{asset_name}")
    class_ = getattr(module, class_name)
    return class_

  def get_dependencies(self) -> Dict:
    """Get assets build dependencies.
    Dependencies are defined by asset's "build" method parameters.

    Returns:
        Dict: A dictionary that represents the build dependencies for each asset.
    """
    dependencies = {}
    for asset in self.assets.values():
      dependencies[asset.name] = asset.as_dagster_deps()

    return dependencies
  
  def get_relationships(self) -> List[Dict]:
    """Get assets relationships.
    Relationships are defined by the assets set of foreign keys.

    Returns:
        List[Dict]: A list of relationships (source -> target)
    """
    relationships = []

    for asset_name, asset in self.assets.items():
      if asset.schema.foreign_keys:
        for foreign_key in asset.schema.foreign_keys:
          reference = foreign_key["reference"]
          relationship = {
            "from": asset_name,
            "to": reference["resource"],
            "on": {
              "source": foreign_key["fields"],
              "target": reference["fields"]
            }
          }
          relationships.append(
            relationship
          )

    return relationships

  def build_assets(self, asset=None):
    """Run transfromation (a.k.a. "build") all assets in the dataset.
    Built assets are stored as dataframes in the underlying assets[<asset>] objects.

    Args:
        asset (str, optional): Limit the build to one asset in particular.

    Raises:
        InvalidStagingLocationException: The passed staging location is not a valid path.
    """
    from transfermarkt_datasets.dagster.jobs import build_job
    self.log.info("Start processing assets")

    if not asset:
      result = build_job.execute_in_process()
    else:
      op_name = self.assets[asset].dagster_build_task_name
      result = build_job.execute_in_process(
        op_selection=[f"*{op_name}"]
      )

    for asset_name, asset in self.assets.items():
      asset.load_from_stage()

  def as_frictionless_package(self, basepath=None, exclude_private=False) -> None:
    """Create an save to local a file descriptor tha defines a "datapackage" for this dataset.

    Args:
        basepath (str, optional): Base path of prepared files. It defaults to the "prep" folder path.
    """
    base_path = basepath or self.prep_folder_path
    package = Package(trusted=True, basepath=base_path)

    # full spec at https://specs.frictionlessdata.io/data-package/
    package.title = "Football Data from Transfermarkt"
    package.description = "Clean, structured and automatically updated football (soccer) data from Transfermarkt"
    package.keywords = [
      "football", "players", "stats", "statistics", "data",
      "soccer", "games", "matches"
    ]
    package.id = "davidcariboo/player-scores"
    package.licenses = [{
      "CC0": "Public Domain"
    }]

    with open('transfermarkt_datasets/datapackage_description.md') as datapackage_description_file:
      package.description = datapackage_description_file.read()
    
    for asset in self.assets.values():
      if not asset.public and exclude_private:
        continue
      package.add_resource(asset.as_frictionless_resource())
    
    return package

  def is_valid(self) -> bool:
    """Check validation report and determine if the validation passed or not.

    Returns:
        bool: Whether the validations did or did not pass.
    """
    for asset in self.assets.values():
      asset.is_valid()

    self.log.info("All validations have passed!")
    return False

  def get_dagster_ops(self) -> List[OpDefinition]:
    build_ops = [asset.as_build_dagster_op() for asset in self.assets.values()]
    if self.config["validation_enabled"]:
      validate_ops = [
        asset.as_validate_dagster_op()
        for asset in self.assets.values()
        if asset.as_frictionless_resource() is not None
      ]
    else:
      validate_ops = []

    return build_ops + validate_ops

  def get_dagster_deps(self) -> Dict:
    deps = {}
    for asset in self.assets.values():
      deps[asset.dagster_build_task_name] = asset.as_dagster_deps()
      if asset.as_frictionless_resource() and self.config["validation_enabled"]:
        deps[asset.dagster_validate_task_name] = {
          "asset": DependencyDefinition(asset.dagster_build_task_name)
        }
    return deps

  def as_dagster_job(self, resource_defs={}) -> JobDefinition:
    """Render dataset assets build as a dagster  JobDefinition

    Args:
        resource_defs (dict, optional): Optional dagster resources. Defaults to {}.

    Returns:
        JobDefinition: A dagster JobDefinition
    """

    ops = self.get_dagster_ops()
    deps = self.get_dagster_deps()

    graph = GraphDefinition(name="build_transfermark_datasets",
      node_defs=ops,
      dependencies=deps
    )

    job = graph.to_job(resource_defs=resource_defs)

    return job
