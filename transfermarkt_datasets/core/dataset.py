import pathlib
from typing import Dict
from dagster import DependencyDefinition, GraphDefinition, JobDefinition
from frictionless.package import Package
from frictionless import validate
import json
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

      self.config = config or read_config(config_file)

      self.prep_folder_path = 'transfermarkt_datasets/stage'
      self.datapackage_descriptor_path = f"{self.prep_folder_path}/dataset-metadata.json"
      self.assets = {}
      self.datapackage = None
      self.validation_report = None

      if self.config.get("logging"):
        logging.config.dictConfig(self.config["logging"])
      else:
        logging.basicConfig()

      self.log = logging.getLogger("main")

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

  def get_asset_def(self, asset_name):
    class_name = inflection.camelize(asset_name) + "Asset"
    module = importlib.import_module(f"{self.assets_module}.{asset_name}")
    class_ = getattr(module, class_name)
    return class_

  def get_dependencies(self):
    dependencies = {}
    for asset in self.assets.values():
      dependencies[asset.name] = asset.as_dagster_deps()

    return dependencies

  def build_assets(self):
    """Run transfromation (a.k.a. "build") all assets in the dataset.
    Built assets are stored as dataframes in the underlying assets[<asset>] objects.

    Args:
        asset (str, optional): Limit the build to one asset in particular.

    Raises:
        InvalidStagingLocationException: The passed staging location is not a valid path.
    """
    from transfermarkt_datasets.dagster.jobs import build_job
    self.log.info("Start processing assets")

    result = build_job.execute_in_process()

    nodes = result._node_def.ensure_graph_def().node_dict.keys()
    for node in nodes:
      if not node.startswith("build_"):
        continue

      asset_name = node.replace("build_", "")
      self.assets[asset_name].load_from_stage()

  def generate_datapackage(self, basepath=None) -> None:
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
      package.add_resource(asset.as_frictionless_resource())

    self.datapackage = package
    package.to_json(self.datapackage_descriptor_path)

  def validate_datapackage(self) -> bool:
    """Run "datapackage" validations for this dataset and save results to a local file.

    Returns:
        bool: Whether the validations did or did not pass.
    """
    package = self.datapackage or Package(self.datapackage_descriptor_path)

    self.log.info("Datapackage resource validation")
    for resource in package.resources:
      self.log.info(f"Validating {resource.name}")

      if self.assets.get(resource.name):
        asset_name = resource.name
      else:
        asset_name = "base_" + resource.name

      asset = self.assets[asset_name]
      validation_report = validate(resource, limit_memory=20000, checks=asset.checks)
      self.assets[asset_name].validation_report = validation_report
      with open(f"transfermarkt_datasets/datapackage_resource_{resource.name}_validation.json", 'w+') as file:
        file.write(
          json.dumps(validation_report, indent=4, sort_keys=True)
        )

    return self.is_valid()

  def is_valid(self) -> bool:
    """Check validation report and determine if the validation passed or not.

    Returns:
        bool: Whether the validations did or did not pass.
    """
    for asset in self.assets.values():
      if not asset.is_valid():
        self.log.error(f"{asset.name} did not pass validations!")
        return False

    self.log.info("All validations have passed!")
    return True

  def as_dagster_job(self, resource_defs={}) -> JobDefinition:
    build_ops = [asset.as_build_dagster_op() for asset in self.assets.values()]
    validate_ops = [
      asset.as_validate_dagster_op()
      for asset in self.assets.values()
      if asset.as_frictionless_resource() is not None
    ]
    
    deps = {}
    for asset in self.assets.values():
      deps[asset.dagster_build_task_name] = asset.as_dagster_deps()
      if asset.as_frictionless_resource():
        deps[asset.dagster_validate_task_name] = {
          "asset": DependencyDefinition(asset.dagster_build_task_name)
        }

    graph = GraphDefinition(name="build_transfermark_datasets",
      node_defs=build_ops + validate_ops,
      dependencies=deps
    )

    job = graph.to_job(resource_defs=resource_defs)

    return job
