from typing import Dict, List
from frictionless.package import Package
from frictionless import validate
import json

import importlib
import yaml

from transfermarkt_datasets.assets.asset import Asset

import logging

import pathlib

def read_config(config_file="config.yml") -> Dict:
  with open(config_file) as config_file:
    config = yaml.load(config_file, yaml.Loader)
    return config

class AssetNotFound(Exception):
  """Exception to be raised when attempting to load an asset that is not defined.
  """
  def __init__(self, asset_name, message=None) -> None:
      self.message = asset_name
      self.asset_name = asset_name
      super().__init__(self.message)

class InvalidStagingLocation(Exception):
  pass

class TransfermarktDatasets:
  def __init__(
    self,
    source_path=None,
    seasons=None,
    assets=None,
    config=None,
    config_file="config.yml"
    ) -> None:

      config = config or read_config(config_file)
      settings = config["settings"]

      self.data_folder_path = source_path or settings["source_path"]
      self.prep_folder_path = 'transfermarkt_datasets/stage'
      self.datapackage_descriptor_path = f"{self.prep_folder_path}/dataset-metadata.json"
      self.assets = {}
      self.datapackage = None
      self.validation_report = None

      settings = config["settings"]

      if settings.get("logging"):
        logging.config.dictConfig(settings["logging"])
      else:
        logging.basicConfig()

      self.log = logging.getLogger("main")

      if seasons is None:
        seasons = settings["seasons"]
    
      for asset_name, asset in config["assets"].items():
          if assets and asset_name not in assets:
            continue
          class_name = asset["class"]
          source_name = asset.get("source")
          try:
            module = importlib.import_module(f'transfermarkt_datasets.assets.{asset_name}')
            class_ = getattr(module, class_name)
            instance = class_(
              name=asset_name,
              seasons=seasons,
              source_path=self.data_folder_path,
              target_path=self.prep_folder_path + '/' + asset_name + '.csv',
              source_files_name=asset.get("source_files_name"),
              settings=settings
            )
            instance.load()
            self.assets[asset_name] = instance

          except ModuleNotFoundError:
            logging.error(f"Found raw asset '{asset_name}' without asset processor")
            raise AssetNotFound(asset_name)

  def prettify_asset_processors(self):
    """Create a printable table with a summary of the assets in the dataset.

    Returns:
        str: A string containing the table.
    """
    from tabulate import tabulate # https://github.com/astanin/python-tabulate
    table = [
      [asset_name, asset.raw_files_path, str(asset.seasons)] 
      for asset_name, asset in self.assets.items()
    ]
    return tabulate(table, headers=['Name', 'Path', 'Seasons'])

  def build_assets(self, asset: str = None):
    """Run transfromation (a.k.a. "build") all assets in the dataset.
    Built assets are stored as dataframes in the underlying assets[<asset>] objects.

    Args:
        asset (str, optional): Limit the build to one asset in particular.

    Raises:
        InvalidStagingLocationException: The passed staging location is not a valid path.
    """
    self.log.info("Start processing assets\n%s", self.prettify_asset_processors())

    # setup stage location
    stage_path = pathlib.Path(self.prep_folder_path)
    if stage_path.exists():
      if not stage_path.is_dir():
        self.log.error(f"Configured 'stage' location {stage_path.name} is not a directory")
        raise Exception("Invalid staging location")
    else:
      stage_path.mkdir()

    if asset:
      self.assets[asset].build()
      self.assets[asset].export()
    else:
      for asset_name, asset in self.assets.items():
        asset.build()
        asset.export()

  @property
  def asset_names(self):
    """Return the names of the asset in the dataset.

    Returns:
        list(str): The list of asset names.
    """
    return self.assets.keys()

  def generate_datapackage(self, basepath=None):
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
      package.add_resource(asset.get_resource(base_path))

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
      asset = self.assets[resource.name]
      validation_report = validate(resource, limit_memory=20000, checks=asset.checks)
      self.assets[resource.name].validation_report = validation_report
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
