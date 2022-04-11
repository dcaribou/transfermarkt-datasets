from typing import List
from frictionless.package import Package
import json

import importlib
import yaml

from prep.assets.base import BaseProcessor

import logging

import pathlib

def read_config():
  with open("prep/config.yml") as config_file:
    config = yaml.load(config_file, yaml.Loader)
    return config

class AssetRunner:
  def __init__(self, data_folder_path='data/raw', season=None) -> None:

      self.data_folder_path = f"{data_folder_path}"
      self.prep_folder_path = 'prep/stage'
      self.datapackage_descriptor_path = f"{self.prep_folder_path}/dataset-metadata.json"
      self.assets = []
      self.datapackage = None
      self.validation_report = None

      config = read_config()
      settings = config["settings"]

      logging.config.dictConfig(settings["logging"])
      self.log = logging.getLogger("main")

      if season is None:
        seasons = settings["seasons"]
      else:
        seasons = [season]
    
      for asset in config["assets"]:
          asset_name = asset["name"]
          class_name = asset["class"]
          source_name = asset.get("source")
          try:
            module = importlib.import_module(f'prep.assets.{asset_name}')
            class_ = getattr(module, class_name)
            instance = class_(
              self.data_folder_path,
              seasons,
              asset_name,
              self.prep_folder_path + '/' + asset_name + '.csv',
              source_name,
              settings
            )
            self.assets.append(
              {'name': asset_name, 'processor': instance, 'seasons': seasons}
            )
          except ModuleNotFoundError:
            logging.warning(f"Found raw asset '{asset_name}' without asset processor")

  def prettify_asset_processors(self):
    from tabulate import tabulate # https://github.com/astanin/python-tabulate
    table = [
      [elem['name'], elem['processor'].raw_files_path, str(elem['seasons'])] 
      for elem in self.assets
    ]
    return tabulate(table, headers=['Name', 'Path', 'Seasons'])

  def process_assets(self):
    self.log.info("Start processing assets\n%s", self.prettify_asset_processors())

    # setup stage location
    stage_path = pathlib.Path(self.prep_folder_path)
    if stage_path.exists():
      if not stage_path.is_dir():
        self.log.error(f"Configured 'stage' location {stage_path.name} is not a directory")
        raise Exception("Invalid staging location")
    else:
      stage_path.mkdir()

    for asset in self.assets:
      self.process_asset(asset['name'], asset['processor'])

  def process_asset(self, asset_name: str, asset_processor: BaseProcessor):
    asset_processor.process()
    asset_processor.export()

  def get_asset_processor(self, name: str):
    for asset_runner in self.assets:
      if asset_runner['name'] == name:
        return asset_runner['processor']
    raise Exception(f"Asset {name} not found")

  def get_asset_df(self, name: str):
    return self.get_asset_processor(name).prep_df

  def generate_datapackage(self, basepath=None):
    """
    Generate datapackage.json for Kaggle Dataset
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

    with open('prep/datapackage_description.md') as datapackage_description_file:
      package.description = datapackage_description_file.read()
    
    for asset in self.assets:
      package.add_resource(asset['processor'].get_resource(base_path))

    self.datapackage = package
    package.to_json(self.datapackage_descriptor_path)

  def validate_resources(self):
    from frictionless import validate

    package = self.datapackage or Package(self.datapackage_descriptor_path)

    self.log.info("Datapackage resource validation")
    for resource in package.resources:
      self.log.info(f"Validating {resource.name}")
      validation_report = validate(resource, limit_memory=20000)
      self.get_asset_processor(resource.name).validation_report = validation_report
      with open(f"prep/datapackage_resource_{resource.name}_validation.json", 'w+') as file:
        file.write(
          json.dumps(validation_report, indent=4, sort_keys=True)
        )

    return self.is_valid()

  def is_valid(self):
    for asset in self.assets:
      if not asset['processor'].is_valid():
        self.log.error(f"{asset['name']} did not pass validations!")
        return False

    self.log.info("All validations have passed!")
    return True
