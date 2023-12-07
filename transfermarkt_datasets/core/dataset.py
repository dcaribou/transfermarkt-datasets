import json
import pathlib
from typing import Dict, List

from frictionless.package import Package

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

      self.prep_folder_path = "data/prep"
      self.assets = {}

      if self.config.get("logging"):
        logging.config.dictConfig(self.config["logging"])
      else:
        logging.basicConfig()

      self.log = logging.getLogger("main")

      for file in pathlib.Path(os.path.join(self.assets_root, self.assets_relative_path)).glob("**/*.py"):
        filename = file.name
        class_ = self.get_asset_def(filename.split(".")[0])
        asset = class_()
        self.assets[asset.name] = asset

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

  def as_frictionless_package(self, basepath=None, exclude_private=False) -> None:
    """Create an save to local a file descriptor tha defines a "datapackage" for this dataset.

    Args:
        basepath (str, optional): Base path of prepared files. It defaults to the "prep" folder path.
    """
    base_path = basepath or self.prep_folder_path
    package = Package(basepath=base_path)

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
  
  def write_datapackage(self):
    pkg = self.as_frictionless_package()
    pkg_as_json = json.loads(pkg.to_json())

    # recursively sort a json object by key
    def sort_dict_by_key(d):
      return {k: sort_dict_by_key(v) if isinstance(v, dict) else v for k, v in sorted(d.items())}
    
    # write the sorted json to a file
    with open("data/prep/dataset-metadata.json", "w") as f:
      json.dump(sort_dict_by_key(pkg_as_json), f, indent=2)
