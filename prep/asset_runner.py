from frictionless.package import Package
from frictionless.validate import validate_package
import json

import importlib

from prep.assets.base import BaseProcessor

import logging
logging.basicConfig(format='%(message)s', level=logging.INFO)

import pathlib

def get_seasons(data_folder_path):
  path = pathlib.Path(data_folder_path)
  seasons = [
    int(str(season_path).split('/')[-1])
    for season_path in path.glob('*') if season_path.is_dir()
  ]
  
  seasons.sort()
  return seasons

def get_assets(data_folder_path):
  path = pathlib.Path(data_folder_path)
  asset_keys = {}
  for asset_path in path.glob('**/*.json'):
    asset_name = (str(asset_path).split('/')[-1]).split('.')[0]
    asset_keys[asset_name] = 'found'

  return list(asset_keys.keys()) + ['competitions']
class AssetRunner:
  def __init__(self, data_folder_path='data/raw') -> None:
      self.data_folder_path = f"{data_folder_path}"
      self.prep_folder_path = 'prep/stage'

      seasons = get_seasons(self.data_folder_path)
      assets = get_assets(self.data_folder_path)

      self.assets = []
      for asset in assets:
        class_name = asset.capitalize()
        try:
          module = importlib.import_module(f'prep.assets.{asset}')
          class_ = getattr(module, class_name + 'Processor')
          instance = class_(
            self.data_folder_path,
            seasons,
            asset,
            self.prep_folder_path + '/' + asset + '.csv'
          )
          self.assets.append(
            {'name': asset, 'processor': instance, 'seasons': seasons}
          )
        except ModuleNotFoundError:
          logging.warning(f"Found raw asset '{asset}' without asset processor")

      self.datapackage = None
      self.validation_report = None

  def prettify_asset_processors(self):
    from tabulate import tabulate # https://github.com/astanin/python-tabulate
    table = [
      [elem['name'], elem['processor'].raw_files_path, str(elem['seasons'])] 
      for elem in self.assets
    ]
    return tabulate(table, headers=['Name', 'Path', 'Seasons'])

  def process_assets(self):
    logging.info(
      f"--- Processing {len(self.assets)} assets ---"
    )
    logging.info(
      self.prettify_asset_processors()
    )
    logging.info("")

    # setup stage location
    stage_path = pathlib.Path(self.prep_folder_path)
    if stage_path.exists():
      if not stage_path.is_dir():
        logging.error(f"Configured 'stage' location {stage_path.name} is not a directory")
        raise Exception("Invalid staging location")
    else:
      stage_path.mkdir()

    for asset in self.assets:
      self.process_asset(asset['name'], asset['processor'])

  def process_asset(self, asset_name: str, asset_processor: BaseProcessor):
    logging.info(f"-> Processing {asset_name}")
    asset_processor.process()
    logging.info(
      asset_processor.output_summary()
    )
    logging.info("")
    asset_processor.export()

  def get_asset_df(self, name: str):
    for asset_runner in self.assets:
      if asset_runner['name'] == name:
        return asset_runner['processor'].prep_df

    raise Exception(f"Asset {name} not found")

  def generate_datapackage(self, basepath=None):
    """
    Generate datapackage.json for Kaggle Dataset
    """
    from prep.checks import checks as custom_checks

    custom_checks = custom_checks
    builtin_checks = []

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

    for asset in self.assets:
      builtin_checks += asset['processor'].get_validations()

    checks = custom_checks + builtin_checks

    self.validation_report = validate_package(package, trusted=True, checks=checks)

    logging.info("--> Datapackage validation report")
    logging.info(self.summarize_validation_report())

    with open("prep/datapackage_validation.json", 'w+') as file:
      file.write(
        json.dumps(self.validation_report, indent=4, sort_keys=True)
      )

    self.datapackage = package
    package.to_json(self.prep_folder_path + '/dataset-metadata.json')

  def summarize_validation_report(self):
    errors = self.validation_report['stats']['errors']
    if errors == 0:
      return "All validations have passed!"
    else:
      return f">={errors} rows did not pass validations!"
