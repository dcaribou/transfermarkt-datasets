from frictionless.package import Package
from frictionless.validate import validate_package
import json

import importlib

from assets.base import BaseProcessor

import logging
logging.basicConfig(format='%(message)s', level=logging.INFO)

import pathlib
class AssetRunner:
  def __init__(self, data_folder_path='data/raw') -> None:
      self.data_folder_path = data_folder_path
      self.prep_folder_path = 'stage'

      self.assets = []
      for file in pathlib.Path(data_folder_path).glob('*.json'):
        file_name = file.name.split('.')[0]
        class_name = file_name.capitalize()
        try:
          module = importlib.import_module(f'assets.{file_name}')
          class_ = getattr(module, class_name + 'Processor')
          instance = class_(
            self.data_folder_path + '/' + file_name + '.json',
            self.prep_folder_path + '/' + file_name + '.csv'
          )
          self.assets.append(
            {'name': file_name, 'processor': instance}
          )
        except ModuleNotFoundError:
          logging.warning(f"Found raw asset '{file_name}' without asset processor")

      self.datapackage = None
      self.validation_report = None

  def prettify_asset_processors(self):
      from tabulate import tabulate # https://github.com/astanin/python-tabulate
      table = [[elem['name'], elem['processor'].raw_file_path] for elem in self.assets]
      return tabulate(table, headers=['Name', 'File'])

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
    asset_processor.validate()
    logging.info(
      asset_processor.validation_summary()
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
    from frictionless import describe_package

    # full spec at https://specs.frictionlessdata.io/data-package/

    base_path = basepath or self.prep_folder_path

    package = Package(trusted=True, basepath=base_path)

    package.title = "Football players' statistics from Transfermarkt website"
    package.keywords = [
      "football", "players", "stats", "statistics", "data",
      "soccer", "games", "matches"
    ]
    package.id = "davidcariboo/player-scores"
    package.image = "https://images.unsplash.com/photo-1590669233095-90608d89c79c?ixlib=rb-1.2.1&ixid=MXwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHw%3D&auto=format&fit=crop&w=2980&q=80"
    package.licenses = [{
      "CC0": "Public Domain"
    }]
    package.description = """
### Context
Football (soccer) datasets containing historical match aggregates are widely available. However, structured, publicly available datasets on a **player level** for those games, such as the one contained in [Transfermarkt's player detailed performance page](https://www.transfermarkt.co.uk/diogo-jota/leistungsdatendetails/spieler/340950/saison/2020/verein/0/liga/0/wettbewerb/GB1/pos/0/trainer_id/0/plus/1), are more difficult to find. This dataset aims to present that data in an accessible, standard format.

### Content
The main resource within this dataset is the `appearances.csv` file, which contains **one row per player appearance** (one row per player per game played). Among other contextual data, on each appearance you will find a `date` and a `player_name`, as well as `goals` and `assists` metrics for that appearance. The contents of this file are automatically updated every week with the latest data from Transfermarkt website.<br/>
The `appearances.csv` resource might contain references to other entities, such as `clubs` and `leagues`, which are also included as part of this dataset.<br/>

:information_source: Have a look at the version history (or the [player-scores](https://github.com/dcaribou/player-scores) commit log) for a record of the changes on this dataset.

| Region  | League Code | Country     | Availablilty       | First Season |
|---------|-------------|-------------|--------------------|--------------|
| Europe  | ES1         | Spain       | :white_check_mark: | current      |
| Europe  | GB1         | England     | :white_check_mark: | current      |
| Europe  | L1          | Germany     | :white_check_mark: | current      |
| Europe  | NL1         | Netherlands | :white_check_mark: | current      |
| Europe  | DK1         | Denmark     | :white_check_mark: | current      |
| Europe  | TR1         | Turkey      | :white_check_mark: | current      |
| Europe  | GR1         | Greece      | :white_check_mark: | current      |
| Europe  | IT1         | Italy       | :white_check_mark: | current      |
| Europe  | BE1         | Belgium     | :white_check_mark: | current      |
| Europe  | PO1         | Portugal    | :white_check_mark: | current      |
| Europe  | FR1         | France      | :white_check_mark: | current      |
| Europe  | RU1         | Russia      | :white_check_mark: | current      |
| Europe  | UKR1        | Ukraine     | :white_check_mark: | current      |
| Europe  | SC1         | Scotland    | :white_check_mark: | current      |
| America | All         | All         | :x:                | -            |
| Asia    | All         | All         | :x:                | -            |
| Africa  | All         | All         | :x:                | -            |


### Acknowledgements
* Source data at [Transfermark website](https://www.transfermarkt.co.uk/)
* Data pipeline code for creating and keeping this dataset up to date is maintained at [player-scores](https://github.com/dcaribou/player-scores) github project
* Scraper code is published as an independent project [here](https://github.com/dcaribou/transfermarkt-scraper)

    """
    for asset in self.assets:
      package.add_resource(asset['processor'].get_resource(base_path))

    self.validation_report = validate_package(package, trusted=True)

    def pretty_print_json(dict_variable):
      json_string = json.dumps(dict_variable, indent=4, sort_keys=True)
      return json_string

    logging.info("--> Datapackage validation report")
    logging.info(self.summarize_validation_report())

    with open("datapackage_validation.json", 'w+') as file:
      file.write(
        pretty_print_json(self.validation_report)
      )

    self.datapackage = package
    package.to_json(self.prep_folder_path + '/dataset-metadata.json')

  def summarize_validation_report(self):
    errors = self.validation_report['stats']['errors']
    if errors == 0:
      return "All validations have passed!"
    else:
      return f">={errors} rows did not pass validations!"
