import pandas as pd
from asset_runner import AssetRunner
import sys

data_location = '../data'

runner = AssetRunner(data_location)

# generate prepared data assets in 'stage' folder
runner.process_assets()

# generate frictionless data package for prepared assets
runner.generate_datapackage()

# if all validations passed, move assets to data/prep
if runner.validation_report['stats']['errors'] == 0:
  runner.replace_prep_files()
  sys.exit(0)
else:
  sys.exit(1)
