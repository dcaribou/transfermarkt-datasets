import pandas as pd
from asset_runner import AssetRunner
import sys

raw_files_location = sys.argv[1] # ../data/raw

runner = AssetRunner(raw_files_location)

# generate prepared data assets in 'stage' folder
runner.process_assets()

# generate frictionless data package for prepared assets
runner.generate_datapackage()

# if all validations passed, move assets to data/prep
if runner.validation_report['stats']['errors'] == 0:
  sys.exit(0)
else:
  sys.exit(1)
