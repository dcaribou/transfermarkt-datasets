import pandas as pd
from asset_runner import AssetRunner
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--raw-files-location', required=False, default='../data/raw')
parser.add_argument('--datapackage-metadata', action='store_const', const=True, required=False, default=False)

args = parser.parse_args()

raw_files_location = args.raw_files_location # ../data/raw
metadata_only = args.datapackage_metadata

runner = AssetRunner(raw_files_location)

if metadata_only:
  # generate frictionless data package for prepared assets
  runner.generate_datapackage('../data/prep')

else:
  # generate prepared data assets in 'stage' folder
  runner.process_assets()
  runner.generate_datapackage()

# if all validations passed, move assets to data/prep
if runner.validation_report['stats']['errors'] == 0:
  sys.exit(0)
else:
  sys.exit(1)
