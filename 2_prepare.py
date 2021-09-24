"""Bulid prepared datasets in data/prep using raw data in data/raw.

Usage:
 > python prep.py [--raw-files-location <path to raw files folder>] [--datapackage-metadata] [--ignore-checks]
"""
from prep.asset_runner import AssetRunner
import argparse

def fail_if_invalid(asset_runner):
  if not asset_runner.validate_resources():
    raise Exception("Validations failed")
  else:
    print("All good \N{winking face}")

parser = argparse.ArgumentParser()
parser.add_argument('--raw-files-location', required=False, default='data/raw')
parser.add_argument('--refresh-metadata', action='store_const', const=True, required=False, default=False)
parser.add_argument('--run-validations', action='store_const', const=True, required=False, default=False)
parser.add_argument('--ignore-checks', help='Ignore validation checks report', action='store_const', const=True, required=False, default=False)

args = parser.parse_args()

raw_files_location = args.raw_files_location # ../data/raw
refresh_metadata = args.refresh_metadata
run_validations = args.run_validations
ignore_checks = args.ignore_checks

runner = AssetRunner(raw_files_location)

if refresh_metadata:
  # generate frictionless data package for prepared assets
  runner.generate_datapackage('data/prep')
elif run_validations:
  # run data package validations
  fail_if_invalid(runner)
else:
  # generate prepared data assets in 'stage' folder
  runner.process_assets()
  runner.generate_datapackage()
  fail_if_invalid(runner)
