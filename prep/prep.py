import json
import pandas as pd
from asset_runner import AssetRunner
import sys
import argparse

def check_validation_results():
  """Check the validation run report and decide if it passes or if it fails
  Returns True if it passes and False if it fails
  """
  with open('datapackage_validation.json') as report_file:
    report = json.load(report_file)

  tasks = report['tasks']
  assert len(tasks) == 5

  for task in tasks:

    errors = task['errors']

    # as a first approximation, allow up to 300 errors on the appearances file
    # this is to account for a common foreign key exception caused by the source data
    if task['resource']['name'] == 'appearances':
      errors_threshold = 300
    # for the rest of the files do nor allow errors at all
    else:
      errors_threshold = 0

    if len(errors) > errors_threshold:
      print(f">={len(errors)} rows did not pass validations!")
      return False
    else:
      return True

parser = argparse.ArgumentParser()
parser.add_argument('--raw-files-location', required=False, default='../data/raw')
parser.add_argument('--datapackage-metadata', action='store_const', const=True, required=False, default=False)
parser.add_argument('--ignore-checks', help='Ignore validation checks report', action='store_const', const=True, required=False, default=False)

args = parser.parse_args()

raw_files_location = args.raw_files_location # ../data/raw
metadata_only = args.datapackage_metadata
ignore_checks = args.ignore_checks

runner = AssetRunner(raw_files_location)

if metadata_only:
  # generate frictionless data package for prepared assets
  runner.generate_datapackage('../data/prep')

else:
  # generate prepared data assets in 'stage' folder
  runner.process_assets()
  runner.generate_datapackage()

if not ignore_checks:
  passed = check_validation_results()
  if not passed:
    raise Exception("Validations failed")
  else:
    print("All good \N{winking face}")
