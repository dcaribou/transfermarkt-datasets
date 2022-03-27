"""
Bulid prepared datasets in data/prep using raw data in data/raw.

usage: 2_prepare.py [-h] {local,cloud} ...

positional arguments:
  {local,cloud}
    local        Run the acquiring step locally
    cloud        Run the acquiring step in the cloud

optional arguments:
  -h, --help     show this help message and exit

"""
import os
from prep.asset_runner import AssetRunner
import argparse

from cloud_lib import submit_batch_job_and_wait

def fail_if_invalid(asset_runner):
  if not asset_runner.validate_resources():
    raise Exception("Validations failed")
  else:
    print("All good \N{winking face}")


def prepare_on_local(raw_files_location, refresh_metadata, run_validations, season, func):

  runner = AssetRunner(raw_files_location, season)

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
    os.system("cp prep/stage/* data/prep")


def prepare_on_cloud(
  job_name, job_queue,
  job_definition, branch,
  func):

  submit_batch_job_and_wait(
    job_name=job_name,
    job_queue=job_queue,
    job_definition=job_definition,
    branch=branch,
    script="2_prepare.py",
    args=[
      "--raw-files-location", "data/raw",
    ],
    vcpus=2,
    memory=9216
  )

# main

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers()

local_parser = subparsers.add_parser('local', help='Run the acquiring step locally')
local_parser.add_argument('--raw-files-location', required=False, default='data/raw')
local_parser.add_argument('--refresh-metadata', action='store_const', const=True, required=False, default=False)
local_parser.add_argument('--run-validations', action='store_const', const=True, required=False, default=False)
local_parser.add_argument('--season', required=False)
local_parser.set_defaults(func=prepare_on_local)

cloud_parser = subparsers.add_parser('cloud', help='Run the acquiring step in the cloud')
cloud_parser.add_argument(
  '--job-name',
  default="on-cli"
)
cloud_parser.add_argument(
  '--job-queue',
  default="transfermarkt-datasets-batch-compute-job-queue"
)
cloud_parser.add_argument(
  '--job-definition',
  default="transfermarkt-datasets-batch-job-definition-dev"
)
cloud_parser.add_argument(
  '--branch',
  required=True
)
cloud_parser.set_defaults(func=prepare_on_cloud)

arguments = parser.parse_args()
arguments.func(**vars(arguments))


