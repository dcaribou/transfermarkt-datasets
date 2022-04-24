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
from transfermarkt_datasets.transfermarkt_datasets import TransfermarktDatasets, read_config
import argparse

from cloud_lib import submit_batch_job_and_wait

def fail_if_invalid(td):
  if not td.valdate_datapackage():
    raise Exception("Validations failed")
  else:
    print("All good \N{winking face}")


def prepare_on_local(raw_files_location, refresh_metadata, run_validations, seasons, func):

  td = TransfermarktDatasets(raw_files_location, seasons)

  if refresh_metadata:
    # generate frictionless data package for prepared assets
    td.generate_datapackage('data/prep')
  elif run_validations:
    # run data package validations
    fail_if_invalid(td)
  else:
    # generate prepared data assets in 'stage' folder
    td.build_assets()
    td.generate_datapackage()
    fail_if_invalid(td)
    os.system("cp prep/stage/* data/prep")


def prepare_on_cloud(
  job_name, job_queue,
  job_definition, branch, message, args,
  func):

  submit_batch_job_and_wait(
    job_name=job_name,
    job_queue=job_queue,
    job_definition=job_definition,
    branch=branch,
    message=message,
    script="2_prepare.py",
    args=args,
    vcpus=4,
    memory=30720
  )

# main

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers()

local_parser = subparsers.add_parser('local', help='Run the acquiring step locally')
local_parser.add_argument('--raw-files-location', required=False, default='data/raw')
local_parser.add_argument('--refresh-metadata', action='store_const', const=True, required=False, default=False)
local_parser.add_argument('--run-validations', action='store_const', const=True, required=False, default=False)
local_parser.add_argument('--seasons', nargs='+', help='Seasons to be built', required=False, default=read_config()["settings"]["seasons"])
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
cloud_parser.add_argument(
  '--message',
  default="🤖 updated prepared dataset files"
)
cloud_parser.add_argument(
  'args'
)
cloud_parser.set_defaults(func=prepare_on_cloud)

arguments = parser.parse_args()
arguments.func(**vars(arguments))


