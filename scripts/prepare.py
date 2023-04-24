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
import argparse


def prepare_on_local(refresh_metadata, asset, func):
  from transfermarkt_datasets.core.dataset import Dataset

  td = Dataset()

  if refresh_metadata:
    # generate frictionless data package for prepared assets
    td.as_frictionless_package("data/prep")
  else:
    # generate prepared data assets in 'stage' folder
    td.discover_assets()
    td.build_assets(asset=asset)

    pkg = td.as_frictionless_package(exclude_private=True)
    pkg.to_json("data/prep/dataset-metadata.json")

    for asset_name, asset in td.assets.items():
      if asset.public:
        os.system(
          "cp transfermarkt_datasets/stage/{} data/prep".format(
            asset.file_name
          )
        )

def prepare_on_cloud(
  job_name, job_queue,
  job_definition, branch, message, args,
  func):

  from transfermarkt_datasets.core.utils import submit_batch_job_and_wait

  submit_batch_job_and_wait(
    job_name=job_name,
    job_queue=job_queue,
    job_definition=job_definition,
    cmd=[
      branch,
      "make",
      "dvc_pull",
      "prepare_local",
      "stash_and_commit",
      args
    ],
    vcpus=4,
    memory=12288
  )

# main

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers()

local_parser = subparsers.add_parser('local', help='Run the acquiring step locally')
local_parser.add_argument('--refresh-metadata', action='store_const', const=True, required=False, default=False)
local_parser.add_argument('--asset', help="Limit...", required=False, default=None)
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
