"""
Upload datasets to Kaggle (dataset 'davidcariboo/player-scores')

Usage:
    python scripts/synching/kaggle.py --message "Dataset sync"
"""

import argparse
from kaggle.api.kaggle_api_extended import KaggleApi

def publish_to_kaggle(folder, message):
  """Push the contents of the folder to Kaggle datasets
  :param folder: dataset folder path
  :param message: a string message with version notes
  """
    
  api = KaggleApi()
  api.authenticate()

  # https://github.com/Kaggle/kaggle-api/blob/master/kaggle/api/kaggle_api_extended.py#L1317
  api.dataset_create_version(
    folder=folder,
    version_notes=message
  )

parser = argparse.ArgumentParser()
parser.add_argument('--message', help='Dataset version notes', required=False, default="Dataset sync")

args = parser.parse_args()

message = args.message

prep_location = 'data/prep'

print("--> Publish to Kaggle")
publish_to_kaggle(prep_location, message)
print("")
