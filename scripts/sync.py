"""
Upload datasets to S3 storage and data hub websites.
Publication to the following sites are supported:

- S3: store scrapy cache and prepared files in s3://transfermarkt-datasets 
- Kaggle: update dataset 'davidcariboo/player-scores'
- data.world: update dataset 'dcereijo/player-scores'

"""

import argparse
import pathlib
import boto3
import os
import requests

def publish_to_kaggle(folder, message):
  """Push the contents of the folder to Kaggle datasets
  :param folder: dataset folder path
  :param message: a string message with version notes
  """
  
  from kaggle.api.kaggle_api_extended import KaggleApi
  
  api = KaggleApi()
  api.authenticate()

  # https://github.com/Kaggle/kaggle-api/blob/master/kaggle/api/kaggle_api_extended.py#L1317
  api.dataset_create_version(
    folder=folder,
    version_notes=message
  )

def publish_to_dataworld(folder):
  """Push the contents of the folder to data.world's dataset dcereijo/player-scores
  :param folder: dataset folder path
  """
  import json

  with open(folder + '/dataset-metadata.json') as metadata_file:
    metadata = json.load(metadata_file)

  dw_files = []
  s3_client = boto3.client('s3')

  for resource in metadata['resources']:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    presigned_url = s3_client.generate_presigned_url(
      'get_object',
      Params={
        'Bucket': 'transfermarkt-datasets',
        'Key': 'snapshots/data/prep/' + resource['path']
      },
      ExpiresIn=120
    )

    dw_files.append(
      {
        'name': resource['title'],
        'description': (resource['description'])[0:120],
        'labels': ['clean data'],
        'source': {
          'url': presigned_url
        }
      }
    )

  metadata['summary'] = metadata['description']
  metadata['description'] = "Clean, structured and automatically updated football (soccer) data from Transfermarkt"
  metadata['tags'] = metadata['keywords']
  metadata['license'] = metadata['licenses'][0]['CC0']

  metadata['visibility'] = 'OPEN'

  del metadata['keywords']
  del metadata['licenses']
  del metadata['resources']

  # update dataset attributes
  # https://apidocs.data.world/docs/dwapi-spec-stoplight/30a65f92585f5-update-a-dataset
  response = requests.patch(
    url='https://api.data.world/v0/datasets/dcereijo/player-scores',
    headers={
      'Content-Type': 'application/json',
      'Authorization': f"Bearer {os.environ['DW_AUTH_TOKEN']}"
    },
    data=json.dumps(metadata)
  )

  print(response.content)
  if response.status_code != 200:
    raise Exception("Publication to data.world failed")

  # update files and trigger sync
  # https://apidocs.data.world/docs/dwapi-spec-stoplight/6e469e9ea3ed4-add-files-from-ur-ls
  response = requests.post(
    url='https://api.data.world/v0/datasets/dcereijo/player-scores/files',
    headers={
      'Content-Type': 'application/json',
      'Authorization': f"Bearer {os.environ['DW_AUTH_TOKEN']}"
    },
    data=json.dumps(
      {"files": dw_files}
    )
  )

  print(response.content)
  if response.status_code != 200:
    raise Exception("Publication to data.world failed")

parser = argparse.ArgumentParser()
parser.add_argument('--message', help='Dataset version notes', required=False, default="Dataset sync")
parser.add_argument('--season', help='Season to be synchronized. It applies to S3 stored objects only', required=False, default=2020)

args = parser.parse_args()

message = args.message
season = args.season

prep_location = 'data/prep'
raw_location = 'data/raw'

print("--> Publish to Kaggle")
publish_to_kaggle(prep_location, message)
print("")

print("--> Publish to data.world")
publish_to_dataworld(prep_location)
