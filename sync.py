"""
Upload datasets to S3 storage and data hubs websites.
Publication to the following sites are supported:

- S3: store scrapy cache and prepared files in s3://player-scores 
- Kaggle: update dataset 'davidcariboo/player-scores'
- data.world: update dataset 'dcereijo/player-scores'

"""

import argparse
import boto3
import os
import requests

def save_to_s3(folder, relative_to):
  """
  Upload folders contents to S3, keeping the folder structure under the relative_to prefix
  :param folder: path to the folder to be uploaded
  :param relative_to: S3 prefix to be upload the folder contents into
  """

  import pathlib
  print(f"+ {folder} to S3 prefix {relative_to}")

  s3_client = boto3.client('s3')

  for elem in pathlib.Path('.').glob(f"{folder}/**/*"):
    if elem.is_dir():
      continue
    path = str(elem)
    key = str(elem.relative_to(folder))
    print(path)
    s3_client.upload_file(
      path,
      'player-scores',
      relative_to + '/' + key
    )

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
        'Bucket': 'player-scores',
        'Key': 'snapshots/prep/' + resource['path']
      },
      ExpiresIn=50
    )

    dw_files.append(
      {
        'name': resource['title'],
        'description': resource['description'],
        'source': {
          'url': presigned_url
        }
      }
    )

  metadata['summary'] = metadata['description']
  metadata['description'] = metadata['title']
  metadata['files'] = dw_files
  metadata['tags'] = metadata['keywords']
  metadata['license'] = metadata['licenses'][0]['CC0']

  del metadata['keywords']
  del metadata['image']
  del metadata['licenses']
  del metadata['resources']

  # https://apidocs.data.world/toolkit/api/api-endpoints/datasets/patchdataset
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

parser = argparse.ArgumentParser()
parser.add_argument('message', help='Dataset version notes')

args = parser.parse_args()

message = args.message

prep_location = 'data/prep'

print("--> Save to S3")
save_to_s3('.scrapy', 'scrapy-httpcache')
save_to_s3(prep_location, 'snapshots')
print("")

print("--> Publish to Kaggle")
publish_to_kaggle(prep_location, message)
print("")

print("--> Publish to data.world")
publish_to_dataworld(prep_location)
