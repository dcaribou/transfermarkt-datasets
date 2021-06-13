"""
Upload datasets to S3 storage and data hub websites.
Publication to the following sites are supported:

- S3: store scrapy cache and prepared files in s3://player-scores 
- Kaggle: update dataset 'davidcariboo/player-scores'
- data.world: update dataset 'dcereijo/player-scores'

"""

import argparse
import pathlib
import boto3
import os
import requests

def zip_cache():
  import zipfile

  path = '.scrapy'
  zipf = zipfile.ZipFile('scrapy-httpcache.zip', 'w', zipfile.ZIP_DEFLATED)

  for root, dirs, files in os.walk(path):
        for file in files:
            zipf.write(os.path.join(root, file), 
                       os.path.relpath(os.path.join(root, file), 
                                       os.path.join(path, '..')))


def save_to_s3(path, relative_to):
  """
  Upload path contents to S3, keeping the folder structure under the relative_to prefix
  :param path: path to the file or folder to be uploaded
  :param relative_to: S3 prefix to be upload the folder contents into
  """

  import pathlib
  import botocore
  import boto3.s3.transfer as s3transfer

  print(f"+ {path} to S3 prefix {relative_to}")

  botocore_config = botocore.config.Config(max_pool_connections=50)
  s3_client = boto3.client('s3', config=botocore_config)

  transfer_config = s3transfer.TransferConfig(
      use_threads=True,
      max_concurrency=50
  )

  bucket_name = 'player-scores'

  s3t = s3transfer.create_transfer_manager(s3_client, transfer_config)

  if pathlib.Path(path).is_dir():
    files = [elem for elem in pathlib.Path('.').glob(f"{path}/**/*") if not elem.is_dir()]
  elif pathlib.Path(path).exists():
    files = [pathlib.Path(path)]
  else:
    files = []

  for elem in files:
    path = str(elem)
    key = path
    s3t.upload(
      path,
      bucket_name,
      relative_to + '/' + key
    )

  s3t.shutdown()  # wait for all the upload tasks to finish

  

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
        'Key': 'snapshots/data/prep/' + resource['path']
      },
      ExpiresIn=50
    )

    dw_files.append(
      {
        'name': resource['title'],
        'description': resource['description'],
        'labels': ['clean data'],
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

  metadata['visibility'] = 'OPEN'

  del metadata['keywords']
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
raw_location = 'data/raw'
season = 2020

scrapy_cache_location = pathlib.Path('.scrapy')
if scrapy_cache_location.exists() and scrapy_cache_location.is_dir():
  print("--> Zip scrapy cache")
  zip_cache()
  save_to_s3('scrapy-httpcache.zip', f"scrapy-httpcache/{season}")
  print("")

print("--> Save assets to S3")
save_to_s3(prep_location, f"snapshots")
save_to_s3(raw_location, f"snapshots/{season}")
save_to_s3('prep/datapackage_validation.json', 'snapshots')
print("")

print("--> Publish to Kaggle")
publish_to_kaggle(prep_location, message)
print("")

print("--> Publish to data.world")
publish_to_dataworld(prep_location)
