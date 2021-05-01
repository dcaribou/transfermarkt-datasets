"""
Publish datasets to data hubs websites. Publication to the following sites are supportes
- Kaggle
- data.world
"""

import argparse


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
  
  import datadotworld as dw
  import json
  import boto3

  with open(folder + '/dataset-metadata.json') as metadata_file:
    metadata = json.load(metadata_file)

  dw_files = {}
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

    dw_files[resource['title']] = {
      'url': presigned_url,
      'description': resource['description']
    }


  metadata['summary'] = metadata['description']
  metadata['description'] = metadata['title']
  metadata['files'] = dw_files

  # https://github.com/datadotworld/data.world-py/blob/master/datadotworld/client/api.py#L163
  dw.api_client().update_dataset(
    'dcereijo/player-scores',
    **metadata
  )


parser = argparse.ArgumentParser()
parser.add_argument('message', help='Dataset version notes')

args = parser.parse_args()

message = args.message

prep_location = 'data/prep'

print("--> Publish to Kaggle")
publish_to_kaggle(prep_location, message)
print("--> Publish to data.world")
publish_to_dataworld(prep_location)



