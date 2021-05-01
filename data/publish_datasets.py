"""
Publish datasets to data hubs websites. Publication to the following sites are supportes
- Kaggle
- data.world
"""

import argparse


def publish_to_kaggle(folder, message):
  
  from kaggle.api.kaggle_api_extended import KaggleApi
  
  api = KaggleApi()
  api.authenticate()

  api.dataset_create_version(
    folder=folder,
    version_notes=message
  )

def publish_to_dataworld(folder, message):
  
  import datadotworld as dw
  import json
  import boto3

  with open(folder + '/dataset-metadata.json') as metadata_file:
    metadata = json.load(metadata_file)

  dw_files = {}
  s3_client = boto3.client('s3')

  for resource in metadata['resources']:
    presigned_url = s3_client.generate_presigned_url(
      'get_object',
      Params={
        'Bucket': 'player-scores',
        'Key': 'snapshots/prep/' + resource['path']
      },
      ExpiresIn=50
    )

    dw_files[resource['title']] = {
      'url': presigned_url
    }


  metadata['summary'] = metadata['description']
  metadata['description'] = metadata['title']
  metadata['files'] = dw_files

  dw.api_client().update_dataset(
    'dcereijo/player-scores',
    **metadata
  )


parser = argparse.ArgumentParser()
parser.add_argument('message', help='Dataset version notes')

args = parser.parse_args()

message = args.message

publish_to_kaggle('prep', message)
publish_to_dataworld('prep', message)



