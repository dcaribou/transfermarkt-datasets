"""
Upload datasets to data.world (update dataset 'dcereijo/player-scores').

Usage:
    python scripts/synching/sync-dataworld.py
"""

import json
import os
import requests

R2_PUBLIC_URL = "https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data"


def publish_to_dataworld(folder):
  """Push the contents of the folder to data.world's dataset dcereijo/player-scores
  :param folder: dataset folder path
  """

  with open(folder + '/dataset-metadata.json') as metadata_file:
    metadata = json.load(metadata_file)

  dw_files = []
  for resource in metadata['resources']:
    filename = resource['path']
    if not filename.endswith('.gz'):
      filename += '.gz'

    url = f'{R2_PUBLIC_URL}/{filename}'

    dw_files.append(
      {
        'name': resource['title'],
        'description': (resource['description'])[0:120],
        'labels': ['clean data'],
        'source': {
          'url': url
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


prep_location = 'data/prep'

print("--> Publish to data.world")
publish_to_dataworld(prep_location)
