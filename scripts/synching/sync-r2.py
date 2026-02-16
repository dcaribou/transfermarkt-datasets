"""
Upload prepared dataset files to a stable public path in R2.

This makes the latest dataset files available at predictable URLs:
  https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/<filename>

Usage:
    python scripts/synching/sync-r2.py
"""

import glob
import os

import boto3

R2_ENDPOINT = "https://44d7733f7a3f81262349d33c6d569749.r2.cloudflarestorage.com"
R2_BUCKET = "transfermarkt-datasets-public"
R2_PREFIX = "data"


def sync_to_r2(prep_dir):
    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    for filepath in sorted(glob.glob(os.path.join(prep_dir, "*.csv.gz"))):
        filename = os.path.basename(filepath)
        key = f"{R2_PREFIX}/{filename}"
        print(f"  {filename} -> s3://{R2_BUCKET}/{key}")
        s3.upload_file(filepath, R2_BUCKET, key)

    metadata_path = os.path.join(prep_dir, "dataset-metadata.json")
    if os.path.exists(metadata_path):
        key = f"{R2_PREFIX}/dataset-metadata.json"
        print(f"  dataset-metadata.json -> s3://{R2_BUCKET}/{key}")
        s3.upload_file(metadata_path, R2_BUCKET, key)


print("--> Sync to R2 stable path")
sync_to_r2("data/prep")
print("Done")
