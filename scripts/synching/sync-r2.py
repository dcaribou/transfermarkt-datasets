"""
Upload prepared dataset files to a stable public path in R2.

This makes the latest dataset files available at predictable URLs:
  https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/<filename>

Usage:
    python scripts/synching/sync-r2.py
"""

import glob
import os
import zipfile

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

    for extra in ["dataset-metadata.json", "transfermarkt-datasets.zip"]:
        path = os.path.join(prep_dir, extra)
        if os.path.exists(path):
            key = f"{R2_PREFIX}/{extra}"
            print(f"  {extra} -> s3://{R2_BUCKET}/{key}")
            s3.upload_file(path, R2_BUCKET, key)


def build_zip(prep_dir, zip_path):
    with zipfile.ZipFile(zip_path, "w") as zf:
        for filepath in sorted(glob.glob(os.path.join(prep_dir, "*.csv.gz"))):
            zf.write(filepath, os.path.basename(filepath))
    print(f"  Created {zip_path}")


print("--> Sync to R2 stable path")
prep_dir = "data/prep"
zip_path = os.path.join(prep_dir, "transfermarkt-datasets.zip")
build_zip(prep_dir, zip_path)
sync_to_r2(prep_dir)
print("Done")
