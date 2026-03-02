"""
upload_to_gcs.py
Uploads the generated incidents CSV to GCP Cloud Storage.
"""

import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
LOCAL_FILE = "data/incidents.csv"
DESTINATION_BLOB = "raw/incidents.csv"


def create_bucket_if_not_exists(client, bucket_name):
    """Create GCS bucket if it doesn't exist."""
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")
    except Exception:
        bucket = client.create_bucket(bucket_name, location="US")
        print(f"✅ Created bucket '{bucket_name}'.")
    return bucket


def upload_file(local_path, destination_blob):
    client = storage.Client(project=PROJECT_ID)
    bucket = create_bucket_if_not_exists(client, BUCKET_NAME)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded '{local_path}' → gs://{BUCKET_NAME}/{destination_blob}")


if __name__ == "__main__":
    upload_file(LOCAL_FILE, DESTINATION_BLOB)
