"""
load_to_bigquery.py
Reads incidents CSV from GCP Cloud Storage and loads into BigQuery.
Creates dataset and table if they don't exist.
"""

import os
import pandas as pd
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET", "it_ops")
TABLE_ID = os.getenv("BQ_TABLE", "incidents")
SOURCE_BLOB = "raw/incidents.csv"

SCHEMA = [
    bigquery.SchemaField("incident_id", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("resolved_at", "TIMESTAMP"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("severity", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("affected_system", "STRING"),
    bigquery.SchemaField("assigned_team", "STRING"),
    bigquery.SchemaField("reporter", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("resolution_notes", "STRING"),
    bigquery.SchemaField("resolution_time_hours", "FLOAT"),
    # AI-enriched fields (added later by gemini_enrich.py)
    bigquery.SchemaField("ai_severity_label", "STRING"),
    bigquery.SchemaField("ai_summary", "STRING"),
    bigquery.SchemaField("ai_resolution_suggestion", "STRING"),
    bigquery.SchemaField("ai_enriched", "BOOL"),
]


def create_dataset_if_not_exists(client):
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset '{DATASET_ID}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✅ Created dataset '{DATASET_ID}'.")


def create_table_if_not_exists(client):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        client.get_table(table_ref)
        print(f"✅ Table '{TABLE_ID}' already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)
        print(f"✅ Created table '{TABLE_ID}'.")


def read_csv_from_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB)
    content = blob.download_as_text()
    df = pd.read_csv(StringIO(content))
    print(f"✅ Read {len(df)} rows from GCS.")
    return df


def load_to_bq(df):
    bq_client = bigquery.Client(project=PROJECT_ID)
    create_dataset_if_not_exists(bq_client)
    create_table_if_not_exists(bq_client)

    # Add AI enrichment columns as empty (will be filled by gemini_enrich.py)
    df["ai_severity_label"] = None
    df["ai_summary"] = None
    df["ai_resolution_suggestion"] = None
    df["ai_enriched"] = False

    # Fix timestamp parsing
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce")

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=SCHEMA,
    )

    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows into BigQuery → {table_ref}")


if __name__ == "__main__":
    df = read_csv_from_gcs()
    load_to_bq(df)
