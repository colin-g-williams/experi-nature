import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone
import sys
import os

# --- Config ---
CSV_PATH = "/home/colin/data/bird_sightings_sample.csv"
TABLE_ID = "core-era-462406-g2.Nature.staging_bird_sightings"
BATCH_ID = f"BATCH_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

schema = [
    bigquery.SchemaField("when", "TIMESTAMP"),
    bigquery.SchemaField("observed_by", "STRING"),
    bigquery.SchemaField("species", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("latitude", "STRING"),
    bigquery.SchemaField("longitude", "STRING"),
    bigquery.SchemaField("notes", "STRING")
]
# --- Load CSV ---
try:
    df = pd.read_csv(CSV_PATH)
except Exception as e:
    print(f"Error loading CSV: {e}")
    sys.exit(1)

# --- BigQuery client ---
client = bigquery.Client(project="core-era-462406-g2")

# --- Define schema (optional but recommended for staging) ---
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # if your CSV has a header
)

# --- Upload to BigQuery ---
try:
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {TABLE_ID} with batch_id {BATCH_ID}")
except Exception as e:
    print(f"Error uploading to BigQuery: {e}")
    sys.exit(1)
