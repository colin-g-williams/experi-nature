import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import sys
import os

# --- Config ---
CSV_PATH = "/home/colin/data/bird_sightings_sample.csv"
TABLE_ID = "core-era-462406-g2.Nature.staging_bird_sightings"
BATCH_ID = f"BATCH_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

# --- Load CSV ---
try:
    df = pd.read_csv(CSV_PATH)
except Exception as e:
    print(f"Error loading CSV: {e}")
    sys.exit(1)

# --- Add audit fields ---
df["batch_id"] = BATCH_ID
df["load_timestamp"] = datetime.utcnow().isoformat()

# --- BigQuery client ---
client = bigquery.Client()

# --- Define schema (optional but recommended for staging) ---
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
)

# --- Upload to BigQuery ---
try:
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {TABLE_ID} with batch_id {BATCH_ID}")
except Exception as e:
    print(f"Error uploading to BigQuery: {e}")
    sys.exit(1)
