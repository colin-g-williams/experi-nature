from google.cloud import bigquery

PROJECT_ID = "core-era-462406-g2"
DATASET_ID = "Nature"
STAGING_TABLE = "staging_bird_sightings"
FINAL_TABLE = "bird_sightings"

client = bigquery.Client(project=PROJECT_ID)

# Define the query to move data
query = f"""
INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE}` (
  `when`, observed_by, species, location, latitude, longitude, notes
)
SELECT
  `when`, observed_by, species, location, latitude, longitude, notes
FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}`
WHERE species IS NOT NULL
  AND location IS NOT NULL
  AND `when` IS NOT NULL

"""

# Run the query
job = client.query(query)
job.result()  # Wait for completion

print(f"✅ Loaded data from {STAGING_TABLE} to {FINAL_TABLE}")
