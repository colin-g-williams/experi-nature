from google.cloud import bigquery
from datetime import datetime, timezone

# Define expected schema
EXPECTED_COLUMNS = {
    "when": "TIMESTAMP",
    "observed_by": "STRING",
    "species": "STRING",
    "location": "STRING",
    "latitude": "STRING",
    "longitude": "STRING",
    "notes": "STRING"
}

CRITICAL_FIELDS = ["when", "species", "location"]

def fetch_staging_data(project_id: str, dataset_id: str, table_id: str) -> bigquery.table.RowIterator:
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    return client.query(query).result()

def validate_schema(rows: bigquery.table.RowIterator) -> bool:
    actual_fields = {field.name: field.field_type for field in rows.schema}
    missing = [col for col in EXPECTED_COLUMNS if col not in actual_fields]
    mismatched = {
        col: (actual_fields[col], EXPECTED_COLUMNS[col])
        for col in EXPECTED_COLUMNS
        if col in actual_fields and actual_fields[col] != EXPECTED_COLUMNS[col]
    }

    print(f"Missing columns: {missing}")
    print(f"Type mismatches: {mismatched}")
    return not missing and not mismatched

def validate_nulls(rows: bigquery.table.RowIterator) -> bool:
    null_counts = {field: 0 for field in CRITICAL_FIELDS}
    total_rows = 0

    for row in rows:
        total_rows += 1
        for field in CRITICAL_FIELDS:
            if row.get(field) is None:
                null_counts[field] += 1

    print(f"Checked {total_rows} rows.")
    print(f"Nulls in critical fields: {null_counts}")
    return sum(null_counts.values()) == 0

def log_batch_metadata(rows: bigquery.table.RowIterator) -> None:
    first_row = next(iter(rows), None)
    if first_row:
        batch_id = getattr(first_row, "batch_id", "UNKNOWN")
        timestamp = getattr(first_row, "load_timestamp", datetime.now(timezone.utc).isoformat())
        print(f"Batch ID: {batch_id}")
        print(f"Load timestamp: {timestamp}")
    else:
        print("No rows found in staging table.")

def main():
    project_id = "core-era-462406-g2"
    dataset_id = "Nature"
    table_id = "staging_bird_sightings"

    print("Fetching staging data...")
    rows = fetch_staging_data(project_id, dataset_id, table_id)

    print("Validating schema...")
    schema_ok = validate_schema(rows)

    print("Validating nulls...")
    rows = fetch_staging_data(project_id, dataset_id, table_id)  # re-fetch for iteration
    nulls_ok = validate_nulls(rows)

    print("Logging batch metadata...")
    rows = fetch_staging_data(project_id, dataset_id, table_id)  # re-fetch again
    log_batch_metadata(rows)

    if schema_ok and nulls_ok:
        print("✅ Validation passed.")
    else:
        print("❌ Validation failed.")

if __name__ == "__main__":
    main()
