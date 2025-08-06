import os
import pandas as pd
import logging
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from modules.utils.constants import *
from modules.utils.decorators import record_task_timing


# Fork-safe pyarrow S3 client
s3_fs = pafs.S3FileSystem(
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    endpoint_override="minio:9000",
    scheme="http"
)


# Ensure checkpoint directory exists
os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)

def is_already_processed(file_path):
    print(f"already processed fn {os.path.exists(CHECKPOINT_FILE)}, {CHECKPOINT_FILE}")
    if not os.path.exists(CHECKPOINT_FILE):
        return False
    with open(CHECKPOINT_FILE, 'r') as f:
        return file_path in f.read().splitlines()

def mark_as_processed(file_path):
    with open(CHECKPOINT_FILE, 'a') as f:
        f.write(f"{file_path}\n")

def get_latest_config_file():
    # List all files in the config prefix
    config_path = f"{BUCKET_NAME}/{CONFIG_PREFIX}"
    file_infos = s3_fs.get_file_info(pafs.FileSelector(config_path, recursive=True))

    # Filter for .csv files only
    csv_files = [info.path for info in file_infos if info.is_file and info.path.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError(f"No config CSV files found in: {config_path}")

    # Return the latest one (based on filename sort)
    latest_file = sorted(csv_files)[-1]
    return f"s3://{latest_file}"

@record_task_timing
def ingest_raw_data(**context):
    # Compute run time
    execution_time = context['execution_date']
    date_str = execution_time.strftime('%Y-%m-%d')
    hour_str = execution_time.strftime('%H')

    # S3 path to hourly parquet file
    telemetry_path = f"{BUCKET_NAME}/{RAW_PREFIX}/{date_str}/{hour_str}/telemetry.parquet"
    telemetry_uri = f"s3://{telemetry_path}"

    if is_already_processed(telemetry_path):
        logging.info(f"Already processed: {telemetry_path}")
        return

    # Read telemetry parquet
    try:
        logging.info(f"Reading from telemetry file: {telemetry_uri}")
        parquet_file = pq.ParquetFile(telemetry_path, filesystem=s3_fs)
        table = parquet_file.read()
        telemetry_df = table.to_pandas()

    except Exception as e:
        logging.exception(f"Failed to read telemetry file: {telemetry_uri} — {e}")
        raise

    # Read latest config
    try:
        config_uri = get_latest_config_file()
        config_df = pd.read_csv(config_uri, storage_options={
            'client_kwargs': {
                'endpoint_url': MINIO_ENDPOINT,
                'aws_access_key_id': MINIO_ACCESS_KEY,
                'aws_secret_access_key': MINIO_SECRET_KEY
            }
        })
    except Exception as e:
        logging.exception(f"Failed to read config file: {str(e)}")
        raise

    # Log ingestion metadata
    logging.info(f"Ingested {len(telemetry_df)} telemetry records from {telemetry_path}")
    logging.info(f"Ingested {len(config_df)} config records from {config_uri}")

    # Save raw data to /opt/airflow/data/interim/ for next steps
    
    os.makedirs(INTERIM_PATH, exist_ok=True)
    telemetry_df.to_parquet(f"{INTERIM_PATH}/telemetry.parquet", index=False)
    config_df.to_csv(f"{INTERIM_PATH}/config.csv", index=False)

    mark_as_processed(telemetry_path)
