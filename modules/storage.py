import os
import logging
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from modules.utils.constants import (
    PROCESSED_PATH,
    ICEBERG_TABLE,
    BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_REGION
)
import pandas as pd
from modules.utils.decorators import record_task_timing

con = duckdb.connect(database=':memory:')

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        duckdb_type = "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        duckdb_type = "DOUBLE"
    elif pd.api.types.is_bool_dtype(dtype):
        duckdb_type = "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        duckdb_type = "TIMESTAMP"
    else:
        duckdb_type = "VARCHAR"
    return duckdb_type

@record_task_timing
def load_to_iceberg(**kwargs):
    logging.info("Starting Iceberg storage process...")

    try:
        # Load transformed file from local processed path
        processed_file = os.path.join(PROCESSED_PATH, "transformed.parquet")
        if not os.path.exists(processed_file):
            logging.warning("Processed file not found.")
            return

        table = pq.read_table(processed_file)
        df = table.to_pandas()

        # Connect to DuckDB and configure Iceberg
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("CREATE SCHEMA IF NOT EXISTS iceberg;")
        con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
        con.execute(f"SET s3_region='{MINIO_REGION}';")
        con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
        
        # con.execute(f"SET iceberg_catalog_path='s3://{BUCKET_NAME}/iceberg';")
        # con.execute("SET iceberg_catalog_type='hadoop';")

        # Check if table exists
        table_exists = con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{ICEBERG_TABLE}'"
        ).fetchone()[0]

        if table_exists == 0:
            logging.info("Creating Iceberg table...")

            # Create table without partitioned by (DuckDB doesn't support that)
            con.execute(f"""
                CREATE TABLE iceberg.{ICEBERG_TABLE} (
                    device_id VARCHAR,
                    event_ts TIMESTAMP,
                    device_type VARCHAR,
                    calibrated_temperature DOUBLE,
                    calibrated_humidity DOUBLE,
                    anomaly_flag BOOLEAN,
                    ingestion_ts TIMESTAMP,
                    event_date DATE,
                    event_hour INTEGER,
                    day_avg_temp DOUBLE,
                    day_avg_humid DOUBLE,
                    hour_avg_temp DOUBLE,
                    hour_avg_humid DOUBLE,
                    rolling_7d_temp DOUBLE,
                    rolling_7d_humid DOUBLE
                );
            """)

            # #Set Iceberg partition spec manually after creation
            # con.execute(f"""
            #     CALL set_iceberg_partition_spec('s3', '{BUCKET_NAME}', '{ICEBERG_TABLE}',
            #         {{'event_date': 'identity', 'device_type': 'identity'}});
            # """)

        
        # Schema evolution: add missing columns
        existing_cols = [row[0] for row in con.execute(f"DESCRIBE iceberg.{ICEBERG_TABLE}").fetchall()]
        for col in df.columns:
            if col not in existing_cols:
                dtype = map_dtype(df[col].dtype)
                con.execute(f'ALTER TABLE iceberg.{ICEBERG_TABLE} ADD COLUMN "{col}" {dtype};')
                logging.info(f"Added missing column: {col} ({dtype}) {df[col].dtype}")

        # ✅ Perform upsert using MERGE INTO
        con.register("temp_data", df)
        
        # Refresh schema after evolution
        final_schema = con.execute(f"DESCRIBE iceberg.{ICEBERG_TABLE}").fetchdf()
        final_cols = final_schema["column_name"].tolist()

        insert_cols = ", ".join([f"{col}" for col in final_cols])
        insert_vals = ", ".join([f"temp_data.{col}" for col in final_cols])
        quoted_cols = ", ".join([f'"{col}"' for col in final_cols])
        # merge_sql = f"""
        #     MERGE INTO iceberg.{ICEBERG_TABLE} AS target
        #     USING temp_data AS source
        #     ON target.device_id = source.device_id AND target.event_ts = source.event_ts
        #     WHEN MATCHED THEN UPDATE SET *
        #     WHEN NOT MATCHED THEN INSERT *;
        # """
        # con.execute(merge_sql)
        
        # Remove existing matching records
        con.execute(f"""
            DELETE FROM iceberg.{ICEBERG_TABLE}
            USING temp_data
            WHERE iceberg.{ICEBERG_TABLE}.device_id = temp_data.device_id
            AND iceberg.{ICEBERG_TABLE}.event_ts = temp_data.event_ts;
        """)
        logging.info(f"Dataframe data columns: {df.head()}")
        # Now insert new data
        con.execute(f"INSERT INTO iceberg.{ICEBERG_TABLE} ({quoted_cols}) SELECT {quoted_cols} FROM temp_data;")

        logging.info("Upsert completed successfully.")

    except Exception as e:
        logging.exception(f"Iceberg storage failed: {e}")
        raise

