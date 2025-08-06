import pandas as pd
import os
from modules.storage import load_to_iceberg, con
from modules.utils.constants import ICEBERG_TABLE, PROCESSED_PATH, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_REGION, MINIO_SECRET_KEY
import duckdb


def test_storage_upsert(tmp_path, monkeypatch):
    
    processed_file = os.path.join(PROCESSED_PATH, "transformed.parquet")
    
    # Prepare transformed.parquet
    df = pd.DataFrame({
        "device_id":[1,2],
        "event_ts": pd.to_datetime(["2025-08-01 01:00","2025-08-01 02:00"]),
        "device_type":["A","B"],
        "calibrated_temperature":[10.0,20.0],
        "calibrated_humidity":[50.0,60.0],
        "anomaly_flag":[False,True],
        "ingestion_ts":[pd.Timestamp.now(),pd.Timestamp.now()],
        "event_date":[pd.Timestamp("2025-08-01").date()]*2,
        "event_hour":[1,2],
        "day_avg_temp":[10.0,20.0],
        "day_avg_humid":[50.0,60.0],
        "hour_avg_temp":[10.0,20.0],
        "hour_avg_humid":[50.0,60.0],
        "rolling_7d_temp":[10.0,20.0],
        "rolling_7d_humid":[50.0,60.0],
    })
    
    df.to_parquet(processed_file)
    load_to_iceberg()
    
    # verify Iceberg table via duckdb
    # con = duckdb.connect(database=":memory:")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_region='{MINIO_REGION}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    res = con.execute(f"SELECT COUNT(*) FROM iceberg.{ICEBERG_TABLE}").fetchdf()
    print(f"Table data count in iceberg is: {res.iloc[0,0]}")
    assert res.iloc[0,0] == 2, "Iceberge table count mismatch."
