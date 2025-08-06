import pandas as pd
import numpy as np
from modules.transformation import transform_data
from modules.ingestion import ingest_raw_data
from modules.utils.constants import *
from datetime import datetime


def test_transformation(tmp_path, monkeypatch):
    context={
        'execution_date': datetime.strptime('2025-08-01', '%Y-%m-%d')
    }
    ingest_raw_data(**context)
    transform_data(**context)
    transformed_data = pd.read_parquet(f"{PROCESSED_PATH}/transformed.parquet")
    print(f"Transformed data is:  {transformed_data}")
    columns = ['temperature', 'humidity', 'ingestion_ts', 'event_ts', 'event_date', 'event_hour',
               'hour_avg_temp', 'hour_avg_humid', 'day_avg_temp','day_avg_humid', 'device_id', 'rolling_7d_temp',
               'rolling_7d_humid', 'anomaly_flag', 'calibrated_temperature', 'calibrated_humidity']
    
    assert all([col in transformed_data.columns for col in columns])