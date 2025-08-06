# generate_telemetry_data.py
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta
import random

def generate_telemetry_data(date, hour, num_devices=10):
    records = []
    for device_id in range(1, num_devices + 1):
        record = {
            "device_id": f"D{device_id:04}",
            "event_ts": f"{date} {hour:02}:00:00",
            "temperature": round(random.uniform(10, 40), 2),
            "humidity": round(random.uniform(30, 90), 2),
            "sensor_type": random.choice(["temp", "humid", "multi"]),
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Output path: /data/raw/telemetry/YYYY-MM-DD/HH/*.parquet
    path = f"data/raw/telemetry/{date}/{hour:02}"
    os.makedirs(path, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), f"{path}/telemetry.parquet")
    print(f"Generated telemetry data: {path}/telemetry.parquet")

# Example usage:
for d in ["2025-08-01", "2025-08-02"]:
    for h in range(24):  # 0–23 hours
        generate_telemetry_data(d, h)
