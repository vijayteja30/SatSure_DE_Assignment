# generate_config_data.py
import pandas as pd
import os
from datetime import datetime
import random

def generate_config_data(date, num_devices=10):
    data = []
    for device_id in range(1, num_devices + 1):
        row = {
            "device_id": f"D{device_id:04}",
            "device_type": random.choice(["soil", "weather", "irrigation"]),
            "scale": round(random.uniform(0.8, 1.2), 2),
            "offset": round(random.uniform(-2.0, 2.0), 2),
            "calibration_date": date
        }
        data.append(row)
    
    df = pd.DataFrame(data)
    
    path = f"data/raw/config/{date}"
    os.makedirs(path, exist_ok=True)
    df.to_csv(f"{path}/devices.csv", index=False)
    print(f"Generated device config: {path}/devices.csv")

# Example usage:
generate_config_data("2025-07-29")
