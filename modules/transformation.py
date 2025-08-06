import pandas as pd
import os
import logging
from datetime import datetime
from modules.utils.constants import *
from modules.utils.decorators import record_task_timing

@record_task_timing
def transform_data(**context):
    try:
        telemetry_path = f"{INTERIM_PATH}/telemetry.parquet"
        config_path = f"{INTERIM_PATH}/config.csv"

        telemetry_df = pd.read_parquet(telemetry_path)
        config_df = pd.read_csv(config_path)

        # Merge telemetry with config on device_id
        merged_df = pd.merge(telemetry_df, config_df, on="device_id", how="left")

        # Apply calibration
        for sensor in ["temperature", "humidity"]:
            merged_df[f"calibrated_{sensor}"] = (
                merged_df[sensor] * merged_df["scale"] + merged_df["offset"]
            )

        # Add timestamps
        now = datetime.utcnow().isoformat()
        merged_df["ingestion_ts"] = now
        merged_df["event_ts"] = pd.to_datetime(merged_df["event_ts"])

        # Derived fields
        merged_df["event_date"] = merged_df["event_ts"].dt.date
        merged_df["event_hour"] = merged_df["event_ts"].dt.hour

        # Compute per-device hour avg (can be used later for reporting)
        merged_df["hour_avg_temp"] = merged_df.groupby(["device_id", "event_hour"])["calibrated_temperature"].transform("mean")
        merged_df["hour_avg_humid"] = merged_df.groupby(["device_id", "event_hour"])["calibrated_humidity"].transform("mean")
        
        # Compute per-device day average
        merged_df["day_avg_temp"] = merged_df.groupby(["device_id", "event_date"])["calibrated_temperature"].transform("mean")
        merged_df["day_avg_humid"] = merged_df.groupby(["device_id", "event_date"])["calibrated_humidity"].transform("mean")


        # Compute rolling 7d average (assuming you have historical data in future)
        merged_df.sort_values(by=["device_id", "event_ts"], inplace=True)
        merged_df["rolling_7d_temp"] = merged_df.groupby("device_id")["calibrated_temperature"].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
        merged_df["rolling_7d_humid"] = merged_df.groupby("device_id")["calibrated_humidity"].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )

        # Add anomaly flag — dummy thresholds, tune later
        merged_df["anomaly_flag"] = (
            (merged_df["calibrated_temperature"] > TEMPERATURE_HOT_THRESHOLD)
            | (merged_df["calibrated_temperature"] < TEMPERATURE_COLD_THRESHOLD)
            | (merged_df["calibrated_humidity"] < HUMIDITY_THRESHOLD)
        )

        # Log summary
        logging.info(f"Transformed {len(merged_df)} rows, added calibration, anomaly flags.")

        # Save transformed output
        os.makedirs(PROCESSED_PATH, exist_ok=True)
        merged_df.to_parquet(f"{PROCESSED_PATH}/transformed.parquet", index=False)

    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        raise
