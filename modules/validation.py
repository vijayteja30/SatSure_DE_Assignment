import pandas as pd
import os
import logging
from datetime import datetime
from modules.utils.constants import (
    PROCESSED_PATH,
    TEMPERATURE_HOT_THRESHOLD,
    TEMPERATURE_COLD_THRESHOLD,
    HUMIDITY_THRESHOLD
)
from modules.utils.decorators import record_task_timing


@record_task_timing
def validate_data(**context):
    try:
        input_path = f"{PROCESSED_PATH}/transformed.parquet"
        quality_dir = f"{PROCESSED_PATH}/../quality"
        quality_report_path = os.path.join(quality_dir, "data_quality_issues.csv")
        os.makedirs(quality_dir, exist_ok=True)

        df = pd.read_parquet(input_path)

        issues = []

        # 1. Null checks
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                issues.append({
                    "issue_type": "null_check",
                    "column": col,
                    "count": int(count),
                    "description": f"Column '{col}' has {count} nulls"
                })

        # 2. Duplicate checks
        dup_count = df.duplicated(subset=["device_id", "event_ts"]).sum()
        if dup_count > 0:
            issues.append({
                "issue_type": "duplicate_check",
                "column": "device_id + event_ts",
                "count": int(dup_count),
                "description": f"{dup_count} duplicate records found"
            })

        # 3. Calibration validation
        if "calibrated_temperature" not in df.columns or "calibrated_humidity" not in df.columns:
            issues.append({
                "issue_type": "calibration_check",
                "column": "calibrated_temperature/humidity",
                "count": 0,
                "description": "Missing calibrated columns"
            })

        # 4. Outlier checks
        outlier_temp = df[
            (df["calibrated_temperature"] > TEMPERATURE_HOT_THRESHOLD) |
            (df["calibrated_temperature"] < TEMPERATURE_COLD_THRESHOLD)
        ]
        if len(outlier_temp):
            issues.append({
                "issue_type": "outlier_check",
                "column": "calibrated_temperature",
                "count": len(outlier_temp),
                "description": f"{len(outlier_temp)} temperature outliers"
            })

        outlier_humid = df[df["calibrated_humidity"] < HUMIDITY_THRESHOLD]
        if len(outlier_humid):
            issues.append({
                "issue_type": "outlier_check",
                "column": "calibrated_humidity",
                "count": len(outlier_humid),
                "description": f"{len(outlier_humid)} humidity outliers"
            })

        def is_numeric(series):
            return pd.to_numeric(series, errors='coerce').notnull().all()

        for col in ['temperature', 'humidity', 'calibrated_temperature', 'calibrated_humidity']:
            if col in df.columns and not is_numeric(df[col]):
                issues.append({
                    "issue_type": "type_mismatch",
                    "column": col,
                    "count": df[col].shape[0],
                    "description": f"Non-numeric values found in '{col}'"
                })

        # Save report
        issues_df = pd.DataFrame(issues)
        issues_df["validation_ts"] = datetime.utcnow().isoformat()
        issues_df.to_csv(quality_report_path, index=False)

        logging.info(f"Validation completed. {len(issues)} issues found.")
        logging.info(f"Saved validation report: {quality_report_path}")

    except Exception as e:
        logging.error(f"Validation failed: {str(e)}")
        raise
