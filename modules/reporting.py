import pandas as pd
import os
import logging
from datetime import datetime
import duckdb
from modules.utils.constants import *



def generate_report(**context):
    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)

        # Load transformed data
        transformed_path = os.path.join(PROCESSED_PATH, "transformed.parquet")
        df = pd.read_parquet(transformed_path)
        total_records = len(df)
        anomaly_count = df["anomaly_flag"].sum()

        # Load validation report (optional)
        quality_report_path = os.path.join(PROCESSED_PATH, "../quality/data_quality_issues.csv")
        if os.path.exists(quality_report_path):
            quality_df = pd.read_csv(quality_report_path)
            total_quality_issues = len(quality_df)
        else:
            total_quality_issues = 0

        # Iceberg snapshot info
        # con = duckdb.connect(database=":memory:")
        # con.execute("INSTALL iceberg; LOAD iceberg;")
        # con.execute(f"""
        #     SET s3_endpoint='{MINIO_ENDPOINT}';
        #     SET s3_region='{MINIO_REGION}';
        #     SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        #     SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        # """)
        # # snapshot_df = con.execute(f"""
        # #     SELECT * FROM iceberg.table_snapshots('iceberg.{ICEBERG_TABLE}')
        # # """).fetchdf()
        
        # snapshot_df = con.execute(f"""
        #                 SELECT * FROM "iceberg.{ICEBERG_TABLE}.snapshots"
        #             """).fetchdf()
        # latest_snapshot = snapshot_df.sort_values(by='committed_at', ascending=False).head(1)
        # snapshot_id = latest_snapshot['snapshot_id'].values[0]
        # committed_at = latest_snapshot['committed_at'].values[0]
        # operation = latest_snapshot['operation'].values[0]

        # Task durations via Airflow context (if available)
        ti = context.get("ti")
        task_ids = ["ingest_raw_data", "transform_data", "validate_data", "load_to_iceberg", "generate_report"]
        task_durations = {}

        for task_id in task_ids:
            try:
                start = ti.xcom_pull(task_ids=task_id, key="start_time")
                end = ti.xcom_pull(task_ids=task_id, key="end_time")
                if start and end:
                    duration = (pd.to_datetime(end) - pd.to_datetime(start)).total_seconds()
                    task_durations[task_id] = duration
            except Exception:
                task_durations[task_id] = None

        # Compile report
        report = {
            "run_ts": datetime.utcnow().isoformat(),
            "records_processed": total_records,
            "anomalies_detected": int(anomaly_count),
            "validation_issues": total_quality_issues,
            # "iceberg_snapshot_id": snapshot_id,
            # "iceberg_operation": operation,
            # "iceberg_committed_at": committed_at,
        }
        # Add task durations
        for task_id, duration in task_durations.items():
            report[f"{task_id}_duration_sec"] = duration

        report_df = pd.DataFrame([report])
        report_file = os.path.join(REPORTS_DIR, "dag_run_report.csv")

        # Append to report file (log history)
        if os.path.exists(report_file):
            report_df.to_csv(report_file, mode='a', header=False, index=False)
        else:
            report_df.to_csv(report_file, index=False)

        logging.info(f"Generated DAG run report: {report_file}")

    except Exception as e:
        logging.exception(f"Report generation failed: {str(e)}")
        raise
