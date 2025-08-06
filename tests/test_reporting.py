import pandas as pd
import pytest
from modules.reporting import generate_report
from modules.validation import validate_data
from modules.utils.constants import PROCESSED_PATH, REPORTS_DIR
import os


@pytest.fixture(autouse=True)
def setup_reporting(tmp_path, monkeypatch):
    # Create a fake transformed.parquet
    df = pd.DataFrame({
        "device_id": [1, 2, 3],
        "event_ts": pd.to_datetime(["2025-08-05 01:00", "2025-08-05 02:00", "2025-08-05 03:00"]),
        "calibrated_temperature": [20.5, 22.0, 19.0],
        "calibrated_humidity": [55, 60, 50],
        "anomaly_flag": [False, True, False]
    })
    input_path = f"{PROCESSED_PATH}/transformed.parquet"
    test_results_file = f"{PROCESSED_PATH}/../quality/data_quality_issues.csv"
    
    
    df.to_parquet(input_path)
    validate_data()
    test_validation_results_df = pd.read_csv(test_results_file)
    
    print(f"Validation issue count: {len(test_validation_results_df)}")
    
    return df, test_validation_results_df
    
def test_generate_report_creates_csv(tmp_path, setup_reporting):
    proc, reports = setup_reporting
    
    report_file = os.path.join(REPORTS_DIR, 'dag_run_report.csv')
    if os.path.exists(report_file):
        print("removing the file")
        os.remove(report_file)
    
    generate_report(context={"ti": None})

    report_file = os.path.join(REPORTS_DIR, "dag_run_report.csv")
    assert os.path.exists(report_file), "Validation report data not exists"

    report_df = pd.read_csv(report_file)
    print(report_df)

    assert report_df.loc[0, "records_processed"] == 3
    assert report_df.loc[0, "anomalies_detected"] == 1
    assert report_df.loc[0, "validation_issues"] == 0
    # assert "iceberg_snapshot_id" in report_df.columns
    # assert "ingest_raw_data_duration_sec" in report_df.columns
