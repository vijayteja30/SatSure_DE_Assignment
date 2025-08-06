import pandas as pd
import os
from modules.ingestion import ingest_raw_data
from modules.transformation import transform_data
from modules.validation import validate_data
from modules.utils.constants import PROCESSED_PATH
import numpy as np

def test_validation_null_and_duplicates(tmp_path, monkeypatch):
    df = pd.DataFrame({
        "device_id":[1,1,1],
        "event_ts":[pd.Timestamp("2025-08-01 01:00"),
                    pd.Timestamp("2025-08-01 01:00"),
                    pd.Timestamp("2025-08-01 02:00")],
        "calibrated_temperature":[100,np.nan,20],
        "calibrated_humidity":[5,10,20]
    })
    input_path = f"{PROCESSED_PATH}/transformed.parquet"
    test_results_file = f"{PROCESSED_PATH}/../quality/data_quality_issues.csv"

    df.to_parquet(input_path)
    validate_data()
    test_results_df = pd.read_csv(test_results_file)
    print(f"Test results issue types: {test_results_df['issue_type'].unique()}")
    issue_exists = not ( test_results_df['issue_type'].isin(["null_check","duplicate_check","outlier_check", 'type_mismatch']).any() )
    assert issue_exists, "Data validation failure with any null_check / duplicate_check / outlier_check / type_mismatch"