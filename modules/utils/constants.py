BUCKET_NAME = 'satsure-iot-data'
RAW_PREFIX = 'raw/telemetry'
CONFIG_PREFIX = 'raw/config'
CHECKPOINT_FILE = '/opt/airflow/data/checkpoints/processed_files.txt'
MINIO_ENDPOINT= 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY='password123'
MINIO_REGION='us-east-1'
INTERIM_PATH = "/opt/airflow/data/interim"
PROCESSED_PATH = "/opt/airflow/data/processed"

TEMPERATURE_HOT_THRESHOLD=45
TEMPERATURE_COLD_THRESHOLD=-5
HUMIDITY_THRESHOLD=10

# Define local Iceberg-compatible warehouse path
ICEBERG_WAREHOUSE = "/opt/airflow/data/warehouse"
ICEBERG_TABLE = "iot_telemetry"

REPORTS_DIR = "/opt/airflow/data/reports"