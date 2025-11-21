import pandas as pd
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary
import csv
import os

# -----------------------------
# Configuration
# -----------------------------
POSTGRES_CONFIG = {
    "db_host": "localhost",
    "db_name": "mydatabase",
    "db_user": "myuser",
    "db_password": "mypassword",
    "db_port": 5434
}

# Replace these with your actual parquet files
FACILITIES_PARQUET_PATH = "/parquet_data/facility_type_avg_time_spent_per_visit_date/facilities.parquet"
PATIENTS_PARQUET_PATH   = "/parquet_data/patient_sum_treatment_cost_per_facility_type/patients.parquet"
VISITS_PARQUET_PATH     = "/parquet_data/facility_name_min_time_spent_per_visit_date/visits.parquet"

REPORT_FILE_PATH = "dq_report.csv"

# -----------------------------
# Step 1: Read PostgreSQL data
# -----------------------------
with PostgresConnectorContextManager(**POSTGRES_CONFIG) as conn:
    postgres_facilities_df = pd.read_sql("SELECT * FROM facilities;", conn)
    postgres_patients_df   = pd.read_sql("SELECT * FROM patients;", conn)
    postgres_visits_df     = pd.read_sql("SELECT * FROM visits;", conn)

# -----------------------------
# Step 2: Read Parquet data
# -----------------------------
parquet_reader = ParquetReader()

parquet_facilities_df = parquet_reader.process(FACILITIES_PARQUET_PATH)
parquet_patients_df   = parquet_reader.process(PATIENTS_PARQUET_PATH)
parquet_visits_df     = parquet_reader.process(VISITS_PARQUET_PATH)

# Ensure DataFrames
if isinstance(parquet_facilities_df, list):
    parquet_facilities_df = pd.DataFrame(parquet_facilities_df)
if isinstance(parquet_patients_df, list):
    parquet_patients_df = pd.DataFrame(parquet_patients_df)
if isinstance(parquet_visits_df, list):
    parquet_visits_df = pd.DataFrame(parquet_visits_df)

# -----------------------------
# Step 3: Run DQ checks
# -----------------------------
dq_results = []

def run_checks(name, source_df, target_df):
    # 1. Dataset not empty
    for df, label in [(source_df, "source"), (target_df, "target")]:
        try:
            DataQualityLibrary.check_dataset_is_not_empty(df)
            dq_results.append({"dataset": f"{name}-{label}", "check": "not_empty", "result": "PASS"})
        except Exception as e:
            dq_results.append({"dataset": f"{name}-{label}", "check": "not_empty", "result": f"FAIL: {str(e)}"})

    # 2. Required columns match
    source_columns = set(source_df.columns)
    target_columns = set(target_df.columns)
    missing_in_target = source_columns - target_columns
    missing_in_source = target_columns - source_columns
    if missing_in_target:
        dq_results.append({"dataset": name, "check": "columns_in_target", "result": f"FAIL: Missing {missing_in_target}"})
    else:
        dq_results.append({"dataset": name, "check": "columns_in_target", "result": "PASS"})
    if missing_in_source:
        dq_results.append({"dataset": name, "check": "columns_in_source", "result": f"FAIL: Extra {missing_in_source}"})
    else:
        dq_results.append({"dataset": name, "check": "columns_in_source", "result": "PASS"})

    # 3. Row count match
    if len(source_df) == len(target_df):
        dq_results.append({"dataset": name, "check": "row_count", "result": "PASS"})
    else:
        dq_results.append({"dataset": name, "check": "row_count", "result": f"FAIL: source={len(source_df)}, target={len(target_df)}"})

# Run checks for each table
run_checks("facilities", postgres_facilities_df, parquet_facilities_df)
run_checks("patients", postgres_patients_df, parquet_patients_df)
run_checks("visits", postgres_visits_df, parquet_visits_df)

# -----------------------------
# Step 4: Write report
# -----------------------------
with open(REPORT_FILE_PATH, mode="w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["dataset", "check", "result"])
    writer.writeheader()
    writer.writerows(dq_results)

print(f"Data Quality check report written to {REPORT_FILE_PATH}")
