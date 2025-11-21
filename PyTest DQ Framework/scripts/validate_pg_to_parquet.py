import pandas as pd
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary
import csv
import os

# Map source tables to corresponding Parquet file paths
TABLE_PARQUET_MAP = {
    "facilities": "tests/dq checks/parquet_files/facilities.parquet",
    "patients": "tests/dq checks/parquet_files/patients.parquet",
    "visits": "tests/dq checks/parquet_files/visits.parquet"
}

REPORT_FILE = "dq_report.csv"


def validate_table(table_name, parquet_path, parquet_reader):
    """Validate one table and return results"""
    result = {"table": table_name, "row_count_match": True, "columns_match": True, "dataset_not_empty": True,
              "errors": ""}
    try:
        # Read from Postgres
        with PostgresConnectorContextManager() as conn:
            pg_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        # Read from Parquet
        try:
            pq_df = parquet_reader.process(parquet_path)
        except Exception as e:
            result["errors"] = f"Parquet read error: {str(e)}"
            return result

        # Check dataset is not empty
        try:
            DataQualityLibrary.check_dataset_is_not_empty(pq_df)
        except Exception as e:
            result["dataset_not_empty"] = False
            result["errors"] += f" | Dataset empty: {str(e)}"

        # Check row counts
        try:
            DataQualityLibrary.check_count(pg_df, pq_df)
        except Exception as e:
            result["row_count_match"] = False
            result["errors"] += f" | Row count mismatch: {str(e)}"

        # Check required columns
        try:
            required_columns = list(pg_df.columns)
            DataQualityLibrary.check_required_columns(pq_df, required_columns)
        except Exception as e:
            result["columns_match"] = False
            result["errors"] += f" | Columns mismatch: {str(e)}"

    except Exception as e:
        result["errors"] += f" | Unexpected error: {str(e)}"

    return result


def main():
    parquet_reader = ParquetReader()
    results = []

    for table, parquet_path in TABLE_PARQUET_MAP.items():
        if not os.path.exists(parquet_path):
            print(f"Parquet file not found for table {table}: {parquet_path}")
            results.append({"table": table, "row_count_match": False, "columns_match": False,
                            "dataset_not_empty": False, "errors": "Parquet file not found"})
            continue

        res = validate_table(table, parquet_path, parquet_reader)
        results.append(res)
        print(f"Validated table {table}: {res}")

    # Write results to CSV
    with open(REPORT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f,
                                fieldnames=["table", "row_count_match", "columns_match", "dataset_not_empty", "errors"])
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"Data Quality check report written to {REPORT_FILE}")


if __name__ == "__main__":
    main()
