import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

OUTPUT_DIR = "parquet_data"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "expected.parquet")

data = [
    ["Specialty Center", "2025-11-21", 35],
    ["Hospital", "2025-11-21", 52],
    ["Clinic", "2025-11-21", 28],
    ["Specialty Center", "2025-11-20", 43],
    ["Hospital", "2025-11-20", 25.5],
    ["Clinic", "2025-11-20", 35.33],
    ["Specialty Center", "2025-11-19", 31.33],
    ["Hospital", "2025-11-19", 49],
    ["Clinic", "2025-11-19", 36],
    ["Specialty Center", "2025-11-18", 22],
    ["Hospital", "2025-11-18", 41.5],
    ["Clinic", "2025-11-17", 42],
    ["Specialty Center", "2025-11-17", 40.67],
    ["Hospital", "2025-11-16", 39.5],
    ["Clinic", "2025-11-16", 43.5],
    ["Specialty Center", "2025-11-16", 41],
    ["Hospital", "2025-11-15", 20],
    ["Clinic", "2025-11-15", 31],
    ["Specialty Center", "2025-11-15", 35],
]

df = pd.DataFrame(data, columns=[
    "Facility Type",
    "Visit Date",
    "Average Time Spent"
])

print("FINAL DATAFRAME:")
print(df)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

pq.write_table(pa.Table.from_pandas(df), OUTPUT_PATH)

print("\nSUCCESS — Parquet saved at:")
print(OUTPUT_PATH)
