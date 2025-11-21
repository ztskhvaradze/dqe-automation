# Create sample Parquet file
import pandas as pd
from pathlib import Path

parquet_path = Path("tests/dq checks/parquet_files/sample_test.parquet")

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "value": [10, 20, 30]
})

parquet_path.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(parquet_path, index=False)

print(f"Created Parquet file at {parquet_path}")
