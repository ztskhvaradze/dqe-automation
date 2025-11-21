import pytest
import pandas as pd
import os

@pytest.fixture
def sample_parquet(tmp_path):
    # Create a small DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30]
    })
    # Save it as a Parquet file
    file_path = tmp_path / "sample_test.parquet"
    df.to_parquet(file_path)
    return file_path

def test_parquet_reader_can_read(sample_parquet, parquet_reader):
    data = parquet_reader.process(str(sample_parquet))  # should return a DataFrame

    # Check number of rows
    assert len(data) == 3

    # Check columns exist
    assert set(data.columns) == {"id", "name", "value"}

    # Optional: check first row values
    first_row = data.iloc[0]
    assert first_row["id"] == 1
    assert first_row["name"] == "Alice"
    assert first_row["value"] == 10

