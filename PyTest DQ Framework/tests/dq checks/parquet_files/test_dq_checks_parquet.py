import pytest
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary

@pytest.fixture
def parquet_reader():
    return ParquetReader()

@pytest.fixture
def sample_parquet(tmp_path):
    import pandas as pd

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

def test_parquet_data_quality(parquet_reader, sample_parquet):
    # Read the parquet data
    data = parquet_reader.process(str(sample_parquet))  # returns list of dicts

    # Use static methods from DataQualityLibrary
    DataQualityLibrary.check_dataset_is_not_empty(data)
    DataQualityLibrary.check_duplicates(data)  # no duplicates expected
    DataQualityLibrary.check_required_columns(data, ["id", "name", "value"])
