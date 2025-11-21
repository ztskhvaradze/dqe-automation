# tests/test_data_quality_library.py

import pytest
from src.data_quality.data_quality_validation_library import DataQualityLibrary

@pytest.fixture
def sample_data():
    return [
        {"id": 1, "name": "Alice", "value": 10},
        {"id": 2, "name": "Bob", "value": 20},
        {"id": 3, "name": "Charlie", "value": 30},
    ]

def test_check_dataset_is_not_empty(sample_data):
    assert DataQualityLibrary.check_dataset_is_not_empty(sample_data)

def test_check_count_pass(sample_data):
    # same dataset -> should pass
    assert DataQualityLibrary.check_count(sample_data, sample_data)

def test_check_required_columns_pass(sample_data):
    required_columns = ["id", "name", "value"]
    assert DataQualityLibrary.check_required_columns(sample_data, required_columns)

def test_check_required_columns_fail(sample_data):
    required_columns = ["id", "name", "missing_column"]
    with pytest.raises(ValueError):
        DataQualityLibrary.check_required_columns(sample_data, required_columns)
