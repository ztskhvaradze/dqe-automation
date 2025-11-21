import pytest
import csv
import os
from src.connectors.file_system.csv_reader import CsvReader

@pytest.fixture
def sample_csv(tmp_path):
    file_path = tmp_path / "sample.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "value"])
        writer.writerow([1, "Alice", 10])
        writer.writerow([2, "Bob", 20])
        writer.writerow([3, "Charlie", 30])

    return file_path

@pytest.fixture
def csv_reader():
    return CsvReader()

def test_csv_reader_can_read(sample_csv, csv_reader):
    data = csv_reader.process(str(sample_csv))
    assert len(data) == 3
    assert set(data[0].keys()) == {"id", "name", "value"}
