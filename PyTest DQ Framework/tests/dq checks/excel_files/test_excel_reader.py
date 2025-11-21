import pytest
import pandas as pd
from src.connectors.file_system.excel_reader import ExcelReader

@pytest.fixture
def sample_excel(tmp_path):
    file_path = tmp_path / "sample.xlsx"

    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30]
    })
    df.to_excel(file_path, index=False)
    return file_path

@pytest.fixture
def excel_reader():
    return ExcelReader()

def test_excel_reader_can_read(sample_excel, excel_reader):
    data = excel_reader.process(str(sample_excel))
    assert len(data) == 3
    assert set(data[0].keys()) == {"id", "name", "value"}
