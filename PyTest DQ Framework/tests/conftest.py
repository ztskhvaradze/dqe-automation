import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
import pandas as pd


import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager

@pytest.fixture(scope="session")
def db_connection():
    # Correct container credentials
    host = "localhost"
    db_name = "mydatabase"
    user = "myuser"
    password = "mypassword"
    port = 5434

    with PostgresConnectorContextManager(
        db_host=host,
        db_name=db_name,
        db_user=user,
        db_password=password,
        db_port=port
    ) as conn:
        yield conn


@pytest.fixture(scope="module")
def parquet_reader():
    return ParquetReader()


@pytest.fixture
def sample_parquet(tmp_path):
    """
    Create a small sample Parquet file for testing purposes.
    Returns the file path.
    """
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30]
    })
    file_path = tmp_path / "sample_test.parquet"
    df.to_parquet(file_path)
    return file_path
