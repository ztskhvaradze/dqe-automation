import pytest
import os 
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
import pandas as pd


@pytest.fixture(scope="session")
def db_connection():
    # CRITICAL FIX: Use 'host.docker.internal' instead of a specific IP.
    # This resolves to the host machine's internal IP address, bypassing 
    # the external Windows Firewall that was causing the 'Connection timed out'.
    host = "host.docker.internal"

    db_name = "mydatabase"
    user = "myuser"
    
    # Retrieve the password from the environment variable set by Jenkins
    password = os.environ.get("POSTGRES_PASSWORD") 
    
    port = 5434

    # Ensure password is set before attempting connection
    if not password:
        raise ValueError("POSTGRES_PASSWORD environment variable is not set. Cannot connect to database.")

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
