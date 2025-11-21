def test_postgres_connector_smoke(db_connection):
    assert db_connection is not None
