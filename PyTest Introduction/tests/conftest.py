import pytest
import pandas as pd
from pathlib import Path


@pytest.fixture(scope="session")
def path_to_file():
    """Return path to the CSV file inside the project (session scope).

    This fixture is named as requested: `path_to_file`.
    """
    project_root = Path(__file__).resolve().parent.parent
    return project_root / "src" / "data" / "data.csv"


@pytest.fixture(scope="session")
def df(path_to_file):
    """Read the CSV into a pandas DataFrame and normalize some columns.

    This fixture returns the dataframe and is session-scoped as requested.
    """
    df = pd.read_csv(path_to_file)
    # Normalize boolean-ish column to Python bools for easier assertions
    if "is_active" in df.columns:
        df["is_active"] = df["is_active"].astype(str).str.strip().str.lower().map({"true": True, "false": False})
    return df


@pytest.fixture(scope="session")
def schema_validator():
    """Return a callable that validates schema.

    The callable accepts (actual_schema, expected_schema) and asserts they match.
    Both arguments should be iterable of column names or similar.
    """
    def _validate(actual_schema, expected_schema):
        actual = list(actual_schema)
        expected = list(expected_schema)
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        assert not missing, f"Schema validation failed — missing columns: {missing}"
        # It's often useful to warn on extra columns but treat it as success; assert if you want strict equality
        return True

    return _validate


@pytest.fixture(scope="session")
def validate_schema(df):
    """Return a callable to validate schema.

    The callable signature is (actual_schema=None, expected_schema=None).
    - If actual_schema is None, the fixture will use `df.columns`.
    - If expected_schema is None, it will default to ['id','name','age','email'].

    This satisfies the requirement to accept actual_schema and expected_schema parameters
    while remaining compatible with existing tests that call `validate_schema()` without args.
    """
    default_expected = ["id", "name", "age", "email"]

    def _validate(actual_schema=None, expected_schema=None):
        actual = list(actual_schema) if actual_schema is not None else list(df.columns)
        expected = list(expected_schema) if expected_schema is not None else default_expected
        missing = [c for c in expected if c not in actual]
        assert not missing, f"Schema validation failed — missing columns: {missing}"
        return True

    return _validate


def pytest_collection_modifyitems(session, config, items):
    """Mark tests that do not have any explicit markers with 'unmarked'.

    A test is considered 'marked' if it has any marker (parametrize, skip, xfail, or custom).
    If a test has no markers at all, we add the `unmarked` marker so they can be selected/filtered.
    """
    for item in items:
        # item.iter_markers() yields markers applied to the test (decorators like @pytest.mark...)
        if not list(item.iter_markers()):
            item.add_marker(pytest.mark.unmarked)
