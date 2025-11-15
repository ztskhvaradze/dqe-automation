import pytest
import re


def test_file_not_empty(df):
    """Validate that the CSV file is not empty."""
    row_count = len(df)
    assert row_count > 0, f"CSV file is empty, expected at least one row but found {row_count}"


@pytest.mark.validate_csv
def test_validate_schema(validate_schema):
    """Validate the CSV has required schema: id, name, age, email."""
    assert validate_schema(), "CSV schema validation failed: required columns missing"


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Demonstration: skipping age range validation for now")
def test_age_column_valid(df):
    """Validate that the age column contains values between 0 and 100 (inclusive)."""
    assert "age" in df.columns, "age column is missing from CSV"
    invalid = df[~df["age"].apply(lambda x: isinstance(x, (int, float)) and 0 <= x <= 100)]
    assert invalid.empty, f"Found ages outside 0-100 range:\n{invalid[['id','age']].to_string(index=False)}"


@pytest.mark.validate_csv
def test_email_column_valid(df):
    """Validate that the email column contains addresses matching a basic email pattern."""
    assert "email" in df.columns, "email column is missing from CSV"
    email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    invalid = df[~df["email"].astype(str).apply(lambda e: bool(email_re.match(e)))]
    assert invalid.empty, f"Invalid email addresses found:\n{invalid[['id','email']].to_string(index=False)}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="There are known duplicate rows in the sample data")
def test_duplicates(df):
    """Validate that there are no duplicate rows in the CSV."""
    dupes = df[df.duplicated(keep=False)]
    assert dupes.empty, f"Duplicate rows found:\n{dupes.to_string(index=False)}"


@pytest.mark.parametrize("id, is_active",
                         [
                             (1, False),
                             (2, True),
                         ])
def test_is_active_parametrized(df, id, is_active):
    """Parametrized test that verifies is_active for given ids."""
    row = df[df["id"] == id]
    assert not row.empty, f"No row found with id={id}"
    actual = bool(row.iloc[0]["is_active"]) if "is_active" in row.columns else None
    assert actual == is_active, f"is_active for id={id} expected {is_active} but got {actual}"


def test_is_active_for_id_2(df):
    """Same as previous test but only for id=2 and without parametrization marker."""
    id = 2
    expected = True
    row = df[df["id"] == id]
    assert not row.empty, f"No row found with id={id}"
    actual = bool(row.iloc[0]["is_active"]) if "is_active" in row.columns else None
    assert actual == expected, f"is_active for id={id} expected {expected} but got {actual}"
