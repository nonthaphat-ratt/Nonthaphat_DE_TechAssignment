"""
test_pipeline.py

Unit tests for transform.py. Tests use small, hand-crafted DataFrames
(2-4 rows) and never touch a live database connection. Pattern: Arrange,
Act, Assert (AAA).
"""

import pandas as pd
import pytest

from transform import clean_customers


# ---------------------------------------------------------------------------
# clean_customers
# ---------------------------------------------------------------------------


def test_clean_customers_deduplicates_keeping_latest_signup_date():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "Alice", "email": "a@x.com",
             "phone": "111-222", "signup_date": "2023-01-01"},
            {"customer_id": 1, "full_name": "Alice", "email": "alice@x.com",
             "phone": "111-333", "signup_date": "2023-06-01"},
            {"customer_id": 2, "full_name": "Bob", "email": "b@x.com",
             "phone": "222-444", "signup_date": "2023-02-01"},
        ]
    )

    # Act
    df_out = clean_customers(df_in)

    # Assert
    cust_1_rows = df_out[df_out["customer_id"] == 1]
    assert len(cust_1_rows) == 1
    assert cust_1_rows.iloc[0]["email"] == "alice@x.com"


def test_clean_customers_standardizes_phone_to_digits_only():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "Alice", "email": "a@x.com",
             "phone": "(123) 456-7890", "signup_date": "2023-01-01"},
            {"customer_id": 2, "full_name": "Carl", "email": "c@x.com",
             "phone": "1-800-555-DINO", "signup_date": "2023-01-02"},
        ]
    )

    # Act
    df_out = clean_customers(df_in)

    # Assert
    phone_1 = df_out[df_out["customer_id"] == 1].iloc[0]["phone"]
    phone_2 = df_out[df_out["customer_id"] == 2].iloc[0]["phone"]
    assert phone_1 == "1234567890"
    # Letters are stripped out entirely (not keypad-mapped); digits only remain.
    assert phone_2 == "1800555"


def test_clean_customers_leaves_missing_phone_as_null():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "Alice", "email": "a@x.com",
             "phone": None, "signup_date": "2023-01-01"},
        ]
    )

    # Act
    df_out = clean_customers(df_in)

    # Assert
    result_phone = df_out[df_out["customer_id"] == 1].iloc[0]["phone"]
    assert result_phone is None


def test_clean_customers_fills_missing_email_with_placeholder():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "Alice", "email": None,
             "phone": "1234567890", "signup_date": "2023-01-01"},
        ]
    )

    # Act
    df_out = clean_customers(df_in)

    # Assert
    result_email = df_out[df_out["customer_id"] == 1].iloc[0]["email"]
    assert result_email == "unknown@domain.com"


def test_clean_customers_appends_unknown_dummy_row():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"customer_id": 1, "full_name": "Alice", "email": "a@x.com",
             "phone": "1234567890", "signup_date": "2023-01-01"},
        ]
    )

    # Act
    df_out = clean_customers(df_in)

    # Assert
    unknown_rows = df_out[df_out["customer_id"] == -1]
    assert len(unknown_rows) == 1
    assert unknown_rows.iloc[0]["full_name"] == "Unknown"
    assert pd.isna(unknown_rows.iloc[0]["signup_date"])