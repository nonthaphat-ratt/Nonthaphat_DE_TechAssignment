"""
test_pipeline.py

Unit tests for transform.py. Tests use small, hand-crafted DataFrames
(2-4 rows) and never touch a live database connection. Pattern: Arrange,
Act, Assert (AAA).
"""

import pandas as pd
import pytest

from transform import clean_customers, clean_orders


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

# ---------------------------------------------------------------------------
# clean_orders
# ---------------------------------------------------------------------------


def test_clean_orders_filters_out_non_positive_amounts():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"order_id": 1, "customer_id": 1, "order_date": "2023-05-01",
             "currency": "USD", "total_amount": 100},
            {"order_id": 2, "customer_id": 1, "order_date": "2023-05-01",
             "currency": "USD", "total_amount": 0},
            {"order_id": 3, "customer_id": 1, "order_date": "2023-05-01",
             "currency": "USD", "total_amount": -50},
        ]
    )
    rates_df = pd.DataFrame(columns=["currency", "date", "rate_to_usd"])
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert set(df_out["order_id"]) == {1}


def test_clean_orders_remaps_orphan_customer_id_to_negative_one():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"order_id": 1, "customer_id": 99, "order_date": "2023-05-01",
             "currency": "USD", "total_amount": 100},
        ]
    )
    rates_df = pd.DataFrame(columns=["currency", "date", "rate_to_usd"])
    valid_customer_ids = [1, 2]  # 99 is not a valid customer

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert df_out.iloc[0]["customer_id"] == -1


def test_clean_orders_usd_amount_uses_exact_date_rate_match():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"order_id": 1, "customer_id": 1, "order_date": "2023-05-03",
             "currency": "EUR", "total_amount": 100},
        ]
    )
    rates_df = pd.DataFrame(
        [
            {"currency": "EUR", "date": "2023-05-03", "rate_to_usd": 1.1},
        ]
    )
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert df_out.iloc[0]["usd_amount"] == pytest.approx(110.0)


def test_clean_orders_usd_amount_uses_locf_when_exact_date_missing():
    # Arrange: order is on 05-10, but rates only exist up to 05-05.
    df_in = pd.DataFrame(
        [
            {"order_id": 1, "customer_id": 1, "order_date": "2023-05-10",
             "currency": "EUR", "total_amount": 100},
        ]
    )
    rates_df = pd.DataFrame(
        [
            {"currency": "EUR", "date": "2023-05-04", "rate_to_usd": 1.05},
            {"currency": "EUR", "date": "2023-05-05", "rate_to_usd": 1.10},
        ]
    )
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert: should carry forward the latest available rate (05-05 = 1.10).
    assert df_out.iloc[0]["usd_amount"] == pytest.approx(110.0)


def test_clean_orders_usd_amount_falls_back_to_one_when_no_prior_rate():
    # Arrange: no rate exists on or before the order_date at all.
    df_in = pd.DataFrame(
        [
            {"order_id": 1, "customer_id": 1, "order_date": "2023-04-01",
             "currency": "EUR", "total_amount": 100},
        ]
    )
    rates_df = pd.DataFrame(
        [
            {"currency": "EUR", "date": "2023-05-01", "rate_to_usd": 1.1},
        ]
    )
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert df_out.iloc[0]["usd_amount"] == 100.0


def test_clean_orders_null_order_date_usd_currency_uses_amount_as_is():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"order_id": 117, "customer_id": 1, "order_date": None,
             "currency": "USD", "total_amount": 250},
        ]
    )
    rates_df = pd.DataFrame(columns=["currency", "date", "rate_to_usd"])
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert df_out.iloc[0]["usd_amount"] == 250.0


def test_clean_orders_null_order_date_non_usd_falls_back_to_one():
    # Arrange
    df_in = pd.DataFrame(
        [
            {"order_id": 118, "customer_id": 1, "order_date": None,
             "currency": "EUR", "total_amount": 250},
        ]
    )
    rates_df = pd.DataFrame(
        [
            {"currency": "EUR", "date": "2023-05-01", "rate_to_usd": 1.1},
        ]
    )
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert: no order_date to join on -> fallback rate 1.0
    assert df_out.iloc[0]["usd_amount"] == 250.0


def test_clean_orders_missing_currency_assumed_usd():
    # Arrange: currency is NULL (missing) -> spec says assume USD.
    df_in = pd.DataFrame(
        [
            {"order_id": 107, "customer_id": 1, "order_date": "2023-05-05",
             "currency": None, "total_amount": 120},
        ]
    )
    rates_df = pd.DataFrame(
        [
            {"currency": "EUR", "date": "2023-05-05", "rate_to_usd": 1.1},
        ]
    )
    valid_customer_ids = [1]

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert df_out.iloc[0]["usd_amount"] == pytest.approx(120.0)


def test_clean_orders_empty_dataframe_returns_empty_result():
    # Arrange
    df_in = pd.DataFrame(
        columns=["order_id", "customer_id", "order_date", "currency", "total_amount"]
    )
    rates_df = pd.DataFrame(columns=["currency", "date", "rate_to_usd"])
    valid_customer_ids = []

    # Act
    df_out = clean_orders(df_in, rates_df, valid_customer_ids)

    # Assert
    assert df_out.empty
    assert "usd_amount" in df_out.columns