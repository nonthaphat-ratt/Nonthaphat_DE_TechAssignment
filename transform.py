"""
transform.py

Pure pandas transformation logic for the ShopData ETL pipeline.
No database connections and no Prefect decorators here — this module is
designed to be unit-testable in isolation (see test_pipeline.py).
"""

import re

import pandas as pd


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw customer data.

    Steps:
      1) Deduplicate by customer_id, keeping the record with the latest signup_date.
      2) Standardize phone numbers to digits only. Missing/unparseable phone
         values remain NULL (no fabricated placeholder).
      3) Fill missing email with 'unknown@domain.com'.
      4) Append a dummy 'Unknown' customer row (customer_id = -1) used later
         to remap orphan foreign keys in fct_orders.

    Args:
        df: Raw customers DataFrame with columns
            [customer_id, full_name, email, phone, signup_date].

    Returns:
        Cleaned customers DataFrame, ready to load into dim_customers.
    """
    df = df.copy()

    # 1) Deduplicate: keep the row with the latest signup_date per customer_id.
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df = (
        df.sort_values("signup_date")
        .drop_duplicates(subset="customer_id", keep="last")
        .reset_index(drop=True)
    )

    # 2) Standardize phone -> digits only; missing/invalid stays NULL.
    def _standardize_phone(value):
        if pd.isna(value):
            return None
        digits_only = re.sub(r"\D", "", str(value))
        return digits_only if digits_only else None

    df["phone"] = df["phone"].apply(_standardize_phone)

    # 3) Fill missing email with a placeholder (email, unlike phone, is safe
    #    to fill since the placeholder itself signals "unknown").
    df["email"] = df["email"].fillna("unknown@domain.com")

    # 4) Append the dummy "Unknown" customer row for the Unknown Dimension
    #    Pattern. signup_date is left as NaT -> becomes 'Unknown' cohort
    #    later via COALESCE(strftime(...), 'Unknown') in clv_report.sql.
    unknown_row = pd.DataFrame(
        [
            {
                "customer_id": -1,
                "full_name": "Unknown",
                "email": None,
                "phone": None,
                "signup_date": pd.NaT,
            }
        ]
    )
    df = pd.concat([df, unknown_row], ignore_index=True)

    return df


def clean_orders(
    df: pd.DataFrame,
    rates_df: pd.DataFrame,
    valid_customer_ids,
) -> pd.DataFrame:
    """
    Clean raw order data and convert total_amount to USD.

    Steps:
      1) Filter out total_amount <= 0 (system-error orders).
      2) Remap orphan customer_id (not present in valid_customer_ids) to -1.
      3) Convert total_amount to USD (usd_amount):
         - currency == 'USD' or currency is NULL/missing -> usd_amount = total_amount (rate 1.0)
         - currency is NULL/missing -> Impute missing currency with 'USD' per spec
         - order_date is NULL         -> cannot join on date -> fallback rate 1.0
         - exact-date rate exists     -> use it
         - no exact-date rate exists  -> LOCF: use the latest available rate
                                          on or before order_date
         - no earlier rate exists at all -> fallback rate 1.0

    Args:
        df: Raw orders DataFrame with columns
            [order_id, customer_id, order_date, currency, total_amount].
        rates_df: Exchange rates DataFrame with columns
            [currency, date, rate_to_usd].
        valid_customer_ids: Collection of customer_id values that exist in
            the cleaned customers dimension (used to detect orphan FKs).

    Returns:
        Cleaned orders DataFrame (with an added usd_amount column), ready
        to load into fct_orders.
    """
    df = df.copy()
    rates_df = rates_df.copy()

    # 1) Filter out zero/negative amounts (system errors).
    df = df[df["total_amount"] > 0].reset_index(drop=True)

    # 2) Remap orphan customer_id -> -1 (Unknown Dimension Pattern).
    df["customer_id"] = df["customer_id"].where(
        df["customer_id"].isin(valid_customer_ids), -1
    )
    # 3) Impute missing currency with 'USD' per spec
    df["currency"] = df["currency"].fillna("USD")

    # Parse dates for the as-of join.
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    rates_df["date"] = pd.to_datetime(rates_df["date"], errors="coerce")
    rates_df = rates_df.sort_values("date")

    def _resolve_rate(row) -> float:
        if row["currency"] == "USD" or pd.isna(row["currency"]):
            # If missing currency, then assume USD per spec
            return 1.0

        if pd.isna(row["order_date"]):
            # No order_date to join on -> treat like "no rate found".
            return 1.0

        candidates = rates_df[
            (rates_df["currency"] == row["currency"])
            & (rates_df["date"] <= row["order_date"])
        ]
        if candidates.empty:
            # No exact or prior rate available -> final fallback.
            return 1.0

        # LOCF: the most recent rate on/before order_date.
        return candidates.iloc[-1]["rate_to_usd"]

    df["usd_amount"] = df.apply(
        lambda row: row["total_amount"] * _resolve_rate(row), axis=1
    )

    return df