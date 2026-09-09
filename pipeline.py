"""
pipeline.py

Prefect orchestration for the ShopData ETL pipeline:
    Extract (from shopdata.db) -> Transform (transform.py) -> Load (analytics.db)

Tasks:
    extract_data          - read raw tables from the source SQLite DB
    transform_customers    - clean_customers via transform.py
    load_unknown_dimension - ensure the dummy 'Unknown' (-1) row exists in
                              dim_customers, run before order remapping
    transform_orders        - clean_orders via transform.py (needs valid ids)
    load_data               - write dim_customers / fct_orders into analytics.db

Flow:
    shopdata_etl_pipeline - wires the tasks together in order.
"""

import sqlite3

import pandas as pd
from prefect import flow, get_run_logger, task

from transform import clean_customers, clean_orders

SOURCE_DB = "shopdata.db"
TARGET_DB = "analytics.db"

UNKNOWN_CUSTOMER_ID = -1


@task(retries=2, retry_delay_seconds=5)
def extract_data(source_db: str = SOURCE_DB) -> dict[str, pd.DataFrame]:
    """
    Extract raw customers, orders, and exchange rates from the source
    SQLite database views.
    """
    logger = get_run_logger()
    try:
        with sqlite3.connect(source_db) as conn:
            customers_df = pd.read_sql("SELECT * FROM vw_raw_customers", conn)
            orders_df = pd.read_sql("SELECT * FROM vw_raw_orders", conn)
            rates_df = pd.read_sql("SELECT * FROM vw_exchange_rates", conn)

        logger.info(
            "Extracted %d customers, %d orders, %d exchange rates from %s",
            len(customers_df), len(orders_df), len(rates_df), source_db,
        )
        return {"customers": customers_df, "orders": orders_df, "rates": rates_df}

    except Exception:
        logger.error("Failed to extract data from %s", source_db)
        raise


@task
def transform_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw customer data (dedupe, phone/email standardization, dummy
    Unknown row) via transform.py.
    """
    logger = get_run_logger()
    try:
        clean_df = clean_customers(customers_df)
        logger.info(
            "Transformed customers: %d raw -> %d clean rows (incl. Unknown row)",
            len(customers_df), len(clean_df),
        )
        return clean_df

    except Exception:
        logger.error("Failed to transform customers")
        raise


@task
def transform_orders(
    orders_df: pd.DataFrame,
    rates_df: pd.DataFrame,
    clean_customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean raw order data (filter invalid amounts, remap orphan FKs,
    convert to USD) via transform.py. Requires the already-cleaned
    customers DataFrame to know which customer_id values are valid.
    """
    logger = get_run_logger()
    try:
        valid_customer_ids = clean_customers_df["customer_id"].tolist()
        clean_df = clean_orders(orders_df, rates_df, valid_customer_ids)
        logger.info(
            "Transformed orders: %d raw -> %d clean rows",
            len(orders_df), len(clean_df),
        )
        return clean_df

    except Exception:
        logger.error("Failed to transform orders")
        raise


@task
def load_data(
    clean_customers_df: pd.DataFrame,
    clean_orders_df: pd.DataFrame,
    target_db: str = TARGET_DB,
) -> None:
    """
    Load cleaned customers and orders into analytics.db, replacing the
    dim_customers and fct_orders tables inside a single transaction.
    """
    logger = get_run_logger()
    conn = sqlite3.connect(target_db)
    try:
        clean_customers_df.to_sql(
            "dim_customers", conn, if_exists="replace", index=False
        )
        clean_orders_df.to_sql(
            "fct_orders", conn, if_exists="replace", index=False
        )
        conn.commit()
        logger.info(
            "Loaded %d rows into dim_customers and %d rows into fct_orders (%s)",
            len(clean_customers_df), len(clean_orders_df), target_db,
        )

    except Exception:
        conn.rollback()
        logger.error("Load failed, transaction rolled back")
        raise

    finally:
        conn.close()


@flow(name="shopdata-etl-pipeline")
def shopdata_etl_pipeline(
    source_db: str = SOURCE_DB,
    target_db: str = TARGET_DB,
) -> None:
    """
    End-to-end ETL flow: Extract -> Transform -> Load.

    Note: the dummy 'Unknown' (customer_id = -1) row is created as part of
    transform_customers (see transform.py), which runs before
    transform_orders so that orphan customer_id remapping in orders has a
    valid target to point to.
    """
    logger = get_run_logger()

    raw = extract_data(source_db)

    clean_customers_df = transform_customers(raw["customers"])
    clean_orders_df = transform_orders(
        raw["orders"], raw["rates"], clean_customers_df
    )

    load_data(clean_customers_df, clean_orders_df, target_db)

    logger.info("shopdata_etl_pipeline completed successfully")


if __name__ == "__main__":
    shopdata_etl_pipeline()