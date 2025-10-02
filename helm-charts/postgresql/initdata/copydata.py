# postgresl/inputdata.py
import os
import uuid

import pandas as pd
import psycopg2
from helpers import load_cfg
from psycopg2 import OperationalError, extras, sql
from utils import (
    connect_to_db,
    execute_query,
    fast_insert_with_copy,
    preprocess_and_export_csv,
)

CFG_FILE = "../auth-values.yaml"
CSV_FILE = "input_data.csv"

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_table(conn):
    """Creates the 'product_reviews' table if it doesn't exist."""
    logger.info("Creating table 'product_reviews' if not exists...")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS product_reviews (
      review_id VARCHAR(50) PRIMARY KEY,
      created_at TIMESTAMP,
      updated_at TIMESTAMP,
      product_id VARCHAR(15),
      user_id VARCHAR(15),
      review VARCHAR(1000),
      is_deleted BOOLEAN DEFAULT FALSE,
      source VARCHAR(50),
      sentiment VARCHAR(50)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            logger.info("Table 'product_reviews' created.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()


def main():
    """Main function to orchestrate the process."""
    logger.info("Starting the data generation and insertion process...")
    cfg = load_cfg(CFG_FILE)
    auth = cfg.get("auth")
    if not auth:
        logger.info("Error: 'auth' section not found in config file.")
        return

    # 1. Connect to the database
    conn = connect_to_db(auth)
    if conn is None:
        return
    try:
        # 2. Create the table
        create_table(conn)

        # 3. Insert data from CSV
        # preprocess_and_export_csv(CSV_FILE, "preprocessed_data.csv")
        fast_insert_with_copy(conn, "preprocessed_data.csv")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    # finally:
    #     # 4. Close the connection
    #     if conn:
    #         conn.close()
    #         logger.info("Database connection closed.")

    df = pd.read_csv("preprocessed_data.csv")
    logger.info("Sample data from preprocessed_data.csv:")
    print(df.columns)
    logger.info(f"Total records: {df.shape[0]}")
    print(df.shape)
    logger.info("First 5 records:")
    print(df.head(5))

    query_creat_at = """
    UPDATE product_reviews
    SET created_at = timestamp '2025-08-01 00:00:00'
        + random() * (timestamp '2025-11-01 23:59:59' - timestamp '2025-08-01 00:00:00');
    """
    execute_query(conn, query_creat_at)

    query_product_id = """
    UPDATE product_reviews
    SET product_id = 'PRD' || TO_CHAR(FLOOR(RANDOM() * 100) + 1, 'FM000');
    """
    execute_query(conn, query_product_id)

    query_user_id = """
    UPDATE product_reviews
    SET user_id = 'USR' || TO_CHAR(FLOOR(RANDOM() * 100) + 1, 'FM000');
    """
    execute_query(conn, query_user_id)

    query_source = """
    UPDATE product_reviews
    SET source = (
        array['FACEBOOK', 'ZALO', 'WEBSITE', 'CHATBOX', 'TIKTOK', 'SMS', 'IOS', 'ANDROID']

        )[floor(random() * 8 + 1)];
    """
    execute_query(conn, query_source)

    if conn:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
