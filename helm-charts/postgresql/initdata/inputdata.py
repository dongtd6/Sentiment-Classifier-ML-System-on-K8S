# postgresl/inputdata.py
import os
import uuid

import pandas as pd
import psycopg2
from helpers import load_cfg

CFG_FILE = "../auth-values.yaml"
CSV_FILE = "input_data.csv"

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_db(auth):
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=auth["host"],
            port=auth["port"],
            database=auth["database"],
            user=auth["username"],
            password=auth["password"],
        )
        logger.info("Connection to PostgreSQL successful!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None


def create_table(conn):
    """Creates the 'product_reviews' table if it doesn't exist."""
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


def execute_query(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
            logger.info("Queey execute successfully.")
    except Exception as e:
        print(f"Error execute query {e}")
        conn.rollback()


def insert_data_from_csv(conn, csv_file):
    """Inserts data from a CSV file into the 'product_reviews' table."""
    try:
        if not os.path.exists(csv_file):
            print(f"Error: CSV file '{csv_file}' not found.")
            return
        logger.info("CSV file found, proceeding to read data...")
        df = pd.read_csv(csv_file)

        # Rename columns to match the database schema
        df.rename(columns={"comment": "review", "label": "sentiment"}, inplace=True)

        # Add additional columns with default values
        df["review_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()
        df["product_id"] = "undefined"
        df["user_id"] = "undefined"
        df["is_deleted"] = False
        df["source"] = "csv_upload"

        # Define the columns to insert
        columns_to_insert = [
            "review_id",
            "created_at",
            "updated_at",
            "product_id",
            "user_id",
            "review",
            "is_deleted",
            "source",
            "sentiment",
        ]
        logger.info("Inserting data into 'product_reviews' ...")
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO product_reviews ({', '.join(columns_to_insert)})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO NOTHING;
                """
                # Create a tuple of values to insert
                data = tuple(row[col] for col in columns_to_insert)
                cur.execute(insert_query, data)
            conn.commit()
            print(f"Successfully inserted {len(df)} rows into 'product_reviews'.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()


def main():
    """Main function to orchestrate the process."""
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
        insert_data_from_csv(conn, CSV_FILE)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    # finally:
    #     # 4. Close the connection
    #     if conn:
    #         conn.close()
    #         logger.info("Database connection closed.")

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
