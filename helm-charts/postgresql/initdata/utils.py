import logging
import uuid

import pandas as pd
import psycopg2
from helpers import load_cfg

CFG_FILE = "../auth-values.yaml"
CSV_FILE = "input_data.csv"
PREPROCESSED_CSV_FILE = "preprocessed_data.csv"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_db(auth):
    """Establishes a connection to the PostgreSQL database."""
    logger.info("Connecting to PostgreSQL database...")
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


def fast_insert_with_copy(conn, csv_file):
    """Inserts data into the 'product_reviews' table using PostgreSQL's COPY command."""
    logger.info(f"Inserting data from {csv_file} using COPY command...")
    try:
        with conn.cursor() as cur:
            with open(csv_file, "r", encoding="utf-8") as f:
                copy_sql = """
                COPY product_reviews (
                    review_id,
                    created_at,
                    updated_at,
                    product_id,
                    user_id,
                    review,
                    is_deleted,
                    source,
                    sentiment
                )
                FROM STDIN WITH (FORMAT csv, HEADER);
                """
                cur.copy_expert(copy_sql, f)
            conn.commit()
            logger.info("Data loaded via COPY successfully.")
    except Exception as e:
        print(f"Error loading via COPY: {e}")
        conn.rollback()


def preprocess_and_export_csv(csv_in, csv_out):
    """Preprocesses the input CSV and exports it to a new CSV file."""
    logger.info(f"Preprocessing data from {csv_in} and exporting to {csv_out}...")
    df = pd.read_csv(csv_in)

    df.rename(columns={"comment": "review", "label": "sentiment"}, inplace=True)
    df["review_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df["created_at"] = pd.Timestamp.now()
    df["updated_at"] = pd.Timestamp.now()
    df["product_id"] = "undefined"
    df["user_id"] = "undefined"
    df["is_deleted"] = False
    df["source"] = "csv_upload"

    columns = [
        "review_id",
        "created_at",
        "updated_at",
        "product_id",
        "user_id",
        "review",
        "is_deleted",
        "source",
        "sentiment",
    ]  # Define the desired column order

    df = df[columns]  # Reorder columns

    df.to_csv(csv_out, index=False)
    logger.info(f"Preprocessed data exported to {csv_out}")


def execute_query(conn, query):
    logger.info(f"Executing query: {query}")
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
    logger.info(f"Inserting data from {csv_file} into 'product_reviews' ...")
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
