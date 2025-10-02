import psycopg2

conn = psycopg2.connect(
    dbname="your_db",
    user="your_user",
    password="your_pass",
    host="your_postgres_ip",  # GKE IP hoặc internal service name
    port="5432",
)

cur = conn.cursor()

with open("data.csv", "r") as f:
    cur.copy_expert("COPY your_table FROM STDIN WITH CSV HEADER", f)

conn.commit()
cur.close()
conn.close()

sql = """
UPDATE product_reviews
SET created_at = timestamp '2025-08-01 00:00:00'
    + random() * (timestamp '2025-11-01 23:59:59' - timestamp '2025-08-01 00:00:00');
"""

sql2 = """

UPDATE product_reviews
SET product_id = 'PRD' || TO_CHAR(FLOOR(RANDOM() * 100) + 1, 'FM000');


UPDATE product_reviews
SET user_id = 'USR' || TO_CHAR(FLOOR(RANDOM() * 100) + 1, 'FM000');

"""

create_table_query = """
CREATE TABLE IF NOT EXISTS product_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    product_id VARCHAR(15),
    user_id VARCHAR(15),
    review VARCHAR(1000),
    is_deleted BOOLEAN DEFAULT FALSE,
    source VARCHAR(50)
);
"""
sql3 = """

UPDATE product_reviews
SET source = (
    array['FACEBOOK', 'ZALO', 'WEBSITE', 'CHATBOX', 'TIKTOK', 'SMS', 'IOS', 'ANDROID']

    )[floor(random() * 8 + 1)];

"""
