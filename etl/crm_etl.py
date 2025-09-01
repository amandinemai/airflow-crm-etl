import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
from io import StringIO

DB_CONN= {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

def extract_data(ti):
    df = pd.read_csv("data/amazon.csv")
    print(df.head())
    ti.xcom_push(key="extracted_data", value=df.to_json())
    return df

def transform_data(ti):
    raw_data = ti.xcom_pull(key="extracted_data", task_ids="extract_data")
    df = pd.read_json(StringIO(raw_data))

    product_count = df.groupby('category')['product_id'].count()
    product_count_df = product_count.reset_index()
    product_count_df.columns = ['category', 'product_count']

    ti.xcom_push(key="transformed_data", value=product_count_df.to_json())
    return product_count_df

def load_to_db(ti):
    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")
    df = pd.read_json(StringIO(transformed_data))

    # Prepare bulk insert values
    values = [(row['category'], int(row['product_count'])) for _, row in df.iterrows()]

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_category (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(255),
                    product_count INTEGER,
                    transformed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Bulk insert
            execute_values(cursor, """
                INSERT INTO product_category (category, product_count)
                VALUES %s
            """, values)
    conn.commit()
    cursor.close()
    conn.close()
