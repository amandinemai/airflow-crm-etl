import pandas as pd
import yfinance as yf
import psycopg2

DB_CONN= {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

def extract_data(ti):
    data = {}
    for stock in STOCKS:
        df = yf.download(stock, period="1d", interval="1h")
        data[stock] = df.reset_index().to_json()
    ti.xcom_push(key="raw_data", value=data)

def transform_data(ti):
    raw_data = ti.xcom_pull(key="raw_data", task_ids="extract_stock_data")
    transformed = {}
    for stock, raw_json in raw_data.items():
        df = pd.read_json(raw_json)
        df["SMA_5"] = df["Close"].rolling(5).mean()
        transformed[stock] = df.to_json()
    ti.xcom_push(key="transformed_data", value=transformed)

def load_to_db(ti):
    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")
    conn = psycopg2.connect(**DB_CONN)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            stock VARCHAR(10),
            datetime TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adj_close FLOAT,
            volume BIGINT,
            sma_5 FLOAT
        );
    """)
    for stock, json_data in transformed_data.items():
        df = pd.read_json(json_data)
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stock_prices (stock, datetime, open, high, low, close, adj_close, volume, sma_5)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                stock, row["Datetime"], row["Open"], row["High"], row["Low"],
                row["Close"], row["Adj Close"], row["Volume"], row["SMA_5"]
            ))
    conn.commit()
    cursor.close()
    conn.close()
