import pandas as pd
import numpy as np
import psycopg
import credentials


def get_connection():
    return psycopg.connect(
        host="debprodserver.postgres.database.azure.com",
        dbname=credentials.DB_USER,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD)


def run_query(sql, params):
    conn = get_connection()
    try:
        df = pd.read_sql(sql, con=conn, params=params)
    finally:
        # conn.close()
        pass
    return df
