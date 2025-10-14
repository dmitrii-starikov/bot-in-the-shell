import os
import psycopg2
from psycopg2 import sql

def create_logs_table():
    DB_PARAMS = {
        'dbname': os.environ['POSTGRES_DB'],
        'user': os.environ['POSTGRES_USER'],
        'password': os.environ['POSTGRES_PASSWORD'],
        'host': os.environ['POSTGRES_HOST'],
        'port': os.environ['POSTGRES_PORT']
    }

    CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        level VARCHAR(50) NOT NULL,
        message TEXT NOT NULL,
        data JSONB
    );
    """

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    try:
        cur.execute(CREATE_TABLE_QUERY)
        conn.commit()
        print("Table 'logs' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


#if __name__ == "__main__":
create_logs_table()
