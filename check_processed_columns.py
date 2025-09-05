#!/usr/bin/env python3
import psycopg2
import os

# Load secrets from .streamlit/secrets.toml
def load_secrets():
    import toml
    secrets_path = os.path.join('.streamlit', 'secrets.toml')
    if os.path.exists(secrets_path):
        return toml.load(secrets_path)
    return None

def check_processed_table_columns(table_name):
    """Check column names for a processed table"""
    try:
        secrets = load_secrets()
        if not secrets:
            print("Could not load secrets")
            return
            
        conn = psycopg2.connect(
            host=secrets['DB_CONFIG']['host'],
            port=secrets['DB_CONFIG']['port'],
            database=secrets['DB_CONFIG']['database'],
            user=secrets['DB_CONFIG']['user'],
            password=secrets['DB_CONFIG']['password']
        )
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = 'processed'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        column_names = [col[0] for col in columns]
        
        print(f"processed.{table_name} columns: {column_names}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error checking processed.{table_name}: {e}")

if __name__ == "__main__":
    check_processed_table_columns('clean_bts_coin_info')
    check_processed_table_columns('clean_arb_opportunity')
