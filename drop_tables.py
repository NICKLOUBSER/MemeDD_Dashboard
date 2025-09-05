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

def drop_processed_tables():
    """Drop all processed tables to recreate them"""
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
        
        # Drop tables if they exist
        tables = [
            'processed.clean_bts_coin_info',
            'processed.clean_arb_opportunity',
            'processed.processed_arb',
            'processed.processed_bts'
        ]
        
        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"Dropped {table}")
            except Exception as e:
                print(f"Error dropping {table}: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("All processed tables dropped successfully")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    drop_processed_tables()
