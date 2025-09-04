import hashlib
import json
from datetime import datetime
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
from pipeline_config import db_config, logger, SCHEMA_NAME

def setup_processed_schema():
    """Create the processed schema and necessary tables"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        
        # Create pipeline tracker table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.pipeline_tracker (
                table_name VARCHAR(100) PRIMARY KEY,
                last_processed_id BIGINT DEFAULT 0,
                last_processed_ts TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info(f"✅ Schema '{SCHEMA_NAME}' and pipeline_tracker table created")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error setting up schema: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def get_last_processed_id(table_name):
    """Get the last processed ID for a table"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        cursor.execute(f"""
            SELECT last_processed_id FROM {SCHEMA_NAME}.pipeline_tracker 
            WHERE table_name = %s
        """, (table_name,))
        
        result = cursor.fetchone()
        return result['last_processed_id'] if result else 0
        
    except Exception as e:
        logger.error(f"❌ Error getting last processed ID for {table_name}: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def update_pipeline_tracker(table_name, last_id, last_ts=None):
    """Update the pipeline tracker with the last processed ID/timestamp"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        if last_ts:
            cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}.pipeline_tracker (table_name, last_processed_id, last_processed_ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_processed_id = EXCLUDED.last_processed_id,
                    last_processed_ts = EXCLUDED.last_processed_ts,
                    updated_at = CURRENT_TIMESTAMP
            """, (table_name, last_id, last_ts))
        else:
            cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}.pipeline_tracker (table_name, last_processed_id)
                VALUES (%s, %s)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_processed_id = EXCLUDED.last_processed_id,
                    updated_at = CURRENT_TIMESTAMP
            """, (table_name, last_id))
        
        conn.commit()
        logger.info(f"📌 Updated pipeline_tracker → {table_name}: ID {last_id}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error updating pipeline tracker: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def generate_row_hash(data_dict):
    """Generate a hash for a row to prevent duplicates"""
    # Sort keys to ensure consistent hashing
    sorted_data = dict(sorted(data_dict.items()))
    data_string = json.dumps(sorted_data, sort_keys=True, default=str)
    return hashlib.sha256(data_string.encode()).hexdigest()

def convert_decimal_to_string(value):
    """Convert Decimal to string for database storage"""
    if isinstance(value, Decimal):
        return str(value)
    return value

def convert_timestamps(data_list):
    """Convert timestamp fields to ISO format"""
    for row in data_list:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
    return data_list

def create_table_if_not_exists(table_name, columns_definition):
    """Create a table if it doesn't exist"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
                {columns_definition}
            )
        """)
        
        # Create unique index on row_hash if it exists
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_row_hash 
            ON {SCHEMA_NAME}.{table_name} (row_hash)
        """)
        
        conn.commit()
        logger.info(f"✅ Table {SCHEMA_NAME}.{table_name} created/verified")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error creating table {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def batch_insert(table_name, data_list, columns):
    """Insert data in batches with conflict resolution"""
    if not data_list:
        logger.info(f"⚠️ No data to insert for {table_name}")
        return
    
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        # Prepare the insert query
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        insert_query = f"""
            INSERT INTO {SCHEMA_NAME}.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (row_hash) DO NOTHING
        """
        
        # Prepare data for insertion
        insert_data = []
        for row in data_list:
            row_data = [row.get(col) for col in columns]
            insert_data.append(row_data)
        
        # Execute batch insert
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        
        logger.info(f"✅ Inserted {len(data_list)} rows into {SCHEMA_NAME}.{table_name}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error inserting into {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def fetch_data_with_pagination(raw_table, last_id, batch_size, additional_where=""):
    """Fetch data from raw table with pagination"""
    cursor, conn = db_config.get_raw_cursor()
    
    try:
        where_clause = f"WHERE id > {last_id}"
        if additional_where:
            where_clause += f" AND {additional_where}"
        
        query = f"""
            SELECT * FROM {raw_table} 
            {where_clause}
            ORDER BY id ASC 
            LIMIT {batch_size}
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        logger.error(f"❌ Error fetching data from {raw_table}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
