import sys
import os
import hashlib
import logging
from datetime import datetime
import time
from psycopg2.extras import RealDictCursor

# Add the pipeline directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pipeline_config import db_config, logger, SCHEMA_NAME

def setup_processed_schema():
    """Setup the processed schema and pipeline tracker table"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        
        # Create pipeline tracker table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.pipeline_tracker (
                table_name VARCHAR(255) PRIMARY KEY,
                last_processed_id BIGINT DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info(f"✅ Schema '{SCHEMA_NAME}' and pipeline_tracker table created")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error setting up processed schema: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def get_last_processed_id(table_name):
    """Get the last processed ID for a table"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        cursor.execute(f"""
            SELECT last_processed_id 
            FROM {SCHEMA_NAME}.pipeline_tracker 
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

def update_pipeline_tracker(table_name, last_id):
    """Update the pipeline tracker with the last processed ID"""
    cursor, conn = db_config.get_processed_cursor()
    
    try:
        # First check what columns exist in the pipeline_tracker table
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = 'pipeline_tracker'
        """, (SCHEMA_NAME,))
        
        columns = [row['column_name'] for row in cursor.fetchall()]
        
        # Use the appropriate query based on existing columns
        if 'last_updated' in columns:
            # New schema
            cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}.pipeline_tracker (table_name, last_processed_id, last_updated)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_processed_id = EXCLUDED.last_processed_id,
                    last_updated = CURRENT_TIMESTAMP
            """, (table_name, last_id))
        elif 'updated_at' in columns:
            # Old schema
            cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}.pipeline_tracker (table_name, last_processed_id, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_processed_id = EXCLUDED.last_processed_id,
                    updated_at = CURRENT_TIMESTAMP
            """, (table_name, last_id))
        else:
            # Fallback - just update the ID
            cursor.execute(f"""
                INSERT INTO {SCHEMA_NAME}.pipeline_tracker (table_name, last_processed_id)
                VALUES (%s, %s)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_processed_id = EXCLUDED.last_processed_id
            """, (table_name, last_id))
        
        conn.commit()
        logger.info(f"📌 Updated pipeline_tracker → {table_name}: ID {last_id}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error updating pipeline tracker for {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def generate_row_hash(data_dict):
    """Generate a hash for a row of data"""
    # Create a string representation of the data (excluding row_hash itself)
    data_str = ""
    for key, value in sorted(data_dict.items()):
        if key != 'row_hash':
            data_str += f"{key}:{value}|"
    
    # Generate SHA-256 hash
    return hashlib.sha256(data_str.encode()).hexdigest()

def convert_datetime_to_iso(data_list):
    """Convert datetime objects to ISO format strings"""
    for row in data_list:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
    return data_list

def create_table_if_not_exists(table_name, columns_definition, max_retries=3, retry_delay=5):
    """Create a table if it doesn't exist with retry logic"""
    for attempt in range(max_retries):
        cursor = None
        conn = None
        
        try:
            conn = db_config.get_processed_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # First check if table already exists
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """, (SCHEMA_NAME, table_name))
            
            table_exists = cursor.fetchone()['exists']
            
            if table_exists:
                logger.info(f"✅ Table {SCHEMA_NAME}.{table_name} already exists")
                return
            
            # Create table if it doesn't exist
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
                    {columns_definition}
                )
            """)
            
            # Create unique index on row_hash if it exists in the columns definition
            if 'row_hash' in columns_definition.lower():
                cursor.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_row_hash 
                    ON {SCHEMA_NAME}.{table_name} (row_hash)
                """)
            
            conn.commit()
            logger.info(f"✅ Table {SCHEMA_NAME}.{table_name} created/verified")
            return  # Success, exit retry loop
            
        except Exception as e:
            if cursor and conn:
                try:
                    conn.rollback()
                except:
                    pass  # Ignore rollback errors
            
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Attempt {attempt + 1} failed creating table {table_name}: {e}")
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"❌ All {max_retries} attempts failed creating table {table_name}: {e}")
                raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass

def batch_insert_with_retry(table_name, data_list, columns, max_retries=3, retry_delay=5):
    """Insert data in batches with retry logic and connection management"""
    if not data_list:
        logger.info(f"⚠️ No data to insert for {table_name}")
        return
    
    for attempt in range(max_retries):
        cursor = None
        conn = None
        
        try:
            # Get fresh connection for each attempt
            conn = db_config.get_processed_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
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
            return  # Success, exit retry loop
            
        except Exception as e:
            if cursor and conn:
                try:
                    conn.rollback()
                except:
                    pass  # Ignore rollback errors
            
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Attempt {attempt + 1} failed for {table_name}: {e}")
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"❌ All {max_retries} attempts failed for {table_name}: {e}")
                raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass

def batch_insert(table_name, data_list, columns):
    """Legacy batch_insert function - now calls the retry version"""
    batch_insert_with_retry(table_name, data_list, columns)

def fetch_data_with_pagination(raw_table, last_id, batch_size, additional_where=""):
    """Fetch data from raw table with pagination and retry logic"""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        cursor = None
        conn = None
        
        try:
            conn = db_config.get_raw_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
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
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Attempt {attempt + 1} failed fetching from {raw_table}: {e}")
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"❌ All {max_retries} attempts failed fetching from {raw_table}: {e}")
                raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
