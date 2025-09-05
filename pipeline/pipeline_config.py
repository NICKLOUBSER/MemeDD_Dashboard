import os
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
import logging

# Load environment variables from Streamlit secrets
def get_secret(key, default=None):
    """Get secret from Streamlit secrets or environment variable"""
    try:
        return st.secrets[key]
    except:
        return os.getenv(key, default)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    def __init__(self):
        # Use the same DB_CONFIG as the dashboard
        self.db_config = {
            "host": st.secrets.DB_CONFIG.host,
            "port": st.secrets.DB_CONFIG.port,
            "database": st.secrets.DB_CONFIG.database,
            "user": st.secrets.DB_CONFIG.user,
            "password": st.secrets.DB_CONFIG.password,
            # Add connection parameters for better stability
            "connect_timeout": 60,
            "options": "-c statement_timeout=600000",  # 10 minutes
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5
        }
        
        # For now, use the same database for both raw and processed
        # You can modify this if you need separate databases
        self.raw_connection_params = self.db_config
        self.processed_connection_params = self.db_config

    def get_raw_connection(self):
        """Get connection to raw data database"""
        return psycopg2.connect(**self.raw_connection_params)

    def get_processed_connection(self):
        """Get connection to processed data database"""
        return psycopg2.connect(**self.processed_connection_params)

    def get_raw_cursor(self):
        """Get cursor for raw data database with RealDictCursor"""
        conn = self.get_raw_connection()
        return conn.cursor(cursor_factory=RealDictCursor), conn

    def get_processed_cursor(self):
        """Get cursor for processed data database with RealDictCursor"""
        conn = self.get_processed_connection()
        return conn.cursor(cursor_factory=RealDictCursor), conn

# Global config instance
db_config = DatabaseConfig()

# Constants
BATCH_SIZE = 1000  # Reduced from 5000 to make processing more resilient
CONCURRENT_LIMIT = 10
SCHEMA_NAME = 'processed'

# API Configuration - Using Helius API
HELIUS_API_KEY = st.secrets.HELIUS_CONFIG.api_key
HELIUS_RPC_URL = f"https://rpc.helius.xyz/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else None
