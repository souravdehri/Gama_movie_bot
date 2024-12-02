import logging
from psycopg2 import pool
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Connection pool
connection_pool = None

def initialize_pool():
    """Initialize the PostgreSQL connection pool."""
    global connection_pool
    try:
        connection_pool = pool.SimpleConnectionPool(
            1, 10,  # Minimum and maximum number of connections
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info("Database connection pool initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database connection pool: {e}")
        raise

def get_connection():
    """Get a connection from the pool."""
    if connection_pool:
        logger.info("Successfully retrieved a connection from the pool.")
        return connection_pool.getconn()
    else:
        raise Exception("Connection pool is not initialized.")

def release_connection(conn):
    """Release a connection back to the pool."""
    if connection_pool and conn:
        connection_pool.putconn(conn)
        logger.info("Connection returned to the pool.")

def close_pool():
    """Close all connections in the pool."""
    if connection_pool:
        connection_pool.closeall()
        logger.info("Database connection pool closed.")
