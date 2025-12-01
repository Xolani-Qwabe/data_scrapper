# database_setup.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    """Create database if it doesn't exist"""
    try:
        # Connect to PostgreSQL server (without specifying database)
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'nba_stats'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE nba_stats")
            print("✅ Database 'nba_stats' created successfully")
        else:
            print("✅ Database 'nba_stats' already exists")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Database creation error: {e}")
        # If we can't create the database, try to continue (it might already exist)