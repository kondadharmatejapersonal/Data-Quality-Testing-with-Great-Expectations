import sqlite3
from pathlib import Path

def init_db():
    # Create data directory if it doesn't exist
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # Create database file if it doesn't exist
    db_path = Path('data/ecommerce.db')
    if db_path.exists():
        db_path.unlink()  # Remove existing database
    
    conn = sqlite3.connect('data/ecommerce.db')
    db_cursor = conn.cursor()
    
    # Create tables
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_customer (
            customer_id INTEGER PRIMARY KEY,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            datetime_created DATETIME,
            datetime_updated DATETIME
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_state (
            state_id INTEGER PRIMARY KEY,
            state_code TEXT,
            state_name TEXT
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS non_validated_base_customer (
            customer_id INTEGER,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            datetime_created DATETIME,
            datetime_updated DATETIME,
            etl_inserted DATETIME
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS base_customer (
            customer_id INTEGER,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            datetime_created DATETIME,
            datetime_updated DATETIME,
            etl_inserted DATETIME
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS non_validated_base_state (
            state_id INTEGER,
            state_code TEXT,
            state_name TEXT,
            etl_inserted DATETIME
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS base_state (
            state_id INTEGER,
            state_code TEXT,
            state_name TEXT,
            etl_inserted DATETIME
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS non_validated_dim_customer (
            customer_id INTEGER,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            state_name TEXT,
            datetime_created DATETIME,
            datetime_updated DATETIME,
            etl_inserted DATETIME
        )
    """)
    
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id INTEGER,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            state_name TEXT,
            datetime_created DATETIME,
            datetime_updated DATETIME,
            etl_inserted DATETIME
        )
    """)
    
    # Insert sample data
    db_cursor.execute("""
        INSERT INTO raw_state (state_id, state_code, state_name)
        VALUES 
            (1, 'CA', 'California'),
            (2, 'NY', 'New York'),
            (3, 'TX', 'Texas')
    """)
    
    db_cursor.execute("""
        INSERT INTO raw_customer (customer_id, zipcode, city, state_code, datetime_created, datetime_updated)
        VALUES 
            (1, '90001', 'Los Angeles', 'CA', '2023-01-01', '2023-01-01'),
            (2, '10001', 'New York', 'NY', '2023-01-02', '2023-01-02'),
            (3, '75001', 'Dallas', 'TX', '2023-01-03', '2023-01-03')
    """)
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db() 