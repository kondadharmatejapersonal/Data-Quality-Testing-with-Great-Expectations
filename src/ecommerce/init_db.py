import sqlite3
from pathlib import Path

def init_db():
    # Create database file if it doesn't exist
    db_path = Path('ecommerce.db')
    if db_path.exists():
        db_path.unlink()  # Remove existing database
    
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE raw_customer (
            customer_id TEXT PRIMARY KEY,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            datetime_created TEXT,
            datetime_updated TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE raw_state (
            state_id TEXT PRIMARY KEY,
            state_code TEXT,
            state_name TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE non_validated_base_customer (
            customer_id TEXT PRIMARY KEY,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            datetime_created TEXT,
            datetime_updated TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE base_customer (
            customer_id TEXT PRIMARY KEY,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            datetime_created TEXT,
            datetime_updated TEXT,
            etl_inserted TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE non_validated_base_state (
            state_id INTEGER PRIMARY KEY,
            state_code TEXT,
            state_name TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE base_state (
            state_id INTEGER PRIMARY KEY,
            state_code TEXT,
            state_name TEXT,
            etl_inserted TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE non_validated_dim_customer (
            customer_id TEXT PRIMARY KEY,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            state_name TEXT,
            datetime_created TEXT,
            datetime_updated TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE dim_customer (
            customer_id TEXT PRIMARY KEY,
            zipcode TEXT,
            city TEXT,
            state_code TEXT,
            state_name TEXT,
            datetime_created TEXT,
            datetime_updated TEXT,
            etl_inserted TEXT
        )
    """)
    
    # Insert sample data
    cursor.execute("""
        INSERT INTO raw_state (state_id, state_code, state_name)
        VALUES 
            ('1', 'CA', 'California'),
            ('2', 'NY', 'New York'),
            ('3', 'TX', 'Texas')
    """)
    
    cursor.execute("""
        INSERT INTO raw_customer (customer_id, zipcode, city, state_code, datetime_created, datetime_updated)
        VALUES 
            ('C001', '90001', 'Los Angeles', 'CA', '2023-01-01', '2023-01-01'),
            ('C002', '10001', 'New York', 'NY', '2023-01-02', '2023-01-02'),
            ('C003', '75001', 'Dallas', 'TX', '2023-01-03', '2023-01-03')
    """)
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db() 