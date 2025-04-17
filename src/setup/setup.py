import sqlite3
import os
from pathlib import Path

# Get the directory containing this script
script_dir = Path(__file__).parent

# Remove existing database if it exists
if os.path.exists('data/ecommerce.db'):
    os.remove('data/ecommerce.db')

# Connect to the database
conn = sqlite3.connect('data/ecommerce.db')
cursor = conn.cursor()

# Read and execute the create tables SQL
with open(script_dir / '1-create-tables.sql', 'r') as f:
    cursor.executescript(f.read())

# Read and execute the populate tables SQL
with open(script_dir / '2-populate-raw-tables.sql', 'r') as f:
    cursor.executescript(f.read())

# Commit changes and close connection
conn.commit()
conn.close()

print("Database setup completed successfully!") 