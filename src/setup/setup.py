import sqlite3
import os

# Remove existing database if it exists
if os.path.exists('ecommerce.db'):
    os.remove('ecommerce.db')

# Connect to the database
conn = sqlite3.connect('ecommerce.db')
cursor = conn.cursor()

# Read and execute the create tables SQL
with open('setup/1-create-tables.sql', 'r') as f:
    cursor.executescript(f.read())

# Read and execute the populate tables SQL
with open('setup/2-populate-raw-tables.sql', 'r') as f:
    cursor.executescript(f.read())

# Commit changes and close connection
conn.commit()
conn.close()

print("Database setup completed successfully!") 