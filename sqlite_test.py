import sqlite3
import time
import csv
import os


# Path to your CSV file
file_path = '~/projects/1brc/data/measurements.txt'
# Expand the tilde to the user's home directory
file_path = os.path.expanduser(file_path)

# Specify the column names
column_names = ['station_name', 'measurement']

# SQLite database file (it will be created if it doesn't exist)
db_path = 'measurements.db'

# Define the SQLite query (same logic as the DuckDB query)
query = """
    SELECT
        station_name,
        MIN(measurement) as Min,
        MAX(measurement) as Max,
        AVG(measurement) as Mean
    FROM measurements
    GROUP BY station_name
    ORDER BY station_name
"""

# Start timing
start_time = time.time()

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Create table (adjust column types and names as necessary)
cur.execute('DROP TABLE IF EXISTS measurements')
cur.execute('''
    CREATE TABLE IF NOT EXISTS measurements (
        station_name TEXT,
        measurement REAL
    )
''')
conn.commit()

# Import CSV data into the SQLite table
# This step is required only once; comment it out after the first run to avoid duplicating data
with open(file_path, 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';', fieldnames=column_names)
    to_db = [(row['station_name'], row['measurement']) for row in reader]
    cur.executemany("INSERT INTO measurements (station_name, measurement) VALUES (?, ?);", to_db)
    conn.commit()

# Execute the query and fetch the results
cur.execute(query)
result = cur.fetchall()

# Finish timing
end_time = time.time()

# Print results
print('SQLite Results')
#print(result)
print(f'Elapsed time: {end_time - start_time} seconds')

# Close the database connection
conn.close()

