import duckdb
import time


file_path = '~/projects/1brc/data/measurements.txt'

# Define query
query = f"""
    select
        station_name,
        min(measurement) as Min,
        max(measurement) as Max,
        avg(measurement) as Mean

    from read_csv_auto('{file_path}', sep=';', columns = {{'station_name': 'STRING', 'measurement': 'FLOAT'}})
    
    group by station_name

    order by station_name
"""
start_time = time.time()  # Start timing

# Connect to DuckDB (in-memory mode)
conn = duckdb.connect(database=':memory:', read_only=False)

# Execute the query and fetch the results
result = conn.execute(query).fetchall()

# Execute the query
# result = duckdb.query(query).df()

end_time = time.time()  # Finish timing


# Print results
print('DuckDB')
# print(result)
print(f'Elapsed time: {end_time - start_time} seconds')
