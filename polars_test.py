import polars as pl
import time


file_path = '~/projects/1brc/data/measurements.txt'

column_names = ['station_name', 'measurement']
# Specify dtypes
dtypes = {
    'station_name': pl.Utf8,
    'measurement': pl.Float32
}

start_time = time.time()  # Start timing

df = (
    # Read the file with the specified dtypes
    pl.scan_csv(
        file_path, 
        separator=';', 
        dtypes=dtypes,
        new_columns=column_names)
    # Perform aggregations
    .group_by("station_name")
    .agg([
        pl.col('measurement').min().alias('Min'),
        pl.col('measurement').max().alias('Max'),
        pl.col('measurement').mean().alias('Mean')
    ])
    .sort('station_name')
    .collect()
)

end_time = time.time()  # Finish timing

# Print results
print('Polars')
# print(df)
print(f'Elapsed time: {end_time - start_time} seconds')
