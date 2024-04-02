import pandas as pd
import time

file_path = '~/projects/1brc/data/measurements.txt'

column_names = ['station_name', 'measurement']
# Specify dtypes
dtypes = {
        'station_name': 'string',
        'measurement': 'float32'
}

start_time = time.time()  # Start timing

# Read the file with the specified dtypes
df = pd.read_csv(file_path, sep=';', dtype=dtypes, header=None, names=column_names)

# Perform aggregations
sorted_df = (
        df.groupby('station_name')['measurement']
        .agg(Min='min', Max='max', Avg='mean')
        .sort_values(by='station_name')
)

end_time = time.time()  # Finish timing

# Print results
print('Pandas')
# print(sorted_df)
print(f'Elapsed time: {end_time - start_time} seconds')

