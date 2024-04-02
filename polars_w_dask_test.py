import dask.dataframe as dd
import polars as pl
import time


def process_partition(dask_partition):
    # Convert the Dask DataFrame partition to a Polars DataFrame
    polars_df = pl.from_pandas(dask_partition)
    # Perform Polars operations on this partition
    result_df = (
        polars_df.group_by("station_name")
        .agg([
            pl.col("measurement").min().alias("Min"),
            pl.col("measurement").max().alias("Max"),
            pl.col("measurement").mean().alias("Mean"),
        ])
        .sort("station_name")
    )
    return result_df


file_path = '~/projects/1brc/data/measurements.txt'

column_names = ['station_name', 'measurement']
# Specify dtypes
dtypes = {
    'station_name': object,
    'measurement': float
}

start_time = time.time()  # Start timing

dask_df = dd.read_csv(
    file_path,
    sep=';',
    names=column_names,
    header=None,
    dtype=dtypes
    )

# Define meta data for Dask to know the structure of the output DataFrame after processing
meta = {'station_name': 'object', 'Min': 'float32', 'Max': 'float32', 'Mean': 'float32'}

start_time = time.time()  # Start timing

# Use `map_partitions` to apply processing to each partition
processed = dask_df.map_partitions(process_partition, meta=meta)

# Trigger computation to get the final result as a Pandas DataFrame
final_result = processed.compute()

# Measure and print elapsed time
elapsed_time = time.time() - start_time
print('Polars with Dask')
print(f"Elapsed time: {elapsed_time} seconds")

