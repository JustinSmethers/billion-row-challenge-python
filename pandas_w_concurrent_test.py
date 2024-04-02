import pandas as pd
import time
import concurrent.futures
import os

file_path = os.path.expanduser('~/projects/1brc/data/measurements_300M.txt')

column_names = ['station_name', 'measurement']
dtypes = {
    'station_name': 'string',
    'measurement': 'float32'
}

def process_chunk(chunk_start, chunk_size):
    """
    Function to process a chunk of the file.
    """
    chunk = pd.read_csv(
        file_path,
        sep=';',
        dtype=dtypes,
        header=None,
        names=column_names,
        skiprows=chunk_start,
        nrows=chunk_size
    )
    aggregated = (
        chunk.groupby('station_name')['measurement']
        .agg(Min='min', Max='max', Avg='mean')
    )
    return aggregated

def combine_aggregations(aggregations):
    """
    Combine aggregations from all chunks.
    """
    combined = pd.concat(aggregations)
    final_result = combined.groupby(combined.index).agg(Min=('Min', 'min'), Max=('Max', 'max'), Avg=('Avg', 'mean'))
    return final_result.sort_values(by='station_name')

if __name__ == "__main__":
    start_time = time.time()

    # Determine optimal chunk size; you may need to adjust this.
    chunk_size = 1000000  # Number of rows per chunk
    total_rows = sum(1 for _ in open(file_path)) - 1  # Adjust for header if present
    num_chunks = (total_rows // chunk_size) + 1

    # Use multiprocessing to process chunks
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, i*chunk_size, chunk_size) for i in range(num_chunks)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    sorted_df = combine_aggregations(results)

    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")

