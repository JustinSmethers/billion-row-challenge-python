import pandas as pd
import os
from multiprocessing import Pool, cpu_count, Lock, Value
import time

file_path = os.path.expanduser('~/projects/1brc/data/measurements_100M.txt')
column_names = ['station_name', 'measurement']
dtypes = {
    'station_name': 'string',
    'measurement': 'float32'
}

counter = Value('i', 0)
lock = Lock()

def process_chunk(args):
    with lock:
        counter.value += 1
        print(f'Processed {counter.value} chunks')
    chunk_start, chunk_size = args
    try:
        chunk = pd.read_csv(
            file_path,
            sep=';',
            dtype=dtypes,
            header=None,
            names=column_names,
            skiprows=chunk_start,
            nrows=chunk_size,
            engine='python'
        )
        aggregated = (
            chunk.groupby('station_name')['measurement']
            .agg(Min='min', Max='max', Avg='mean')
        ).reset_index()
        return aggregated
    except Exception as e:
        print(f"Error processing chunk starting at {chunk_start}: {e}")
        return pd.DataFrame(columns=column_names + ['Min', 'Max', 'Avg'])

def main():
    start_time = time.time()
    
    chunk_size = 50000  # Adjust based on your system's memory and the file's characteristics
    total_rows = sum(1 for _ in open(file_path)) - 1
    num_chunks = (total_rows // chunk_size) + 1
    chunks = [(i * chunk_size + 1, chunk_size) for i in range(num_chunks)]

    print('starting multiprocessing')
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_chunk, chunks)
    
    print('collecting result')
    final_result = pd.concat(results).groupby('station_name').agg({'Min': 'min', 'Max': 'max', 'Avg': 'mean'}).sort_values(by='station_name')
    
    end_time = time.time()
    print('Pandas with multiprocessing')
    print(f"Total time taken: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()

