import time
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from os import cpu_count
from typing import Callable, Any, Optional

import pandas as pd
from pymongo import MongoClient


def timeit(function: Callable) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.perf_counter()
        function(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {function.__name__} "
              f"with args {[v.__name__ for v in kwargs.values() if hasattr(v, '__name__')]} "
              f"took {total_time:.4f} seconds")
        return total_time

    return wrapper


def insert_data(data_chunk: pd.DataFrame) -> None:
    client = MongoClient("mongodb://localhost:27141/")
    collection = client["sea"]["vessels"]
    data_chunk_dict = data_chunk.to_dict("records")
    collection.insert_many(data_chunk_dict)
    client.close()


@timeit
def read_csv_parallel(
        csv_filepath: str,
        max_workers: int = cpu_count(),
        chunk_size: int = 100000,
        n_rows: Optional[int] = None,
) -> None:
    chunks = pd.read_csv(csv_filepath, chunksize=chunk_size, nrows=n_rows)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        executor.map(insert_data, chunks)


if __name__ == "__main__":
    print("Starting CSV reading and insertion into MongoDB...")
    read_csv_parallel(r'.\data\dataset\dataset.csv', max_workers=cpu_count() - 4)
    print("CSV reading and insertion into MongoDB completed.")
