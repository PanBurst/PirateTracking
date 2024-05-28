from concurrent.futures import ProcessPoolExecutor
from os import cpu_count
from typing import Optional

import pandas as pd
from pymongo import MongoClient

from utils import timeit


def insert_to_db(data_chunk: pd.DataFrame) -> None:
    # Create a client, insert chunk, close connection
    client = MongoClient("mongodb://localhost:27141/")
    collection = client["sea"]["vessels"]
    data_chunk_dict = data_chunk.to_dict("records")
    collection.insert_many(data_chunk_dict)
    client.close()


@timeit
def insert_data_in_parallel(
        csv_filepath: str,
        max_workers: int = cpu_count(),
        chunk_size: int = 100000,
        n_rows: Optional[int] = None,
) -> None:
    # Split the data into smaller chunks
    chunks = pd.read_csv(csv_filepath, chunksize=chunk_size, nrows=n_rows)

    # Create a process pool to insert all chunks
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        executor.map(insert_to_db, chunks)


if __name__ == "__main__":
    insert_data_in_parallel(r".\data\dataset\dataset.csv", max_workers=cpu_count() - 4)
    print("Done.")
