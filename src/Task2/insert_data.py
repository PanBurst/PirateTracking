from concurrent.futures import ProcessPoolExecutor
from os import cpu_count
from typing import Optional

import pandas as pd
from pymongo import MongoClient


def insert_data(data_chunk: pd.DataFrame) -> None:
    client = MongoClient("mongodb://localhost:27141/")
    collection = client["sea"]["vessels"]
    data_chunk_dict = data_chunk.to_dict("records")
    collection.insert_many(data_chunk_dict)
    client.close()


def read_csv_parallel(
        csv_filepath: str,
        num_cores: int = cpu_count(),
        chunk_size: int = 100000,
        n_rows: Optional[int] = None,
) -> None:
    chunks = pd.read_csv(csv_filepath, chunksize=chunk_size, nrows=n_rows)

    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        executor.map(insert_data, chunks)


if __name__ == "__main__":
    print("Starting CSV reading and insertion into MongoDB...")
    read_csv_parallel(r'.\data\dataset\dataset.csv', num_cores=5)
    print("CSV reading and insertion into MongoDB completed.")
