from multiprocessing import Process, Queue
from os import cpu_count
from queue import Empty
from typing import Optional, List

import pandas as pd
from pymongo import MongoClient

from utils import timeit


class InsertProcess(Process):
    def __init__(self, data_queue: Queue) -> None:
        super(InsertProcess, self).__init__()
        self.data_queue = data_queue

    def run(self) -> None:
        client = MongoClient("mongodb://localhost:27141/")

        # Continuously take chucks from the queue and insert into the database
        while True:
            try:
                chunk = self.data_queue.get(timeout=1)

                if chunk is None:
                    break

                collection = client["sea"]["vessels"]
                data_chunk_dict = chunk.to_dict("records")
                collection.insert_many(data_chunk_dict)

            except Empty:
                pass

        client.close()


def _execute(executors: List[Process]) -> None:
    for executor in executors:
        executor.start()

    for executor in executors:
        executor.join()


@timeit
def insert_data_in_parallel(
        csv_filepath: str,
        max_workers: int = cpu_count(),
        chunk_size: int = 100000,
        n_rows: Optional[int] = None,
) -> None:
    data_queue = Queue()

    # Split the data into smaller chunks and put it in a queue
    for chunk in pd.read_csv(csv_filepath, chunksize=chunk_size, nrows=n_rows):
        data_queue.put(chunk)

    # Add the stopping condition for each process
    for _ in range(max_workers):
        data_queue.put(None)

    # Create and execute all insertion processes
    _execute([InsertProcess(data_queue) for _ in range(max_workers)])


if __name__ == "__main__":
    insert_data_in_parallel(r".\data\dataset\dataset.csv", n_rows=1000000)
    print("Done.")
