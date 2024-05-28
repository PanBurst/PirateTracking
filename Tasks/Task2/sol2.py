import time
from functools import wraps
from multiprocessing import Process, Queue
from os import cpu_count
from queue import Empty
from typing import Callable, Any, Optional, List

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


class MongoProcess(Process):
    def __init__(self, data_queue: Queue) -> None:
        super(MongoProcess, self).__init__()
        self.data_queue = data_queue

    def run(self) -> None:
        client = MongoClient("mongodb://localhost:27141/")

        while True:
            try:
                chunk = self.data_queue.get(timeout=5)
                if chunk is None:
                    break
                insert(client, chunk)
            except Empty:
                break
        client.close()


def insert(client, chunk: pd.DataFrame) -> None:
    collection = client["sea"]["vessels"]
    data_chunk_dict = chunk.to_dict("records")
    collection.insert_many(data_chunk_dict)


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

    for chunk in pd.read_csv(csv_filepath, chunksize=chunk_size, nrows=n_rows):
        data_queue.put(chunk)

    for _ in range(max_workers):
        data_queue.put(None)

    _execute([MongoProcess(data_queue) for _ in range(max_workers)])


if __name__ == "__main__":
    insert_data_in_parallel(r'.\data\dataset\dataset.csv', max_workers=cpu_count() - 4)
    print("Done.")
