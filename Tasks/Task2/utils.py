import time
from functools import wraps
from typing import Callable, Any


def timeit(function: Callable) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.perf_counter()
        function(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {function.__name__} took {total_time:.4f} seconds")
        return total_time

    return wrapper
