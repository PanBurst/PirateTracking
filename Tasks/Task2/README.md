# Task 2: Data Insertion in Parallel

Two solutions were implemented for parallel data insertion:

## Solution 1: Using concurrent.futures.ProcessPoolExecutor

In this approach, the `insert_to_db` function is executed on each chunk of data using
the `ProcessPoolExecutor`. While this method is straightforward, it has a slight drawback:
it creates a new `MongoClient` instance for each chunk of data being processed.

## Solution 2: Pool of custom processes (`InsertProcess`)

In this approach, the same number of `MongoClient` objects are created as there are processes in the pool.
Each process in this pool, encapsulated in the `InsertProcess` class, maintains its own `MongoClient` instance.
This approach avoids the repetitive creation and deletion of client instances.
