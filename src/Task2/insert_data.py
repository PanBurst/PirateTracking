import pandas as pd
from pymongo import MongoClient
import threading
from multiprocessing import Pool, cpu_count

# Function to insert data into MongoDB
def insert_data(data_chunk):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']
    collection = db['vessels']
    collection.insert_many(data_chunk)
    print(f"Inserted {len(data_chunk)} records")


def process_chunk(chunk):
    # Process each chunk as needed
    # For example, you can filter, transform, or analyze the data
    # Here, I'm just printing the length of each chunk
    print(len(chunk))
    # You can return the processed data if needed

def read_csv_parallel(filename):
    # Determine the number of CPU cores available
    num_cores = cpu_count()

    # Create a multiprocessing Pool
    pool = Pool(num_cores)

    # Read the CSV file in chunks
    chunks = pd.read_csv(filename, chunksize=10000)  # Adjust chunksize as needed

    # Apply the process_chunk function to each chunk in parallel
    pool.map(process_chunk, chunks)

    # Close the pool to release resources
    pool.close()
    pool.join()

if __name__ == "__main__":
    # Replace 'filename.csv' with the path to your CSV file
    read_csv_parallel(r'.\data\dataset\dataset.csv')