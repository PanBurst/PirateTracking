import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from functools import partial

def create_client() -> MongoClient:
    return MongoClient('mongodb://localhost:27141/')



def insert_data(data_chunk):

    client = create_client()
    db = client['sea']
    collection = db['vessels']
    data_chunk_dict = data_chunk.to_dict('records')
    collection.insert_many(data_chunk_dict)
    print(f"Inserted {len(data_chunk_dict)} records")
    client.close()

def process_chunk(chunk):
    insert_data(chunk)

def read_csv_parallel(filename):
    num_cores = 5 #cpu_count()

    pool = Pool(num_cores)

    chunks = pd.read_csv(filename, chunksize=100000)
    pool.map(process_chunk, chunks)

    pool.close()
    pool.join()

if __name__ == "__main__":
    print("Starting CSV reading and insertion into MongoDB...")
    read_csv_parallel(r'.\data\dataset\dataset.csv')
    print("CSV reading and insertion into MongoDB completed.")
