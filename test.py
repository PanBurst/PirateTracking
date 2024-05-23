import pandas as pd
import numpy as np
from pymongo import MongoClient
from multiprocessing import Pool, Manager, Lock
import math
from tqdm import tqdm
from functools import partial

def include_document(document):
    if math.isnan(document["MMSI"]):
        return False
    if document["Navigational status"] == "Unknown value":
        return False
    if math.isnan(document["Latitude"]):
        return False
    if math.isnan(document["Longitude"]):
        return False
    if math.isnan(document["ROT"]):
        return False
    if math.isnan(document["SOG"]):
        return False
    if math.isnan(document["COG"]):
        return False
    if math.isnan(document["Heading"]):
        return False
    return True

def filter_data(skip_n: int, limit_n: int) -> tuple[dict[int, int], list[dict]]:
    client = MongoClient('mongodb://localhost:27141/')
    db = client['sea']
    collection = db['vessels']
    mmsi_instance_counts: dict[str, int] = {}
    filtered_data = []
    cursor = collection.find({}).skip(skip_n).limit(limit_n)
    for document in cursor:
        if include_document(document):
            mmsi = document['MMSI']
            mmsi_instance_counts[mmsi] = mmsi_instance_counts.get(mmsi, 0) + 1
            filtered_data.append(document)

    client.close()
    return (mmsi_instance_counts, filtered_data)

def process_chunk(lock, list, chunk_range: tuple[int, int]) -> list[dict]:
    _, results = filter_data(chunk_range[0], chunk_range[1])
    with lock:
        list.extend(results)
        print(len(list))



def safe_append(lock, shared_list, data):
    with lock:
        shared_list.extend(data)

def filter_database():
    manager = Manager()
    shared_list = manager.list()
    lock = manager.Lock()

    client = MongoClient('mongodb://localhost:27141/')
    db = client['sea']
    collection = db['vessels']
    total_document_count = collection.count_documents({})
    client.close()

    chunk_size = 100_000
    chunk_ranges = [(i, min(i + chunk_size, total_document_count)) for i in range(0, total_document_count, chunk_size)]
    partial_process_chunk = partial(process_chunk, lock, shared_list)
    with Pool(processes=6) as pool:
        pool.map(partial_process_chunk, chunk_ranges)
        
        pool.close()
        pool.join()

    return shared_list

if __name__ == '__main__':
    # Process the database in chunks
    processed_data = filter_database()
    tmp = 1
