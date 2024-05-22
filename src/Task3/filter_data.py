import math
import numpy as np
from pymongo import MongoClient
from multiprocessing import Pool, cpu_count

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
    client = MongoClient('mongodb://localhost:27017/')
    db = client['sea']
    collection = db['vessels']
    mmsi_instance_counts: dict[str, int] = {}
    filtered_data = []
    cursor = collection.find({}).skip(skip_n).limit(limit_n)
    for document in cursor:
        if include_document(document) == True:
            mmsi = document['MMSI']
            mmsi_instance_counts[mmsi] = mmsi_instance_counts.get(mmsi, 0) + 1
            filtered_data.append(document)

    client.close()
    return (mmsi_instance_counts, filtered_data)

def insert_data(documents: list, valid_mmsi_instances: set[int]):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['sea']
    collection = db['filtered_vessels']

    filtered_documents = []
    for document in documents:
        if document['MMSI'] in valid_mmsi_instances:
            filtered_documents.append(document)

    collection.insert_many(filtered_documents)
    client.close()

def filter_data_paralel():
    num_cores = 4
    pool = Pool(num_cores)

    client = MongoClient('mongodb://localhost:27017/')
    db = client['sea']
    collection = db['vessels']
    # total_document_count = collection.count_documents({})
    total_document_count = 3000000

    chunk_size = math.ceil(total_document_count / num_cores)

    pending_results = []
    for i in range(num_cores):
        skip_n = i * chunk_size
        limit_n = min((i+1) * chunk_size, total_document_count) - skip_n
        pending_result = pool.apply_async(filter_data, (skip_n, limit_n))
        pending_results.append(pending_result)

    results: list[tuple[dict[int, int], list[dict]]] = [pending_result.get() for pending_result in pending_results]

    total_mmsi_instance_counts: dict[int, int] = {}
    for (mmsi_instance_counts, _) in results:
        for (mmsi_instance, count) in mmsi_instance_counts.items():
            total_mmsi_instance_counts[mmsi_instance] = total_mmsi_instance_counts.get(mmsi_instance, 0) + count

    valid_mmsi_instances: set[int] = set()
    for (mmsi_instance, count) in total_mmsi_instance_counts.items():
        if count >= 100:
            valid_mmsi_instances.add(mmsi_instance)
    
    for (_, documents) in results:
        pool.apply(insert_data, (documents, valid_mmsi_instances))

    pool.close()
    pool.join()

if __name__ == "__main__":
    filter_data_paralel()