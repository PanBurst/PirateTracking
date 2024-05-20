import pandas as pd
from pymongo import MongoClient
import threading

# Function to insert data into MongoDB
def insert_data(data_chunk):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']
    collection = db['vessels']
    collection.insert_many(data_chunk)
    print(f"Inserted {len(data_chunk)} records")

# Load data from CSV
data = pd.read_csv(r'.\data\dataset\dataset.csv', nrows=1000)
data_dict = data.to_dict('records')

num_threads = 4

# Split data into chunks for each thread
chunk_size = len(data_dict) // num_threads
data_chunks = [data_dict[i:i + chunk_size] for i in range(0, len(data_dict), chunk_size)]

# Thread list
threads = []

# Create and start threads
for i in range(num_threads):
    thread = threading.Thread(target=insert_data, args=(data_chunks[i],))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All data inserted.")