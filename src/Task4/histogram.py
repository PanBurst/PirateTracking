from tqdm import tqdm
import pymongo
from datetime import datetime
import matplotlib.pyplot as plt

client = pymongo.MongoClient("mongodb://localhost:27141")

db = client["sea"]


collection = db["filtered_vesells"]


import random


pipeline = [
    {
        '$group': {
            '_id': '$MMSI',
            'record_count': {'$sum': 1}
        }
    }
]

MMSI_Ranges = []
result = list(collection.aggregate(pipeline))

range_limit = 4
for record in result:
    if record['record_count'] > range_limit+1:
        MMSI_Ranges.append((record["_id"],random.choice(range(1, record['record_count'] - range_limit))))


random_MMSI_ranges = random.choices(MMSI_Ranges, k=100)
delta_t = []



def to_datetime(date_str):
    return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')

for (MMSI, range_index) in tqdm(random_MMSI_ranges):
    results = list(collection.find({"MMSI": MMSI}).sort("# Timestamp", 1).skip(range_index).limit(range_limit))
    for idx, result in enumerate(results):
        if idx == 0:
            continue
        delta_t.append((to_datetime(result["# Timestamp"]) - to_datetime(results[idx-1]["# Timestamp"])).total_seconds())

client.close()


plt.figure(figsize=(10, 6))
plt.hist(delta_t, bins=50, edgecolor='black')
plt.title('Histogram of Time Differences (delta_t)')
plt.xlabel('Time Difference (seconds)')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()
tmp = 1