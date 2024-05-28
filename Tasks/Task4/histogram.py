from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime
import matplotlib.pyplot as plt
import random
from tqdm import tqdm

TIME_STAMP_LIMIT = 100

def sample_mmsi_timestamp_ranges(collection: Collection) -> list[tuple[int, int]]:
    group_by_mmsi_query = [
        {
            '$group': {
                '_id': '$MMSI',
                'count': {'$sum': 1}
            }
        }
    ]

    mmsi_ranges = []
    mmsi_groups = list(collection.aggregate(group_by_mmsi_query))

    for mmsi_group in mmsi_groups:
        upper_mmsi_range_limit = mmsi_group['count'] - TIME_STAMP_LIMIT
        timestamp_skip = random.choice(range(0, upper_mmsi_range_limit))
        mmsi_ranges.append((mmsi_group["_id"], timestamp_skip))

    # mmsi_ranges = random.choices(mmsi_ranges, k=1000)
    return mmsi_ranges


def to_datetime(date_str):
    return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')

def calculate_sample_time_deltas(collection: Collection, mmsi_ranges: list[tuple[int, int]]) -> list[int]:
    delta_t = []
    for (mmsi, timestamp_skip) in tqdm(mmsi_ranges):
        sample = list(collection.find({"MMSI": mmsi}, {"# Timestamp": 1}) \
                                        .sort("# Timestamp", 1) \
                                        .skip(timestamp_skip).limit(TIME_STAMP_LIMIT))
        
        last_time_record = to_datetime(sample[0]["# Timestamp"])
        for record in sample[1:]:
            current_time_record = to_datetime(record["# Timestamp"])
            time_delta_seconds = (current_time_record - last_time_record).total_seconds()
            delta_t.append(time_delta_seconds)
            last_time_record = current_time_record
    
    return delta_t


def plot_histogram(delta_t: list[int]):
    plt.figure(figsize=(10, 6))
    plt.hist(delta_t, bins=1000, edgecolor='black')
    plt.title('Histogram of Time Differences (delta_t)')
    plt.xlabel('Time Difference (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27141")
    db = client["sea"]
    filtered_vessels = db["filtered_vessels"]
    filtered_vessels.create_index("MMSI")
    filtered_vessels.create_index("# Timestamp")

    mmsi_ranges = sample_mmsi_timestamp_ranges(filtered_vessels)
    delta_t = calculate_sample_time_deltas(filtered_vessels, mmsi_ranges)
    plot_histogram(delta_t)
    client.close()
