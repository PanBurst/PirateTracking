import random
from datetime import datetime

import matplotlib.pyplot as plt
from pymongo import MongoClient
from pymongo.collection import Collection
from tqdm import tqdm

TIME_STAMP_LIMIT = 100


def sample_mmsi_timestamp_ranges(collection: Collection) -> list[tuple[int, int]]:
    # Define an aggregation pipeline to group documents by the MMSI field
    # and count the number of documents for each MMSI.
    group_by_mmsi_query = [
        {
            "$group": {
                "_id": "$MMSI",
                "count": {"$sum": 1}
            }
        }
    ]

    # Execute the aggregation query on the collection and iterate over the results.
    # For each group (each unique MMSI), create a tuple consisting of the MMSI and
    # a random starting index for sampling timestamps, ensuring the range is within
    # the bounds of available documents (count - TIME_STAMP_LIMIT).
    return [
        (mmsi_group["_id"], random.choice(range(0, mmsi_group["count"] - TIME_STAMP_LIMIT)))
        for mmsi_group in collection.aggregate(group_by_mmsi_query)
    ]


def calculate_sample_time_deltas(collection: Collection, mmsi_ranges: list[tuple[int, int]]) -> list[float]:
    delta_t = []
    for mmsi, timestamp_skip in tqdm(mmsi_ranges):
        # Query the collection for documents with the specified MMSI,
        # skipping the specified number of timestamps, and limiting
        # the number of documents retrieved.
        sample = list(collection.find(
            {"MMSI": mmsi}, {"# Timestamp": 1}
        ).sort("# Timestamp", 1).skip(timestamp_skip).limit(TIME_STAMP_LIMIT))

        # Extract timestamps from the sample and convert them to datetime objects
        timestamps = [datetime.strptime(record["# Timestamp"], "%d/%m/%Y %H:%M:%S") for record in sample]

        # Calculate time deltas between consecutive timestamps and store them the list
        delta_t.extend(
            (timestamps[i] - timestamps[i - 1]).total_seconds()
            for i in range(1, len(timestamps))
        )

    return delta_t


def plot_histogram(delta_t: list[float]):
    plt.figure(figsize=(10, 6))
    plt.hist(delta_t, bins=1000, edgecolor="black")
    plt.title("Histogram of Time Differences (delta_t)")
    plt.xlabel("Time Difference (seconds)")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.savefig("histogram.png")
    plt.show()


def run() -> None:
    client = MongoClient("mongodb://localhost:27141")
    db = client["sea"]
    filtered_vessels = db["filtered_vessels"]
    filtered_vessels.create_index("MMSI")
    filtered_vessels.create_index("# Timestamp")

    mmsi_ranges = sample_mmsi_timestamp_ranges(filtered_vessels)
    delta_t = calculate_sample_time_deltas(filtered_vessels, mmsi_ranges)
    plot_histogram(delta_t)
    client.close()


if __name__ == "__main__":
    run()
