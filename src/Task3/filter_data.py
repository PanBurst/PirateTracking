from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import math

VALID_DOCUMENT_COUNT = 100

def create_filtered_vessels_view(db: Database, filter_query) -> Collection:
    db.drop_collection("vessels_view")
    vessels_view = db.create_collection(
        "vessels_view",
        viewOn="vessels",
        pipeline=filter_query
    )

    return vessels_view


def get_valid_mmsi_ids(vessels_view: Collection) -> list[int]:
    group_by_mmsi_query = [
        {
            "$group": {
                "_id": "$MMSI",
                "count": {"$sum": 1}
            }
        }
    ]
    mmsi_groups = vessels_view.aggregate(group_by_mmsi_query)

    return [mmsi_group["_id"] for mmsi_group in mmsi_groups if mmsi_group["count"] >= VALID_DOCUMENT_COUNT]


def insert_valid_documents(db: Database, vessels_view: Collection, valid_mmsi_ids: list[int]) -> Collection:
    insert_valid_documents_query = [
        {
            "$match": {
                "MMSI": {"$in": valid_mmsi_ids}
            }
        },
        {
            "$out": "filtered_vessels"
        }
    ]

    db.drop_collection("filtered_vessels")
    vessels_view.aggregate(insert_valid_documents_query)
    return db["filtered_vessels"]


def filter_data():
    client = MongoClient('mongodb://localhost:27141/')
    db = client['sea']
    db['vessels'].create_index("MMSI")

    print("Creating filtered vessels view...")
    filter_query = [
        {
            "$match": {
                "$and": [
                    {"MMSI": {"$ne": math.nan}},
                    {"Heading": {"$ne": math.nan}},
                    {"Navigational status": {"$ne": "Unknown value", "$ne": math.nan}},
                    {"Latitude": {"$ne": math.nan}},
                    {"Longitude": {"$ne": math.nan}},
                    {"ROT": {"$ne": math.nan}},
                    {"SOG": {"$ne": math.nan}},
                    {"COG": {"$ne": math.nan}},
                    {"Heading": {"$ne": math.nan}}
                ]
            }
        }
    ]
    vessels_view = create_filtered_vessels_view(db, filter_query)
    print("Filtered vessels view created.")

    print("Getting valid MMSI IDs...")
    valid_mmsi_ids = get_valid_mmsi_ids(vessels_view)
    print("Valid MMSI IDs obtained.")

    print("Inserting valid documents...")
    insert_valid_documents(db, vessels_view, valid_mmsi_ids)
    print("Valid documents inserted.")
    
    client.close()

if __name__ == "__main__":
    filter_data()