import pymongo


client = pymongo.MongoClient("mongodb://localhost:27017")

db = client["sea"]

collection = db["vessels"]


pipeline = [
    {
        "$group": {
            "_id": "$MMSI",
            "count": {"$sum": 1},
            "members": {"$push": "$$ROOT"}
        }
    }
]

result = collection.aggregate(pipeline)

for group in result:
    mmsi_value = group["_id"]
    count = group["count"]
    members = group["members"]
    
    print(f"MMSI: {mmsi_value}, Count: {count}")
    print("Members of the group:")
    for member in members:
        print(member)
        

client.close()