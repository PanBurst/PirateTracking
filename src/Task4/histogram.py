import pymongo


client = pymongo.MongoClient("mongodb://localhost:27141")

db = client["sea"]

collection = db["vessels"]

unique_MMSI = collection.distinct("MMSI") # this is needed since grouping query will exceed memory limit
tmp = 1
for group in result:
    mmsi_value = group["_id"]
    count = group["count"]
    members = group["members"]
    
    print(f"MMSI: {mmsi_value}, Count: {count}")
    print("Members of the group:")
    for member in members:
        print(member)
        

client.close()