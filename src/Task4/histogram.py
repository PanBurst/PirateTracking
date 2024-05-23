import pymongo


client = pymongo.MongoClient("mongodb://localhost:27141")

db = client["sea"]

collection = db["vessels"]

unique_MMSI = collection.distinct("MMSI") 

pipeline = [
    {
        '$sort': {'MMSI': 1, '# Timestamp': 1}  # Sort by MMSI and Time ascending
    },
    {
        '$group': {
            '_id': '$MMSI',
            'records': {
                '$push': '$$ROOT'  # Push entire document to the records array
            }
        }
    },
    {
        '$project': {
            'MMSI': '$_id',
            'records': {
                '$slice': ['$records', 3]  # Take the first 3 records
            }
        }
    },
    {
        '$unwind': '$records'  # Unwind the records array to get individual documents
    },
    {
        '$replaceRoot': { 'newRoot': '$records' }  # Replace the root with the records documents
    }
]

# Execute the aggregation
result = list(collection.aggregate(pipeline))

client.close()