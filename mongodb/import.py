import csv
import time
from pymongo import MongoClient
from pymongo.errors import OperationFailure

client = MongoClient('mongodb://172.25.0.7:27017/catchem')

db = client.catchem
collection = db.treasure_stages
print(collection.count_documents({}))
print(db.command("dbstats"))

# Read and insert CSV data into MongoDB
with open('treasure_stages.csv', 'r') as file:
    header = [x.strip("\n").strip("\"") for x in next(file).split(",")]
    print(header)
    csv_reader = csv.DictReader(file, fieldnames=header, delimiter=',')
    for row in csv_reader:
        print(row)
        record = collection.insert_one(row)
        print(record.inserted_id)

# Close the connection
client.close()
