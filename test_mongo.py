from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv("configs/.env")

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DATABASE")
MONGO_COLLECTION = os.getenv("MONGODB_COLLECTION")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

for doc in collection.find().limit(5):
    print(doc)
