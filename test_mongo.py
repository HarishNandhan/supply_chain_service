from pymongo import MongoClient
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Load .env reliably (works even if you run from different folders)
dotenv_path = find_dotenv(filename="configs/.env", usecwd=True)
if not dotenv_path:
    raise FileNotFoundError("Could not find configs/.env from current working directory.")
load_dotenv(dotenv_path=dotenv_path)

# Read env
MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DATABASE")
MONGO_COLLECTION = os.getenv("MONGODB_COLLECTION")

# Guard clauses for missing vars
missing = [k for k in ["MONGODB_URI", "MONGODB_DATABASE", "MONGODB_COLLECTION"] if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required env var(s): {', '.join(missing)}. "
                       f"Loaded from: {dotenv_path}\n"
                       f"Tip: ensure keys match exactly and have no spaces around '='.")

# Connect & test
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

for doc in collection.find().limit(5):
    print(doc)
