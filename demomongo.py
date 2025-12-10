import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/api/mongodb/.env")
MONGODB_URI = os.getenv("MONGODB_API_KEY")

def print_db_collections(db_name):
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")

    client = MongoClient(MONGODB_URI)
    db = client[db_name]

    # SORT collections by ASC
    collections = sorted(db.list_collection_names())

    print(f"\n{db_name}")
    for c in collections:
        count = db[c].estimated_document_count()
        print(f"{c}: {count}")


while True:
    print_db_collections("Telemarketing")
    print_db_collections("CONVERSION")
    print_db_collections("J8MS_A8MS")
    print_db_collections("UEA8")

    time.sleep(10)