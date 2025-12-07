import os 
import time
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/api/mongodb/.env")
MONGODB_URI = os.getenv("MONGODB_API_KEY")

def Telemarketing():
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
    client = MongoClient(MONGODB_URI)
    db = client["Telemarketing"]

    collections = db.list_collection_names()

    for c in collections:
        count = db[c].estimated_document_count()
        print(f"{c}: {count}")

def Conversion():
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
    client = MongoClient(MONGODB_URI)
    db = client["CONVERSION"]

    collections = db.list_collection_names()

    for c in collections:
        count = db[c].estimated_document_count()
        print(f"{c}: {count}")

def J8MS_A8MS():
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
    client = MongoClient(MONGODB_URI)
    db = client["J8MS_A8MS"]

    collections = db.list_collection_names()

    for c in collections:
        count = db[c].estimated_document_count()
        print(f"{c}: {count}")

def UEA8():
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
    client = MongoClient(MONGODB_URI)
    db = client["UEA8"]

    collections = db.list_collection_names()

    for c in collections:
        count = db[c].estimated_document_count()
        print(f"{c}: {count}")

while True:
    print("Telemarketing")
    Telemarketing()
    print("\nConversion")
    Conversion()
    print("\nJ8MS_A8MS")
    J8MS_A8MS()
    print("\nUEA8")
    UEA8()

    time.sleep(5)