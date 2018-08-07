from pymongo import MongoClient

MONGO_DB_URL = 'localhost'
MONGO_DB_PORT = 27017
DB_NAME = 'Test'

client = MongoClient(MONGO_DB_URL, MONGO_DB_PORT)

def get_db(db=DB_NAME):
    db = client[db]
    return db
