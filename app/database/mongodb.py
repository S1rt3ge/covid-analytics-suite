from pymongo import MongoClient
from app.config import MONGO_URI, MONGO_DB, MONGO_COUNTRY_COL

def _mongo():
    if not MONGO_URI:
        raise RuntimeError("MONGODB_URI not set")
    cli = MongoClient(MONGO_URI)
    return cli[MONGO_DB]

def get_country_stats_collection():
    db = _mongo()
    return db[MONGO_COUNTRY_COL]