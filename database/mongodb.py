# MongoDB saving/loading logicfrom pymongo import MongoClient
from config import MONGO_URI, DB_NAME, VIDEO_COLLECTION
from pymongo import MongoClient


client = MongoClient(MONGO_URI)
db = client[DB_NAME]
video_collection = db[VIDEO_COLLECTION]

def save_video_metadata(movie_id, name, year, language, video_path):
    try:
        result = video_collection.insert_one({
            "_id": movie_id,
            "name": name,
            "year": year,
            "language": language,
            "video_path": video_path
        })
        print(f"✅ MongoDB Inserted ID: {result.inserted_id}")
    except Exception as e:
        print(f"❌ MongoDB Insert Failed: {e}")
