# Kafka, MongoDB, and other config settings# config.py
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "video-uploaded"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "video_db"
VIDEO_COLLECTION = "videos"

UPLOAD_FOLDER = "uploads"
