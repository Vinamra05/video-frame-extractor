import os
import json
import faiss
import pickle
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer
from pymongo import MongoClient
from bson import ObjectId

# MongoDB setup
client = MongoClient("mongodb://localhost:27017")
db = client["video_db"]
frames_collection = db["frames"]

# FAISS and id_map setup
INDEX_DIR = "vector_store"
os.makedirs(INDEX_DIR, exist_ok=True)
index_path = os.path.join(INDEX_DIR, "faiss_index.index")
id_map_path = os.path.join(INDEX_DIR, "id_map.pkl")

# Load or initialize FAISS
embedding_dim = 384
model = SentenceTransformer("all-MiniLM-L6-v2")

if os.path.exists(index_path) and os.path.exists(id_map_path):
    index = faiss.read_index(index_path)
    with open(id_map_path, "rb") as f:
        id_map = pickle.load(f)
    print("📦 FAISS index loaded.")
else:
    index = faiss.IndexFlatIP(embedding_dim)
    id_map = []
    print("📦 New FAISS index created.")

# Kafka Consumer
consumer = KafkaConsumer(
    "frame-captioned",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="vector_indexer"
)

print("🧠 Vector Indexer Kafka consumer running...")

# Consume and store vectors
for msg in consumer:
    data = msg.value
    frame_id = data.get("frame_id")

    frame = frames_collection.find_one({"_id": ObjectId(frame_id)})
    if not frame or frame.get("vector_stored"):
        print(f"⚠️ Skipped frame {frame_id} (missing or already processed)")
        continue

    caption = frame.get("caption", "").strip()
    if not caption or len(caption) < 10:
        print(f"⚠️ Weak caption for frame {frame_id}, skipping")
        continue

    vector = model.encode([caption])[0]
    faiss.normalize_L2(vector.reshape(1, -1))
    index.add(vector.reshape(1, -1))

    id_map.append({
        "frame_id": str(frame["_id"]),
        "movie_id": frame["movie_id"],
        "frame_path": frame["frame_path"],
        "timestamp": frame["timestamp"],
        "caption": caption
    })

    frames_collection.update_one({"_id": frame["_id"]}, {"$set": {"vector_stored": True}})

    faiss.write_index(index, index_path)
    with open(id_map_path, "wb") as f:
        pickle.dump(id_map, f)

    print(f"✅ Indexed frame {frame_id} | Caption: {caption[:150]}")
    print(f"📊 Total vectors: {index.ntotal}")
