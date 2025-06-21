from flask import Flask, request, jsonify, send_file
from sentence_transformers import SentenceTransformer
from pymongo import MongoClient
import faiss
import pickle
import os
import numpy as np

app = Flask(__name__)

# MongoDB Setup
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["video_db"]
frames_collection = db["frames"]
videos_collection = db["videos"]

# FAISS Index & ID Map
INDEX_DIR = "vector_store"
index_path = os.path.join(INDEX_DIR, "faiss_index.index")
id_map_path = os.path.join(INDEX_DIR, "id_map.pkl")

if not os.path.exists(index_path) or not os.path.exists(id_map_path):
    raise FileNotFoundError("FAISS index or ID map not found.")

index = faiss.read_index(index_path)
with open(id_map_path, "rb") as f:
    id_map = pickle.load(f)

# Embedding Model
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")


def timestamp_to_seconds(timestamp):
    parts = list(map(int, timestamp.split(":")))
    if len(parts) == 3:
        h, m, s = parts
        return h * 3600 + m * 60 + s
    elif len(parts) == 2:
        m, s = parts
        return m * 60 + s
    return int(parts[0])  # fallback


# Smart Search API
@app.route("/smart-search", methods=["POST"])
def smart_search():
    data = request.get_json()
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "Missing 'query' field"}), 400

    query_vector = embedding_model.encode([query])[0]
    faiss.normalize_L2(query_vector.reshape(1, -1))
    query_vector = query_vector.reshape(1, -1)

    k = 10
    similarity_threshold = 0.5  # Cosine similarity threshold

    distances, indices = index.search(query_vector, k)

    results = []
    for dist, idx in zip(distances[0], indices[0]):
        if idx >= len(id_map):
            continue

        similarity = dist  # Already normalized cosine similarity
        if similarity < similarity_threshold:
            continue

        frame_meta = id_map[idx]
        short_caption = frame_meta["caption"].strip().split("\n")[0][:120]
        movie = videos_collection.find_one({"_id": frame_meta["movie_id"]})

        if not movie:
            continue

        seconds = max(0, timestamp_to_seconds(frame_meta["timestamp"]) - 3)
        scene_url = f"http://localhost:5001/play-scene?movie_id={frame_meta['movie_id']}#t={seconds}"


        results.append({
            "caption": short_caption,
            "timestamp": frame_meta["timestamp"],
            "frame_path": frame_meta["frame_path"],
            "similarity": round(float(similarity), 3),
            "scene_url": scene_url,
            "movie": {
                "id": str(frame_meta["movie_id"]),
                "name": movie.get("name", "Unknown"),
                "year": movie.get("year", "Unknown"),
                "language": movie.get("language", "Unknown")
            }
        })

    if not results:
        return jsonify({"message": "No relevant scenes found for your query."}), 200

    return jsonify({"results": results})


# Route to serve video playback
@app.route("/play-scene")
def play_scene():
    from flask import send_file
    movie_id = request.args.get("movie_id")
    if not movie_id:
        return "Missing movie_id", 400

    movie = videos_collection.find_one({"_id": movie_id})
    if not movie:
        return "Movie not found", 404

    video_path = movie.get("video_path")
    if not video_path or not os.path.exists(video_path):
        return "Video file not found", 404

    return send_file(video_path, mimetype="video/mp4")


if __name__ == "__main__":
    app.run(debug=True, port=5001)
