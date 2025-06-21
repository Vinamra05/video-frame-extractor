import os
import cv2
import json
import uuid
import base64
import datetime
from kafka import KafkaConsumer
import google.generativeai as genai
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
load_dotenv()
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer

# Add at the top
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# --- Environment Config ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

# MongoDB setup
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["video_db"]
frames_collection = db["frames"]
videos_collection = db["videos"]

# Frame save path
FRAME_FOLDER = "frames"
os.makedirs(FRAME_FOLDER, exist_ok=True)

# Gemini model
caption_model = genai.GenerativeModel("gemini-2.5-flash")


# --- Caption Generator ---
from concurrent.futures import ThreadPoolExecutor, as_completed

def generate_caption(frame_path):
    with open(frame_path, "rb") as img_file:
        image_data = base64.b64encode(img_file.read()).decode("utf-8")

    response = caption_model.generate_content([
       { "text": (
            "Describe the scene in this video frame vividly and specifically:\n"
            "- Focus on what people are doing (actions, expressions, interactions).\n"
            "- Mention appearance, clothing, or props if visible.\n"
            "- If the relationship between characters is shown in the frame then include in the caption.\n"
            "- If a known actor is recognizable in the frame, include their name.\n"
            "Respond in one detailed sentence. Avoid vague or generic terms."
        )},
        {"inline_data": {"mime_type": "image/jpeg", "data": image_data}}
    ])

    return response.text.strip()

def generate_and_store_caption(frame_doc_id, frame_path):
    try:
        caption = generate_caption(frame_path)
        frames_collection.update_one(
            {"_id": frame_doc_id},
            {"$set": {"caption": caption}}
        )
        producer.send("frame-captioned", {"frame_id": str(frame_doc_id)})
        print(f"🧠 Captioned frame: {caption}")
    except Exception as e:
        print(f"❌ Captioning failed for {frame_path}: {e}")

def extract_and_enrich_frames(video_path, movie_id):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_interval = int(fps * 5)

    count = 0
    frame_num = 0
    tasks = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            if count % frame_interval == 0:
                timestamp = str(datetime.timedelta(seconds=int(count / fps)))
                frame_filename = f"{movie_id}_frame{frame_num}.jpg"
                frame_path = os.path.join(FRAME_FOLDER, frame_filename)

                # Optional: Resize to speed up processing
                frame = cv2.resize(frame, (512, 512))

                cv2.imwrite(frame_path, frame)

                frame_doc = {
                    "movie_id": movie_id,
                    "frame_path": frame_path,
                    "timestamp": timestamp,
                }
                result = frames_collection.insert_one(frame_doc)
                frame_id = result.inserted_id

                print(f"✅ Saved frame: {frame_path} at {timestamp}")

                # Launch captioning task in parallel
                tasks.append(executor.submit(generate_and_store_caption, frame_id, frame_path))
                frame_num += 1

            count += 1

        # Wait for all captioning to complete
        for task in as_completed(tasks):
            task.result()

    cap.release()
    print(f"✅ Finished processing video: {video_path}")




# --- Kafka Consumer ---
consumer = KafkaConsumer(
    "video-uploaded",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="earliest",
    group_id="frame_extractor"
)

print("🟢 Frame extractor Kafka consumer running...")

for msg in consumer:
    data = msg.value
    print(f"\n🎥 Received message: {data}")

    movie_id = data["movie_id"]
    video_path = data["video_path"]

    extract_and_enrich_frames(video_path, movie_id)
