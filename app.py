from flask import Flask, request, jsonify
import os
import uuid
from config import UPLOAD_FOLDER
from database.mongodb import save_video_metadata
from kafka_producers.video_upload_producer import send_video_uploaded_message

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/upload', methods=['POST'])
def upload_video():
    video = request.files.get("video")
    name = request.form.get("name")
    year = request.form.get("year")
    language = request.form.get("language")

    if not all([video, name, year, language]):
        print("❌ Missing one or more required fields.")
        return jsonify({"error": "Missing required fields"}), 400

    movie_id = str(uuid.uuid4())
    filename = f"{movie_id}_{video.filename}"
    video_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    print(f"📥 Saving video to: {video_path}")
    video.save(video_path)

    # Save to MongoDB
    print("📦 Saving metadata to MongoDB...")
    save_video_metadata(movie_id, name, year, language, video_path)
    print("✅ Metadata saved.")

    # Send Kafka message
    print("📤 Sending Kafka message to 'video-uploaded' topic...")
    send_video_uploaded_message({
        "movie_id": movie_id,
        "video_path": video_path
    })
    print("✅ Kafka message sent.")

    return jsonify({"message": "Video uploaded successfully", "movie_id": movie_id})

if __name__ == '__main__':
    app.run(debug=True)
