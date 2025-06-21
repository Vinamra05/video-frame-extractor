# Sends video upload info to Kafkafrom kafka import KafkaProducer
import json
from config import KAFKA_BROKER, KAFKA_TOPIC
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_video_uploaded_message(data):
    producer.send(KAFKA_TOPIC, value=data)
    producer.flush()
