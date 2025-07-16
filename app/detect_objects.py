# app/detect_objects.py

import os
import json
from ultralytics import YOLO
from dotenv import load_dotenv
import psycopg2

load_dotenv()

model = YOLO('yolov8n.pt')  # or yolov8m.pt / yolov8s.pt for higher accuracy

# DB connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

# Create table if not exists
cur.execute("""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.image_detections (
    id SERIAL PRIMARY KEY,
    message_id BIGINT,
    detected_object_class TEXT,
    confidence_score NUMERIC,
    image_name TEXT
);
""")
conn.commit()

# Load known images
image_dir = "data/images"
for root, _, files in os.walk(image_dir):
    for file in files:
        if not file.endswith(".jpg"): continue
        image_path = os.path.join(root, file)
        print(f"🔍 Detecting objects in: {image_path}")

        results = model(image_path)
        boxes = results[0].boxes

        # Extract message_id from image filename (e.g., lobelia4cosmetics_123.jpg)
        parts = file.split("_")
        try:
            message_id = int(parts[-1].split(".")[0])
        except:
            message_id = None

        for box in boxes:
            cls = results[0].names[int(box.cls[0])]
            conf = round(float(box.conf[0]), 4)

            cur.execute("""
            INSERT INTO raw.image_detections (message_id, detected_object_class, confidence_score, image_name)
            VALUES (%s, %s, %s, %s)
            """, (message_id, cls, conf, file))

conn.commit()
cur.close()
conn.close()
