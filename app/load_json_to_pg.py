# app/load_json_to_pg.py

import os
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# DB config from .env
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cur = conn.cursor()
root_dir = "data/raw/telegram_messages"

def flatten_message(msg):
    return {
        "message_id": msg.get("id"),
        "channel": msg.get("peer_id", {}).get("channel_id", "unknown"),
        "text": msg.get("message"),
        "date": msg.get("date"),
        "has_image": bool(msg.get("downloaded_image")),
        "image_name": msg.get("downloaded_image", None),
        "raw_json": json.dumps(msg)
    }

cur.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        message_id BIGINT PRIMARY KEY,
        channel TEXT,
        text TEXT,
        date TIMESTAMP,
        has_image BOOLEAN,
        image_name TEXT,
        raw_json JSONB
    );
""")
conn.commit()

# Loop through JSON files
for date_dir in os.listdir(root_dir):
    path = os.path.join(root_dir, date_dir)
    if not os.path.isdir(path): continue

    for file in os.listdir(path):
        with open(os.path.join(path, file), "r", encoding="utf-8") as f:
            messages = json.load(f)
            for msg in messages:
                flat = flatten_message(msg)
                try:
                    cur.execute("""
                        INSERT INTO raw.telegram_messages
                        (message_id, channel, text, date, has_image, image_name, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id) DO NOTHING
                    """, (
                        flat["message_id"], flat["channel"], flat["text"], flat["date"],
                        flat["has_image"], flat["image_name"], flat["raw_json"]
                    ))
                except Exception as e:
                    print("❌ Error inserting message:", e)

conn.commit()
cur.close()
conn.close()
