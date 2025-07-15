# app/scraper.py

import os, json
import logging
from datetime import datetime
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "scraper_session")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# Setup logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=f"{log_dir}/scraper.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

async def scrape_channel(channel_username, max_messages=100):
    await client.start()

    try:
        messages = []
        async for msg in client.iter_messages(channel_username, limit=max_messages):
            msg_data = msg.to_dict()
            # Optional: download image if available
            if isinstance(msg.media, MessageMediaPhoto):
                file_name = f"{channel_username}_{msg.id}.jpg"
                await msg.download_media(file=file_name)
                msg_data['downloaded_image'] = file_name
            messages.append(msg_data)

        # Save to data lake
        today = datetime.now().strftime("%Y-%m-%d")
        save_path = f"data/raw/telegram_messages/{today}"
        os.makedirs(save_path, exist_ok=True)
        file_path = os.path.join(save_path, f"{channel_username}.json")
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)

        logging.info(f"✅ Scraped {len(messages)} messages from {channel_username}")

    except Exception as e:
        logging.error(f"❌ Failed to scrape {channel_username}: {str(e)}")

if __name__ == "__main__":
    import asyncio

    # Add as many as you want here
    channels = [
        "lobelia4cosmetics",
        "tikvahpharma",
        "chemedchannel"  # You can use username or channel ID
    ]

    for ch in channels:
        asyncio.run(scrape_channel(ch))
