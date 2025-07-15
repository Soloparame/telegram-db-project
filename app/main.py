# app/main.py
from flask import Flask
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

@app.route('/')
def hello():
    telegram_api_key = os.getenv("TELEGRAM_API_KEY")
    return f"Hello World! Telegram Key: {telegram_api_key[:5]}***"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
