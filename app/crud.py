# app/crud.py
from .database import get_connection

def get_top_products(limit=10):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT LOWER(word) AS product, COUNT(*) AS count
        FROM (
            SELECT unnest(string_to_array(text, ' ')) AS word
            FROM analytics.fct_messages
        ) AS words
        WHERE LENGTH(word) > 3
        GROUP BY word
        ORDER BY count DESC
        LIMIT %s
    """, (limit,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def get_channel_activity(channel_name):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DATE(sent_at) AS date, COUNT(*) AS message_count
        FROM analytics.fct_messages
        WHERE channel = %s
        GROUP BY date
        ORDER BY date
    """, (channel_name,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def search_messages(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT message_id, channel, sent_at, message_length, has_image, text
        FROM analytics.fct_messages
        WHERE text ILIKE %s
        LIMIT 50
    """, (f"%{query}%",))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results
