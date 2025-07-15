{{ config(materialized='view') }}

SELECT
    message_id,
    channel,
    text,
    CAST(date AS TIMESTAMP) AS sent_at,
    has_image,
    LENGTH(text) AS message_length
FROM raw.telegram_messages
