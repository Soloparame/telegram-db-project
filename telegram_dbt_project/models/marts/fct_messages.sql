{{ config(materialized='table') }}

SELECT
    m.message_id,
    m.channel_id,
    m.sent_at,
    m.message_length,
    m.has_image,
    d.date_id,
    c.channel_name
FROM {{ ref('stg_telegram_messages') }} m
JOIN {{ ref('dim_channels') }} c ON m.channel = c.channel_id
JOIN {{ ref('dim_dates') }} d ON m.sent_at::date = d.date
