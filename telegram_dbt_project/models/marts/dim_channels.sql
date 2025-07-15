SELECT DISTINCT
    channel AS channel_id,
    INITCAP(REPLACE(channel, '_', ' ')) AS channel_name
FROM {{ ref('stg_telegram_messages') }}
