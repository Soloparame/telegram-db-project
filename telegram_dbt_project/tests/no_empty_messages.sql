-- tests/no_empty_messages.sql
SELECT *
FROM {{ ref('stg_telegram_messages') }}
WHERE text IS NULL OR text = ''
