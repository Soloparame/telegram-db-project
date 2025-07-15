WITH dates AS (
    SELECT generate_series('2024-01-01'::DATE, CURRENT_DATE, '1 day')::DATE AS date
)
SELECT
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    TO_CHAR(date, 'Day') AS weekday
FROM dates
