{{ config(materialized='table') }}

SELECT
    id,
    message_id,
    detected_object_class,
    confidence_score,
    image_name
FROM raw.image_detections
