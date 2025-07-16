# jobs.py
from dagster import job
from .ops import scrape_telegram_data, load_raw_to_postgres, run_dbt_transformations, run_yolo_enrichment

@job
def telegram_data_pipeline():
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()
