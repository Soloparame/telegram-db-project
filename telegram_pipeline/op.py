# ops.py
from dagster import op
import subprocess

@op
def scrape_telegram_data():
    subprocess.run(["python", "../app/scraper.py"], check=True)

@op
def load_raw_to_postgres():
    subprocess.run(["python", "../app/load_json_to_pg.py"], check=True)

@op
def run_dbt_transformations():
    subprocess.run(["dbt", "run", "--project-dir", "../telegram_dbt_project"], check=True)

@op
def run_yolo_enrichment():
    subprocess.run(["python", "../app/detect_objects.py"], check=True)
