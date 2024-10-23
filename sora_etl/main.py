
import json
import logging
from prefect import flow
from google.cloud import bigquery
from sora_etl.logger_config import setup_logger
from sora_etl.create_tables import PROJECT_NAME


logger = setup_logger(
    name='main',
    log_file='./logs/main.log',
    level=logging.INFO
)

path = "./.credentials/google.json"
client = bigquery.Client.from_service_account_json(path)


# @flow(name="Sora Union ETL")

@flow(name="Create Table")
def create_data_flow():
    from sora_etl.etl import run_etl
    from sora_etl.validation import validate_data
    from sora_etl.create_tables import create_dim_tables, create_fact_table

    float_path = "data/Float.csv"
    clickup_path = "data/Clickup.csv"

    logger.info("Running ETL")
    dimensions, time_df, fact_df = run_etl(float_path, clickup_path)

    logger.info("Validating data")
    validate_data(dimensions, time_df, fact_df)

    logger.info("Creating tables")
    create_dim_tables(client, dimensions)
    create_fact_table(client, fact_df)