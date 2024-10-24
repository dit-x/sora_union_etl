import logging
from prefect import task, flow
from prefect.futures import PrefectFuture
import google.cloud.bigquery as bigquery
from sora_etl.logger_config import setup_logger
from sora_etl.utils import client, bigquery_schema, PROJECT_NAME, DATASET_NAME


logger = setup_logger(
    name=__name__,
    log_file='./logs/destination.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


@task(task_run_name="{table_name}", log_prints=True, tags=["destination"] )
def load_to_bq(table_name, df):
    
    try:
        table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"
        schema = bigquery_schema[table_name]
        job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV
    )

        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    except Exception as e:
        logger.error(f"Error loading {table_name} to BigQuery: {e}")
        raise Exception(f"Error loading {table_name} to BigQuery: {e}")
    


@task(task_run_name="Load Data To BQ")
def load_data_flow(table_data: dict, fact_table: dict):

    load_to_bq_future = []
    for table, df in table_data.items():
        if table in ["float", "clickup"]:
            continue
        load_to_bq_future.append(load_to_bq.submit(table, df))

    for table, df in fact_table.items():
        load_to_bq_future.append(load_to_bq.submit(table, df))

    for future in load_to_bq_future:
        future.result()
    return True

