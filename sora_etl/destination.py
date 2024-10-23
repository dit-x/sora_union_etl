import logging
from prefect import task, flow
from sora_etl.logger_config import setup_logger
from sora_etl.create_tables import PROJECT_NAME, DATASET_NAME


logger = setup_logger(
    name=__name__,
    log_file='./logs/destination.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


@task(name="Load data - {table_name}", log_prints=True, tags=["destination"] )
def load_to_bq(client, table_name, df):
    
    try:
        table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{table_name}"
        client.load_table_from_dataframe(df, table_id).result()
    except Exception as e:
        logger.error(f"Error loading {table_name} to BigQuery: {e}")
        return False
    
    return True


@flow(name="Load Data")
def load_data_flow(table_data: dict):
    
    for table, df in table_data.items():
        load_to_bq(table, df)
    return dimensions, time_df, fact_df
