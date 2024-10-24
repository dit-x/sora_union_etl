

import json
import logging
from prefect import flow
from sora_etl.logger_config import setup_logger
from sora_etl.utils import client

from sora_etl.create_tables import create_table_flow
from sora_etl.etl import dimension_flow, fact_flow
from sora_etl.validation import validate_schema
from sora_etl.destination import load_data_flow


logger = setup_logger(
    name='main',
    log_file='./logs/main.log',
    level=logging.INFO
)



@flow(name="Sora Union ETL")
def sora_union_etl(config_path: str = './config.json'):
    with open(config_path, 'r') as file:
        config = json.load(file)
    
    # Create tables
    c_result = create_table_flow.submit()
    c_result.result()  # Ensure this is completed before proceeding
    
    # Extract, transform, and load data
    table_data_future = dimension_flow.submit(
        float_path=config['float_path'],
        clickup_path=config['clickup_path'],
        check=c_result
    )
    table_data = table_data_future.result()  # Wait for the future to resolve

    fact_data_future = fact_flow.submit(table_data, wait_for=[table_data_future])
    fact_data = fact_data_future.result()  # Wait for the fact data to complete
    
    # Validate the schema
    validation_result = validate_schema(table_data=table_data, fact_table=fact_data)
    
    # Load data to BigQuery
    load_data_flow_future = load_data_flow.submit(table_data=table_data, fact_table=fact_data)
    load_data_flow_future.result()  # Wait for the load task to complete
    
    return True


@flow(name="Sora Union ETL")
def sora_union_etl(config_path: str = './config.json'):
    with open(config_path, 'r') as file:
        config = json.load(file)
    
    # Create tables
    c_result = create_table_flow.submit()
    c_result.result()

    # Extract, transform, and load data
    table_data_future = dimension_flow.submit(
        float_path=config['float_path'],
        clickup_path=config['clickup_path'],
        check=c_result
    )
    table_data = table_data_future.result()

    fact_data_future = fact_flow.submit(table_data, wait_for=[table_data_future])
    fact_data = fact_data_future.result()
    
    # Validate the schema
    validation_result = validate_schema(table_data=table_data, fact_table=fact_data)
    
    # Load data to BigQuery using the subflow
    load_data_flow_future = load_data_flow.submit(table_data=table_data, fact_table=fact_data, wait_for=[table_data, fact_data_future,validation_result])
    load_data_flow_future.result()
    
    return True



if __name__ == '__main__':
    sora_union_etl()