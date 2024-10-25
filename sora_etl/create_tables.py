import logging
from prefect import task, flow
from prefect.futures import PrefectFuture
from google.cloud.exceptions import NotFound, Conflict
from sora_etl.logger_config import setup_logger
from sora_etl.utils import client, ddl_queries, PROJECT_NAME, DATASET_NAME

logger = setup_logger(
    name=__name__,
    log_file='./logs/create_table.log',
    level=logging.INFO,
)

# All DDL queries to create BigQuery tables
@task(task_run_name="{table}", log_prints=True, tags=["create-tables"])
def create_table(table: str, query: str):
    # Run the query to create the table
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        logger.info(f"Table created successfully: {table}")
    except Conflict:
        logger.warning(f"Table already exists: {table}")
    except Exception as e:
        logger.error(f"Error creating {table}: {e}")
        raise Exception(f"Error creating {table}: {e}")


@task(task_run_name="Create Tables")
def create_table_flow():
    logger.info("Creating tables in BigQuery")
    
    try:
        dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
        client.get_dataset(dataset_id)
    except NotFound:
        logger.warning(f"Dataset not found: {dataset_id}")
        client.create_dataset(dataset_id)
        logger.info(f"Dataset created successfully: {dataset_id}")
    except Conflict:
        logger.warning(f"Dataset already exists: {dataset_id}")
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        raise Exception(f"Error creating dataset: {e}")
    
    # Launch all create_table tasks concurrently
    create_table_futures: list[PrefectFuture] = []
    for table, query in ddl_queries.items():
        create_table_futures.append(create_table.submit(table, query))
    
    # Wait for all the table creation tasks to finish
    for future in create_table_futures:
        future.result()  # This line ensures all tasks complete, but they run concurrently

    logger.info("All tables have been created.")
    return True
