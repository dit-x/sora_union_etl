import logging
from prefect import task, flow
from sora_etl.logger_config import setup_logger
from sora_etl.logger_config import setup_logger

PROJECT_NAME = "your_project"
DATASET_NAME = "your_dataset"

logger = setup_logger(
    name=__name__,
    log_file='./logs/create_table.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


create_dim_clients = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.dim_clients` (
    client_id STRING NOT NULL,
    client_name STRING NOT NULL,
    -- Additional client attributes can be added here
    PRIMARY KEY (client_id)
);
"""

create_dim_projects = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.dim_projects` (
    project_id STRING NOT NULL,
    project_name STRING NOT NULL,
    -- Additional project attributes can be added here
    PRIMARY KEY (project_id)
)
  CLUSTER BY project_id;
"""

create_dim_persons = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.dim_persons` (
    person_id STRING NOT NULL,
    person_name STRING NOT NULL,
    -- Additional person attributes can be added here
    PRIMARY KEY (person_id)
);
"""

create_dim_roles = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.dim_roles` (
    role_id STRING NOT NULL,
    role_name STRING NOT NULL,
    PRIMARY KEY (role_id)
);
"""

create_dim_tasks = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.dim_tasks` (
    task_id STRING NOT NULL,
    task_name STRING NOT NULL,
    -- Additional task attributes can be added here
    PRIMARY KEY (task_id)
)
CLUSTER BY task_id;   
"""

create_dim_time = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.dim_time` (
    date DATE NOT NULL,
    day_of_week STRING,      
    day_of_week_number INT64,
    day_of_month INT64,      
    day_of_year INT64,       
    week_of_year INT64,      
    month INT64,             
    month_name STRING,       
    quarter INT64,           
    year INT64,              
    is_weekend BOOL,         
    PRIMARY KEY (date)
);
"""

create_fact_work_tracking = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_NAME}.{DATASET_NAME}.fact_work_tracking` (
    work_tracking_id STRING NOT NULL,
    client_id STRING NOT NULL,     
    project_id STRING NOT NULL,    
    person_id STRING NOT NULL,     
    role_id STRING NOT NULL,       
    task_id STRING NOT NULL,       
    date DATE NOT NULL,           
    hours_logged FLOAT64 NOT NULL,    
    estimated_hours FLOAT64,          
    billable BOOL NOT NULL,           
    start_date DATE,              
    end_date DATE,                
    PRIMARY KEY (work_tracking_id)
) PARTITION BY DATE(date)        -- Partition by the date to optimize time-based queries
  CLUSTER BY project_id, client_id, person_id, task_id;  -- Cluster by frequently queried fields for performance
"""

ddl_queries = {
    "dim_clients": create_dim_clients,
    "dim_projects": create_dim_projects,
    "dim_persons": create_dim_persons,
    "dim_roles": create_dim_roles,
    "dim_tasks": create_dim_tasks,
    "dim_time": create_dim_time,
    "fact_work_tracking": create_fact_work_tracking
}


@task(log_prints=True, tags=["create-tables"])
def create_table(table: str, query: str, client):

    try:
        # Run the query to create the table
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        
        logger.info(f"Table created successfully: {table}")
    except Exception as e:
        logger.error(f"Error creating {table}: {e}")



@flow(name="Create Tables")
def create_table_flow(client):
    for table, query in ddl_queries.items():
        create_table(table, query, client)

    return True