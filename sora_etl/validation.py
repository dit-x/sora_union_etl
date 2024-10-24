import logging
import pandas as pd
from prefect import task, flow
import google.cloud.bigquery as bigquery
from sora_etl.logger_config import setup_logger


logger = setup_logger(
    name=__name__,
    log_file='./logs/validation.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)



expected_client_schema = {
    'client_name': pd.StringDtype(),
    'client_id': pd.StringDtype()
}

expected_project_schema = {
    'project_name': pd.StringDtype(),
    'project_id': pd.StringDtype()
}

expected_person_schema = {
    'person_name': pd.StringDtype(),
    'person_id': pd.StringDtype()
}

expected_role_schema = {
    'role_name': pd.StringDtype(),
    'role_id': pd.StringDtype()
}

expected_task_schema = {
    'task_name': pd.StringDtype(),
    'task_id': pd.StringDtype(),
}

expected_dim_time_schema = {
    'date': pd.StringDtype(),       
    'day_of_week': pd.StringDtype(),
    'day_of_week_number': 'int32',  
    'day_of_month': 'int32',        
    'day_of_year': 'int32',         
    'week_of_year': 'int32',        
    'month': 'int32',               
    'month_name': pd.StringDtype(), 
    'quarter': 'int32',             
    'year': 'int32',                
    'is_weekend': 'bool'            
}

expected_fact_schema = {
    'work_tracking_id': pd.StringDtype(),
    'client_id': pd.StringDtype(),
    'project_id': pd.StringDtype(),
    'role_id': pd.StringDtype(),
    'person_id': pd.StringDtype(),
    'task_id': pd.StringDtype(),
    'date': pd.StringDtype(),
    'billable': 'bool',
    'hours_logged': 'float64',
    'estimated_hours': 'float64',
    'task_note': pd.StringDtype(),
    'start_date': pd.StringDtype(),
    'end_date': pd.StringDtype(),
}


# @task(log_prints=True, tags=["validate_data"])
def table_schema(df: pd.DataFrame, expected_schema: dict) -> bool:
    """
    Validates the schema of a Pandas DataFrame against an expected schema.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to validate.
    expected_schema (dict): A dictionary defining the expected schema. 
                            Keys are column names, values are expected data types (as Python types or Pandas dtypes).
    
    Returns:
    bool: True if schema is valid, False otherwise. Prints issues if found.
    """
    
    missing_columns = [col for col in expected_schema.keys() if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns in DataFrame: {missing_columns}")
        raise ValueError(f"Missing columns in DataFrame: {missing_columns}")
    
    for col, expected_dtype in expected_schema.items():
        actual_dtype = df[col].dtype
        
        # Handle string columns (either StringDtype or object)
        if expected_dtype == pd.StringDtype():
            if not pd.api.types.is_string_dtype(df[col]):
                raise ValueError(f"Column '{col}' has incorrect type: expected string, but got {actual_dtype}")
        elif expected_dtype == 'int32' and actual_dtype == 'int64':
            continue
        elif expected_dtype == 'int32' and actual_dtype == 'int32':
            continue
        elif expected_dtype == 'int64' and actual_dtype == 'int64':
            continue
        elif actual_dtype != expected_dtype:
            raise ValueError(f"Column '{col}' has incorrect type: expected {expected_dtype}, but got {actual_dtype}")
    
    logger.info("Schema validation passed successfully.")
    return True


@task(name="Validate Schema")
def validate_schema(table_data: dict, fact_table: dict):

    table_data = table_data | fact_table
    for table, df in table_data.items():
        if table == 'dim_clients':
            table_schema(df, expected_client_schema)
        elif table == 'dim_projects':
            table_schema(df, expected_project_schema)
        elif table == 'dim_persons':
            table_schema(df, expected_person_schema)
        elif table == 'dim_roles':
            table_schema(df, expected_role_schema)
        elif table == 'dim_tasks':
            table_schema(df, expected_task_schema)
        elif table == 'dim_time':
            table_schema(df, expected_dim_time_schema)
        elif table == 'fact_work_tracking':
            table_schema(df, expected_fact_schema)
    return True