import logging
import pandas as pd
from prefect import task
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
    'date': 'datetime64[ns]',       
    'day_of_week': pd.StringDtype(),
    'day_of_week_number': 'int64',  
    'day_of_month': 'int64',        
    'day_of_year': 'int64',         
    'week_of_year': 'int64',        
    'month': 'int64',               
    'month_name': pd.StringDtype(), 
    'quarter': 'int64',             
    'year': 'int64',                
    'is_weekend': 'bool'            
}

expected_fact_schema = {
    'work_tracking_id': pd.StringDtype(),
    'client_id': pd.StringDtype(),
    'project_id': pd.StringDtype(),
    'role_id': pd.StringDtype(),
    'person_id': pd.StringDtype(),
    'task_id': pd.StringDtype(),
    'date': 'datetime64[ns]',
    'billable': 'bool',
    'hours_logged': 'float64',
    'task_note': pd.StringDtype(),
    'start_date': 'datetime64[ns]',
    'end_date': 'datetime64[ns]'
}


@task(log_prints=True, tags=["validate_data"])
def validate_schema(df: pd.DataFrame, expected_schema: dict) -> bool:
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
        return False
    
    for column, expected_dtype in expected_schema.items():
        actual_dtype = df[column].dtype
        if not pd.api.types.is_dtype_equal(actual_dtype, expected_dtype):
            logger.error(f"Column '{column}' has incorrect type: expected {expected_dtype}, but got {actual_dtype}")
            return False
    
    print("Schema validation passed.")
    return True