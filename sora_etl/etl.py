
import logging
import pandas as pd
import hashlib
from prefect import task, flow
from prefect.futures import PrefectFuture
from sora_etl.utils import table_name
from sora_etl.logger_config import setup_logger
from sora_etl.create_tables import PROJECT_NAME, DATASET_NAME


name = "etl"
logger = setup_logger(
    name=__name__,
    log_file='./logs/etl.log',
    level=logging.INFO,
)


def generate_deterministic_id(value: str) -> str:
    hash_object = hashlib.sha256()
    hash_object.update(value.encode('utf-8'))
    return hash_object.hexdigest()[:15]

def load_datasets(float_path: str, clickup_path: str):
    float_data = pd.read_csv(float_path)
    clickup_data = pd.read_csv(clickup_path)
    return float_data, clickup_data


def create_dimension(df: pd.DataFrame, column_name: str, id_column_name: str) -> pd.DataFrame:
    """Processes a dimension table, generates unique IDs, and returns the cleaned dataframe."""
    try:
        rename_column = f"{column_name.lower()}_name"
        if column_name == "Name":
            rename_column = "person_name"

        dimension_df = df[column_name].drop_duplicates().reset_index().rename(columns={column_name: rename_column})
        dimension_df[id_column_name] = dimension_df[rename_column].apply(generate_deterministic_id)
        dimension_df.drop(columns=["index"], inplace=True)

        logger.info(f"{column_name} dimension created successfully")
    except Exception as e:
        logger.error(f"Error creating {column_name} dimension: {e}")
        raise Exception(f"Error creating {column_name} dimension: {e}")

    return dimension_df


# @task(task_run_name="Time", log_prints=True)
def create_time_dimension(start_date='2020-01-01', end_date='2030-12-31'):
    try:
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        time_df = pd.DataFrame(date_range, columns=['date'])
        
        time_df['day_of_week'] = time_df['date'].dt.day_name()
        time_df['day_of_week_number'] = time_df['date'].dt.weekday + 1
        time_df['day_of_month'] = time_df['date'].dt.day
        time_df['day_of_year'] = time_df['date'].dt.dayofyear
        time_df['week_of_year'] = time_df['date'].dt.isocalendar().week.astype(int)
        time_df['month'] = time_df['date'].dt.month
        time_df['month_name'] = time_df['date'].dt.month_name()
        time_df['quarter'] = time_df['date'].dt.quarter
        time_df['year'] = time_df['date'].dt.year
        time_df['is_weekend'] = time_df['day_of_week'].isin(['Saturday', 'Sunday'])
        time_df["date"] = time_df["date"].dt.strftime('%Y-%m-%d')

        logger.info("Time dimension created successfully")
    except Exception as e:
        logger.error(f"Error creating time dimension: {e}")
        raise Exception(f"Error creating time dimension: {e}")
    
    return time_df


# @task(task_run_name="Fact", log_prints=True)
def create_fact_table(table_data):
    try:
        float_data = table_data["float"]
        clickup_data = table_data["clickup"]
        fact_df = pd.merge(float_data, clickup_data, on=["Client", "Project", "Name"], how="left")
        
        fact_df.rename(columns={"Task_x": "assigned_task", "Task_y": "task_action"}, inplace=True)
        fact_df["client_id"] = fact_df["Client"].map(table_data["dim_clients"].set_index("client_name")["client_id"])
        fact_df["project_id"] = fact_df["Project"].map(table_data["dim_projects"].set_index("project_name")["project_id"])
        fact_df["task_id"] = fact_df["task_action"].map(table_data["dim_tasks"].set_index("task_name")["task_id"])
        fact_df["role_id"] = fact_df["Role"].map(table_data["dim_roles"].set_index("role_name")["role_id"])
        fact_df["person_id"] = fact_df["Name"].map(table_data["dim_persons"].set_index("person_name")["person_id"])

        
        fact_df["date"] = pd.to_datetime(fact_df["Date"]).dt.strftime('%Y-%m-%d')
        fact_df["Estimated Hours"] = fact_df["Estimated Hours"].astype(float)

        # Generate the work_tracking_id using deterministic IDs based on combined fields
        fact_df["work_tracking_id"] = (fact_df["client_id"] + fact_df["project_id"] + 
                                    fact_df["task_id"] + fact_df["role_id"] + 
                                    fact_df["person_id"] + fact_df["date"])
        fact_df["work_tracking_id"] = fact_df["work_tracking_id"].apply(generate_deterministic_id)
        # convert billable to boolean
        fact_df["Billable"] = fact_df["Billable"].map({"Yes": True, "No": False})
        fact_df["Start Date"] = pd.to_datetime(fact_df["Start Date"]).dt.strftime('%Y-%m-%d')
        fact_df["End Date"] = pd.to_datetime(fact_df["End Date"]).dt.strftime('%Y-%m-%d')

        fact_columns = [
            "work_tracking_id", "client_id", "project_id", "role_id", "person_id", 
            "task_id", "date", "Billable", "Hours", 'Estimated Hours', "Note", "Start Date", "End Date"
        ]

        logger.info("Fact table created successfully")
    except Exception as e:
        logger.error(f"Error creating fact table: {e}")
        raise Exception(f"Error creating fact table: {e}")
    
    return fact_df[fact_columns].rename(columns={
        'Billable': 'billable',
        'Hours': 'hours_logged',
        'Estimated Hours': 'estimated_hours',
        'Note': 'task_note',
        'Start Date': 'start_date',
        'End Date': 'end_date'
    })


@task(task_run_name="Prepare Dimension Data", tags=["dimension"])
def dimension_flow(float_path: str, clickup_path: str, check: bool = False):
    float_data, clickup_data = load_datasets(float_path, clickup_path)

    client_df = create_dimension(float_data, "Client", "client_id")
    project_df = create_dimension(float_data, "Project", "project_id")
    role_df = create_dimension(float_data, "Role", "role_id")
    person_df = create_dimension(float_data, "Name", "person_id")
    task_df = create_dimension(clickup_data, "Task", "task_id")
    time_df = create_time_dimension()

    table_data = {
        "float": float_data,
        "clickup": clickup_data,
        'dim_clients': client_df,
        'dim_projects': project_df,
        'dim_roles': role_df,
        'dim_persons': person_df,
        'dim_tasks': task_df,
        'dim_time': time_df
    }

    return table_data

    



@task(task_run_name="Prepare Fact Data")
def fact_flow(table_data):
    fact_df = create_fact_table(table_data)
    return  {'fact_work_tracking': fact_df}
