import pandas as pd
import hashlib
from prefect import task, flow
from sora_etl.create_tables import PROJECT_NAME, DATASET_NAME

def generate_deterministic_id(value: str) -> str:
    hash_object = hashlib.sha256()
    hash_object.update(value.encode('utf-8'))
    return hash_object.hexdigest()[:15]  

def load_datasets(float_path: str, clickup_path: str):
    float_data = pd.read_csv(float_path)
    clickup_data = pd.read_csv(clickup_path)
    return float_data, clickup_data


@task(name="Create {column_name} Dimension Table")
def create_dimension(df: pd.DataFrame, column_name: str, id_column_name: str) -> pd.DataFrame:
    """Processes a dimension table, generates unique IDs, and returns the cleaned dataframe."""
    rename_column = f"{column_name.lower()}_name"
    if column_name == "Name":
        rename_column = "person_name"
        print(rename_column)
    dimension_df = df[column_name].drop_duplicates().reset_index().rename(columns={column_name: rename_column})
    dimension_df[id_column_name] = dimension_df[rename_column].apply(generate_deterministic_id)
    return dimension_df


@task(name="Create Time Dimension Table")
def create_time_dimension(start_date='2020-01-01', end_date='2030-12-31'):
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    time_df = pd.DataFrame(date_range, columns=['date'])
    
    time_df['day_of_week'] = time_df['date'].dt.day_name()
    time_df['day_of_week_number'] = time_df['date'].dt.weekday + 1
    time_df['day_of_month'] = time_df['date'].dt.day
    time_df['day_of_year'] = time_df['date'].dt.dayofyear
    time_df['week_of_year'] = time_df['date'].dt.isocalendar().week
    time_df['month'] = time_df['date'].dt.month
    time_df['month_name'] = time_df['date'].dt.month_name()
    time_df['quarter'] = time_df['date'].dt.quarter
    time_df['year'] = time_df['date'].dt.year
    time_df['is_weekend'] = time_df['day_of_week'].isin(['Saturday', 'Sunday'])
    
    return time_df


@task(name="Create Fact Table")
def create_fact_table(float_data, clickup_data, dimensions):
    fact_df = pd.merge(float_data, clickup_data, on=["Client", "Project", "Name"], how="left")
    
    fact_df.rename(columns={"Task_x": "assigned_task", "Task_y": "task_action"}, inplace=True)
    fact_df["client_id"] = fact_df["Client"].map(dimensions["client"].set_index("client_name")["client_id"])
    fact_df["project_id"] = fact_df["Project"].map(dimensions["project"].set_index("project_name")["project_id"])
    fact_df["task_id"] = fact_df["task_action"].map(dimensions["task"].set_index("task_name")["task_id"])
    fact_df["role_id"] = fact_df["Role"].map(dimensions["role"].set_index("role_name")["role_id"])
    fact_df["person_id"] = fact_df["Name"].map(dimensions["person"].set_index("person_name")["person_id"])

    
    fact_df["date"] = pd.to_datetime(fact_df["Date"])

    # Generate the work_tracking_id using deterministic IDs based on combined fields
    fact_df["work_tracking_id"] = (fact_df["client_id"] + fact_df["project_id"] + 
                                   fact_df["task_id"] + fact_df["role_id"] + 
                                   fact_df["person_id"] + fact_df["date"].dt.strftime('%Y-%m-%d'))
    fact_df["work_tracking_id"] = fact_df["work_tracking_id"].apply(generate_deterministic_id)

    fact_columns = [
        "work_tracking_id", "client_id", "project_id", "role_id", "person_id", 
        "task_id", "date", "Billable", "Hours", 'Estimated Hours', "Note", "Start Date", "End Date"
    ]
    
    return fact_df[fact_columns].rename(columns={
        'Billable': 'billable',
        'Hours': 'hours_logged',
        'Estimated Hours': 'estimated_hours',
        'Note': 'task_note',
        'Start Date': 'start_date',
        'End Date': 'end_date'
    })


@flow(name="Create BigQuery Tables")
def run_etl(float_path: str, clickup_path: str):
    float_data, clickup_data = load_datasets(float_path, clickup_path)

    client_df = create_dimension(float_data, "Client", "client_id")
    project_df = create_dimension(float_data, "Project", "project_id")
    role_df = create_dimension(float_data, "Role", "role_id")
    person_df = create_dimension(float_data, "Name", "person_id")
    task_df = create_dimension(clickup_data, "Task", "task_id")
    time_df = create_time_dimension()

    table_data = {
        'client': client_df,
        'project': project_df,
        'role': role_df,
        'person': person_df,
        'task': task_df
    }

    fact_df = create_fact_table(float_data, clickup_data, table_data)

    table_data = table_data | {'time': time_df, 'fact': fact_df}
    
    return table_data






# float_path = 'data/float_allocations.csv'
# clickup_path = 'data/clickUp.csv'
# dimensions, time_df, fact_df = run_etl(float_path, clickup_path)

