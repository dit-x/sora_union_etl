import sys
import os
sys.path.append(os.path.abspath('/Users/dit/Work/interviews/sora/sora_etl/'))

import google.cloud.bigquery as bigquery


DATASET_NAME = "sora_dataset"

path = "./.credentials/google.json"
client = bigquery.Client.from_service_account_json(path)
PROJECT_NAME = client.project

table_name = {
    'Client': 'dim_clients',
    'Project': 'dim_projects',
    'Role': 'dim_roles',
    'Name': 'dim_persons',
    'Task': 'dim_tasks',
    'Time': 'dim_time'
}

ddl_queries = {
    "dim_clients": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.dim_clients` (
            client_id STRING NOT NULL,
            client_name STRING NOT NULL
            -- Additional client attributes can be added here
        );
    """,
    "dim_projects": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.dim_projects` (
            project_id STRING NOT NULL,
            project_name STRING NOT NULL
            -- Additional project attributes can be added here
        )
        CLUSTER BY project_id;
    """,
    "dim_persons": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.dim_persons` (
            person_id STRING NOT NULL,
            person_name STRING NOT NULL
            -- Additional person attributes can be added here
        );
    """,
    "dim_roles": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.dim_roles` (
            role_id STRING NOT NULL,
            role_name STRING NOT NULL
        );
    """,
    "dim_tasks": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.dim_tasks` (
            task_id STRING NOT NULL,
            task_name STRING NOT NULL
            -- Additional task attributes can be added here
        )
        CLUSTER BY task_id;
    """,
    "dim_time": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.dim_time` (
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
            is_weekend BOOL         
        );
    """,
    "fact_work_tracking": f"""
        CREATE TABLE `{PROJECT_NAME}.{DATASET_NAME}.fact_work_tracking` (
            work_tracking_id STRING NOT NULL,
            client_id STRING NOT NULL,     
            project_id STRING NOT NULL,    
            person_id STRING NOT NULL,     
            role_id STRING NOT NULL,       
            task_id STRING NOT NULL,       
            date DATE NOT NULL,         
            task_note STRING,  
            hours_logged FLOAT64 NOT NULL,    
            estimated_hours FLOAT64,          
            billable BOOL NOT NULL,           
            start_date DATE,              
            end_date DATE
        )
        PARTITION BY date
        CLUSTER BY project_id, client_id, person_id, task_id;
    """
}


# create bq schema

bq_dim_clients_schema = [
    bigquery.SchemaField("client_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("client_name", "STRING", mode="REQUIRED")
]

bq_dim_projects_schema = [
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("project_name", "STRING", mode="REQUIRED")
]

bq_dim_persons_schema = [
    bigquery.SchemaField("person_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("person_name", "STRING", mode="REQUIRED")
]

bq_dim_roles_schema = [
    bigquery.SchemaField("role_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("role_name", "STRING", mode="REQUIRED")
]

bq_dim_tasks_schema = [
    bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("task_name", "STRING", mode="REQUIRED")
]


bq_dim_time_schema = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("day_of_week", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("day_of_week_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("day_of_month", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("day_of_year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("week_of_year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("quarter", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("is_weekend", "BOOLEAN", mode="REQUIRED")
]

bq_fact_schema = [
    bigquery.SchemaField("work_tracking_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("client_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("role_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("person_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("task_note", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("billable", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("hours_logged", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("estimated_hours", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("start_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("end_date", "DATE", mode="REQUIRED")
]

bigquery_schema = {
    "dim_clients": bq_dim_clients_schema,
    "dim_projects": bq_dim_projects_schema,
    "dim_persons": bq_dim_persons_schema,
    "dim_roles": bq_dim_roles_schema,
    "dim_tasks": bq_dim_tasks_schema,
    "dim_time": bq_dim_time_schema,
    "fact_work_tracking": bq_fact_schema
}
