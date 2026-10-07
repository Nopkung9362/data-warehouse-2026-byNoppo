from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime

profile_config = ProfileConfig(
    profile_name="olist_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dwh",
        profile_args={"schema": "olist_marts"},
    ),
)

with DAG(
    dag_id='lab_olist_cosmos_integration',
    start_date=datetime(2017, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['dbt', 'cosmos', 'olist'],
) as dag:

    dbt_transformation = DbtTaskGroup(
        group_id="dbt_transformation_layer",
        project_config=ProjectConfig("/opt/airflow/dbt_projects/olist_dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
    )
