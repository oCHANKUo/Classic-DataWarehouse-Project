from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'chanuka',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='load_datawarehouse_dag',
    default_args=default_args,
    description='Run SQL Server stored procedures: load_staging and load_datawarehouse',
    schedule_interval=None, 
    catchup=False,
    tags=['datawarehouse', 'etl'],
) as dag:

    load_staging = MsSqlOperator(
        task_id='load_staging',
        mssql_conn_id='mssql_staging',
        sql="EXEC dbo.load_staging;",       # load staging table
        autocommit=True,  
    )

    load_datawarehouse = MsSqlOperator(
        task_id='load_datawarehouse',
        mssql_conn_id='mssql_datawarehouse',
        sql="EXEC dbo.load_datawarehouse;",       # load datawarehouse
        autocommit=True,  
    )

    load_staging >> load_datawarehouse
