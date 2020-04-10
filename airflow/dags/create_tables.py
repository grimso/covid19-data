from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

dir_path = os.path.dirname(os.path.realpath(__file__))
start_time = datetime.now().isoformat()
default_args = {
    "owner": "grimso",
    "depends_on_past": False,
    "start_date": start_time,
    "catchup": False,
    "email_on_retry": False,
}

dag = DAG(
    "create_tables",
    default_args=default_args,
    description="create tables",
    #    schedule_interval='@daily'
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

with open(f"{dir_path}/../create_tables.sql", "r") as f:
    create_tables_sql = f.read()

with open(f"{dir_path}/../drop_tables.sql", "r") as f:
    drop_tables_sql = f.read()

drop_tables = PostgresOperator(
    task_id="drop_tables", dag=dag, postgres_conn_id="redshift", sql=drop_tables_sql
)

create_tables = PostgresOperator(
    task_id="create_tables", dag=dag, postgres_conn_id="redshift", sql=create_tables_sql
)


end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> drop_tables >> create_tables >> end_operator
