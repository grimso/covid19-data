from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import urllib.request, json 
from helpers import SqlQueries
from airflow.operators.etl_plugin import CovidApiToS3Operator
from airflow.operators.etl_plugin import AirPollutionApiToS3Operator
dir_path = os.path.dirname(os.path.realpath(__file__))
start_time=datetime.now()



default_args = {
    'owner': 'grimso',
    'depends_on_past':False,
    'start_date':start_time.isoformat(),
    'catchup':False,
    'email_on_retry':False,
}

dag = DAG('Fetch_data_from_apis',
          default_args=default_args,
          description='create tables',
      #    schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

covid_api_fetch=CovidApiToS3Operator(
    task_id="Fetch_covid_data",
    dag=dag,
    s3_bucket="covid-19de",
    s3_key="covid_numbers",
date=start_time)    

pm10_api_fetch=AirPollutionApiToS3Operator(
    date=start_time,
    task_id="Fetch_pm10_data",
    dag=dag,
    s3_bucket="covid-19de",
    s3_key="air_pollution",
    pollutant="PM10",
    
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >>[covid_api_fetch,pm10_api_fetch]>> end_operator

