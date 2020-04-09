from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.etl_plugin import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

dir_path = os.path.dirname(os.path.realpath(__file__))
start_time=datetime.now().isoformat()
default_args = {
    'owner': 'grimso',
    'start_date': start_time,
  #  'end_date': datetime(2018, 2, 11),
    'depends_on_past':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_retry':False,
}

dag = DAG('covid_and_air_pollution_ETL',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
      #    schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_air_pollution_to_redshift = StageToRedshiftOperator(
    task_id='Stage_air_pollution_data',
    dag=dag,
    table="air_pollution_staging",
    s3_bucket="covid-19de",
    s3_key="air_pollution",
    parameter=["IGNOREHEADER 1","DELIMITER ';'","DATEFORMAT 'DD.MM.YYYY'","IGNOREBLANKLINES","MAXERROR 6","TRUNCATECOLUMNS","REMOVEQUOTES","GZIP" ]
)

stage_air_stations_to_redshift = StageToRedshiftOperator(
    task_id='Stage_air_stations',
    dag=dag,
    table="air_station_staging",
    s3_bucket="covid-19de",
    s3_key="air_stations_ger.csv",
    parameter=["IGNOREHEADER 1","DELIMITER ','"]
)

stage_population_to_redshift = StageToRedshiftOperator(
    task_id='Stage_population',
    dag=dag,
    table="population_staging",
    s3_bucket="covid-19de",
    s3_key="population_germany.csv",
    parameter=["IGNOREHEADER 1","DELIMITER ','","IGNOREBLANKLINES"]
)

stage_covid_to_redshift = StageToRedshiftOperator(
    task_id='Stage_covid_numbers',
    dag=dag,
    table="covid_staging",
    s3_bucket="covid-19de",
    s3_key="covid_numbers",
    parameter=["FORMAT AS JSON 's3://covid-19de/rki_covid_json_path.json'","TRUNCATECOLUMNS","GZIP" ]
)
data_cleaning_air_pollution_staging=("""UPDATE air_pollution_staging SET time=NULL WHERE time='-'; 
UPDATE air_pollution_staging SET value=NULL WHERE value='-'; 
    """)
clean_pollution_staging = PostgresOperator(
    task_id="clean_pollution_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.data_cleaning_air_pollution_staging
)

air_pollution_load=LoadFactOperator(
    task_id="load_air_pollution_facts",
    dag=dag,
    sql_query=SqlQueries.air_pollution_table_insert 
)

air_stations_load=LoadDimensionOperator(
    task_id="load_air_stations",
    dag=dag,
    sql_query=SqlQueries.air_stations_table_insert,
    truncate_table="air_stations"
)

pollutants_load=LoadDimensionOperator(
    task_id="load_pollutants",
    dag=dag,
    sql_query=SqlQueries.pollutants_table_insert,
    truncate_table="pollutants"
)

covid_numbers_load=LoadFactOperator(
    task_id="load_covid_numbers",
    dag=dag,
    sql_query=SqlQueries.covid_numbers_table_insert
)

counties_load=LoadDimensionOperator(
    task_id="load_counties",
    dag=dag,
    sql_query=SqlQueries.counties_table_insert,
    truncate_table="counties"
)

population_load=LoadDimensionOperator(
    task_id="load_county_population",
    dag=dag,
    sql_query=SqlQueries.county_population_table_insert,
    truncate_table="county_population"
)

end_staging_operator = DummyOperator(task_id='Finished_staging',  dag=dag)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  >> [stage_air_pollution_to_redshift,stage_air_stations_to_redshift,stage_population_to_redshift,stage_covid_to_redshift]
stage_air_pollution_to_redshift>>clean_pollution_staging
[clean_pollution_staging,stage_air_stations_to_redshift,stage_population_to_redshift,stage_covid_to_redshift]>> end_staging_operator
end_staging_operator>>[air_pollution_load,air_stations_load,pollutants_load]>>end_operator
end_staging_operator>>[covid_numbers_load,counties_load,population_load]>>end_operator
