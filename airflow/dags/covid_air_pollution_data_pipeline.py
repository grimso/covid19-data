from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.etl_plugin import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    DimensionTableQualityOperator,
    CovidApiToS3Operator,
    AirPollutionApiToS3Operator,
)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

dir_path = os.path.dirname(os.path.realpath(__file__))
start_time = datetime.now().isoformat()
default_args = {
    "owner": "grimso",
    "start_date": start_time,
    #  'end_date': datetime(2018, 2, 11),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

dag = DAG(
    "covid_and_air_pollution_data_pipeline",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    #    schedule_interval='@daily'
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

covid_api_fetch = CovidApiToS3Operator(
    task_id="Fetch_covid_data",
    dag=dag,
    s3_bucket="covid-19de",
    s3_key="covid_numbers",
    date=start_time,
)

pm10_api_fetch = AirPollutionApiToS3Operator(
    date=start_time,
    task_id="Fetch_pm10_data",
    dag=dag,
    s3_bucket="covid-19de",
    s3_key="air_pollution",
    pollutant="PM10",
)


stage_air_pollution_to_redshift = StageToRedshiftOperator(
    task_id="Stage_air_pollution_data",
    dag=dag,
    table="air_pollution_staging",
    s3_bucket="covid-19de",
    s3_key="air_pollution",
    parameter=[
        "IGNOREHEADER 1",
        "DELIMITER ';'",
        "DATEFORMAT 'DD.MM.YYYY'",
        "IGNOREBLANKLINES",
        "MAXERROR 6",
        "TRUNCATECOLUMNS",
        "REMOVEQUOTES",
        "GZIP",
    ],
)

stage_air_stations_to_redshift = StageToRedshiftOperator(
    task_id="Stage_air_stations",
    dag=dag,
    table="air_station_staging",
    s3_bucket="covid-19de",
    s3_key="air_stations_ger.csv",
    parameter=["IGNOREHEADER 1", "DELIMITER ','"],
)

stage_population_to_redshift = StageToRedshiftOperator(
    task_id="Stage_population",
    dag=dag,
    table="population_staging",
    s3_bucket="covid-19de",
    s3_key="population_germany.csv",
    parameter=["IGNOREHEADER 1", "DELIMITER ','", "IGNOREBLANKLINES"],
)

stage_covid_to_redshift = StageToRedshiftOperator(
    task_id="Stage_covid_numbers",
    dag=dag,
    table="covid_staging",
    s3_bucket="covid-19de",
    s3_key="covid_numbers",
    parameter=[
        "FORMAT AS JSON 's3://covid-19de/rki_covid_json_path.json'",
        "TRUNCATECOLUMNS",
        "GZIP",
    ],
)
data_cleaning_air_pollution_staging = """UPDATE air_pollution_staging SET time=NULL WHERE time='-'; 
UPDATE air_pollution_staging SET value=NULL WHERE value='-'; 
    """
clean_pollution_staging = PostgresOperator(
    task_id="clean_pollution_staging",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.data_cleaning_air_pollution_staging,
)

air_pollution_load = LoadFactOperator(
    task_id="load_air_pollution_facts",
    dag=dag,
    sql_query=SqlQueries.air_pollution_table_insert,
)

air_stations_load = LoadDimensionOperator(
    task_id="load_air_stations",
    dag=dag,
    sql_query=SqlQueries.air_stations_table_insert,
    truncate_table="air_stations",
)

pollutants_load = LoadDimensionOperator(
    task_id="load_pollutants",
    dag=dag,
    sql_query=SqlQueries.pollutants_table_insert,
    truncate_table="pollutants",
)

covid_numbers_load = LoadFactOperator(
    task_id="load_covid_numbers",
    dag=dag,
    sql_query=SqlQueries.covid_numbers_table_insert,
)

counties_load = LoadDimensionOperator(
    task_id="load_counties",
    dag=dag,
    sql_query=SqlQueries.counties_table_insert,
    truncate_table="counties",
)

population_load = LoadDimensionOperator(
    task_id="load_county_population",
    dag=dag,
    sql_query=SqlQueries.county_population_table_insert,
    truncate_table="county_population",
)


run_quality_checks_staging = DataQualityOperator(
    task_id="Data_quality_check_staging",
    dag=dag,
    tables_to_check=["air_pollution_staging", "air_station_staging", "covid_staging"],
)

run_quality_checks_final = DataQualityOperator(
    task_id="Data_quality_check_empty_tables",
    dag=dag,
    tables_to_check=[
        "air_pollution",
        "air_stations",
        "pollutants",
        "covid_numbers",
        "counties",
        "county_population",
    ],
)

run_check_pollutants = DimensionTableQualityOperator(
    task_id="Check_pollutant_complete",
    dag=dag,
    fact_table="air_pollution",
    dimension_table="pollutants",
    table_key="station_id",
)

run_check_air_stations = DimensionTableQualityOperator(
    task_id="Check_air_stations_complete",
    dag=dag,
    fact_table="air_pollution",
    dimension_table="air_stations",
    table_key="pollutant_id",
)

run_check_counties = DimensionTableQualityOperator(
    task_id="Check_counties_complete",
    dag=dag,
    fact_table="covid_numbers",
    dimension_table="counties",
    table_key="county_id",
)

run_check_county_population = DimensionTableQualityOperator(
    task_id="Check_county_population_complete",
    dag=dag,
    fact_table="covid_numbers",
    dimension_table="county_population",
    table_key="county_id",
)

data_api_fetched = DummyOperator(task_id="Fetched_data_from_api", dag=dag)
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> [covid_api_fetch, pm10_api_fetch] >> data_api_fetched
data_api_fetched >> [
    stage_air_pollution_to_redshift,
    stage_air_stations_to_redshift,
    stage_population_to_redshift,
    stage_covid_to_redshift,
]
stage_air_pollution_to_redshift >> clean_pollution_staging
[
    clean_pollution_staging,
    stage_air_stations_to_redshift,
    stage_population_to_redshift,
    stage_covid_to_redshift,
] >> run_quality_checks_staging
run_quality_checks_staging >> [
    air_pollution_load,
    air_stations_load,
    pollutants_load,
    covid_numbers_load,
    counties_load,
    population_load,
] >> run_quality_checks_final

run_quality_checks_final >> [
    run_check_pollutants,
    run_check_air_stations,
    run_check_counties,
    run_check_county_population,
] >> end_operator
