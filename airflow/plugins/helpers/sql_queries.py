class SqlQueries:
    data_cleaning_air_pollution_staging = """UPDATE air_pollution_staging SET time=NULL WHERE time='-'; 
UPDATE air_pollution_staging SET value=NULL WHERE value='-'; 
    """
    air_pollution_table_insert = """
           INSERT INTO air_pollution(
            station_id,
            pollutant_id,
            date,
            time_hour,
            value
        )
        SELECT  station_code, md5(pollutant||evaluation_kind||unit), date,time::int , REPLACE (
     value,
   ',',
   '.'
   )::float
        FROM air_pollution_staging
    """

    air_stations_table_insert = """
        INSERT INTO air_stations(
    station_id,
    station_name,
    state_name,
    station_surrounding,
    station_type,
    longitude,
    latitude,
    altitude
)
SELECT pollution.station_code,pollution.station_name,pollution.state,pollution.station_surrounding,pollution.station_type,station.longitude,station.latitude,station.altitude
FROM (SELECT DISTINCT station_code,station_name,state,station_surrounding,station_type FROM  air_pollution_staging) pollution
LEFT JOIN air_station_staging station
ON pollution.station_code=station.station_code
    """

    pollutants_table_insert = """
        INSERT INTO pollutants(
            pollutant_id,
            pollutant_name,
            evaluation_kind,
            unit
        )
        SELECT DISTINCT  md5(pollutant||evaluation_kind||unit), pollutant,evaluation_kind,unit
        FROM air_pollution_staging
    """

    covid_numbers_table_insert = """
        INSERT INTO covid_numbers
SELECT county_id,age_group,gender,cases,death_cases,reporting_date,data_status,cases_new,death_cases_new,reference_date,cases_recovered_new,cases_recovered
FROM covid_staging
    """

    counties_table_insert = """
        INSERT INTO counties
SELECT DISTINCT county_id,county_name,county_type,latitude,longitude FROM population_staging
    """

    county_population_table_insert = """
        INSERT INTO county_population
SELECT DISTINCT county_id,age_group,value,age_from::int,age_to::int,gender FROM population_staging
    """
