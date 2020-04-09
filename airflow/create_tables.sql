CREATE TABLE IF NOT EXISTS public.air_pollution_staging(
    state varchar(256),
    station_code varchar(256),
    station_name varchar(256),
    station_surrounding varchar(256),
    station_type varchar(256),
    pollutant varchar(256),
    evaluation_kind varchar(256),
    date date,
    time varchar(2),
    value varchar(32),
    unit varchar(256)   
);

CREATE TABLE IF NOT EXISTS public.air_station_staging(
    station_code varchar(7),
    longitude numeric (10,7),
    latitude numeric (10,7),
    altitude int
);

CREATE TABLE IF NOT EXISTS public.population_staging(
    county_id varchar(12),
    county_name varchar(256),
    county_type varchar(17),
    age_group varchar(22),
    value int,
    age_from float,
    age_to float,
    gender char,
    latitude numeric (10,7),
    longitude numeric (10,7)
);

CREATE TABLE IF NOT EXISTS public.covid_staging(
    state_id int4,
    state_name varchar(100),--Bundesland
    county varchar(100),--Landkreis
    age_group varchar(10),--Altersgruppe
    gender char, --Geschlecht
    cases int, --AnzahlFall
    death_cases int,--AnzahlTodesfall
     object_id int,--ObjectId
     reporting_date varchar(100),--Meldedatum
     county_id varchar(12),--IdLandkreis
     data_status varchar(22),--Datenstand 
     cases_new int,--NeuerFall
     death_cases_new int,--NeuerTodesfall
     reference_date varchar(22),--Refdatum
     cases_recovered_new int,--NeuGenesen
     cases_recovered int --AnzahlGenesen
);

CREATE TABLE IF NOT EXISTS public.air_pollution(
    station_id varchar(256) NOT NULL,
    pollutant_id varchar(32) NOT NULL,
    date date NOT NULL,
    time_hour int ,
    value float 
);

CREATE TABLE IF NOT EXISTS public.air_stations(
    station_id varchar(256) PRIMARY KEY,
    station_name varchar(256) NOT NULL,
    state_name varchar(256) NOT NULL,
    station_surrounding varchar(256) NOT NULL,
    station_type varchar(256) NOT NULL,
    longitude numeric(10,7),
    latitude numeric(10,7) ,
    altitude int
);

CREATE TABLE IF NOT EXISTS public.pollutants(
    pollutant_id varchar(256),
    pollutant_name varchar(256),
    evaluation_kind varchar(256),
    unit varchar(256)
);

CREATE TABLE IF NOT EXISTS public.covid_numbers(
     county_id varchar(12),--IdLandkreis
     age_group varchar(10),--Altersgruppe
     gender char,--Geschlecht
     cases int,--AnzahlFall
     death_cases int,--AnzahlTodesfall
     reporting_date varchar(100),--Meldedatum
     data_status  varchar(22),--Datenstand
     cases_new int,--NeuerFall
     death_cases_new int,--NeuerTodesfall
     reference_date varchar(22),--Refdatum
     cases_recovered_new int,--NeuGenesen 
     cases_recovered int--AnzahlGenesen
);

CREATE TABLE IF NOT EXISTS public.counties(
    county_id varchar(12),--IdLandkreis
    county_name varchar(100),--Landkreis
    county_type varchar(17),
    latitude numeric (10,7),
    longitude numeric (10,7)
);

CREATE TABLE IF NOT EXISTS public.county_population(
    county_id varchar(12),--IdLandkreis
    age_group varchar(22),
    value int,
    age_from float,
    age_to float,
    gender char
);