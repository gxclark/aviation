-- s3 bucket with parquet files is expected to be mounted to dbx at /mnt/mount_s3

-- air_oai_facts.airline_flights_scheduled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_scheduled;
CREATE TABLE air_oai_facts.airline_flights_scheduled USING PARQUET LOCATION '/mnt/mount_s3/OTP/schedule/parquet';

-- air_oai_facts.airline_flights_completed
DROP TABLE IF EXISTS air_oai_facts.airline_flights_completed;
CREATE TABLE air_oai_facts.airline_flights_completed USING PARQUET LOCATION '/mnt/mount_s3/OTP/completed/parquet';

-- air_oai_facts.airline_flights_cancelled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_cancelled;
CREATE TABLE air_oai_facts.airline_flights_cancelled USING PARQUET LOCATION '/mnt/mount_s3/OTP/cancelled/parquet';

-- air_oai_facts.airline_flights_diverted
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted;
CREATE TABLE air_oai_facts.airline_flights_diverted USING PARQUET LOCATION '/mnt/mount_s3/OTP/diverted/parquet';

-- air_oai_facts.airline_flights_diverted_legs
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted_legs;
CREATE TABLE air_oai_facts.airline_flights_diverted_legs USING PARQUET LOCATION '/mnt/mount_s3/OTP/diverted-legs/parquet';

-- air_oai_facts.airline_traffic_market
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market USING PARQUET LOCATION '/mnt/mount_s3/T100/market/parquet/airline_traffic_market'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_traffic_segment
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment USING PARQUET LOCATION '/mnt/mount_s3/T100/segment/parquet/airline_traffic_segment'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_itinerary
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_itinerary;
CREATE TABLE air_oai_facts.airfare_survey_itinerary USING PARQUET LOCATION '/mnt/mount_s3/DB1B/market/parquet/airfare_survey_itinerary'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_coupon
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_coupon;
CREATE TABLE air_oai_facts.airfare_survey_coupon USING PARQUET LOCATION '/mnt/mount_s3/DB1B/coupon/parquet/airfare_survey_coupon'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_market
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_market;
CREATE TABLE air_oai_facts.airfare_survey_market USING PARQUET LOCATION '/mnt/mount_s3/DB1B/market/parquet/airfare_survey_market'
OPTIONS (recursiveFileLookup=true);

-- air_oai_dims.aircraft_configurations
DROP TABLE IF EXISTS air_oai_dims.aircraft_configurations;
CREATE TABLE air_oai_dims.aircraft_configurations USING PARQUET LOCATION '/mnt/mount_s3/DIMS/parquet/aircraft_configurations.parquet';

-- air_oai_dims.aircraft_types
DROP TABLE IF EXISTS air_oai_dims.aircraft_types;
CREATE TABLE air_oai_dims.aircraft_types USING PARQUET LOCATION '/mnt/mount_s3/DIMS/parquet/aircraft_types.parquet';

-- air_oai_dims.airline_entities
DROP TABLE IF EXISTS air_oai_dims.airline_entities;
CREATE TABLE air_oai_dims.airline_entities USING PARQUET LOCATION '/mnt/mount_s3/DIMS/parquet/airline_entities.parquet';

-- air_oai_dims.airline_service_classes
DROP TABLE IF EXISTS air_oai_dims.airline_service_classes;
CREATE TABLE air_oai_dims.airline_service_classes USING PARQUET LOCATION '/mnt/mount_s3/DIMS/parquet/airline_service_classes.parquet';

-- air_oai_dims.airport_history
DROP TABLE IF EXISTS air_oai_dims.airport_history;
CREATE TABLE air_oai_dims.airport_history USING PARQUET LOCATION '/mnt/mount_s3/DIMS/parquet/airport_history.parquet';

-- air_oai_dims.world_areas
DROP TABLE IF EXISTS air_oai_dims.world_areas;
CREATE TABLE air_oai_dims.world_areas USING PARQUET LOCATION '/mnt/mount_s3/DIMS/parquet/world_areas.parquet';

-- air_faa_reg.aircraft_registry
DROP TABLE IF EXISTS air_faa_reg.aircraft_registry;
CREATE TABLE air_faa_reg.aircraft_registry USING PARQUET LOCATION '/mnt/mount_s3/FAA/parquet/aircraft_registry.parquet';

-- air_faa_reg.aircraft_reference
DROP TABLE IF EXISTS air_faa_reg.aircraft_reference;
CREATE TABLE air_faa_reg.aircraft_reference USING PARQUET LOCATION '/mnt/mount_s3/FAA/parquet/aircraft_reference.parquet';

-- air_faa_reg.engine_reference
DROP TABLE IF EXISTS air_faa_reg.engine_reference;
CREATE TABLE air_faa_reg.engine_reference USING PARQUET LOCATION '/mnt/mount_s3/FAA/parquet/engine_reference.parquet';
