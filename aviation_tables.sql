-- an external location pointing to mstr-aviation-oai s3 bucket with parquet files is expected to be created

-- air_oai_facts.airline_flights_scheduled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_scheduled;
CREATE TABLE air_oai_facts.airline_flights_scheduled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/schedule/parquet';

-- air_oai_facts.airline_flights_completed
DROP TABLE IF EXISTS air_oai_facts.airline_flights_completed;
CREATE TABLE air_oai_facts.airline_flights_completed USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/completed/parquet';

-- air_oai_facts.airline_flights_cancelled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_cancelled;
CREATE TABLE air_oai_facts.airline_flights_cancelled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/cancelled/parquet';

-- air_oai_facts.airline_flights_diverted
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted;
CREATE TABLE air_oai_facts.airline_flights_diverted USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/diverted/parquet';

-- air_oai_facts.airline_flights_diverted_legs
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted_legs;
CREATE TABLE air_oai_facts.airline_flights_diverted_legs USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/diverted-legs/parquet';

-- air_oai_facts.airline_traffic_market
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/market/parquet/airline_traffic_market'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_traffic_segment
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/segment/parquet/airline_traffic_segment'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_itinerary
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_itinerary;
CREATE TABLE air_oai_facts.airfare_survey_itinerary USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/ticket/parquet/old_export'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_coupon
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_coupon;
CREATE TABLE air_oai_facts.airfare_survey_coupon USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/coupon/parquet/old_export/'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_market
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_market;
CREATE TABLE air_oai_facts.airfare_survey_market USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/market/parquet/old_export'
OPTIONS (recursiveFileLookup=true);

-- air_oai_dims.aircraft_configurations
DROP TABLE IF EXISTS air_oai_dims.aircraft_configurations;
CREATE TABLE air_oai_dims.aircraft_configurations USING PARQUET LOCATION 's3://mstr-aviation-oai/DIMS/parquet/aircraft_configurations.parquet';

-- air_oai_dims.aircraft_types
DROP TABLE IF EXISTS air_oai_dims.aircraft_types;
CREATE TABLE air_oai_dims.aircraft_types USING PARQUET LOCATION 's3://mstr-aviation-oai/DIMS/parquet/aircraft_types.parquet';

-- air_oai_dims.airline_entities
DROP TABLE IF EXISTS air_oai_dims.airline_entities;
CREATE TABLE air_oai_dims.airline_entities USING PARQUET LOCATION 's3://mstr-aviation-oai/DIMS/parquet/airline_entities.parquet';

-- air_oai_dims.airline_service_classes
DROP TABLE IF EXISTS air_oai_dims.airline_service_classes;
CREATE TABLE air_oai_dims.airline_service_classes USING PARQUET LOCATION 's3://mstr-aviation-oai/DIMS/parquet/airline_service_classes.parquet';

-- air_oai_dims.airport_history
DROP TABLE IF EXISTS air_oai_dims.airport_history;
CREATE TABLE air_oai_dims.airport_history USING PARQUET LOCATION 's3://mstr-aviation-oai/DIMS/parquet/airport_history.parquet';

-- air_oai_dims.world_areas
DROP TABLE IF EXISTS air_oai_dims.world_areas;
CREATE TABLE air_oai_dims.world_areas USING PARQUET LOCATION 's3://mstr-aviation-oai/DIMS/parquet/world_areas.parquet';

-- air_faa_reg.aircraft_registry
DROP TABLE IF EXISTS air_faa_reg.aircraft_registry;
CREATE TABLE air_faa_reg.aircraft_registry USING PARQUET LOCATION 's3://mstr-aviation-oai/FAA/parquet/aircraft_registry.parquet';

-- air_faa_reg.aircraft_reference
DROP TABLE IF EXISTS air_faa_reg.aircraft_reference;
CREATE TABLE air_faa_reg.aircraft_reference USING PARQUET LOCATION 's3://mstr-aviation-oai/FAA/parquet/aircraft_reference.parquet';

-- air_faa_reg.engine_reference
DROP TABLE IF EXISTS air_faa_reg.engine_reference;
CREATE TABLE air_faa_reg.engine_reference USING PARQUET LOCATION 's3://mstr-aviation-oai/FAA/parquet/engine_reference.parquet';

-- calendar_dbx.calendar_date_alpha
DROP TABLE IF EXISTS calendar_dbx.calendar_date_alpha;
CREATE TABLE calendar_dbx.calendar_date_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/calendar_date_alpha.parquet';

-- calendar_dbx.day_of_week_alpha
DROP TABLE IF EXISTS calendar_dbx.day_of_week_alpha;
CREATE TABLE calendar_dbx.day_of_week_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/day_of_week_alpha.parquet';

-- calendar_dbx.gregorian_month_of_year_alpha
DROP TABLE IF EXISTS calendar_dbx.gregorian_month_of_year_alpha;
CREATE TABLE calendar_dbx.gregorian_month_of_year_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/gregorian_month_of_year_alpha.parquet';

-- calendar_dbx.gregorian_quarter_of_year_alpha
DROP TABLE IF EXISTS calendar_dbx.gregorian_quarter_of_year_alpha;
CREATE TABLE calendar_dbx.gregorian_quarter_of_year_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/gregorian_quarter_of_year_alpha.parquet';

-- calendar_dbx.gregorian_year_alpha
DROP TABLE IF EXISTS calendar_dbx.gregorian_year_alpha;
CREATE TABLE calendar_dbx.gregorian_year_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/gregorian_year_alpha.parquet';

-- calendar_dbx.gregorian_year_month_alpha
DROP TABLE IF EXISTS calendar_dbx.gregorian_year_month_alpha;
CREATE TABLE calendar_dbx.gregorian_year_month_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/gregorian_year_month_alpha.parquet';

-- calendar_dbx.gregorian_year_quarter_alpha
DROP TABLE IF EXISTS calendar_dbx.gregorian_year_quarter_alpha;
CREATE TABLE calendar_dbx.gregorian_year_quarter_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/gregorian_year_quarter_alpha.parquet';

-- calendar_dbx.hour_of_day_alpha
DROP TABLE IF EXISTS calendar_dbx.hour_of_day_alpha;
CREATE TABLE calendar_dbx.hour_of_day_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/hour_of_day_alpha.parquet';

-- calendar_dbx.minute_of_hour_alpha
DROP TABLE IF EXISTS calendar_dbx.minute_of_hour_alpha;
CREATE TABLE calendar_dbx.minute_of_hour_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/minute_of_hour_alpha.parquet';

-- calendar_dbx.year_week_alpha
DROP TABLE IF EXISTS calendar_dbx.year_week_alpha;
CREATE TABLE calendar_dbx.year_week_alpha USING PARQUET LOCATION 's3://mstr-aviation-oai/CAL/parquet/year_week_alpha.parquet';
