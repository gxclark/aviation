-- this script gives the ability to change partitioning scheme for fact tables by re-pointing external tables location to parquet exports with various partitioning settings

-- OTP data partitioning schemes: MO/QT/WK/YR (Month/Quarter/Week/Year)
-- T100 data partitioning schemes: MO/QT/YR 
-- DB1B data partitioning schemes: QT/YR 

-----------
-- OTP - MO
-----------
-- air_oai_facts.airline_flights_scheduled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_scheduled;
CREATE TABLE air_oai_facts.airline_flights_scheduled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/scheduled/MO/airline_flights_scheduled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_completed
DROP TABLE IF EXISTS air_oai_facts.airline_flights_completed;
CREATE TABLE air_oai_facts.airline_flights_completed USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/completed/MO/airline_flights_completed'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_cancelled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_cancelled;
CREATE TABLE air_oai_facts.airline_flights_cancelled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/cancelled/MO/airline_flights_cancelled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted;
CREATE TABLE air_oai_facts.airline_flights_diverted USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted/MO/airline_flights_diverted'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted_legs
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted_legs;
CREATE TABLE air_oai_facts.airline_flights_diverted_legs USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted-legs/MO/airline_flights_diverted_legs'
OPTIONS (recursiveFileLookup=true);

-----------
-- OTP - QT
-----------
-- air_oai_facts.airline_flights_scheduled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_scheduled;
CREATE TABLE air_oai_facts.airline_flights_scheduled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/scheduled/QT/airline_flights_scheduled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_completed
DROP TABLE IF EXISTS air_oai_facts.airline_flights_completed;
CREATE TABLE air_oai_facts.airline_flights_completed USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/completed/QT/airline_flights_completed'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_cancelled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_cancelled;
CREATE TABLE air_oai_facts.airline_flights_cancelled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/cancelled/QT/airline_flights_cancelled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted;
CREATE TABLE air_oai_facts.airline_flights_diverted USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted/QT/airline_flights_diverted'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted_legs
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted_legs;
CREATE TABLE air_oai_facts.airline_flights_diverted_legs USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted-legs/QT/airline_flights_diverted_legs'
OPTIONS (recursiveFileLookup=true);

-----------
-- OTP - WK
-----------
-- air_oai_facts.airline_flights_scheduled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_scheduled;
CREATE TABLE air_oai_facts.airline_flights_scheduled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/scheduled/WK/airline_flights_scheduled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_completed
DROP TABLE IF EXISTS air_oai_facts.airline_flights_completed;
CREATE TABLE air_oai_facts.airline_flights_completed USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/completed/WK/airline_flights_completed'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_cancelled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_cancelled;
CREATE TABLE air_oai_facts.airline_flights_cancelled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/cancelled/WK/airline_flights_cancelled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted;
CREATE TABLE air_oai_facts.airline_flights_diverted USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted/WK/airline_flights_diverted'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted_legs
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted_legs;
CREATE TABLE air_oai_facts.airline_flights_diverted_legs USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted-legs/WK/airline_flights_diverted_legs'
OPTIONS (recursiveFileLookup=true);

-----------
-- OTP - YR
-----------
-- air_oai_facts.airline_flights_scheduled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_scheduled;
CREATE TABLE air_oai_facts.airline_flights_scheduled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/scheduled/YR/airline_flights_scheduled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_completed
DROP TABLE IF EXISTS air_oai_facts.airline_flights_completed;
CREATE TABLE air_oai_facts.airline_flights_completed USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/completed/YR/airline_flights_completed'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_cancelled
DROP TABLE IF EXISTS air_oai_facts.airline_flights_cancelled;
CREATE TABLE air_oai_facts.airline_flights_cancelled USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/cancelled/YR/airline_flights_cancelled'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted;
CREATE TABLE air_oai_facts.airline_flights_diverted USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted/YR/airline_flights_diverted'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_flights_diverted_legs
DROP TABLE IF EXISTS air_oai_facts.airline_flights_diverted_legs;
CREATE TABLE air_oai_facts.airline_flights_diverted_legs USING PARQUET LOCATION 's3://mstr-aviation-oai/OTP/parquet/diverted-legs/YR/airline_flights_diverted_legs'
OPTIONS (recursiveFileLookup=true);

-----------
-- T100 - MO 
-----------
-- air_oai_facts.airline_traffic_market
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/market/parquet/MO/airline_traffic_market'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_traffic_segment
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/segment/parquet/MO/airline_traffic_segment'
OPTIONS (recursiveFileLookup=true);

-----------
-- T100 - QT
-----------
-- air_oai_facts.airline_traffic_market
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/market/parquet/QT/airline_traffic_market'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_traffic_segment
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/segment/parquet/QT/airline_traffic_segment'
OPTIONS (recursiveFileLookup=true);

-----------
-- T100 - YR 
-----------
-- air_oai_facts.airline_traffic_market
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/market/parquet/YR/airline_traffic_market'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airline_traffic_segment
DROP TABLE IF EXISTS air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment USING PARQUET LOCATION 's3://mstr-aviation-oai/T100/segment/parquet/YR/airline_traffic_segment'
OPTIONS (recursiveFileLookup=true);

-----------
-- DB1B - QT
-----------
-- air_oai_facts.airfare_survey_itinerary
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_itinerary;
CREATE TABLE air_oai_facts.airfare_survey_itinerary USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/ticket/parquet/QT/airfare_survey_itinerary'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_coupon
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_coupon;
CREATE TABLE air_oai_facts.airfare_survey_coupon USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/coupon/parquet/QT/airfare_survey_coupon'
OPTIONS (recursiveFileLookup=true);

-----------
-- DB1B - YR
-----------
-- air_oai_facts.airfare_survey_itinerary
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_itinerary;
CREATE TABLE air_oai_facts.airfare_survey_itinerary USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/ticket/parquet/YR/airfare_survey_itinerary'
OPTIONS (recursiveFileLookup=true);

-- air_oai_facts.airfare_survey_coupon
DROP TABLE IF EXISTS air_oai_facts.airfare_survey_coupon;
CREATE TABLE air_oai_facts.airfare_survey_coupon USING PARQUET LOCATION 's3://mstr-aviation-oai/DB1B/coupon/parquet/YR/airfare_survey_coupon'
OPTIONS (recursiveFileLookup=true);
