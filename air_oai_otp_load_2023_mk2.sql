
-- In order to process the Flight Performance data, we need a valid time zone name for each airport.
-- The dimension table from OAI does not contain this data, so we have to load the time zone boundaries, and then update the airport history dimension: 

create schema geography;

-- Shape file for time zone boundaries was located here:
-- https://github.com/evansiroky/timezone-boundary-builder/releases/download/2023b/timezones-with-oceans.shapefile.zip
-- This is after the shape file was loaded via shp2pgsql command line tool:
-- shp2pgsql -I -s 4326 combined-shapefile-with-oceans.shp | psql -p 5432 -d aviation 
select * from public."combined-shapefile-with-oceans" limit 10;
alter table public."combined-shapefile-with-oceans" rename to timezone_boundaries;
alter table public.timezone_boundaries set schema geography;
alter table geography.timezone_boundaries rename to time_zone_boundaries;

SELECT * FROM air_oai_dims.airport_history limit 100;
SELECT time_zone_name, count(*) FROM air_oai_dims.airport_history group by 1 order by count(*) desc;

update air_oai_dims.airport_history
set	time_zone_name = c.time_zone_name, updated_by = 'gxclark', updated_tsmt = now()
from (
select a.airport_history_id, a.airport_oai_code, a.effective_from_date, b.time_zone_name 
from (select airport_history_id, airport_oai_code, effective_from_date, point_geom 
      from air_oai_dims.airport_history where time_zone_name is null /*limit 10000*/) a
cross join (select gid, tzid as time_zone_name, geom as time_zone_geom from geography.time_zone_boundaries) b
where ST_Contains(b.time_zone_geom, a.point_geom) is true
) c
where air_oai_dims.airport_history.airport_history_id = c.airport_history_id
and air_oai_dims.airport_history.time_zone_name is null
; 
-- With CentOS 7 and Postgres 9.x (?) these were the durations for PostGIS update processing:
-- 12 in 4.86 secs; 470 in 116.3 secs; 429 in 115.6 secs, 408 in 115.4 secs, 840 in 232.6 secs, 772 in 231.5 secs, 674 in 213.8 secs; 0 in 54 secs (done)
-- With RHEL 9.2 and Postgres 15, this was the the duration for PostGIS update processing:
-- 19,131 updates in 5.319 secs

/* -- Data Quality Check for airports having a time zone name:
select time_zone_name, min(airport_oai_code) as min_oai_code, max(airport_oai_code) as max_oai_code, count(*) 
from air_oai_dims.airport_history group by 1 order by count(*) desc;
select * from air_oai_dims.airport_history where airport_oai_code = 'ZZZ'; -- Unknown Point in Alaska
*/

-- In Postgres, we can set the timezone variable and view it like this:
-- SET TIME ZONE 'UTC';
-- SELECT  current_setting('TIMEZONE'); -- America/Los_Angeles -- Asia/Tokyo

--------------------------------------------------
-- air_oai_facts.airline_flight_performance_fdw --
-- This is a Foreign Table from a file to load  --
--------------------------------------------------

-- drop materialized view if exists air_oai_facts.airline_flight_performance_mv;
-- drop foreign table if exists air_oai_facts.airline_flight_performance_fdw;
create foreign table air_oai_facts.airline_flight_performance_fdw
	( year_nbr                 			smallint null
	, quarter_nbr              			smallint null
	, month_nbr                			smallint null
	, day_of_month           			smallint null
	, day_of_week            			smallint null
	, flight_date           			date null
	, airline_unique_oai_code        	varchar(10) null
	, airline_usdot_id            		integer null
	, airline_oai_code              	char(3) null
	, tail_nbr              			varchar(7) null
	, flight_nbr            			varchar(4) null
	, depart_airport_oai_id				integer null
	, depart_airport_seq_id				integer null
	, depart_city_market_id				integer null
	, depart_airport_oai_code           char(3) null
	, depart_city_name       			varchar(125) null
	, depart_state_iso_code         	char(2) null
	, depart_state_fips_code      		varchar(3) null
	, depart_state_name      			varchar(125) null
	, depart_world_area_oai_id          smallint null
	, arrive_airport_oai_id				integer null
	, arrive_airport_seq_oai_id			integer null
	, arrive_city_market_id				integer null
	, arrive_airport_oai_code          	char(3) null
	, arrive_city_name         			varchar(125) null
	, arrive_state_iso_code            	char(2) null
	, arrive_state_fips_code        	varchar(3) null
	, arrive_state_name        			varchar(125) null
	, arrive_world_area_oai_id          smallint null
	, report_depart_time_lcl      		char(4) null
	, actual_depart_time_lcl      		char(4) null
	, depart_delay_min            		float4 null
	, depart_delay_pos_min      		float4 null
	, depart_delay_15min_ind      		float4 null
	, depart_delay_group_id				smallint null
	, depart_time_block           		varchar(10) null
	, taxi_out_min              		float4 null
	, wheels_off_time_lcl         		char(4) null
	, wheels_on_time_lcl          		char(4) null
	, taxi_in_min               		float4 null
	, report_arrive_time_lcl      		char(4) null
	, actual_arrive_time_lcl      		char(4) null
	, arrive_delay_min            		float4 null
	, arrive_delay_pos_min      		float4 null
	, arrive_delay_15min_ind      		float4 null
	, arrive_delay_group_id   			smallint null
	, arrive_time_block           		varchar(10) null
	, cancelled_ind            			float4 null
	, cancellation_code     			varchar(10) null
	, diverted_ind             			float4 null
	, report_elapsed_time_min     		float4 null
	, actual_elapsed_time_min    		float4 null
	, airborne_time_min           		float4 null
	, flight_count              		float4 null
	, distance_smi             			float4 null
	, distance_group_id        			float4 null
	, airline_delay_min         		float4 null
	, weather_delay_min         		float4 null
	, nas_delay_min             		float4 null
	, security_delay_min        		float4 null
	, late_aircraft_delay_min    		float4 null
	, first_gate_depart_time      		varchar(10) null
	, total_ground_time        			varchar(10) null
	, longest_ground_time      			varchar(10) null
	, diverted_airport_landing_count	float4 null
	, diverted_reached_dest_ind			float4 null
	, diverted_actual_elapsed_time_min 	float4 null
	, diverted_arrive_delay_min			float4 null
	, diverted_distance_smi       		float4 null
	, diverted1_airport_oai_code		char(3) null
	, diverted1_airport_oai_id			integer null
	, diverted1_airport_seq_oai_id		integer null
	, diverted1_wheels_on_time_lcl		char(4) null
	, diverted1_total_ground_time_min	float4 null
	, diverted1_longest_ground_time_min	float4 null
	, diverted1_wheels_off_time_lcl		char(4) null
	, diverted1_tail_nbr				varchar(7) null
	, diverted2_airport_oai_code		char(3) null
	, diverted2_airport_oai_id			integer null
	, diverted2_airport_seq_oai_id		integer null
	, diverted2_wheels_on_time_lcl		char(4) null
	, diverted2_total_ground_time_min	float4 null
	, diverted2_longest_ground_time_min	float4 null
	, diverted2_wheels_off_time_lcl		char(4) null
	, diverted2_tail_nbr				varchar(7) null
	, diverted3_airport_oai_code		char(3) null
	, diverted3_airport_oai_id			integer null
	, diverted3_airport_seq_oai_id		integer null
	, diverted3_wheels_on_time_lcl		char(4) null
	, diverted3_total_ground_time_min	float4 null
	, diverted3_longest_ground_time_min	float4 null
	, diverted3_wheels_off_time_lcl		char(4) null
	, diverted3_tail_nbr				varchar(7) null
	, diverted4_airport_oai_code		char(3) null
	, diverted4_airport_oai_id			integer null
	, diverted4_airport_seq_oai_id		integer null
	, diverted4_wheels_on_time_lcl		char(4) null
	, diverted4_total_ground_time_min	float4 null
	, diverted4_longest_ground_time_min	float4 null
	, diverted4_wheels_off_time_lcl		char(4) null
	, diverted4_tail_nbr				varchar(7) null
	, diverted5_airport_oai_code		char(3) null
	, diverted5_airport_oai_id			integer null
	, diverted5_airport_seq_oai_id		integer null
	, diverted5_wheels_on_time_lcl		char(4) null
	, diverted5_total_ground_time_min	float4 null
	, diverted5_longest_ground_time_min	float4 null
	, diverted5_wheels_off_time_lcl		char(4) null
	, diverted5_tail_nbr				varchar(7) null
	, filler							varchar(10) null
	)
server mochida options 
	( format 'csv'
	, header 'true'
	, filename '/opt/_data/_air/_oai/_otp/OTP_Reporting_Carrier_2023_04.csv'
	-- '/opt/_data/_oai/On_Time_On_Time_Performance_2015_1.csv'
	, delimiter ','
	, null ''
	);

-- select count(*) from air_oai_facts.airline_flight_performance_fdw; -- 561,441
-- select * from air_oai_facts.airline_flight_performance_fdw limit 100;

---------------------------------------------------------------------------
-- air_oai_facts.airline_flight_performance_mv                           --
-- The approach iss to just load data into memory as a materialized view --
-- Decided to do some data quality work in this layer, removed spaces    --
---------------------------------------------------------------------------

/* Check on embedded spaces in the "*time_lcl" data columns 
select fp.airline_oai_code||'|'||fp.flight_nbr||'|'||fp.flight_date::text||'|'||fp.depart_airport_oai_code as flight_key_comp
     , wheels_off_time_lcl
     , replace(wheels_off_time_lcl,'    ',null) as wheels_off_time_lcl
     , count(*)
from air_oai_facts.airline_flight_performance_mv fp
group by 1,2,3 order by 2 asc;
*/

-- drop materialized view air_oai_facts.airline_flight_performance_integrated_mv;
-- drop materialized view air_oai_facts.airline_flight_performance_mv;
-- create materialized view air_oai_facts.airline_flight_performance_mv as 
select flight_date
	 , airline_oai_code
	 , tail_nbr
	 , flight_nbr
	 , depart_airport_oai_code
	 , arrive_airport_oai_code
	 , report_depart_time_lcl
	 , case when replace(actual_depart_time_lcl,' ','') = '' then null 
	        else actual_depart_time_lcl end::char(4) as actual_depart_time_lcl
	 , depart_delay_min
	 , depart_delay_pos_min
	 , depart_delay_15min_ind
	 , depart_delay_group_id
	 , depart_time_block
	 , taxi_out_min
	 , case when replace(wheels_off_time_lcl,' ','') = '' then null 
	        else wheels_off_time_lcl end::char(4) as wheels_off_time_lcl
	 , case when replace(wheels_on_time_lcl,' ','') = '' then null 
	        else wheels_on_time_lcl end::char(4) as wheels_on_time_lcl
	 , taxi_in_min
	 , report_arrive_time_lcl
	 , case when replace(actual_arrive_time_lcl, ' ','') = '' then null 
	        else actual_arrive_time_lcl end::char(4) as actual_arrive_time_lcl
	 , arrive_delay_min
	 , arrive_delay_pos_min
	 , arrive_delay_15min_ind
	 , arrive_delay_group_id
	 , arrive_time_block
	 , cancelled_ind
	 , cancellation_code
	 , diverted_ind
	 , report_elapsed_time_min
	 , actual_elapsed_time_min
	 , airborne_time_min
	 , flight_count
	 , distance_smi
	 , distance_group_id
	 , airline_delay_min
	 , weather_delay_min
	 , nas_delay_min
	 , security_delay_min
	 , late_aircraft_delay_min
	 , case when replace(first_gate_depart_time, ' ','') = '' then null 
	        else first_gate_depart_time end::char(4) as first_gate_depart_time
	 , total_ground_time
	 , longest_ground_time
	 , diverted_airport_landing_count
	 , diverted_reached_dest_ind
	 , diverted_actual_elapsed_time_min
	 , diverted_arrive_delay_min
	 , diverted_distance_smi
	 , diverted1_airport_oai_code
	 --, diverted1_wheels_on_time_lcl
	 , case when replace(diverted1_wheels_on_time_lcl, ' ','') = '' then null 
	        else diverted1_wheels_on_time_lcl end::char(4) as diverted1_wheels_on_time_lcl
	 , diverted1_total_ground_time_min
	 , diverted1_longest_ground_time_min
	 --, diverted1_wheels_off_time_lcl
	 , case when replace(diverted1_wheels_off_time_lcl, ' ','') = '' then null 
	        else diverted1_wheels_off_time_lcl end::char(4) as diverted1_wheels_off_time_lcl
	 , diverted1_tail_nbr
	 , diverted2_airport_oai_code
	 --, diverted2_wheels_on_time_lcl
	 , case when replace(diverted2_wheels_on_time_lcl, ' ','') = '' then null 
	        else diverted2_wheels_on_time_lcl end::char(4) as diverted2_wheels_on_time_lcl
	 , diverted2_total_ground_time_min
	 , diverted2_longest_ground_time_min
	 --, diverted2_wheels_off_time_lcl
	 , case when replace(diverted2_wheels_off_time_lcl, ' ','') = '' then null 
	        else diverted2_wheels_off_time_lcl end::char(4) as diverted2_wheels_off_time_lcl
	 , diverted2_tail_nbr
	 , diverted3_airport_oai_code
	 --, diverted3_wheels_on_time_lcl
	 , case when replace(diverted3_wheels_on_time_lcl, ' ','') = '' then null 
	        else diverted3_wheels_on_time_lcl end::char(4) as diverted3_wheels_on_time_lcl
	 , diverted3_total_ground_time_min
	 , diverted3_longest_ground_time_min
	 --, diverted3_wheels_off_time_lcl
	 , case when replace(diverted3_wheels_off_time_lcl, ' ','') = '' then null 
	        else diverted3_wheels_off_time_lcl end::char(4) as diverted3_wheels_off_time_lcl
	 , diverted3_tail_nbr
	 , diverted4_airport_oai_code
	 --, diverted4_wheels_on_time_lcl
	 , case when replace(diverted4_wheels_on_time_lcl, ' ','') = '' then null 
	        else diverted4_wheels_on_time_lcl end::char(4) as diverted4_wheels_on_time_lcl
	 , diverted4_total_ground_time_min
	 , diverted4_longest_ground_time_min
	 --, diverted4_wheels_off_time_lcl
	 , case when replace(diverted4_wheels_off_time_lcl, ' ','') = '' then null 
	        else diverted4_wheels_off_time_lcl end::char(4) as diverted4_wheels_off_time_lcl
	 , diverted4_tail_nbr
	 , diverted5_airport_oai_code
	 --, diverted5_wheels_on_time_lcl
	 , case when replace(diverted5_wheels_on_time_lcl, ' ','') = '' then null 
	        else diverted5_wheels_on_time_lcl end::char(4) as diverted5_wheels_on_time_lcl
	 , diverted5_total_ground_time_min
	 , diverted5_longest_ground_time_min
	 --, diverted5_wheels_off_time_lcl
	 , case when replace(diverted5_wheels_off_time_lcl, ' ','') = '' then null 
	        else diverted5_wheels_off_time_lcl end::char(4) as diverted5_wheels_off_time_lcl
	 , diverted5_tail_nbr
from air_oai_facts.airline_flight_performance_fdw; -- 8.1 secs

/*
select case when wheels_on_time_lcl is null then 'null' when length(replace(wheels_on_time_lcl,' ','')) > 1 then 'data' else 'zzz' end as wheels_on_time_data
     , wheels_on_time_lcl, count(*) 
from air_oai_facts.airline_flight_performance_fdw group by 1,2 order by 1 desc;

select case when wheels_on_time_lcl is null then 'null' when length(replace(wheels_on_time_lcl,' ','')) > 1 then 'data' else 'zzz' end as wheels_on_time_data
     , wheels_on_time_lcl, count(*) 
from air_oai_facts.airline_flight_performance_mv group by 1,2 order by 1 desc;
*/

create index airline_flight_performance_mv_depart_airport_idx on air_oai_facts.airline_flight_performance_mv (depart_airport_oai_code);
create index airline_flight_performance_mv_arrive_airport_idx on air_oai_facts.airline_flight_performance_mv (arrive_airport_oai_code);
create index airline_flight_performance_mv_flight_date_idx on air_oai_facts.airline_flight_performance_mv (flight_date);

--1015.3 secs
-- 288.3 secs

/* -- save the table created in the older fashion .... still correct, but processing work is redundant:
alter table air_oai_facts.airline_flights_cancelled rename to airline_flights_cancelled_bak;
alter table air_oai_facts.airline_flights_completed rename to airline_flights_completed_bak;
alter table air_oai_facts.airline_flights_diverted rename to airline_flights_diverted_bak;
alter table air_oai_facts.airline_flights_scheduled rename to airline_flights_scheduled_bak;
*/

-- drop materialized view air_oai_facts.airline_flight_performance_integrated_mv;
create materialized view air_oai_facts.airline_flight_performance_integrated_mv as
select md5(fp.airline_oai_code||'|'||fp.flight_nbr||'|'||fp.flight_date::text||'|'||fp.depart_airport_oai_code)::char(32) as flight_key
     , fp.airline_oai_code||'|'||fp.flight_nbr||'|'||fp.flight_date::text||'|'||fp.depart_airport_oai_code as flight_key_comp
     , fp.flight_date::date								as flight_date
	 , fp.airline_oai_code::varchar(3)					as airline_oai_code
	 , ae.source_from_date								as airline_entity_from_date
	 , ae.airline_entity_id								as airline_entity_id
	 , ae.airline_entity_key							as airline_entity_key
	 , lpad(fp.flight_nbr,4,'0')::char(4)				as flight_nbr
	 , fp.flight_count::smallint						as flight_count
	 , fp.tail_nbr::varchar(10)							as tail_nbr
	 , fp.depart_airport_oai_code::char(3)				as depart_airport_oai_code
	 , a.effective_from_date							as depart_airport_from_date 
	 , a.airport_history_id								as depart_airport_history_id
	 , a.airport_history_key							as depart_airport_history_key
	 , a.time_zone_name									as depart_time_zone_name
	 , fp.arrive_airport_oai_code::char(3)				as arrive_airport_oai_code
	 , b.effective_from_date							as arrive_airport_from_date 
	 , b.airport_history_id								as arrive_airport_history_id
	 , b.airport_history_key							as arrive_airport_history_key
	 , b.time_zone_name									as arrive_time_zone_name
	 , case when cancelled_ind = 1 then 'cancelled' when diverted_ind = 1 then 'diverted' 
	        when fp.airline_delay_min::smallint is not null then 'arrived_delayed'
	        else 'arrived_on_time' end::varchar(25) as flight_status
     , fp.cancelled_ind::smallint						as cancelled_ind
	 , fp.cancellation_code::varchar(25)				as cancellation_code
	 , fp.diverted_ind::smallint						as diverted_ind
	 , fp.distance_smi::smallint						as distance_smi
	 , (fp.distance_smi / 1.1508::float)::smallint		as distance_nmi
	 , (fp.distance_smi * 1.60934::float)::smallint		as distance_kmt
	 , fp.distance_group_id::smallint					as distance_group_id
	 , fp.depart_time_block
	 , fp.arrive_time_block
	 , fp.report_depart_time_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(report_depart_time_lcl::time)::char(8))::timestamp) at time zone a.time_zone_name as report_depart_tmstz_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(report_depart_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 			  as report_depart_tmstz_utc
	 , fp.report_arrive_time_lcl
	 , timezone(b.time_zone_name, (flight_date::char(10)||' '||(report_arrive_time_lcl::time)::char(8))::timestamp) at time zone b.time_zone_name as report_arrive_tmstz_lcl
	 , timezone(b.time_zone_name, (flight_date::char(10)||' '||(report_arrive_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 			  as report_arrive_tmstz_utc
	 , fp.report_elapsed_time_min -- redundant?
	 , fp.actual_depart_time_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(actual_depart_time_lcl::time)::char(8))::timestamp) at time zone a.time_zone_name as actual_depart_tmstz_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(actual_depart_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 			  as actual_depart_tmstz_utc
	 , fp.actual_arrive_time_lcl
	 , timezone(b.time_zone_name, (flight_date::char(10)||' '||(actual_arrive_time_lcl::time)::char(8))::timestamp) at time zone b.time_zone_name as actual_arrive_tmstz_lcl
	 , timezone(b.time_zone_name, (flight_date::char(10)||' '||(actual_arrive_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 			  as actual_arrive_tmstz_utc
	 , fp.actual_elapsed_time_min -- redundant?
	 , fp.wheels_off_time_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(wheels_off_time_lcl::time)::char(8))::timestamp) at time zone a.time_zone_name as wheels_off_tmstz_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(wheels_off_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		   as wheels_off_tmstz_utc
	 , fp.wheels_on_time_lcl
	 , timezone(b.time_zone_name, (flight_date::char(10)||' '||(wheels_on_time_lcl::time)::char(8))::timestamp) at time zone b.time_zone_name as wheels_on_tmstz_lcl
	 , timezone(b.time_zone_name, (flight_date::char(10)||' '||(wheels_on_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 			  as wheels_on_tmstz_utc
	 , fp.airborne_time_min -- redundant?
	 , fp.taxi_out_min::smallint						as taxi_out_min  -- redundant?
	 , fp.taxi_in_min::smallint							as taxi_in_min  -- redundant?
	 , fp.first_gate_depart_time
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(first_gate_depart_time::time)::char(8))::timestamp) at time zone a.time_zone_name as first_gate_depart_tmstz_lcl
	 , timezone(a.time_zone_name, (flight_date::char(10)||' '||(first_gate_depart_time::time)::char(8))::timestamp) at time zone 'UTC' 			  as first_gate_depart_tmstz_utc
	 , (fp.total_ground_time::numeric(3,0))::smallint	as total_ground_time
	 , (fp.longest_ground_time::numeric(3,0))::smallint	as longest_ground_time
	 , fp.airline_delay_min::smallint					as airline_delay_min
	 , fp.weather_delay_min::smallint					as weather_delay_min
	 , fp.nas_delay_min::smallint						as nas_delay_min
	 , fp.security_delay_min::smallint					as security_delay_min
	 , fp.late_aircraft_delay_min::smallint				as late_aircraft_delay_min
	 , fp.diverted_airport_landing_count::smallint		as diverted_airport_landing_count
	 , fp.diverted_reached_dest_ind::smallint			as diverted_reached_dest_ind
	 , fp.diverted_actual_elapsed_time_min::smallint	as diverted_actual_elapsed_time_min
	 , fp.diverted_arrive_delay_min::smallint			as diverted_arrive_delay_min
	 , fp.diverted_distance_smi::integer				as diverted_distance_smi
	 , fp.diverted1_airport_oai_code::char(3)			as diverted1_airport_oai_code
	 , d1.effective_from_date							as diverted1_airport_from_date 
     , d1.airport_history_id							as diverted1_airport_history_id
	 , d1.airport_history_key							as diverted1_airport_history_key
	 , d1.time_zone_name								as diverted1_time_zone_name
	 , fp.diverted2_airport_oai_code::char(3)			as diverted2_airport_oai_code
	 , d2.effective_from_date							as diverted2_airport_from_date 
     , d2.airport_history_id							as diverted2_airport_history_id
	 , d2.airport_history_key							as diverted2_airport_history_key
	 , d2.time_zone_name								as diverted2_time_zone_name
	 , fp.diverted3_airport_oai_code::char(3)			as diverted3_airport_oai_code
	 , d3.effective_from_date							as diverted3_airport_from_date 
     , d3.airport_history_id							as diverted3_airport_history_id
	 , d3.airport_history_key							as diverted3_airport_history_key
	 , d3.time_zone_name								as diverted3_time_zone_name
	 , fp.diverted4_airport_oai_code::char(3)			as diverted4_airport_oai_code
	 , d4.effective_from_date							as diverted4_airport_from_date 
     , d4.airport_history_id							as diverted4_airport_history_id
	 , d4.airport_history_key							as diverted4_airport_history_key
	 , d4.time_zone_name								as diverted4_time_zone_name
	 , fp.diverted5_airport_oai_code::char(3)			as diverted5_airport_oai_code
	 , d5.effective_from_date							as diverted5_airport_from_date 
     , d5.airport_history_id							as diverted5_airport_history_id
	 , d5.airport_history_key							as diverted5_airport_history_key
	 , d5.time_zone_name								as diverted5_time_zone_name
	 , fp.diverted1_tail_nbr::varchar(10)				as diverted1_tail_nbr
	 , fp.diverted2_tail_nbr::varchar(10)				as diverted2_tail_nbr
	 , fp.diverted3_tail_nbr::varchar(10)				as diverted3_tail_nbr
	 , fp.diverted4_tail_nbr::varchar(10)				as diverted4_tail_nbr
	 , fp.diverted5_tail_nbr::varchar(10)				as diverted5_tail_nbr
	 , fp.diverted1_wheels_on_time_lcl
	 , timezone(d1.time_zone_name, (flight_date::char(10)||' '||(diverted1_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone d1.time_zone_name as diverted1_wheels_on_tmstz_lcl
	 , timezone(d1.time_zone_name, (flight_date::char(10)||' '||(diverted1_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 			 as diverted1_wheels_on_tmstz_utc
	 , fp.diverted1_wheels_off_time_lcl
	 , timezone(d1.time_zone_name, (flight_date::char(10)||' '||(diverted1_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone d1.time_zone_name as diverted1_wheels_off_tmstz_lcl
	 , timezone(d1.time_zone_name, (flight_date::char(10)||' '||(diverted1_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		      as diverted1_wheels_off_tmstz_utc
	 , fp.diverted1_total_ground_time_min::smallint		as diverted1_total_ground_time_min
	 , fp.diverted1_longest_ground_time_min::smallint	as diverted1_longest_ground_time_min
	 , fp.diverted2_wheels_on_time_lcl
	 , timezone(d2.time_zone_name, (flight_date::char(10)||' '||(diverted2_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone d2.time_zone_name as diverted2_wheels_on_tmstz_lcl
	 , timezone(d2.time_zone_name, (flight_date::char(10)||' '||(diverted2_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		      as diverted2_wheels_on_tmstz_utc
	 , fp.diverted2_wheels_off_time_lcl
	 , timezone(d2.time_zone_name, (flight_date::char(10)||' '||(diverted2_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone d2.time_zone_name as diverted2_wheels_off_tmstz_lcl
	 , timezone(d2.time_zone_name, (flight_date::char(10)||' '||(diverted2_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		       as diverted2_wheels_off_tmstz_utc
	 , fp.diverted2_total_ground_time_min::smallint		as diverted2_total_ground_time_min
	 , fp.diverted2_longest_ground_time_min::smallint	as diverted2_longest_ground_time_min
	 , fp.diverted3_wheels_on_time_lcl
	 , timezone(d3.time_zone_name, (flight_date::char(10)||' '||(diverted3_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone d3.time_zone_name as diverted3_wheels_on_tmstz_lcl
	 , timezone(d3.time_zone_name, (flight_date::char(10)||' '||(diverted3_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		      as diverted3_wheels_on_tmstz_utc
	 , fp.diverted3_wheels_off_time_lcl
	 , timezone(d3.time_zone_name, (flight_date::char(10)||' '||(diverted3_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone d3.time_zone_name as diverted3_wheels_off_tmstz_lcl
	 , timezone(d3.time_zone_name, (flight_date::char(10)||' '||(diverted3_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		       as diverted3_wheels_off_tmstz_utc
	 , fp.diverted3_total_ground_time_min::smallint		as diverted3_total_ground_time_min
	 , fp.diverted3_longest_ground_time_min::smallint	as diverted3_longest_ground_time_min
	 , fp.diverted4_wheels_on_time_lcl
	 , timezone(d4.time_zone_name, (flight_date::char(10)||' '||(diverted4_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone d4.time_zone_name as diverted4_wheels_on_tmstz_lcl
	 , timezone(d4.time_zone_name, (flight_date::char(10)||' '||(diverted4_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		      as diverted4_wheels_on_tmstz_utc
	 , fp.diverted4_wheels_off_time_lcl
	 , timezone(d4.time_zone_name, (flight_date::char(10)||' '||(diverted4_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone d4.time_zone_name as diverted4_wheels_off_tmstz_lcl
	 , timezone(d4.time_zone_name, (flight_date::char(10)||' '||(diverted4_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		       as diverted4_wheels_off_tmstz_utc
	 , fp.diverted4_total_ground_time_min::smallint		as diverted4_total_ground_time_min
	 , fp.diverted4_longest_ground_time_min::smallint	as diverted4_longest_ground_time_min
	 , fp.diverted5_wheels_on_time_lcl
	 , timezone(d5.time_zone_name, (flight_date::char(10)||' '||(diverted5_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone d5.time_zone_name as diverted5_wheels_on_tmstz_lcl
	 , timezone(d5.time_zone_name, (flight_date::char(10)||' '||(diverted5_wheels_on_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		      as diverted5_wheels_on_tmstz_utc
	 , fp.diverted5_wheels_off_time_lcl
	 , timezone(d5.time_zone_name, (flight_date::char(10)||' '||(diverted5_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone d5.time_zone_name as diverted5_wheels_off_tmstz_lcl
	 , timezone(d5.time_zone_name, (flight_date::char(10)||' '||(diverted5_wheels_off_time_lcl::time)::char(8))::timestamp) at time zone 'UTC' 		       as diverted5_wheels_off_tmstz_utc
	 , fp.diverted5_total_ground_time_min::smallint		as diverted5_total_ground_time_min
	 , fp.diverted5_longest_ground_time_min::smallint	as diverted5_longest_ground_time_min
FROM air_oai_facts.airline_flight_performance_mv fp
left outer
join (select airline_entity_id, airline_entity_key, airline_oai_code, source_from_date, source_thru_date from air_oai_dims.airline_entities 
      where operating_region_code = 'Domestic') ae
  on fp.airline_oai_code = ae.airline_oai_code and fp.flight_date between ae.source_from_date and coalesce(ae.source_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) a 
  on fp.depart_airport_oai_code = a.airport_oai_code and fp.flight_date between a.effective_from_date and coalesce(a.effective_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) b
  on fp.arrive_airport_oai_code = b.airport_oai_code and fp.flight_date between b.effective_from_date and coalesce(b.effective_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) d1 
  on fp.diverted1_airport_oai_code = d1.airport_oai_code and fp.flight_date between d1.effective_from_date and coalesce(d1.effective_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) d2
  on fp.diverted2_airport_oai_code = d2.airport_oai_code and fp.flight_date between d2.effective_from_date and coalesce(d2.effective_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) d3
  on fp.diverted3_airport_oai_code = d3.airport_oai_code and fp.flight_date between d3.effective_from_date and coalesce(d3.effective_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) d4
  on fp.diverted4_airport_oai_code = d4.airport_oai_code and fp.flight_date between d4.effective_from_date and coalesce(d4.effective_thru_date, current_date)
left outer
join (select airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date, time_zone_name from air_oai_dims.airport_history) d5
  on fp.diverted5_airport_oai_code = d5.airport_oai_code and fp.flight_date between d5.effective_from_date and coalesce(d5.effective_thru_date, current_date)
--where diverted_ind = 1
;

-------------------------------
-- layered approach planning --
-------------------------------
/* How many of each should we see?
select 0::smallint as scheduled_ind
     , cancelled_ind::smallint as cancelled_ind
     , diverted_ind::smallint  as diverted_ind
     , count(*) as record_qty
     --, (select count(*)::integer from air_oai_facts.airline_flight_performance_fdw) as total_records
     , (count(*)::float / (select count(*)::integer from air_oai_facts.airline_flight_performance_fdw))::numeric(4,3) as record_pct
from air_oai_facts.airline_flight_performance_fdw
group by 2,3
union
select 1::smallint as scheduled_ind
     , 0::smallint as cancelled_ind
     , 0::smallint as diverted_ind
     , count(*) as record_qty
     , (count(*)::float / count(*))::numeric(4,3) as record_pct
from air_oai_facts.airline_flight_performance_fdw
order by 5 desc;

1	0	0	561441	1.000 -- scheduled
0	0	0	550249	0.980 -- completed (on-time & delayed)
0	1	0	9589	0.017 -- cancelled
0	0	1	1603	0.003 -- diverted
*/

-- Check on which data elements are present for each flight subtype:
select case when flight_date is null then 'n' else 'd' end::char(1) as flt_dt
	 , case when airline_oai_code is null then 'n' else 'd' end::char(1) as arln_oai_cd
	 , case when flight_nbr is null then 'n' else 'd' end::char(1) as flt_nbr
	 , flight_count as flt_qty
	 , case when tail_nbr is null then 'n' else 'd' end::char(1) as tail_nbr
	 , case when depart_airport_oai_code is null then 'n' else 'd' end::char(1) as dpt_arprt_oai_cd
	 , case when arrive_airport_oai_code is null then 'n' else 'd' end::char(1) as arv_arprt_oai_cd
	 , case when cancelled_ind = 1 then 'cancelled' when diverted_ind = 1 then 'diverted' else 'completed' end::varchar(25) as flt_stat
	 , case when cancellation_code is null then 'n' else 'd' end::char(1) as cncl_cd
	 , case when distance_smi is null then 'n' else 'd' end::char(1) as dist_smi
	 , case when distance_group_id is null then 'n' else 'd' end::char(1) as dist_grp
	 , case when depart_time_block is null then 'n' when length(replace(depart_time_block::text,' ','')) = 0 then 's' else 'd' end::char(1) as dpt_blk
	 , case when arrive_time_block is null then 'n' when length(replace(arrive_time_block::text,' ','')) = 0 then 's' else 'd' end::char(1) as arv_blk
	 , case when report_depart_time_lcl is null then 'n' else 'd' end::char(1) as rpt_dpt_tm_lcl
	 , case when report_arrive_time_lcl is null then 'n' else 'd' end::char(1) as rpt_arv_tm_lcl
	 , case when report_elapsed_time_min  is null then 'n' else 'd' end::char(1) as rpt_elpsd_tm_min -- redundant?
	 , case when actual_depart_time_lcl is null then 'n' when length(replace(actual_depart_time_lcl,' ','')) = 0 then 's' else 'd' end::char(1) as actl_dpt_tm_lcl
	 , case when actual_arrive_time_lcl is null then 'n' when length(replace(actual_arrive_time_lcl,' ','')) = 0 then 's' else 'd' end::char(1) as actl_arv_tm_lcl
	 , case when actual_elapsed_time_min is null then 'n' when length(replace(actual_elapsed_time_min::text,' ','')) = 0 then 's' else 'd' end::char(1) as actl_elpsd_tm_min -- redundant?
	 , case when wheels_off_time_lcl is null then 'n' when length(replace(wheels_off_time_lcl,' ','')) = 0 then 's' else 'd' end::char(1) as whls_of_tm_lcl
	 , case when wheels_on_time_lcl is null then 'n' when length(replace(wheels_on_time_lcl,' ','')) = 0 then 's' else 'd' end::char(1) as whls_on_tm_lcl
	 , case when airborne_time_min is null then 'n' when length(replace(airborne_time_min::text,' ','')) = 0 then 's' else 'd' end::char(1) as arbrn_tm_min -- redundant?
	 , case when first_gate_depart_time is null then 'n' when length(replace(first_gate_depart_time::text,' ','')) = 0 then 's' else 'd' end::char(1) as frst_gt_dpt_tm_lcl
	 , case when taxi_out_min is null then 'n' when length(replace(taxi_out_min::text,' ','')) = 0 then 's' else 'd' end::char(1) as taxi_ot_min
	 , case when taxi_in_min is null then 'n' when length(replace(taxi_in_min::text,' ','')) = 0 then 's' else 'd' end::char(1) as  taxi_in_min
	 , case when total_ground_time is null then 'n' when length(replace(total_ground_time::text,' ','')) = 0 then 's' else 'd' end::char(1) as ttl_gnd_tm_min
	 , case when longest_ground_time is null then 'n' when length(replace(longest_ground_time::text,' ','')) = 0 then 's' else 'd' end::char(1) as lngst_gnd_tm_min
	 , count(*)
from (
SELECT flight_date
	 , airline_oai_code
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , arrive_airport_oai_code
     , cancelled_ind
	 , cancellation_code
	 , diverted_ind
	 , distance_smi
	 , distance_group_id
	 , report_depart_time_lcl
	 , report_arrive_time_lcl
	 , report_elapsed_time_min -- redundant?
	 , actual_depart_time_lcl
	 , actual_arrive_time_lcl
	 , actual_elapsed_time_min -- redundant?
	 , wheels_off_time_lcl
	 , wheels_on_time_lcl
	 , airborne_time_min -- redundant?
	 , first_gate_depart_time
	 , depart_time_block
	 , arrive_time_block
	 , taxi_out_min
	 , taxi_in_min
	 , total_ground_time
	 , longest_ground_time
	 , airline_delay_min
	 , weather_delay_min
	 , nas_delay_min
	 , security_delay_min
	 , late_aircraft_delay_min
	 , diverted_airport_landing_count
	 , diverted_reached_dest_ind
	 , diverted_actual_elapsed_time_min
	 , diverted_arrive_delay_min
	 , diverted_distance_smi
	 , diverted1_airport_oai_code
	 , diverted2_airport_oai_code
	 , diverted3_airport_oai_code
	 , diverted4_airport_oai_code
	 , diverted5_airport_oai_code
	 , diverted1_tail_nbr
	 , diverted2_tail_nbr
	 , diverted3_tail_nbr
	 , diverted4_tail_nbr
	 , diverted5_tail_nbr
	 , diverted1_wheels_on_time_lcl
	 , diverted1_wheels_off_time_lcl
	 , diverted1_total_ground_time_min
	 , diverted1_longest_ground_time_min
	 , diverted2_wheels_on_time_lcl
	 , diverted2_wheels_off_time_lcl
	 , diverted2_total_ground_time_min
	 , diverted2_longest_ground_time_min
	 , diverted3_wheels_on_time_lcl
	 , diverted3_wheels_off_time_lcl
	 , diverted3_total_ground_time_min
	 , diverted3_longest_ground_time_min
	 , diverted4_wheels_on_time_lcl
	 , diverted4_wheels_off_time_lcl
	 , diverted4_total_ground_time_min
	 , diverted4_longest_ground_time_min
	 , diverted5_wheels_on_time_lcl
	 , diverted5_wheels_off_time_lcl
	 , diverted5_total_ground_time_min
	 , diverted5_longest_ground_time_min
FROM air_oai_facts.airline_flight_performance_mv
) a group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
order by 8,10, count(*) desc
;

/* Test case identification 
-- select flight_status from (
select flight_key_comp
     , flight_status
     , wheels_off_tmstz_utc
     , wheels_on_tmstz_utc
     , wheels_on_tmstz_utc - wheels_off_tmstz_utc 			as airborne_hhmm
     , airborne_time_min
     , report_arrive_tmstz_utc
     , actual_arrive_tmstz_utc
     , actual_arrive_tmstz_utc - report_arrive_tmstz_utc 	as delay_hhmm
     , airline_delay_min
	 , weather_delay_min
	 , nas_delay_min
	 , security_delay_min
	 , late_aircraft_delay_min
from air_oai_facts.airline_flight_performance_integrated_mv
order by flight_status
-- ) a group by flight_status order by count(*) desc
;

select airline_oai_code, flight_nbr, depart_airport_oai_code, arrive_airport_oai_code
     , flight_date, report_depart_time_lcl, actual_depart_time_lcl
     , report_arrive_time_lcl, actual_arrive_time_lcl
     , wheels_off_time_lcl, wheels_on_time_lcl
from air_oai_facts.airline_flight_performance_mv
where (airline_oai_code||'|'||flight_nbr||'|'||flight_date::text||'|'||depart_airport_oai_code) in
('NK|3055|2023-04-15|MIA'
,'NK|3055|2023-04-16|MIA'
,'NK|3055|2023-04-17|MIA');
*/
  
---------------------------------------------
-- air_oai_facts.airline_flights_scheduled --
---------------------------------------------

-- select count(*) from air_oai_facts.airline_flights_scheduled; -- 6,353,077 too many! ... now 561,441
-- drop table if exists air_oai_facts.airline_flights_scheduled;
-- create table air_oai_facts.airline_flights_scheduled as 
SELECT flight_key --, flight_key_comp
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code
	 , arrive_airport_from_date
	 , arrive_airport_history_id
	 , arrive_airport_history_key
	 , distance_smi
	 , distance_nmi
	 , distance_kmt
	 , distance_group_id
	 , depart_time_block
	 , arrive_time_block
	 , report_depart_tmstz_lcl
	 , report_depart_tmstz_utc
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_lcl + (interval '24 hours')
	        else report_arrive_tmstz_lcl end as report_arrive_tmstz_lcl
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_utc + (interval '24 hours')
	        else report_arrive_tmstz_utc end as report_arrive_tmstz_utc
	 , report_elapsed_time_min
     --, case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	 --       then report_arrive_tmstz_utc + (interval '24 hours')
	 --       else report_arrive_tmstz_utc end - report_depart_tmstz_utc as report_elapsed_time_min1
	 , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where cancelled_ind = 0
and diverted_ind = 0
;
/* Test Cases
--and flight_key_comp in 
--('NK|3055|2023-04-15|MIA'
--,'NK|3055|2023-04-16|MIA'
--,'NK|3055|2023-04-17|MIA')
*/

--select * from air_oai_facts.airline_flights_scheduled order by report_elapsed_time_min desc;

alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_pk primary key (flight_key);
create unique index airline_flights_scheduled_ak on air_oai_facts.airline_flights_scheduled(airline_oai_code, flight_nbr, flight_date, depart_airport_oai_code);

create index airline_flights_scheduled_carrier_idx on air_oai_facts.airline_flights_scheduled (airline_oai_code);
create index airline_flights_scheduled_flight_date_idx on air_oai_facts.airline_flights_scheduled (flight_date);
create index airline_flights_scheduled_flight_lane_idx on air_oai_facts.airline_flights_scheduled (depart_airport_oai_code, arrive_airport_oai_code);
create index airline_flights_scheduled_depart_airport_idx on air_oai_facts.airline_flights_scheduled (depart_airport_oai_code);
create index airline_flights_scheduled_arrive_airport_idx on air_oai_facts.airline_flights_scheduled (arrive_airport_oai_code);

---------------------------------------------
-- air_oai_facts.airline_flights_completed --
---------------------------------------------

-- select count(*) from air_oai_facts.airline_flights_completed; -- 6,353,077 too many! ... now 561,441
-- drop table if exists air_oai_facts.airline_flights_completed;
-- create table air_oai_facts.airline_flights_completed as
SELECT flight_key --, flight_key_comp
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code
	 , arrive_airport_from_date
	 , arrive_airport_history_id
	 , arrive_airport_history_key
	 , distance_smi
	 , distance_nmi
	 , distance_kmt
	 , distance_group_id
	 , depart_time_block
	 , arrive_time_block
	 , report_depart_tmstz_lcl
	 , report_depart_tmstz_utc
	 --, report_arrive_tmstz_lcl as report_arrive_tmstz_lcl0
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_lcl + (interval '24 hours')
	        else report_arrive_tmstz_lcl end as report_arrive_tmstz_lcl
	 --, report_arrive_tmstz_utc as report_arrive_tmstz_utc0
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_utc + (interval '24 hours')
	        else report_arrive_tmstz_utc end as report_arrive_tmstz_utc
	 , report_elapsed_time_min
	 , case when airline_delay_min is not null then 'completed-delayed' 
	        else 'completed-on-time' end::varchar(25) as flight_status
	 , actual_depart_tmstz_lcl
	 , actual_depart_tmstz_utc
	 --, actual_arrive_tmstz_lcl as actual_arrive_tmstz_lcl0
	 , case when actual_arrive_tmstz_utc <= actual_depart_tmstz_utc
	        then actual_arrive_tmstz_lcl + (interval '24 hours')
	        else actual_arrive_tmstz_lcl end as actual_arrive_tmstz_lcl
	 --, actual_arrive_tmstz_utc as actual_arrive_tmstz_utc0
	 , case when actual_arrive_tmstz_utc <= actual_depart_tmstz_utc
	        then actual_arrive_tmstz_utc + (interval '24 hours')
	        else actual_arrive_tmstz_utc end as actual_arrive_tmstz_utc
	 , actual_elapsed_time_min
	 --, actual_arrive_tmstz_utc - actual_depart_tmstz_utc as actual_elapsed_time_min1
	 --, case when actual_arrive_tmstz_utc <= actual_depart_tmstz_utc
	 --       then actual_arrive_tmstz_utc + (interval '24 hours')
	 --       else actual_arrive_tmstz_utc end - actual_depart_tmstz_utc as actual_elapsed_time_min2
	 , wheels_off_tmstz_lcl
	 , wheels_off_tmstz_utc
	 --, wheels_on_tmstz_lcl as wheels_on_tmstz_lcl0
	 , case when wheels_on_tmstz_utc <= wheels_off_tmstz_utc
	        then wheels_on_tmstz_lcl + (interval '24 hours')
	        else wheels_on_tmstz_lcl end as wheels_on_tmstz_lcl
	 --, wheels_on_tmstz_utc as wheels_on_tmstz_utc0
	 , case when wheels_on_tmstz_utc <= wheels_off_tmstz_utc
	        then wheels_on_tmstz_utc + (interval '24 hours')
	        else wheels_on_tmstz_utc end as wheels_on_tmstz_utc
	 --, wheels_on_tmstz_utc - wheels_off_tmstz_utc as airborne_time_min1
	 --, case when wheels_on_tmstz_utc <= wheels_off_tmstz_utc
	 --       then wheels_on_tmstz_utc + (interval '24 hours')
	 --       else wheels_on_tmstz_utc end - wheels_off_tmstz_utc as airborne_time_min2
	 , airborne_time_min
	 , taxi_out_min
	 , taxi_in_min
	 , first_gate_depart_tmstz_lcl
	 , first_gate_depart_tmstz_utc
	 , total_ground_time
	 , longest_ground_time
	 , airline_delay_min
	 , weather_delay_min
	 , nas_delay_min
	 , security_delay_min
	 , late_aircraft_delay_min
	 , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where cancelled_ind = 0
and diverted_ind = 0
;

alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_pk primary key (flight_key);
create unique index airline_flights_completed_ak on air_oai_facts.airline_flights_completed(airline_oai_code, flight_nbr, flight_date, depart_airport_oai_code);

create index airline_flights_completed_carrier_idx on air_oai_facts.airline_flights_completed (airline_oai_code);
create index airline_flights_completed_flight_date_idx on air_oai_facts.airline_flights_completed (flight_date);
create index airline_flights_completed_flight_lane_idx on air_oai_facts.airline_flights_completed (depart_airport_oai_code, arrive_airport_oai_code);
create index airline_flights_completed_depart_airport_idx on air_oai_facts.airline_flights_completed (depart_airport_oai_code);
create index airline_flights_completed_arrive_airport_idx on air_oai_facts.airline_flights_completed (arrive_airport_oai_code);

-- vacuum analyze air_oai_facts.airline_flights_completed;

---------------------------------------------
-- air_oai_facts.airline_flights_cancelled --
---------------------------------------------

-- select count(*) from air_oai_facts.airline_flights_cancelled; -- 
-- drop table if exists air_oai_facts.airline_flights_cancelled;
-- create table air_oai_facts.airline_flights_cancelled as
SELECT flight_key --, flight_key_comp
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code
	 , arrive_airport_from_date
	 , arrive_airport_history_id
	 , arrive_airport_history_key
	 , distance_smi
	 , distance_nmi
	 , distance_kmt
	 , distance_group_id
	 , depart_time_block
	 , arrive_time_block
	 , report_depart_tmstz_lcl
	 , report_depart_tmstz_utc
	 --, report_arrive_tmstz_lcl as report_arrive_tmstz_lcl0
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_lcl + (interval '24 hours')
	        else report_arrive_tmstz_lcl end as report_arrive_tmstz_lcl
	 --, report_arrive_tmstz_utc as report_arrive_tmstz_utc0
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_utc + (interval '24 hours')
	        else report_arrive_tmstz_utc end as report_arrive_tmstz_utc
	 , report_elapsed_time_min
	 , flight_status
	 , actual_depart_tmstz_lcl
	 , actual_depart_tmstz_utc
	 , wheels_off_tmstz_lcl
	 , wheels_off_tmstz_utc
	 , taxi_out_min
	 , first_gate_depart_tmstz_lcl
	 , first_gate_depart_tmstz_utc
	 , total_ground_time
	 , longest_ground_time
	 , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where cancelled_ind = 1
;

alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_pk primary key (flight_key);
create unique index airline_flights_cancelled_ak on air_oai_facts.airline_flights_cancelled (airline_oai_code, flight_nbr, flight_date, depart_airport_oai_code);

create index airline_flights_cancelled_airline_idx on air_oai_facts.airline_flights_cancelled (airline_oai_code);
create index airline_flights_cancelled_flight_date_idx on air_oai_facts.airline_flights_cancelled (flight_date);
create index airline_flights_cancelled_flight_lane_idx on air_oai_facts.airline_flights_cancelled (depart_airport_oai_code, arrive_airport_oai_code);
create index airline_flights_cancelled_depart_airport_idx on air_oai_facts.airline_flights_cancelled (depart_airport_oai_code);
create index airline_flights_cancelled_arrive_airport_idx on air_oai_facts.airline_flights_cancelled (arrive_airport_oai_code);

-- vacuum analyze air_oai_facts.airline_flights_cancelled;

--------------------------------------------
-- air_oai_facts.airline_flights_diverted --
--------------------------------------------

-- select count(*) from air_oai_facts.airline_flights_diverted; -- 1603
-- drop table if exists air_oai_facts.airline_flights_diverted;
-- create table air_oai_facts.airline_flights_diverted as
SELECT flight_key --, flight_key_comp
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code
	 , arrive_airport_from_date
	 , arrive_airport_history_id
	 , arrive_airport_history_key
	 , distance_smi
	 , distance_nmi
	 , distance_kmt
	 , distance_group_id
	 , depart_time_block
	 , arrive_time_block
	 , report_depart_tmstz_lcl
	 , report_depart_tmstz_utc
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_lcl + (interval '24 hours')
	        else report_arrive_tmstz_lcl end as report_arrive_tmstz_lcl
	 , case when report_arrive_tmstz_utc <= report_depart_tmstz_utc 
	        then report_arrive_tmstz_utc + (interval '24 hours')
	        else report_arrive_tmstz_utc end as report_arrive_tmstz_utc
	 , report_elapsed_time_min
	 , flight_status
	 , actual_depart_tmstz_lcl
	 , actual_depart_tmstz_utc
	 , case when actual_arrive_tmstz_utc <= actual_depart_tmstz_utc
	        then actual_arrive_tmstz_lcl + (interval '24 hours')
	        else actual_arrive_tmstz_lcl end as actual_arrive_tmstz_lcl
	 , case when actual_arrive_tmstz_utc <= actual_depart_tmstz_utc
	        then actual_arrive_tmstz_utc + (interval '24 hours')
	        else actual_arrive_tmstz_utc end as actual_arrive_tmstz_utc
	 , actual_elapsed_time_min
	 , wheels_off_tmstz_lcl
	 , wheels_off_tmstz_utc
	 , case when wheels_on_tmstz_utc <= wheels_off_tmstz_utc
	        then wheels_on_tmstz_lcl + (interval '24 hours')
	        else wheels_on_tmstz_lcl end as wheels_on_tmstz_lcl
	 , case when wheels_on_tmstz_utc <= wheels_off_tmstz_utc
	        then wheels_on_tmstz_utc + (interval '24 hours')
	        else wheels_on_tmstz_utc end as wheels_on_tmstz_utc
	 , airborne_time_min
	 , taxi_out_min
	 , taxi_in_min
	 , first_gate_depart_tmstz_lcl
	 , first_gate_depart_tmstz_utc
	 , total_ground_time
	 , longest_ground_time
	 , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where diverted_ind = 1
;

--select * from air_oai_facts.airline_flights_diverted where security_delay_min is not null;

alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_pk primary key (flight_key);
create unique index airline_flights_diverted_ak on air_oai_facts.airline_flights_diverted (airline_oai_code, flight_nbr, flight_date, depart_airport_oai_code);

create index airline_flights_diverted_carrier_idx on air_oai_facts.airline_flights_diverted (airline_oai_code);
create index airline_flights_diverted_flight_date_idx on air_oai_facts.airline_flights_diverted (flight_date);
create index airline_flights_diverted_flight_lane_idx on air_oai_facts.airline_flights_diverted(depart_airport_oai_code, arrive_airport_oai_code);
create index airline_flights_diverted_depart_airport_idx on air_oai_facts.airline_flights_diverted (depart_airport_oai_code);
create index airline_flights_diverted_arrive_airport_idx on air_oai_facts.airline_flights_diverted (arrive_airport_oai_code);

vacuum analyze air_oai_facts.airline_flights_diverted;

-------------------------------------------------
-- air_oai_facts.airline_flights_diverted_legs --
-------------------------------------------------

-- select count(*) from air_oai_facts.airline_flights_diverted_legs; -- 1603
-- drop table if exists air_oai_facts.airline_flights_diverted_legs;
create table air_oai_facts.airline_flights_diverted_legs as
SELECT flight_key --, flight_key_comp
     , 1::smallint as diversion_nbr
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code				as original_arrive_airport_oai_code
	 , arrive_airport_from_date				as original_arrive_airport_from_date
	 , arrive_airport_history_id			as original_arrive_airport_history_id
	 , arrive_airport_history_key			as original_arrive_airport_history_key
     , diverted1_airport_oai_code			as diverted_airport_oai_code
     , diverted1_airport_from_date			as diverted_airport_from_date
     , diverted1_airport_history_id			as diverted_airport_history_id
     , diverted1_airport_history_key		as diverted_airport_history_key
     , diverted1_tail_nbr					as diverted_tail_nbr
     , diverted1_wheels_on_tmstz_lcl		as diverted_wheels_on_tmstz_lcl
     , diverted1_wheels_on_tmstz_utc		as diverted_wheels_on_tmstz_utc
     , diverted1_wheels_off_tmstz_lcl		as diverted_wheels_off_tmstz_lcl
     , diverted1_wheels_off_tmstz_utc		as diverted_wheels_off_tmstz_utc
     , diverted1_total_ground_time_min		as diverted_total_ground_time_min
     , diverted1_longest_ground_time_min	as diverted_longest_ground_time_min
     , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where diverted_ind = 1
and diverted1_airport_history_id is not null
union
SELECT flight_key --, flight_key_comp
     , 2::smallint as diversion_nbr
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code				as original_arrive_airport_oai_code
	 , arrive_airport_from_date				as original_arrive_airport_from_date
	 , arrive_airport_history_id			as original_arrive_airport_history_id
	 , arrive_airport_history_key			as original_arrive_airport_history_key
     , diverted2_airport_oai_code			as diverted_airport_oai_code
     , diverted2_airport_from_date			as diverted_airport_from_date
     , diverted2_airport_history_id			as diverted_airport_history_id
     , diverted2_airport_history_key		as diverted_airport_history_key
     , diverted2_tail_nbr					as diverted_tail_nbr
     , diverted2_wheels_on_tmstz_lcl		as diverted_wheels_on_tmstz_lcl
     , diverted2_wheels_on_tmstz_utc		as diverted_wheels_on_tmstz_utc
     , diverted2_wheels_off_tmstz_lcl		as diverted_wheels_off_tmstz_lcl
     , diverted2_wheels_off_tmstz_utc		as diverted_wheels_off_tmstz_utc
     , diverted2_total_ground_time_min		as diverted_total_ground_time_min
     , diverted2_longest_ground_time_min	as diverted_longest_ground_time_min
     , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where diverted_ind = 1
and diverted2_airport_history_id is not null
union
SELECT flight_key --, flight_key_comp
     , 3::smallint as diversion_nbr
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code				as original_arrive_airport_oai_code
	 , arrive_airport_from_date				as original_arrive_airport_from_date
	 , arrive_airport_history_id			as original_arrive_airport_history_id
	 , arrive_airport_history_key			as original_arrive_airport_history_key
     , diverted3_airport_oai_code			as diverted_airport_oai_code
     , diverted3_airport_from_date			as diverted_airport_from_date
     , diverted3_airport_history_id			as diverted_airport_history_id
     , diverted3_airport_history_key		as diverted_airport_history_key
     , diverted3_tail_nbr					as diverted_tail_nbr
     , diverted3_wheels_on_tmstz_lcl		as diverted_wheels_on_tmstz_lcl
     , diverted3_wheels_on_tmstz_utc		as diverted_wheels_on_tmstz_utc
     , diverted3_wheels_off_tmstz_lcl		as diverted_wheels_off_time_tmstz_lcl
     , diverted3_wheels_off_tmstz_utc		as diverted_wheels_off_time_tmstz_utc
     , diverted3_total_ground_time_min		as diverted_total_ground_time_min
     , diverted3_longest_ground_time_min	as diverted_longest_ground_time_min
     , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where diverted_ind = 1
and diverted3_airport_history_id is not null
union
SELECT flight_key --, flight_key_comp
     , 4::smallint as diversion_nbr
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code				as original_arrive_airport_oai_code
	 , arrive_airport_from_date				as original_arrive_airport_from_date
	 , arrive_airport_history_id			as original_arrive_airport_history_id
	 , arrive_airport_history_key			as original_arrive_airport_history_key
     , diverted4_airport_oai_code			as diverted_airport_oai_code
     , diverted4_airport_from_date			as diverted_airport_from_date
     , diverted4_airport_history_id			as diverted_airport_history_id
     , diverted4_airport_history_key		as diverted_airport_history_key
     , diverted4_tail_nbr					as diverted_tail_nbr
     , diverted4_wheels_on_tmstz_lcl		as diverted_wheels_on_tmstz_lcl
     , diverted4_wheels_on_tmstz_utc		as diverted_wheels_on_tmstz_utc
     , diverted4_wheels_off_tmstz_lcl		as diverted_wheels_off_tmstz_lcl
     , diverted4_wheels_off_tmstz_utc		as diverted_wheels_off_tmstz_utc
     , diverted4_total_ground_time_min		as diverted_total_ground_time_min
     , diverted4_longest_ground_time_min	as diverted_longest_ground_time_min
     , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where diverted_ind = 1
and diverted4_airport_history_id is not null
union
SELECT flight_key --, flight_key_comp
     , 5::smallint as diversion_nbr
	 , flight_date
	 , airline_oai_code
	 , airline_entity_from_date
	 , airline_entity_id
	 , airline_entity_key
	 , flight_nbr
	 , flight_count
	 , tail_nbr
	 , depart_airport_oai_code
	 , depart_airport_from_date
	 , depart_airport_history_id
	 , depart_airport_history_key
	 , arrive_airport_oai_code				as original_arrive_airport_oai_code
	 , arrive_airport_from_date				as original_arrive_airport_from_date
	 , arrive_airport_history_id			as original_arrive_airport_history_id
	 , arrive_airport_history_key			as original_arrive_airport_history_key
     , diverted5_airport_oai_code			as diverted_airport_oai_code
     , diverted5_airport_from_date			as diverted_airport_from_date
     , diverted5_airport_history_id			as diverted_airport_history_id
     , diverted5_airport_history_key		as diverted_airport_history_key
     , diverted5_tail_nbr					as diverted_tail_nbr
     , diverted5_wheels_on_tmstz_lcl		as diverted_wheels_on_tmstz_lcl
     , diverted5_wheels_on_tmstz_utc		as diverted_wheels_on_tmstz_utc
     , diverted5_wheels_off_tmstz_lcl		as diverted_wheels_off_tmstz_lcl
     , diverted5_wheels_off_tmstz_utc		as diverted_wheels_off_tmstz_utc
     , diverted5_total_ground_time_min		as diverted_total_ground_time_min
     , diverted5_longest_ground_time_min	as diverted_longest_ground_time_min
     , 'gxclark'::varchar(32) as created_by
     , current_timestamp::timestamp(0) as created_ts
     , null::varchar(32) as updated_by
     , null::timestamp(0) as updated_ts 
FROM air_oai_facts.airline_flight_performance_integrated_mv
where diverted_ind = 1
and diverted5_airport_history_id is not null
;

alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_pk primary key (flight_key, diversion_nbr);
create unique index airline_flights_diverted_legs_ak on air_oai_facts.airline_flights_diverted_legs (airline_oai_code, flight_nbr, flight_date, depart_airport_oai_code, diversion_nbr);

/*ERROR: could not create unique index "airline_flights_diverted_legs_ak" Detail: Key (airline_oai_code, flight_nbr, flight_date, diversion_nbr)=(AA, 2314, 2023-04-15, 1) is duplicated. 
select * from air_oai_facts.airline_flights_diverted_legs where airline_oai_code = 'AA' and flight_nbr = '2314' and flight_date = '2023-04-15' -- and diversion_nbr = 1
;
select * from air_oai_facts.airline_flight_performance_integrated_mv where airline_oai_code = 'AA' and flight_nbr = '2314' and flight_date = '2023-04-15';
*/

create index airline_flights_diverted_legs_airline_idx on air_oai_facts.airline_flights_diverted_legs (airline_oai_code);
create index airline_flights_diverted_legs_flight_date_idx on air_oai_facts.airline_flights_diverted_legs (flight_date);
create index airline_flights_diverted_legs_flight_lane_idx on air_oai_facts.airline_flights_diverted_legs (depart_airport_oai_code, original_arrive_airport_oai_code);
create index airline_flights_diverted_legs_depart_airport_idx on air_oai_facts.airline_flights_diverted_legs (depart_airport_oai_code);
create index airline_flights_diverted_legs_arrive_airport_idx on air_oai_facts.airline_flights_diverted_legs (original_arrive_airport_oai_code);

vacuum analyze air_oai_facts.airline_flights_diverted_legs;

------------------
-- Foreign Keys --
------------------
-- air_oai_facts.airline_flights_scheduled
-- IDS:
alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);
alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
-- KEYS:
alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);
alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);
alter table air_oai_facts.airline_flights_scheduled add constraint airline_flights_scheduled_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

-- air_oai_facts.airline_flights_completed
-- IDS:
alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);
alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
-- KEYS:
alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);
alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);
alter table air_oai_facts.airline_flights_completed add constraint airline_flights_completed_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

-- air_oai_facts.airline_flights_cancelled
-- IDS:
alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);
alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
-- KEYS:
alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);
alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);
alter table air_oai_facts.airline_flights_cancelled add constraint airline_flights_cancelled_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

-- air_oai_facts.airline_flights_diverted
-- IDS:
alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);
alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
-- KEYS:
alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);
alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);
alter table air_oai_facts.airline_flights_diverted add constraint airline_flights_diverted_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

-- air_oai_facts.airline_flights_diverted_legs
-- IDS:
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_arrive_airport_id_fk 
foreign key (original_arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_diverted_airport_id_fk 
foreign key (diverted_airport_history_id) references air_oai_dims.airport_history (airport_history_id);
-- KEYS:
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_arrive_airport_key_fk 
foreign key (original_arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);
alter table air_oai_facts.airline_flights_diverted_legs add constraint airline_flights_diverted_legs_diverted_airport_key_fk 
foreign key (diverted_airport_history_key) references air_oai_dims.airport_history (airport_history_key);