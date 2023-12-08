
create schema air_oai_facts;

----------------------------
-- Airline Traffic Market --
----------------------------

-- alter table air_oai_facts.f41_traffic_t100_market_archive rename column unique_airline_oai_code to airline_unique_oai_code;

-- DROP TABLE if exists air_oai_facts.f41_traffic_t100_market_archive;
CREATE TABLE air_oai_facts.f41_traffic_t100_market_archive
	( passengers_qty 				float4
	, freight_lbr 					float4
	, mail_lbr 						float4
	, distance_smi 					float4
	, airline_unique_oai_code 		varchar(15) -- unique_airline_oai_code
	, airline_usdot_id 				int4
	, airline_unique_name			varchar(125) -- unique_airline_name
	, entity_unique_oai_code 		varchar(15)  -- unique_entity_oai_code
	, operating_region_code 		varchar(5)
	, airline_oai_code 				varchar(5)
	, airline_name					varchar(125)
	, airline_old_group_nbr 		int4
	, airline_new_group_nbr 		int4
	, depart_airport_oai_id 		int4
	, depart_airport_oai_seq_id 	int4
	, depart_city_market_oai_id 	int4
	, depart_airport_oai_code 		varchar(5)
	, depart_city_name 				varchar(75)
	, depart_subdivision_iso_code 	varchar(5)
	, depart_subdivision_fips_code 	varchar(5)
	, depart_subdivision_name 		varchar(75)
	, depart_country_iso_code 		varchar(5)
	, depart_country_name 			varchar(75)
	, depart_world_area_oai_id 		int4
	, arrive_airport_oai_id 		int4
	, arrive_airport_oai_seq_id 	int4
	, arrive_city_market_oai_id 	int4
	, arrive_airport_oai_code 		varchar(5)
	, arrive_city_name 				varchar(75)
	, arrive_subdivision_iso_code 	varchar(5)
	, arrive_subdivision_fips_code 	varchar(5)
	, arrive_subdivision_name 		varchar(75)
	, arrive_country_iso_code 		varchar(5)
	, arrive_country_name 			varchar(75)
	, arrive_world_area_oai_id 		int4
	, year_nbr 						int4
	, quarter_nbr 					int4
	, month_nbr 					int4
	, distance_group_id 			int4
	, service_class_code 			varchar(5)
	, data_source_code 				varchar(5)
	--, filler_txt 					varchar(10)
	);
	
copy air_oai_facts.f41_traffic_t100_market_archive
from '/opt/_data/_air/_oai/_t100/T_T100_MARKET_ALL_CARRIER_2022.csv'
-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_t100/_data-market/T100_MARKET_ALL_CARRIER_ALL_2019.csv'
delimiter ',' header csv;

select count(*) from air_oai_facts.f41_traffic_t100_market_archive;
select * from air_oai_facts.f41_traffic_t100_market_archive limit 100;
select * from air_oai_dims.airline_entities;
select * from air_oai_dims.airport_history;

-- select * from air_oai_facts.airline_traffic_market_integrate_mv limit 1000;
-- drop materialized view if exists air_oai_facts.airline_traffic_market_integrate_mv;
-- refresh 
-- CREATE materialized view air_oai_facts.airline_traffic_market_integrate_mv as
SELECT (f.year_nbr::char(4) || lpad(f.month_nbr::varchar(2),2,'0'))::integer as year_month_nbr
     , f.service_class_code
     , f.airline_usdot_id
     , f.airline_oai_code
     , f.entity_unique_oai_code as entity_oai_code
     , ae.source_from_date as airline_effective_date
     , ae.airline_entity_id
     , ae.airline_entity_key
     , f.airline_name
     , f.airline_unique_name
     , f.depart_airport_oai_code
     , h1.effective_from_date as depart_airport_effective_date
     , h1.airport_history_id as depart_airport_history_id
     , h1.airport_history_key as depart_airport_history_key
     , f.arrive_airport_oai_code
     , h2.effective_from_date as arrive_airport_effective_date
     , h2.airport_history_id as arrive_airport_history_id
     , h2.airport_history_key as arrive_airport_history_key
     , f.data_source_code
     , f.passengers_qty
     , f.freight_lbr
     , f.mail_lbr
     , 'gxclark' as created_by
     , now() as created_tmst
from air_oai_facts.f41_traffic_t100_market_archive f
--left outer join calendar.year_month_v c on f.year_nbr = c.year_nbr and f.month_nbr = c.month_of_year_nbr
left outer join air_oai_dims.airline_entities ae
  on f.airline_usdot_id = ae.airline_usdot_id
 and f.airline_oai_code = ae.airline_oai_code
 and f.entity_unique_oai_code = ae.entity_unique_oai_code
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= ae.source_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date
   < case when ae.source_thru_date is null then current_date else ae.source_thru_date end
left outer join air_oai_dims.airport_history h1
  on f.depart_airport_oai_id = h1.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h1.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h1.effective_thru_date is null then current_date else h1.effective_thru_date end
left outer join air_oai_dims.airport_history h2
  on f.arrive_airport_oai_id = h2.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h2.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h2.effective_thru_date is null then current_date else h2.effective_thru_date end
order by f.airline_oai_code, f.depart_airport_oai_code, f.arrive_airport_oai_code
--limit 1000
;

--drop table if exists air_oai_facts.airline_traffic_market;
CREATE TABLE air_oai_facts.airline_traffic_market 
	( airline_traffic_market_key				char(32)		not null
	, year_month_nbr							integer			not null
	, airline_oai_code 							varchar(3) 		not null
	, airline_effective_date					date			not null
	, airline_entity_id							integer			not null
	, airline_entity_key						char(32)		not null
	, depart_airport_oai_code 					char(3) 		not null
	, depart_airport_effective_date				date			not null
	, depart_airport_history_id					integer 		not null
	, depart_airport_history_key				char(32)		not null
	, arrive_airport_oai_code 					char(3) 		not null
	, arrive_airport_effective_date				date			not null
	, arrive_airport_history_id					integer 		not null
	, arrive_airport_history_key				char(32) 		not null
	, service_class_code 						char(1) 		not null
	, data_source_code							varchar(5)		not null
	, passengers_qty 							integer			not null
	, freight_kgm 								numeric(10,1)	not null
	, mail_kgm 									numeric(10,1)	not null
	, t100_records_qty 							smallint		not null
	, metadata_key								varchar(32)
	, created_by 								varchar(32)		not null
	, created_tmst 								timestamp(0)	not null
	, updated_by 								varchar(32)
	, updated_tmst 								timestamp(0)
	, constraint airline_traffic_market_pk PRIMARY KEY (airline_traffic_market_key) 
	);

INSERT INTO air_oai_facts.airline_traffic_market
( airline_traffic_market_key, year_month_nbr, service_class_code
, airline_oai_code, airline_effective_date, airline_entity_id, airline_entity_key
, depart_airport_oai_code, depart_airport_effective_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_effective_date, arrive_airport_history_id, arrive_airport_history_key
, data_source_code, passengers_qty, freight_kgm, mail_kgm, t100_records_qty
, created_by, created_tmst)
SELECT md5(year_month_nbr::char(6)
    ||'|'||service_class_code
    ||'|'||airline_entity_key
    ||'|'||depart_airport_history_key
    ||'|'||arrive_airport_history_key
    ) as airline_traffic_market_key
	 , year_month_nbr
	 , service_class_code
	 , max(airline_oai_code) as airline_oai_code
	 , max(airline_effective_date) as airline_effective_date
	 , max(airline_entity_id) as airline_entity_id
	 , airline_entity_key
	 , max(depart_airport_oai_code) as depart_airport_oai_code
	 , max(depart_airport_effective_date) as depart_airport_effective_date
	 , max(depart_airport_history_id) as depart_airport_history_id
	 , depart_airport_history_key
	 , max(arrive_airport_oai_code) as arrive_airport_oai_code
	 , max(arrive_airport_effective_date) as arrive_airport_effective_date
	 , max(arrive_airport_history_id) as arrive_airport_history_id
	 , arrive_airport_history_key
	 , max(data_source_code) as data_source_code
	 , sum(passengers_qty) as passengers_qty
	 , (sum(freight_lbr)*0.45359237)::numeric(10,1) as freight_kgm
	 , (sum(mail_lbr)*0.45359237)::numeric(10,1) as mail_kgm
	 , count(*) as t100_records_qty
	 , 'gxclark' as created_by
	 , now() as created_tmst
FROM air_oai_facts.airline_traffic_market_integrate_mv
WHERE year_month_nbr is not null 
and service_class_code is not null 
and airline_entity_key is not null 
and depart_airport_history_key is not null 
and arrive_airport_history_key is not null
--and year_month_nbr between 201001 and 202012 -- 200001 and 200912 -- 199601 and 199912 -- 199101 and 199512
--and year_month_nbr::char(6) like '1990%'
GROUP BY year_month_nbr
     , service_class_code
	 , airline_entity_key --, airline_oai_code, airline_effective_date
	 , depart_airport_history_key --, depart_airport_oai_code, depart_airport_effective_date
	 , arrive_airport_history_key -- , arrive_airport_oai_code, arrive_airport_effective_date
	 ;

-- select * from air_oai_facts.airline_traffic_market;
-- select count(*) from air_oai_facts.airline_traffic_market; -- 317940
-- select count(*) from air_oai_facts.f41_traffic_t100_market_archive; -- 317940
-- drop materialized view if exists air_oai_facts.airline_traffic_market_integrate_mv;
-- drop table if exists air_oai_facts.f41_traffic_t100_market_archive;

-----------------------------
-- Airline Traffic Segment --
-----------------------------

-- DROP TABLE IF EXISTS air_oai_facts.f41_traffic_t100_segment_archive;
CREATE TABLE air_oai_facts.f41_traffic_t100_segment_archive
	( scheduled_departures_qty	 			float4
	, performed_departures_qty	 			float4
	, payload_lbr 							float4
	, available_seat_qty 					float4
	, passengers_qty 						float4
	, freight_lbr 							float4
	, mail_lbr 								float4
	, distance_smi 							float4
	, ramp_to_ramp_min 						float4
	, air_time_min 							float4
	, airline_unique_oai_code 				varchar(10)
	, airline_usdot_id 						int4
	, airline_unique_name 					varchar(125)
	, entity_unique_oai_code 				varchar(15)
	, operating_region_code 				varchar(25)
	, airline_oai_code 						varchar(5)
	, airline_name 							varchar(125)
	, airline_old_group_nbr					int4
	, airline_new_group_nbr 				int4
	, depart_airport_oai_id 				int4
	, depart_airport_oai_seq_id 			int4
	, depart_market_city_oai_id 			int4
	, depart_airport_oai_code 				varchar(3)
	, depart_city_name 						varchar(75)
	, depart_state_cd 						varchar(5)
	, depart_state_fips_cd 					varchar(5)
	, depart_state_nm 						varchar(75)
	, depart_country_iso_code 				varchar(10)
	, depart_country_name 					varchar(75)
	, depart_world_area_oai_id 				int4
	, arrive_airport_oai_id 				int4
	, arrive_airport_oai_seq_id 			int4
	, arrive_market_city_oai_id 			int4
	, arrive_airport_oai_code 				varchar(5)
	, arrive_city_name 						varchar(75)
	, arrive_subdivision_iso_code 			varchar(5)
	, arrive_subdivision_fips_code 			varchar(5)
	, arrive_subdivision_name 				varchar(75)
	, arrive_country_iso_code 				varchar(10)
	, arrive_country_name 					varchar(75)
	, arrive_world_area_oai_id 				int4
	, aircraft_group_oai_nbr				int4 -- 
	, aircraft_type_oai_nbr 				int4 -- 
	, aircraft_configuration_id 			int4
	, year_nbr 								int4
	, quarter_nbr 							int4
	, month_nbr 							int4
	, distance_group_id 					int4
	, service_class_code 					char(1)
	, data_source_code 						varchar(5)
	--, filler_txt 							varchar(10)
	);

copy air_oai_facts.f41_traffic_t100_segment_archive
from '/opt/_data/_air/_oai/_t100/T_T100_SEGMENT_ALL_CARRIER_2022.csv'
delimiter ',' header csv;

-- select * from air_oai_facts.f41_traffic_t100_segment_archive;

/* -- this is only needed for particular older files that require data quality work.
-- drop materialized view air_oai_facts.f41_traffic_t100_segment_load_mv;
-- create materialized view air_oai_facts.f41_traffic_t100_segment_load_mv as
SELECT scheduled_departures_qty
	, performed_departures_qty
	, payload_lbr
	, available_seat_qty
	, passengers_qty
	, freight_lbr
	, mail_lbr
	, distance_smi
	, ramp_to_ramp_min
	, air_time_min
	, airline_unique_oai_code
	, airline_usdot_id
	--, case when airline_oai_code = '5G' and airline_usdot_id is null then 21181
	       --when airline_oai_code = '0OQ' and airline_usdot_id is null then 21287
	       --when airline_oai_code = 'AQ' and airline_usdot_id is null then 19678
	       --when airline_oai_code = 'KH' and airline_usdot_id = 19678 then 21634
	       --airline_oai_code = 'K8' and airline_usdot_id is null then 20310
	       --airline_oai_code = 'XP' and airline_usdot_id is null then 20207
	       --when airline_oai_code = '2HQ' is not null and airline_usdot_id is null then 21712
	--       else airline_usdot_id end::integer as airline_usdot_id
	, airline_unique_name
	, entity_unique_oai_code
	--, case when airline_oai_code = '5G' and airline_usdot_id is null then '71032'
	       --when airline_oai_code = '0OQ' and airline_usdot_id is null then '71056'
	       --when airline_oai_code = 'AQ' and (airline_usdot_id is null or airline_usdot_id = 19678)
	       -- and (depart_country_iso_code in ('US','CA') and arrive_country_iso_code in ('US','CA')) then '05045'
	       --when airline_oai_code = 'AQ' and (airline_usdot_id is null or airline_usdot_id = 19678)
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '15045'
	       --when airline_oai_code = 'K8' and airline_usdot_id is null
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '16076'
	       --when airline_oai_code = 'K8' and airline_usdot_id is null
	       -- and (depart_country_iso_code = 'US' and arrive_country_iso_code = 'US') then '06076'
	       --when airline_oai_code = 'XP' and airline_usdot_id is null
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '16144'
	       --when airline_oai_code = 'XP' and airline_usdot_id is null
	       -- and (depart_country_iso_code = 'US' and arrive_country_iso_code = 'US') then '06144'
	       --when airline_oai_code = '2HQ' is not null and airline_usdot_id is null
	       -- and depart_country_iso_code = 'US' and arrive_country_iso_code = 'US' then '01200'
	       --when airline_oai_code = '2HQ' is not null and airline_usdot_id is null
	       -- and (depart_country_iso_code != 'US' or arrive_country_iso_code != 'US') then '11047'
	--       else unique_entity_oai_code end::varchar(15) as unique_entity_oai_code
	, operating_region_code
	, airline_oai_code
	--, case when airline_oai_code = '39Q' and airline_usdot_id = 21894 then 'AN'
	--       when airline_oai_code = '3GQ' and airline_usdot_id = 21869 then '36Q'
	--       when airline_oai_code = 'A0' and airline_usdot_id = 20234 and unique_entity_oai_code = '9486F' then '8R'
	--       else airline_oai_code end::varchar(5) as airline_oai_code
	, airline_name
	, airline_old_group_nbr
	, airline_new_group_nbr
	, depart_airport_oai_id
	, depart_airport_oai_seq_id
	, depart_market_city_oai_id
	, depart_airport_oai_code
	, depart_city_name
	, depart_state_cd
	, depart_state_fips_cd
	, depart_state_nm
	, depart_country_iso_code
	, depart_country_name
	, depart_world_area_oai_id
	, arrive_airport_oai_id
	, arrive_airport_oai_seq_id
	, arrive_market_city_oai_id
	, arrive_airport_oai_code
	, arrive_city_name
	, arrive_subdivision_iso_code
	, arrive_subdivision_fips_code
	, arrive_subdivision_name
	, arrive_country_iso_code
	, arrive_country_name
	, arrive_world_area_oai_id
	, aircraft_group_oai_nbr
	, aircraft_type_oai_nbr
	, aircraft_configuration_id
	, year_nbr
	, quarter_nbr
	, month_nbr
	, distance_group_id
	, service_class_code
	, data_source_code
	--, filler_txt
FROM air_oai_facts.f41_traffic_t100_segment_archive;
--limit 1000;
*/

select * from air_oai_facts.f41_traffic_t100_segment_load_mv;

-- select * from air_oai.airline_traffic_segment_integrate_mv limit 1000;
-- drop materialized view if exists air_oai.airline_traffic_segment_integrate_mv;
--refresh 
-- CREATE materialized view air_oai_facts.airline_traffic_segment_integrate_mv as
SELECT (f.year_nbr::char(4) || lpad(f.month_nbr::varchar(2),2,'0'))::integer as year_month_nbr
     , f.service_class_code
     , f.airline_usdot_id
     , f.airline_oai_code
     , f.entity_unique_oai_code as entity_oai_code
     , ae.source_from_date as airline_effective_date
     , ae.airline_entity_id
     , ae.airline_entity_key
     , f.airline_name
     , f.airline_unique_name
     , f.depart_airport_oai_code
     , h1.effective_from_date as depart_airport_effective_date
     , h1.airport_history_id as depart_airport_history_id
     , h1.airport_history_key as depart_airport_history_key
     , f.arrive_airport_oai_code
     , h2.effective_from_date as arrive_airport_effective_date
     , h2.airport_history_id as arrive_airport_history_id
     , h2.airport_history_key as arrive_airport_history_key
     , f.aircraft_type_oai_nbr
	 , case when f.aircraft_configuration_id = 0 then 'N/A'
	        when f.aircraft_configuration_id = 1 then 'PAX'
	        when f.aircraft_configuration_id = 2 then 'FRT'
	        when f.aircraft_configuration_id = 3 then 'CMB'
	        when f.aircraft_configuration_id = 4 then 'SEA'
	        when f.aircraft_configuration_id = 9 then 'EXP'
	        else 'UNK' end::char(3) as aircraft_configuration_ref
     , f.data_source_code
     , f.passengers_qty
     , f.freight_lbr
     , f.mail_lbr
     , f.available_seat_qty
     , f.scheduled_departures_qty
     , f.performed_departures_qty
     , f.ramp_to_ramp_min
     , f.air_time_min
     , 'gxclark' as created_by
     , now() as created_tmst
from air_oai_facts.f41_traffic_t100_segment_archive f
-- left outer join calendar.year_month_v c on f.year_nbr = c.year_nbr and f.month_nbr = c.month_of_year_nbr
left outer join air_oai_dims.airline_entities ae
  on f.airline_usdot_id = ae.airline_usdot_id
 and f.airline_oai_code = ae.airline_oai_code
 and f.entity_unique_oai_code = ae.entity_unique_oai_code
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= ae.source_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when ae.source_thru_date is null then current_date else ae.source_thru_date end
left outer join air_oai_dims.airport_history h1
  on f.depart_airport_oai_id = h1.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h1.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h1.effective_thru_date is null then current_date else h1.effective_thru_date end
left outer join air_oai_dims.airport_history h2
  on f.arrive_airport_oai_id = h2.airport_oai_id
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date >= h2.effective_from_date
 and (f.year_nbr::char(4) ||'-'|| lpad(f.month_nbr::varchar(2),2,'0') || '-01')::date 
   < case when h2.effective_thru_date is null then current_date else h2.effective_thru_date end
order by f.airline_oai_code, f.depart_airport_oai_code, f.arrive_airport_oai_code;

-----

--drop table if exists air_oai_facts.airline_traffic_segment;
CREATE TABLE air_oai_facts.airline_traffic_segment 
	( airline_traffic_segment_key				char(32)		not null
	, year_month_nbr							integer			not null
	, service_class_code 						char(1) 		not null	
	, airline_oai_code 							varchar(3) 		not null
	, airline_effective_date					date			not null
	, airline_entity_id							integer			not null
	, airline_entity_key						char(32)		not null
	, depart_airport_oai_code 					char(3) 		not null
	, depart_airport_effective_date				date			not null
	, depart_airport_history_id					integer 		not null
	, depart_airport_history_key				char(32)		not null
	, arrive_airport_oai_code 					char(3) 		not null
	, arrive_airport_effective_date				date			not null
	, arrive_airport_history_id					integer 		not null
	, arrive_airport_history_key				char(32) 		not null
	, aircraft_type_oai_nbr						integer			not null
	, aircraft_configuration_ref				char(3)			not null
	, data_source_code							varchar(5)		not null
	, scheduled_departures_qty					integer			not null
	, performed_departures_qty					integer			not null
	, available_seat_qty						integer			not null
	, passengers_qty 							integer			not null
	, freight_kgm 								numeric(10,1)	not null -- originally lbr, convert to kgm
	, mail_kgm 									numeric(10,1)	not null -- originally lbr, convert to kgm
	, ramp_to_ramp_min							integer			not null
	, air_time_min								integer			not null
	, t100_records_qty							smallint		not null
	, metadata_key								varchar(32)
	, created_by 								varchar(32)		not null
	, created_tmst 								timestamp(0)	not null
	, updated_by 								varchar(32)
	, updated_tmst 								timestamp(0)
	, constraint airline_traffic_segment_pk PRIMARY KEY (airline_traffic_segment_key) 
	);

-----

INSERT INTO air_oai_facts.airline_traffic_segment
( airline_traffic_segment_key, year_month_nbr, service_class_code
, airline_oai_code, airline_effective_date, airline_entity_id, airline_entity_key
, depart_airport_oai_code, depart_airport_effective_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_effective_date, arrive_airport_history_id, arrive_airport_history_key
, aircraft_type_oai_nbr, aircraft_configuration_ref, data_source_code, scheduled_departures_qty, performed_departures_qty
, available_seat_qty, passengers_qty, freight_kgm, mail_kgm, ramp_to_ramp_min, air_time_min, t100_records_qty
, created_by, created_tmst)
SELECT md5(year_month_nbr::char(6)
    ||'|'||service_class_code
    ||'|'||airline_entity_key
    ||'|'||depart_airport_history_key
    ||'|'||arrive_airport_history_key
    ||'|'||lpad(aircraft_type_oai_nbr::varchar(3),3,'0')
    ||'|'||aircraft_configuration_ref::char(3)
    ) as airline_traffic_segment_key
	 , year_month_nbr
	 , service_class_code
	 , max(airline_oai_code) as airline_oai_code
	 , max(airline_effective_date) as airline_effective_date
	 , max(airline_entity_id) as airline_entity_id
	 , airline_entity_key
	 , max(depart_airport_oai_code) as depart_airport_oai_code
	 , max(depart_airport_effective_date) as depart_airport_effective_date
	 , max(depart_airport_history_id) as depart_airport_history_id
	 , depart_airport_history_key
	 , max(arrive_airport_oai_code) as arrive_airport_oai_code
	 , max(arrive_airport_effective_date) as arrive_airport_effective_date
	 , max(arrive_airport_history_id) as arrive_airport_history_id
	 , arrive_airport_history_key
	 , aircraft_type_oai_nbr
	 , aircraft_configuration_ref
	 , max(data_source_code) as data_source_code
	 , sum(scheduled_departures_qty) as scheduled_departures_qty
	 , sum(performed_departures_qty) as performed_departures_qty
	 , sum(available_seat_qty) as available_seat_qty
	 , sum(passengers_qty) as passengers_qty
	 , (sum(freight_lbr)*0.45359237)::numeric(10,1) as freight_kgm
	 , (sum(mail_lbr)*0.45359237)::numeric(10,1) as mail_kgm
	 , sum(ramp_to_ramp_min) as ramp_to_ramp_min
	 , sum(air_time_min) as air_time_min
	 , count(*) as t100_records_qty
	 , 'gxclark' as created_by
	 , now() as created_tmst
FROM air_oai_facts.airline_traffic_segment_integrate_mv
WHERE year_month_nbr is not null and service_class_code is not null and airline_entity_key is not null 
and depart_airport_history_key is not null and arrive_airport_history_key is not null
and aircraft_type_oai_nbr is not null and aircraft_configuration_ref is not null
--and year_month_nbr between 199101 and 199512
--and year_month_nbr::char(6) like '1990%'
GROUP BY year_month_nbr, service_class_code
	 , airline_entity_key --, airline_oai_code, airline_effective_date
	 , depart_airport_history_key --, depart_airport_oai_code, depart_airport_effective_date
	 , arrive_airport_history_key -- , arrive_airport_oai_code, arrive_airport_effective_date
     , aircraft_type_oai_nbr, aircraft_configuration_ref;
     
-- select count(*) from air_oai_facts.airline_traffic_segment; -- 507080
-- select count(*) from air_oai_facts.f41_traffic_t100_segment_archive; -- 507080
-- drop materialized view if exists air_oai_facts.airline_traffic_segment_integrate_mv;
-- drop materialized view air_oai_facts.f41_traffic_t100_segment_load_mv;
-- drop table if exists air_oai_facts.f41_traffic_t100_segment_archive;