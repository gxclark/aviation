
SELECT * FROM pg_extension;
CREATE EXTENSION file_fdw;
CREATE EXTENSION citus;
CREATE EXTENSION PostGIS;
CREATE SERVER abrams_ssd8tb FOREIGN DATA WRAPPER file_fdw;
--CREATE SERVER rokuko_24tb FOREIGN DATA WRAPPER file_fdw;

create schema air_oai_facts;

----------------------------
--airfare_survey_itinerary--
----------------------------

-- select * from air_oai_facts.airfare_survey_ticket_fdw limit 100;
-- drop foreign table if exists air_oai_facts.airfare_survey_ticket_fdw;
create foreign table air_oai_facts.airfare_survey_ticket_fdw
	( itinerary_oai_id								bigint null
	, coupon_qty									float4 null
	, year_nbr										integer null
	, quarter_nbr									integer null
	, depart_airport_oai_code						char(3) null
	, depart_airport_oai_id							integer null
	, depart_airport_oai_seq_id						integer null
	, depart_market_city_oai_id						integer null
	, depart_country_iso_code						char(2) null
	, depart_subdivision_fips_code					char(2) null
	, depart_subdivision_iso_code         			varchar(3) null
	, depart_subdivision_name						varchar(75) null
	, depart_wac_oai_id								integer null
	, round_trip_ind            					float4 null
	, online_ind									float4 null
	, fare_credibility_ind							float4 null
	, fare_per_smi									float4 null
	, reporting_airline_oai_code					varchar(3) null
	, passenger_qty           						float4 null
	, fare_per_person_amount_usd					float4 null
	, bulk_fare_ind									float4 null
	, distance_smi									float4 null
	, distance_group_oai_id							integer null
	, flown_distance_smi							float4 null
	, geographic_type_oai_id						integer null
	, filler										varchar(10) null
	)
server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2023_1.csv'
	, delimiter ','
	, null ''
	);

-- drop table if exists air_oai_facts.airfare_survey_ticket_load;
create table air_oai_facts.airfare_survey_ticket_load
	( itinerary_oai_id								bigint null
	, coupon_qty									float4 null
	, year_nbr										integer null
	, quarter_nbr									integer null
	, depart_airport_oai_code						char(3) null
	, depart_airport_oai_id							integer null
	, depart_airport_oai_seq_id						integer null
	, depart_market_city_oai_id						integer null
	, depart_country_iso_code						char(2) null
	, depart_subdivision_fips_code					char(2) null
	, depart_subdivision_iso_code         			varchar(3) null
	, depart_subdivision_name						varchar(75) null
	, depart_wac_oai_id								integer null
	, round_trip_ind            					float4 null
	, online_ind									float4 null
	, fare_credibility_ind							float4 null
	, fare_per_smi									float4 null
	, reporting_airline_oai_code					varchar(3) null
	, passenger_qty           						float4 null
	, fare_per_person_amount_usd					float4 null
	, bulk_fare_ind									float4 null
	, distance_smi									float4 null
	, distance_group_oai_id							integer null
	, flown_distance_smi							float4 null
	, geographic_type_oai_id						integer null
	, filler										varchar(10) null
	);

SELECT create_distributed_table('air_oai_facts.airfare_survey_ticket_load', 'itinerary_oai_id');

copy air_oai_facts.airfare_survey_ticket_load
from '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2023_1.csv'
delimiter ',' header csv; -- 10 secs!

	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2020_1.csv'
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2020_2.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2020_3.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2020_4.csv' -- y
	
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2021_1.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2021_2.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2021_3.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2021_4.csv' -- y
	
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2022_1.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2022_2.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2022_3.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2022_4.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_ticket/Origin_and_Destination_Survey_DB1BTicket_2023_1.csv' -- y

-- select year_nbr, quarter_nbr, count(*) from air_oai_facts.airfare_survey_ticket_fdw group by 1,2;
select year_nbr, quarter_nbr, count(*) from air_oai_facts.airfare_survey_ticket_load group by 1,2;
select year_quarter_start_date, count(*) from air_oai_facts.airfare_survey_itinerary group by 1 order by 1 desc;

vacuum verbose air_oai_facts.airfare_survey_itinerary;
vacuum analyze air_oai_facts.airfare_survey_ticket_load;
-- ERROR: connection to the remote node localhost:9758 failed with the following error: SSL SYSCALL error: EOF detected 

citus_rebalance_stop()
citus_rebalance_wait()


INSERT INTO air_oai_facts.airfare_survey_itinerary
	( itinerary_oai_id, year_quarter_start_date
	, reporting_airline_entity_id, reporting_airline_entity_key
	, depart_airport_history_id, depart_airport_history_key
	, round_trip_fare_ind, online_purchase_ind, bulk_fare_ind, fare_credibility_ind
	, distance_group_oai_id, geographic_type_oai_id
	, coupon_qty, passenger_qty, distance_smi, flown_distance_smi
	, fare_per_person_usd, fare_per_mile_usd
	, created_by, created_tmst)
SELECT asf.itinerary_oai_id
	 , (asf.year_nbr::text || case when asf.quarter_nbr = 1 then '-01-01' when asf.quarter_nbr = 2 then '-04-01' 
            when asf.quarter_nbr = 3 then '-07-01' when asf.quarter_nbr = 4 then '-10-01' else null end::text)::date as year_quarter_start_date
     , ae.airline_entity_id as reporting_airline_entity_id
     , ae.airline_entity_key as reporting_airline_entity_key
     , ah.airport_history_id as depart_airport_history_id
     , ah.airport_history_key as depart_airport_history_key
     , round_trip_ind, online_ind, bulk_fare_ind, fare_credibility_ind
     , distance_group_oai_id, geographic_type_oai_id
	 , coupon_qty, passenger_qty, distance_smi, flown_distance_smi
	 , fare_per_person_amount_usd, fare_per_smi
	 , current_user, now()
FROM air_oai_facts.airfare_survey_ticket_load asf
-- air_oai_facts.airfare_survey_ticket_fdw asf
left join (select * from air_oai_dims.airline_entities where operating_region_code = 'Domestic') ae 
  on asf.reporting_airline_oai_code = ae.airline_oai_code
left join air_oai_dims.airport_history ah
  on asf.depart_airport_oai_seq_id = ah.airport_oai_seq_id
where (asf.year_nbr::text || 
      case when asf.quarter_nbr = 1 then '-01-01' when asf.quarter_nbr = 2 then '-04-01' 
           when asf.quarter_nbr = 3 then '-07-01' when asf.quarter_nbr = 4 then '-10-01' else null end::text)::date
      between ae.source_from_date and coalesce(ae.source_thru_date, current_date)
;

-- drop table if exists air_oai_facts.airfare_survey_itinerary;
create table air_oai_facts.airfare_survey_itinerary
	 ( itinerary_oai_id								bigint  not null
	 , year_quarter_start_date						date	not null
	 , reporting_airline_entity_id					smallint not null
	 , reporting_airline_entity_key					char(32) not null
	 , depart_airport_history_id					integer	not null
	 , depart_airport_history_key					char(32) not null
	 , round_trip_fare_ind            				smallint null
	 , online_purchase_ind							smallint null
	 , bulk_fare_ind								smallint null
	 , fare_credibility_ind							smallint null
	 , distance_group_oai_id						smallint null
	 , geographic_type_oai_id						smallint null
	 , coupon_qty									smallint null
	 , passenger_qty           						smallint null
	 , distance_smi									integer null
	 , flown_distance_smi							integer null
	 , fare_per_person_usd							integer null
	 , fare_per_mile_usd							numeric(10,5) null
	 , created_by 									varchar(32) DEFAULT 'CURRENT_USER' NOT NULL
	 , created_tmst 								timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL
	 , updated_by 									varchar(32)
	 , updated_tsmt 								timestamp(0)
	 , constraint airfare_survey_itinerary_pk primary key (itinerary_oai_id, year_quarter_start_date)
	 ) partition by range (year_quarter_start_date)
	 ;

SELECT create_distributed_table('air_oai_facts.airfare_survey_itinerary', 'itinerary_oai_id');

VACUUM VERBOSE air_oai_facts.airfare_survey_itinerary;

-- Convert to row-based (heap) storage
-- SELECT alter_table_set_access_method('contestant', 'heap');

-- Convert to columnar storage (indexes will be dropped)
-- SELECT alter_table_set_access_method('contestant', 'columnar');

-- convert older partitions to use columnar storage
-- CALL alter_old_partitions_set_access_method('github_columnar_events', '2015-01-01 06:00:00' /* older_than */, 'columnar');
-- CALL alter_old_partitions_set_access_method('foo', now() - interval '6 months','columnar');

-- the old partitions are now columnar, while the
-- latest uses row storage and can be updated

-- create a year's worth of monthly partitions
-- in table foo, starting from the current time
/*
SELECT create_time_partitions(
  table_name         := 'air_oai_facts.airfare_survey_itinerary',
  partition_interval := '3 month',
  end_at             := '2023-08-31 00:00:00'  -- now() - interval '24 months'
); */

select * from master_get_active_worker_nodes();

-- drop schema air_oai_parts cascade;
-- create schema air_oai_parts;

/*
drop table air_oai_facts.airfare_survey_itinerary_2023Q1;
drop table air_oai_facts.airfare_survey_itinerary_2022Q4;
drop table air_oai_facts.airfare_survey_itinerary_2022Q3;
drop table air_oai_facts.airfare_survey_itinerary_2022Q2;
drop table air_oai_facts.airfare_survey_itinerary_2022Q1;
*/

create table air_oai_facts.airfare_survey_itinerary_2023Q1 partition of air_oai_facts.airfare_survey_itinerary for values from ('2023-01-01') to ('2023-03-31');
create table air_oai_facts.airfare_survey_itinerary_2022Q4 partition of air_oai_facts.airfare_survey_itinerary for values from ('2022-10-01') to ('2022-12-31');
create table air_oai_facts.airfare_survey_itinerary_2022Q3 partition of air_oai_facts.airfare_survey_itinerary for values from ('2022-07-01') to ('2022-09-30');
create table air_oai_facts.airfare_survey_itinerary_2022Q2 partition of air_oai_facts.airfare_survey_itinerary for values from ('2022-04-01') to ('2022-06-30');
create table air_oai_facts.airfare_survey_itinerary_2022Q1 partition of air_oai_facts.airfare_survey_itinerary for values from ('2022-01-01') to ('2022-03-31');

create table air_oai_facts.airfare_survey_itinerary_2021Q4 partition of air_oai_facts.airfare_survey_itinerary for values from ('2021-10-01') to ('2021-12-31');
create table air_oai_facts.airfare_survey_itinerary_2021Q3 partition of air_oai_facts.airfare_survey_itinerary for values from ('2021-07-01') to ('2021-09-30');
create table air_oai_facts.airfare_survey_itinerary_2021Q2 partition of air_oai_facts.airfare_survey_itinerary for values from ('2021-04-01') to ('2021-06-30');
create table air_oai_facts.airfare_survey_itinerary_2021Q1 partition of air_oai_facts.airfare_survey_itinerary for values from ('2021-01-01') to ('2021-03-31');

create table air_oai_facts.airfare_survey_itinerary_2020Q4 partition of air_oai_facts.airfare_survey_itinerary for values from ('2020-10-01') to ('2020-12-31');
create table air_oai_facts.airfare_survey_itinerary_2020Q3 partition of air_oai_facts.airfare_survey_itinerary for values from ('2020-07-01') to ('2020-09-30');
create table air_oai_facts.airfare_survey_itinerary_2020Q2 partition of air_oai_facts.airfare_survey_itinerary for values from ('2020-04-01') to ('2020-06-30');
create table air_oai_facts.airfare_survey_itinerary_2020Q1 partition of air_oai_facts.airfare_survey_itinerary for values from ('2020-01-01') to ('2020-03-31');

CALL alter_old_partitions_set_access_method('air_oai_facts.airfare_survey_itinerary', now(),'columnar');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'air_oai_facts.airfare_survey_itinerary'::regclass;
-- CALL alter_old_partitions_set_access_method('air_oai_facts.airfare_survey_itinerary', '2023-08-03' /* older_than */, 'columnar');

SELECT * FROM columnar.options;

select year_quarter_start_date, count(*) from air_oai_facts.airfare_survey_itinerary group by 1 order by 1 desc;
select year_nbr, quarter_nbr, count(*) from air_oai_facts.db1b_airline_survey_ticket_fdw group by 1,2; -- 4109793
select itinerary_oai_id, count(*) from air_oai_facts.db1b_airline_survey_ticket_fdw group by 1 having count(*) > 1 order by count(*) desc; -- 0

SELECT pg_total_relation_size('air_oai_facts.airfare_survey_itinerary_row') as row_wise_size -- 869,089,280
SELECT pg_total_relation_size('air_oai_facts.airfare_survey_itinerary') as columnar_size -- 184,074,240
     
SELECT pg_total_relation_size('air_oai_facts.airfare_survey_itinerary_row')::numeric/
       pg_total_relation_size('air_oai_facts.airfare_survey_itinerary') AS compression_ratio; -- 4.7214063195371607

------


SELECT asi.year_quarter_start_date
	 , asi.reporting_airline_entity_id
	 , max(ae.airline_name) as airline_name
	 --, reporting_airline_entity_key
	 --, depart_airport_history_id
	 --, depart_airport_history_key
	 --, round_trip_fare_ind
	 --, online_purchase_ind
	 --, bulk_fare_ind
	 --, fare_credibility_ind
	 --, distance_group_oai_id
	 --, geographic_type_oai_id
	 , sum(asi.coupon_qty) as sum_coupon_qty
	 , sum(asi.passenger_qty) as sum_passenger_qty
	 , avg(asi.distance_smi) as avg_distance_smi
	 , avg(asi.flown_distance_smi) as avg_flown_distance_smi
	 , avg(asi.fare_per_person_usd) as avg_fare_per_person_usd
	 , avg(asi.fare_per_mile_usd) as avg_fare_per_mile_usd
	 , count(*)
FROM air_oai_facts.airfare_survey_itinerary asi
LEFT JOIN air_oai_facts.airline_entities ae 
  on asi.reporting_airline_entity_id = ae.airline_entity_id
group by 1,2
order by 1 desc, sum(asi.passenger_qty) desc;



------

select airport_oai_seq_id, count(*) from air_oai_facts.airport_history group by 1 having count(*) > 1 order by count(*) desc;

select reporting_airline_oai_code, count(*)
from (
select asf.reporting_airline_oai_code
     , (asf.year_nbr::text || asf.quarter_nbr::text)::smallint as year_quarter_nbr
     , (asf.year_nbr::text || case when asf.quarter_nbr = 1 then '-01-01' else null end::text)::date as year_qtr_date
     , ae.airline_entity_id
     , ae.airline_entity_key
     , ae.airline_oai_code
     , ae.source_from_date
     , ae.source_thru_date
     , count(*)
FROM air_oai_facts.db1b_airline_survey_ticket_fdw asf
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') ae 
  on asf.reporting_airline_oai_code = ae.airline_oai_code
where (asf.year_nbr::text || case when asf.quarter_nbr = 1 then '-01-01' else null end::text)::date
between ae.source_from_date and coalesce(ae.source_thru_date, current_date)
group by 1,2,3,4,5,6,7,8
order by 1) a group by 1 having count(*) > 1 order by count(*) desc
;


select distinct round_trip_ind from air_oai_facts.db1b_airline_survey_ticket_fdw; -- 0/1
select distinct online_ind from air_oai_facts.db1b_airline_survey_ticket_fdw; -- 0/1
select distinct bulk_fare_ind from air_oai_facts.db1b_airline_survey_ticket_fdw; -- 0/1
select distinct fare_credibility_ind from air_oai_facts.db1b_airline_survey_ticket_fdw; -- 0/1
select distinct distance_group_oai_id from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 1 to 25 int
select distinct geographic_type_oai_id from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 1/2
select distinct coupon_qty from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 1 to 12 int
select distinct passenger_qty from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 1 to 1120 int
select distinct distance_smi from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 31 to 26316 int
select distinct flown_distance_smi from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 31 to 18845 int
select distinct fare_per_person_amount_usd from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 0 to 1345400 int
select distinct fare_per_smi from air_oai_facts.db1b_airline_survey_ticket_fdw order by 1; -- 0 to 5.2093, numeric(7,5)

alter table oai.airfare_survey_itinerary add constraint airfare_survey_itinerary_pk primary key (itinerary_id);
create index airfare_survey_itinerary_reporting_carrier_idx on oai.airfare_survey_itinerary (reporting_carrier_iata_cd);
create index airfare_survey_itinerary_origin_airport_idx on oai.airfare_survey_itinerary (orig_airport_iata_cd);
create index airfare_survey_itinerary_year_quarter_idx on oai.airfare_survey_itinerary (year_nbr, quarter_nbr);

-------------------------
--airline_survey_coupon--
-------------------------

-- select count(*) from air_oai_facts.db1b_airline_survey_coupon_fdw; -- 9,892,092
-- select * from air_oai_facts.db1b_airline_survey_coupon_fdw limit 100;
-- drop foreign table if exists air_oai_facts.db1b_airline_survey_coupon_fdw;
create foreign table air_oai_facts.db1b_airline_survey_coupon_fdw
	( itinerary_oai_id             		bigint null
	, market_oai_id						bigint null
	, flight_pass_seq					integer null
	, flight_pass_qty					integer null
	, year_nbr              			integer null
	, depart_airport_oai_id				integer null
	, depart_airport_oai_seq_id			integer null
	, depart_city_market_oai_id			integer null
	, quarter_nbr              			integer null
	, depart_airport_oai_code      		char(3) null
	, depart_country_iso_code       	char(2) null
	, depart_state_fips_code      		char(2) null
	, depart_state_iso_code         	varchar(3) null
	, depart_state_name      			varchar(75) null
	, depart_world_area_oai_id        	integer null
	, arrive_airport_oai_id				integer null
	, arrive_airport_oai_seq_id			integer	null
	, arrive_city_market_oai_id			integer null
	, arrive_airport_oai_code      		char(3) null
	, arrive_country_iso_code       	char(2) null
	, arrive_state_fips_code       		char(2) null
	, arrive_state_iso_code         	varchar(3) null
	, arrive_state_name        			varchar(75) null
	, arrive_world_area_oai_id        	integer null
	, trip_break_code             		char(1) null
	, flight_pass_type					varchar(5) null
	, ticketing_airline_oai_code 		varchar(3) null
	, operating_airline_oai_code  		varchar(3) null
	, reporting_airline_oai_code  		varchar(3) null
	, passengers_qty          			float4 null
	, airfare_class_code          		varchar(5) null
	, distance_smi             			float4 null
	, distance_group_id        			integer null
	, gateway_ind              			float4 null
	, itinerary_geo_type_id     		integer null
	, coupon_geo_type_id        		integer null
	, filler							varchar(10) null
	)
server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_1.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_2.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_3.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_4.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2023_1.csv' -- y
	, delimiter ','
	, null ''
	);

-- /Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2023_1.csv
-- /Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_4.csv
-- /Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_3.csv
-- /Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_2.csv
-- /Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_coupon/Origin_and_Destination_Survey_DB1BCoupon_2022_1.csv

vacuum verbose air_oai_facts.airfare_survey_coupon;
select year_quarter_start_date, count(*) from air_oai_facts.airfare_survey_coupon group by 1 order by 1 desc;

INSERT INTO air_oai_facts.airfare_survey_coupon
	(itinerary_oai_id, flight_pass_seq, year_quarter_start_date, market_oai_id
	, ticketing_airline_entity_id, ticketing_airline_entity_key
	, operating_airline_entity_id, operating_airline_entity_key
	, reporting_airline_entity_id, reporting_airline_entity_key
	, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_history_id, arrive_airport_history_key
	, trip_break_code, gateway_ind, distance_group_oai_id, airfare_class_code
	, itinerary_geographic_type_oai_id, coupon_geographic_type_oai_id
	, flight_pass_type, flight_pass_qty, passengers_qty, distance_smi
	, created_by, created_tmst)
SELECT ac.itinerary_oai_id
     , ac.flight_pass_seq
     , (ac.year_nbr::text || case when ac.quarter_nbr = 1 then '-01-01' when ac.quarter_nbr = 2 then '-04-01' 
            when ac.quarter_nbr = 3 then '-07-01' when ac.quarter_nbr = 4 then '-10-01' else null end::text)::date as year_quarter_start_date
	 , ac.market_oai_id
	 , aet.airline_entity_id as ticketing_airline_entity_id
	 , aet.airline_entity_key as ticketing_airline_entity_key
	 , aeo.airline_entity_id as operating_airline_entity_id
	 , aeo.airline_entity_key as operating_airline_entity_key
	 , aer.airline_entity_id as reporting_airline_entity_id
	 , aer.airline_entity_key as reporting_airline_entity_key
	 , ahd.airport_history_id as depart_airport_history_id
	 , ahd.airport_history_key as depart_airport_history_key
	 , aha.airport_history_id as arrive_airport_history_id
	 , ahd.airport_history_key as arrive_airport_history_key
	 , case when ac.trip_break_code = 'X' then 1 else 0 end::smallint as trip_break_code
	 , ac.gateway_ind
	 , ac.distance_group_id
	 , ac.airfare_class_code
	 , ac.itinerary_geo_type_id as itinerary_geographic_type_id
	 , ac.coupon_geo_type_id as coupon_geographic_type_id
	 , ac.flight_pass_type
	 , ac.flight_pass_qty
	 , ac.passengers_qty
	 , ac.distance_smi
	 , current_user
	 , current_timestamp
FROM air_oai_facts.db1b_airline_survey_coupon_fdw ac
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') aet
  on ac.ticketing_airline_oai_code = aet.airline_oai_code
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') aeo
  on ac.operating_airline_oai_code = aeo.airline_oai_code
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') aer
  on ac.reporting_airline_oai_code = aer.airline_oai_code
left join air_oai_facts.airport_history ahd
  on ac.depart_airport_oai_seq_id = ahd.airport_oai_seq_id
left join air_oai_facts.airport_history aha
  on ac.arrive_airport_oai_seq_id = aha.airport_oai_seq_id
where (ac.year_nbr::text || 
       case when ac.quarter_nbr = 1 then '-01-01' when ac.quarter_nbr = 2 then '-04-01' 
       when ac.quarter_nbr = 3 then '-07-01' when ac.quarter_nbr = 4 then '-10-01' else null end::text)::date
       between aet.source_from_date and coalesce(aet.source_thru_date, current_date)
and   (ac.year_nbr::text || 
       case when ac.quarter_nbr = 1 then '-01-01' when ac.quarter_nbr = 2 then '-04-01' 
       when ac.quarter_nbr = 3 then '-07-01' when ac.quarter_nbr = 4 then '-10-01' else null end::text)::date
       between aeo.source_from_date and coalesce(aeo.source_thru_date, current_date)
and   (ac.year_nbr::text || 
       case when ac.quarter_nbr = 1 then '-01-01' when ac.quarter_nbr = 2 then '-04-01' 
       when ac.quarter_nbr = 3 then '-07-01' when ac.quarter_nbr = 4 then '-10-01' else null end::text)::date
       between aer.source_from_date and coalesce(aer.source_thru_date, current_date)
;

select itinerary_id, flight_pass_seq, count(*) 
from air_oai_facts.db1b_airline_survey_coupon_fdw group by 1,2 having count(*) > 1 order by count(*) desc; -- unique!

-- drop table if exists air_oai_facts.airfare_survey_coupon;
create table air_oai_facts.airfare_survey_coupon
	( itinerary_oai_id             		bigint 		not null
	, flight_pass_seq					integer 	not null
	, year_quarter_start_date			date		not null
	, market_oai_id						bigint 		not null
	, ticketing_airline_entity_id		smallint	not null
	, ticketing_airline_entity_key		char(32)	not null
	, operating_airline_entity_id		smallint	not null
	, operating_airline_entity_key		char(32)	not null
	, reporting_airline_entity_id		smallint	not null
	, reporting_airline_entity_key		char(32)	not null
	, depart_airport_history_id			integer		not null
	, depart_airport_history_key		char(32)	not null
	, arrive_airport_history_id			integer		not null
	, arrive_airport_history_key		char(32)	not null
	, trip_break_code             		smallint 	not null
	, gateway_ind              			smallint 	not null
	, distance_group_oai_id        		smallint 	not null
	, airfare_class_code          		char(1) 	not null
	, itinerary_geographic_type_oai_id  smallint 	not null
	, coupon_geographic_type_oai_id     smallint 	not null
	, flight_pass_type					char(1) 	not null
	, flight_pass_qty					smallint 	not null
	, passengers_qty          			smallint 	not null
	, distance_smi             			integer 	not null
	, created_by 						varchar(32) DEFAULT 'CURRENT_USER' NOT NULL
	, created_tmst 						timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL
	, updated_by 						varchar(32)
	, updated_tsmt 						timestamp(0)
	, constraint airfare_survey_coupon_pk primary key (itinerary_oai_id, flight_pass_seq, year_quarter_start_date)
	) partition by range (year_quarter_start_date)
	;

-- alter table air_oai_facts.airfare_survey_coupon rename column market_id to market_oai_id; -- done

SELECT create_distributed_table('air_oai_facts.airfare_survey_coupon', 'itinerary_oai_id', colocate_with => 'air_oai_facts.airfare_survey_itinerary');
VACUUM VERBOSE air_oai_facts.airline_survey_coupon;

select count(*) from air_oai_facts.airfare_survey_coupon;

create table air_oai_facts.airfare_survey_coupon_2023Q1 partition of air_oai_facts.airfare_survey_coupon for values from ('2023-01-01') to ('2023-03-31');
create table air_oai_facts.airfare_survey_coupon_2022Q4 partition of air_oai_facts.airfare_survey_coupon for values from ('2022-10-01') to ('2022-12-31');
create table air_oai_facts.airfare_survey_coupon_2022Q3 partition of air_oai_facts.airfare_survey_coupon for values from ('2022-07-01') to ('2022-09-30');
create table air_oai_facts.airfare_survey_coupon_2022Q2 partition of air_oai_facts.airfare_survey_coupon for values from ('2022-04-01') to ('2022-06-30');
create table air_oai_facts.airfare_survey_coupon_2022Q1 partition of air_oai_facts.airfare_survey_coupon for values from ('2022-01-01') to ('2022-03-31');

create table air_oai_facts.airfare_survey_coupon_2021Q4 partition of air_oai_facts.airfare_survey_coupon for values from ('2021-10-01') to ('2021-12-31');
create table air_oai_facts.airfare_survey_coupon_2021Q3 partition of air_oai_facts.airfare_survey_coupon for values from ('2021-07-01') to ('2021-09-30');
create table air_oai_facts.airfare_survey_coupon_2021Q2 partition of air_oai_facts.airfare_survey_coupon for values from ('2021-04-01') to ('2021-06-30');
create table air_oai_facts.airfare_survey_coupon_2021Q1 partition of air_oai_facts.airfare_survey_coupon for values from ('2021-01-01') to ('2021-03-31');

create table air_oai_facts.airfare_survey_coupon_2020Q4 partition of air_oai_facts.airfare_survey_coupon for values from ('2020-10-01') to ('2020-12-31');
create table air_oai_facts.airfare_survey_coupon_2020Q3 partition of air_oai_facts.airfare_survey_coupon for values from ('2020-07-01') to ('2020-09-30');
create table air_oai_facts.airfare_survey_coupon_2020Q2 partition of air_oai_facts.airfare_survey_coupon for values from ('2020-04-01') to ('2020-06-30');
create table air_oai_facts.airfare_survey_coupon_2020Q1 partition of air_oai_facts.airfare_survey_coupon for values from ('2020-01-01') to ('2020-03-31');

CALL alter_old_partitions_set_access_method('air_oai_facts.airfare_survey_coupon', now() /* older_than */, 'columnar');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'air_oai_facts.airfare_survey_coupon'::regclass;


-----------

SELECT asi.year_quarter_start_date
	 --, asi.reporting_airline_entity_id
	 , max(ae.airline_name) as reporting_airline_name
	 --, afsc.operating_airline_entity_id
	 , max(aeo.airline_name) as operating__airline_name
	 --, reporting_airline_entity_key
	 --, depart_airport_history_id
	 --, depart_airport_history_key
	 --, round_trip_fare_ind
	 --, online_purchase_ind
	 --, bulk_fare_ind
	 --, fare_credibility_ind
	 --, distance_group_oai_id
	 --, geographic_type_oai_id
	 , sum(asi.coupon_qty) as sum_coupon_qty
	 , sum(asi.passenger_qty) as sum_passenger_qty
	 , avg(asi.distance_smi)::numeric(6,1) as avg_distance_smi
	 , avg(asi.flown_distance_smi)::numeric(6,1) as avg_flown_distance_smi
	 , avg(asi.fare_per_person_usd)::numeric(7,2) as avg_fare_per_person_usd
	 , avg(asi.fare_per_mile_usd)::numeric(4,3) as avg_fare_per_mile_usd
	 , sum(afsc.flight_pass_qty) as sum_flight_pass_qty
	 , avg(afsc.flight_pass_qty)::numeric(3,2) as avg_flight_pass_qty
	 , count(*)
FROM air_oai_facts.airfare_survey_itinerary asi
JOIN air_oai_facts.airfare_survey_coupon afsc 
  on asi.itinerary_oai_id = afsc.itinerary_oai_id and asi.year_quarter_start_date = afsc.year_quarter_start_date
JOIN air_oai_facts.airline_entities ae on asi.reporting_airline_entity_id = ae.airline_entity_id
JOIN air_oai_facts.airline_entities aeo on afsc.operating_airline_entity_id = aeo.airline_entity_id
where asi.reporting_airline_entity_id != afsc.operating_airline_entity_id
group by asi.year_quarter_start_date, asi.reporting_airline_entity_id, afsc.operating_airline_entity_id
order by 1 desc, sum(asi.passenger_qty) desc;

SELECT table_name, table_size FROM citus_tables;
select * from pg_dist_node;
select * from citus_tables;
select * from citus_shards;
SELECT * FROM master_get_active_worker_nodes();
SELECT * FROM citus_get_active_worker_nodes();
SELECT * FROM citus_check_cluster_node_health();
SELECT * FROM pg_extension;

SELECT * from citus_get_active_worker_nodes();

SELECT pg_size_pretty(citus_table_size('air_oai_facts.airfare_survey_coupon'));

-- SELECT truncate_local_data_after_distributing_table($$public.series$$)

SELECT truncate_local_data_after_distributing_table('air_oai_facts.airfare_survey_itinerary');
SELECT truncate_local_data_after_distributing_table('air_oai_facts.airfare_survey_coupon');
SELECT truncate_local_data_after_distributing_table('air_oai_facts.airfare_survey_market');

-----------

-------------------------
--airfare_survey_market--
-------------------------

-- select year_nbr, quarter_nbr, count(*) from air_oai_facts.airfare_survey_market_fdw group by 1,2;
-- select * from air_oai_facts.airfare_survey_market_fdw limit 100;
-- drop foreign table if exists air_oai_facts.airfare_survey_market_fdw;
create foreign table air_oai_facts.airfare_survey_market_fdw
	( itinerary_oai_id              	bigint null
	, market_oai_id                		bigint null
	, market_coupon_qty		       		integer null
	, year_nbr                 			integer null
	, quarter_nbr              			integer null
	, depart_airport_oai_id				integer null
	, depart_airport_oai_seq_id			integer null
	, depart_city_market_oai_id			integer null
	, depart_airport_oai_code          	char(3) null
	, depart_country_iso_code        	char(2) null
	, depart_state_fips_code      		char(2) null
	, depart_state_iso_code          	varchar(3) null
	, depart_state_name      			varchar(75) null
	, depart_world_area_oai_id          integer null
	, arrive_airport_oai_id				integer null
	, arrive_airport_oai_seq_id			integer	null
	, arrive_city_market_oai_id			integer null
	, arrive_airport_oai_code        	char(3) null
	, arrive_country_iso_code         	char(2) null
	, arrive_state_fips_code        	char(2) null
	, arrive_state_iso_code            	varchar(3) null
	, arrive_state_name       			varchar(75) null
	, arrive_world_area_oai_id          integer null
	, airports_group_oai_code			varchar(255) null
	, world_areas_group_oai_code		varchar(255) null
	, ticketing_airline_change_ind		float4 null
	, ticketing_airline_group_code		varchar(255) null
	, operating_airline_change_ind		float4 null
	, operating_airline_group_code		varchar(255) null
	, reporting_airline_oai_code		varchar(3) null
	, ticketing_airline_oai_code		varchar(3) null
	, operating_airline_oai_code		varchar(3) null
	, bulk_fare_ind						float4 null
	, passenger_qty						float4 null
	, market_fare_amt_usd				float4 null
	, market_distance_smi				float4 null
	, market_distance_group_oai_id		float4 null
	, market_flown_distance_smi			float4 null
	, non_stop_distance_smi				float4 null
	, itinerary_geograhic_type_oai_id   integer null
	, market_geograhic_type_oai_id      integer null
	, filler							varchar(10) null
	)
server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename 
	
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2020_2.csv' -- 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2020_3.csv' -- 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2020_4.csv' -- 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2020_1.csv' -- 
	
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2021_2.csv' -- 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2021_3.csv' -- 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2021_4.csv' -- 
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2021_1.csv' -- 
	
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_1.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_2.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_3.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_4.csv' -- y
	-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2023_1.csv' -- y
	, delimiter ','
	, null ''
	);

-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2023_1.csv'
-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_4.csv'
-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_3.csv'
-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_2.csv'
-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_db1b/_market/Origin_and_Destination_Survey_DB1BMarket_2022_1.csv'

select itinerary_geograhic_type_id, market_geograhic_type_id
     , depart_country_iso_code, depart_state_iso_code
     , arrive_country_iso_code, arrive_state_iso_code
     , count(*) 
from air_oai_facts.airline_survey_market_fdw 
group by 1,2,3,4,5,6;

select itinerary_oai_id, count(*) from air_oai_facts.airline_survey_market_fdw group by 1 having count(*) > 1 order by count(*) desc;
select itinerary_oai_id, market_oai_id, count(*) from air_oai_facts.airline_survey_market_fdw group by 1,2 having count(*) > 1 order by count(*) desc; -- unique
--select * from air_oai_facts.airline_survey_market_fdw where itinerary_oai_id = 202315289895;

select wac_pair_group_oai_code, length(wac_pair_group_oai_code) as len_code, count(*) from air_oai_facts.airline_survey_market_fdw group by 1 order by 2 desc;


INSERT INTO aviation.air_oai_facts.airfare_survey_market
	( itinerary_oai_id, market_oai_id, year_quarter_start_date
	, ticketing_airline_entity_id, ticketing_airline_entity_key, ticketing_airline_change_ind, ticketing_airlines_group_code
	, operating_airline_entity_id, operating_airline_entity_key, operating_airline_change_ind, operating_airlines_group_code
	, reporting_airline_entity_id, reporting_airline_entity_key
	, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_history_id, arrive_airport_history_key
	, airports_group_oai_code, world_areas_group_oai_code
	, itinerary_geograhic_type_oai_id, market_geograhic_type_oai_id, market_distance_group_oai_id
	, bulk_fare_ind, market_coupon_qty, passenger_qty, market_fare_amount_usd
	, market_distance_smi, market_flown_distance_smi, non_stop_distance_smi
	, created_by, created_tmst)
SELECT am.itinerary_oai_id
	 , am.market_oai_id
	 , (am.year_nbr::text || case when am.quarter_nbr = 1 then '-01-01' when am.quarter_nbr = 2 then '-04-01' 
            when am.quarter_nbr = 3 then '-07-01' when am.quarter_nbr = 4 then '-10-01' else null end::text)::date as year_quarter_start_date
     , aet.airline_entity_id as ticketing_airline_entity_id
     , aet.airline_entity_key as ticketing_airline_entity_key
	 , am.ticketing_airline_change_ind
	 , am.ticketing_airline_group_code
	 , aeo.airline_entity_id as operating_airline_entity_id
	 , aeo.airline_entity_key as operating_airline_entity_key
	 , am.operating_airline_change_ind
	 , am.operating_airline_group_code
	 , aer.airline_entity_id as reporting_airline_entity_id
	 , aer.airline_entity_key as reporting_airline_entity_key
	 , ahd.airport_history_id as depart_airport_history_id
	 , ahd.airport_history_key as depart_airport_history_key
     , aha.airport_history_id as arrive_airport_history_id
     , aha.airport_history_key as arrive_airport_history_key
	 , am.airports_group_oai_code
	 , am.world_areas_group_oai_code
	 , am.itinerary_geograhic_type_oai_id
	 , am.market_geograhic_type_oai_id
     , am.market_distance_group_oai_id
     , am.bulk_fare_ind
	 , am.market_coupon_qty
	 , am.passenger_qty
	 , am.market_fare_amt_usd
	 , am.market_distance_smi
	 , am.market_flown_distance_smi
	 , am.non_stop_distance_smi
	 , current_user
	 , current_timestamp
FROM air_oai_facts.airfare_survey_market_fdw am
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') aet
  on am.ticketing_airline_oai_code = aet.airline_oai_code
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') aeo
  on am.operating_airline_oai_code = aeo.airline_oai_code
left join (select * from air_oai_facts.airline_entities where operating_region_code = 'Domestic') aer
  on am.reporting_airline_oai_code = aer.airline_oai_code
left join air_oai_facts.airport_history ahd
  on am.depart_airport_oai_seq_id = ahd.airport_oai_seq_id
left join air_oai_facts.airport_history aha
  on am.arrive_airport_oai_seq_id = aha.airport_oai_seq_id
where (am.year_nbr::text || 
       case when am.quarter_nbr = 1 then '-01-01' when am.quarter_nbr = 2 then '-04-01' 
       when am.quarter_nbr = 3 then '-07-01' when am.quarter_nbr = 4 then '-10-01' else null end::text)::date
       between aet.source_from_date and coalesce(aet.source_thru_date, current_date)
and   (am.year_nbr::text || 
       case when am.quarter_nbr = 1 then '-01-01' when am.quarter_nbr = 2 then '-04-01' 
       when am.quarter_nbr = 3 then '-07-01' when am.quarter_nbr = 4 then '-10-01' else null end::text)::date
       between aeo.source_from_date and coalesce(aeo.source_thru_date, current_date)
and   (am.year_nbr::text || 
       case when am.quarter_nbr = 1 then '-01-01' when am.quarter_nbr = 2 then '-04-01' 
       when am.quarter_nbr = 3 then '-07-01' when am.quarter_nbr = 4 then '-10-01' else null end::text)::date
       between aer.source_from_date and coalesce(aer.source_thru_date, current_date)
;

--select year_quarter_start_date, count(*) from air_oai_facts.airfare_survey_market group by 1 order by 1 desc;

-- drop table if exists air_oai_facts.airfare_survey_market;
create table air_oai_facts.airfare_survey_market
	( itinerary_oai_id             		bigint 		not null
	, market_oai_id						bigint 		not null
	, year_quarter_start_date			date		not null
	, ticketing_airline_entity_id		smallint	not null
	, ticketing_airline_entity_key		char(32)	not null
	, ticketing_airline_change_ind		smallint	not null
	, ticketing_airlines_group_code		varchar(55) not null
	, operating_airline_entity_id		smallint	not null
	, operating_airline_entity_key		char(32)	not null
	, operating_airline_change_ind		smallint	not null
	, operating_airlines_group_code		varchar(55) not null
	, reporting_airline_entity_id		smallint	not null
	, reporting_airline_entity_key		char(32)	not null
	, depart_airport_history_id			integer		not null
	, depart_airport_history_key		char(32)	not null
	, arrive_airport_history_id			integer		not null
	, arrive_airport_history_key		char(32)	not null
	, airports_group_oai_code			varchar(55) not null
	, world_areas_group_oai_code		varchar(55) not null
    , itinerary_geograhic_type_oai_id   smallint 	not null
	, market_geograhic_type_oai_id      smallint 	not null
	, market_distance_group_oai_id		smallint 	not null
	, bulk_fare_ind						smallint 	not null
	, market_coupon_qty					smallint 	not null
	, passenger_qty						smallint 	not null
	, market_fare_amount_usd			numeric(9,2) not null
	, market_distance_smi				integer 	not null
	, market_flown_distance_smi			integer 	not null
	, non_stop_distance_smi				integer 	not null
	, created_by 						varchar(32) DEFAULT 'CURRENT_USER' NOT NULL
	, created_tmst 						timestamp(0) DEFAULT CURRENT_TIMESTAMP NOT NULL
	, updated_by 						varchar(32)
	, updated_tsmt 						timestamp(0)
	, constraint airfare_survey_market_pk primary key (itinerary_oai_id, market_oai_id, year_quarter_start_date)
	) partition by range (year_quarter_start_date)
	;

SELECT create_distributed_table('air_oai_facts.airfare_survey_market', 'itinerary_oai_id', colocate_with => 'air_oai_facts.airfare_survey_itinerary');
VACUUM VERBOSE air_oai_facts.airfare_survey_market;
select count(*) from air_oai_facts.airfare_survey_market;

create table air_oai_facts.airfare_survey_market_2023Q1 partition of air_oai_facts.airfare_survey_market for values from ('2023-01-01') to ('2023-03-31');
create table air_oai_facts.airfare_survey_market_2022Q4 partition of air_oai_facts.airfare_survey_market for values from ('2022-10-01') to ('2022-12-31');
create table air_oai_facts.airfare_survey_market_2022Q3 partition of air_oai_facts.airfare_survey_market for values from ('2022-07-01') to ('2022-09-30');
create table air_oai_facts.airfare_survey_market_2022Q2 partition of air_oai_facts.airfare_survey_market for values from ('2022-04-01') to ('2022-06-30');
create table air_oai_facts.airfare_survey_market_2022Q1 partition of air_oai_facts.airfare_survey_market for values from ('2022-01-01') to ('2022-03-31');

create table air_oai_facts.airfare_survey_market_2021Q4 partition of air_oai_facts.airfare_survey_market for values from ('2021-10-01') to ('2021-12-31');
create table air_oai_facts.airfare_survey_market_2021Q3 partition of air_oai_facts.airfare_survey_market for values from ('2021-07-01') to ('2021-09-30');
create table air_oai_facts.airfare_survey_market_2021Q2 partition of air_oai_facts.airfare_survey_market for values from ('2021-04-01') to ('2021-06-30');
create table air_oai_facts.airfare_survey_market_2021Q1 partition of air_oai_facts.airfare_survey_market for values from ('2021-01-01') to ('2021-03-31');

create table air_oai_facts.airfare_survey_market_2020Q4 partition of air_oai_facts.airfare_survey_market for values from ('2020-10-01') to ('2020-12-31');
create table air_oai_facts.airfare_survey_market_2020Q3 partition of air_oai_facts.airfare_survey_market for values from ('2020-07-01') to ('2020-09-30');
create table air_oai_facts.airfare_survey_market_2020Q2 partition of air_oai_facts.airfare_survey_market for values from ('2020-04-01') to ('2020-06-30');
create table air_oai_facts.airfare_survey_market_2020Q1 partition of air_oai_facts.airfare_survey_market for values from ('2020-01-01') to ('2020-03-31');

CALL alter_old_partitions_set_access_method('air_oai_facts.airfare_survey_market', now() /* older_than */, 'columnar');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'air_oai_facts.airfare_survey_market'::regclass;


-----

SELECT pg_size_pretty(citus_relation_size('air_oai_facts.airfare_survey_market')); -- 0 bytes
SELECT pg_size_pretty(citus_table_size('air_oai_facts.airfare_survey_market')); -- 0 bytes
SELECT pg_size_pretty(citus_total_relation_size('air_oai_facts.airfare_survey_market')); -- 0 bytes

SELECT * FROM citus_tables;
SELECT * FROM citus_shards;
SELECT * from pg_dist_placement;
SELECT * from pg_dist_node;

-----

SELECT asi.year_quarter_start_date
	 --, asi.reporting_airline_entity_id
	 , max(ae.airline_name) as reporting_airline_name
	 --, afsc.operating_airline_entity_id
	 , max(aeo.airline_name) as operating__airline_name
	 --, reporting_airline_entity_key
	 --, depart_airport_history_id
	 --, depart_airport_history_key
	 --, round_trip_fare_ind
	 --, online_purchase_ind
	 --, bulk_fare_ind
	 --, fare_credibility_ind
	 --, distance_group_oai_id
	 --, geographic_type_oai_id
	 , sum(asi.coupon_qty) as sum_coupon_qty
	 , sum(asi.passenger_qty) as sum_passenger_qty
	 , avg(asi.distance_smi)::numeric(6,1) as avg_distance_smi
	 , avg(asi.flown_distance_smi)::numeric(6,1) as avg_flown_distance_smi
	 , avg(asi.fare_per_person_usd)::numeric(7,2) as avg_fare_per_person_usd
	 , avg(asi.fare_per_mile_usd)::numeric(4,3) as avg_fare_per_mile_usd
	 , sum(afsc.flight_pass_qty) as sum_flight_pass_qty
	 , avg(afsc.flight_pass_qty)::numeric(3,2) as avg_flight_pass_qty
	 , count(*)
FROM air_oai_facts.airfare_survey_itinerary asi
JOIN air_oai_facts.airfare_survey_coupon afsc 
  on asi.itinerary_oai_id = afsc.itinerary_oai_id and asi.year_quarter_start_date = afsc.year_quarter_start_date
JOIN air_oai_facts.airline_entities ae on asi.reporting_airline_entity_id = ae.airline_entity_id
JOIN air_oai_facts.airline_entities aeo on afsc.operating_airline_entity_id = aeo.airline_entity_id
where asi.reporting_airline_entity_id != afsc.operating_airline_entity_id
group by asi.year_quarter_start_date, asi.reporting_airline_entity_id, afsc.operating_airline_entity_id
order by 1 desc, sum(asi.passenger_qty) desc;
