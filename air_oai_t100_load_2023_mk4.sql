
-- alter table air_oai_facts.f41_traffic_t100_market_archive rename column unique_airline_oai_code to airline_unique_oai_code;

--DROP TABLE if exists air_oai_facts.f41_traffic_t100_market_archive;
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
	, filler_txt 					varchar(10)
	);
	
copy air_oai_facts.f41_traffic_t100_market_archive
from '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_t100/_data-market/T100_MARKET_ALL_CARRIER_ALL_2019.csv'
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
order by f.airline_oai_code, f.depart_airport_oai_code, f.arrive_airport_oai_code;

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

---------------------------
-- Airline Traffic Facts --
---------------------------

select f.service_class_code -- , max(c.service_class_desc)
     , count(*) 
from air_oai_facts.airline_traffic_market f
-- left outer join oai.air_service_class c on f.service_class_code = c.service_class_cd
group by 1 order by count(*) desc;

/*
F	Scheduled Passenger/ Cargo Service F				11725
G	Scheduled All Cargo Service G						2766
L	Non-Scheduled Civilian Passenger/ Cargo Service L	2606
P	Non-Scheduled Civilian All Cargo Service P			1418
*/

-- drop table air_oai_dims.airline_service_classes
create table air_oai_dims.airline_service_classes as
select f.service_class_code
     , max(case when f.service_class_code in ('F','G') then 1 else 0 end::smallint) as scheduled_ind
     , max(case when f.service_class_code in ('L','P') then 1 else 0 end::smallint) as chartered_ind
     , max(case when f.service_class_code = 'F' then 'Scheduled Passenger / Cargo Service'
            when f.service_class_code = 'G' then 'Scheduled CAll Cargo Service'
            when f.service_class_code = 'L' then 'Non-Scheduled Civilian Passenger / Cargo Service'
            when f.service_class_code = 'P' then 'Non-Scheduled Civilian All Cargo Service'
            else null end::varchar(255)) as service_class_descr
     , 'gxclark'::char(32) as created_by
     , now()::timestamp(0) as created_tmst
     , null::char(32) as updated_by
     , null::timestamp(0) as updated_tmst
from air_oai_facts.airline_traffic_market f
group by 1 order by 1;

alter table air_oai_dims.airline_service_classes 
add constraint airline_service_classes_pk primary key (service_class_code);

select f.aircraft_configuration_ref -- , max(c.service_class_desc)
     , count(*) 
from air_oai_facts.airline_traffic_segment f
group by 1;

CMB
FRT
PAX
SEA

-- drop table air_oai_dims.aircraft_configurations
create table air_oai_dims.aircraft_configurations as
select f.aircraft_configuration_ref
     , max(case when f.aircraft_configuration_ref = 'CMB' then 'Combination Freight and Passenger, Main Deck'
            when f.aircraft_configuration_ref = 'FRT' then 'Freight Only, Main Deck'
            when f.aircraft_configuration_ref = 'PAX' then 'Passenger Only, Main Deck'
            when f.aircraft_configuration_ref = 'SEA' then 'Seaplane'
            else null end::varchar(255)) as aircraft_configuration_descr
     , 'gxclark'::char(32) as created_by
     , now()::timestamp(0) as created_tmst
     , null::char(32) as updated_by
     , null::timestamp(0) as updated_tmst
from air_oai_facts.airline_traffic_segment f
group by 1 order by 1;

alter table air_oai_dims.aircraft_configurations 
add constraint aircraft_configurations_pk primary key (aircraft_configuration_ref);

-------------------
-- Establish FKs --
-------------------

-- air_oai_facts.airline_traffic_segment
alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_service_fk 
foreign key (service_class_code) references air_oai_dims.airline_service_classes (service_class_code);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_aircraft_configuration_fk 
foreign key (aircraft_configuration_ref) references air_oai_dims.aircraft_configurations (aircraft_configuration_ref);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_aircraft_type_fk 
foreign key (aircraft_type_oai_nbr) references air_oai_dims.aircraft_types (aircraft_type_oai_nbr);

-- IDS:
alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

-- KEYS:
alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

alter table air_oai_facts.airline_traffic_segment add constraint airline_traffic_segment_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

-----

-- air_oai_facts.airline_traffic_market
alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_service_fk 
foreign key (service_class_code) references air_oai_dims.airline_service_classes (service_class_code);

-- IDS:
alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_airline_id_fk 
foreign key (airline_entity_id) references air_oai_dims.airline_entities (airline_entity_id);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_depart_airport_id_fk 
foreign key (depart_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_arrive_airport_id_fk 
foreign key (arrive_airport_history_id) references air_oai_dims.airport_history (airport_history_id);

-- KEYS:
alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_airline_key_fk 
foreign key (airline_entity_key) references air_oai_dims.airline_entities (airline_entity_key);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_depart_airport_key_fk 
foreign key (depart_airport_history_key) references air_oai_dims.airport_history (airport_history_key);

alter table air_oai_facts.airline_traffic_market add constraint airline_traffic_market_arrive_airport_key_fk 
foreign key (arrive_airport_history_key) references air_oai_dims.airport_history (airport_history_key);



