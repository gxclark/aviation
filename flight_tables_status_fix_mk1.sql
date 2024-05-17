
-- DROP VIEW if exists airlines_pg.airline_flights_scheduled_v;
CREATE VIEW airlines_pg.airline_flights_scheduled_v AS  
SELECT flight_key, flight_date, flight_nbr, flight_count, tail_nbr, flight_status
    , airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
    , depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
    , arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
    , distance_smi, distance_nmi, distance_kmt, distance_group_id, depart_time_block, arrive_time_block
    , report_depart_tmstz_lcl, (report_depart_tmstz_lcl)::date AS report_depart_date_lcl
    , report_depart_tmstz_utc, (report_depart_tmstz_utc)::date AS report_depart_date_utc
    , report_arrive_tmstz_lcl, (report_arrive_tmstz_lcl)::date AS report_arrive_date_lcl
    , report_arrive_tmstz_utc, (report_arrive_tmstz_utc)::date AS report_arrive_date_utc
    , report_elapsed_time_min
FROM air_oai_facts.airline_flights_scheduled;

alter table air_oai_facts.airline_flights_scheduled rename to airline_flights_scheduled_bak2;
alter table air_oai_facts.airline_flights_scheduled_new rename to airline_flights_scheduled;

-- Start 

create table aviation.air_oai_facts.airline_flights_scheduled_bak as
SELECT * FROM aviation.air_oai_facts.airline_flights_scheduled;

CREATE TABLE aviation.air_oai_facts.airline_flights_scheduled_new
    ( flight_key 					char(32) NOT NULL
    , flight_date 					date
    , airline_oai_code 				varchar(3)
    , airline_entity_from_date 		date
    , airline_entity_id 			smallint
    , airline_entity_key 			char(32)
    , flight_nbr 					char(4)
    , flight_count 					smallint
    , tail_nbr 						varchar(10)
    , depart_airport_oai_code 		char(3)
    , depart_airport_from_date 		date
    , depart_airport_history_id 	integer
    , depart_airport_history_key 	char(32)
    , arrive_airport_oai_code 		char(3)
    , arrive_airport_from_date 		date
    , arrive_airport_history_id 	integer
    , arrive_airport_history_key 	char(32)
    , distance_smi 					smallint
    , distance_nmi 					smallint
    , distance_kmt 					smallint
    , distance_group_id 			smallint
    , depart_time_block 			varchar(10)
    , arrive_time_block 			varchar(10)
    , report_depart_tmstz_lcl 		timestamp
    , report_depart_tmstz_utc 		timestamp
    , report_arrive_tmstz_lcl 		timestamp
    , report_arrive_tmstz_utc 		timestamp
    , report_elapsed_time_min 		float4
    , flight_status					varchar(25)
    , created_by 					varchar(32)
    , created_ts 					timestamp(0)
    , updated_by 					varchar(32)
    , updated_ts 					timestamp(0)
    , PRIMARY KEY 					(flight_key)
);

INSERT INTO aviation.air_oai_facts.airline_flights_scheduled_new
( flight_key, flight_date, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
, flight_nbr, flight_count, tail_nbr
, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
, distance_smi, distance_nmi, distance_kmt, distance_group_id, depart_time_block, arrive_time_block
, report_depart_tmstz_lcl, report_depart_tmstz_utc, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
, report_elapsed_time_min -- , flight_status
, created_by, created_ts, updated_by, updated_ts)
SELECT flight_key, flight_date, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
    , flight_nbr, flight_count, tail_nbr
    , depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
    , arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
    , distance_smi, distance_nmi, distance_kmt, distance_group_id
    , depart_time_block, arrive_time_block
    , report_depart_tmstz_lcl, report_depart_tmstz_utc, report_arrive_tmstz_lcl, report_arrive_tmstz_utc, report_elapsed_time_min
    , created_by, created_ts, updated_by, updated_ts
FROM aviation.air_oai_facts.airline_flights_scheduled
; -- select count(*) from air_oai_facts.airline_flights_scheduled; -- 7,142,354
  -- select count(*) from air_oai_facts.airline_flights_scheduled_new; -- 7,142,354
  
update air_oai_facts.airline_flights_scheduled_new
set updated_by = 'gxclark', updated_ts = now()
  , flight_status = a.flight_status
from (select flight_key, flight_status from air_oai_facts.airline_flights_cancelled) a
where air_oai_facts.airline_flights_scheduled_new.flight_key = a.flight_key; -- zero

update air_oai_facts.airline_flights_scheduled_new
set updated_by = 'gxclark', updated_ts = now()
  , flight_status = a.flight_status
from (select flight_key, flight_status from air_oai_facts.airline_flights_diverted) a
where air_oai_facts.airline_flights_scheduled_new.flight_key = a.flight_key; -- zero

update air_oai_facts.airline_flights_scheduled_new
set updated_by = 'gxclark', updated_ts = now()
  , flight_status = a.flight_status
from (select flight_key, flight_status from air_oai_facts.airline_flights_completed) a
where air_oai_facts.airline_flights_scheduled_new.flight_key = a.flight_key; -- 7,142,354

INSERT INTO aviation.air_oai_facts.airline_flights_scheduled_new
( flight_key, flight_date, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
, flight_nbr, flight_count, tail_nbr
, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
, distance_smi, distance_nmi, distance_kmt, distance_group_id, depart_time_block, arrive_time_block
, report_depart_tmstz_lcl, report_depart_tmstz_utc, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
, report_elapsed_time_min, flight_status
, created_by, created_ts, updated_by, updated_ts)
SELECT flight_key, flight_date, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
    , flight_nbr, flight_count, tail_nbr
    , depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
    , arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
    , distance_smi, distance_nmi, distance_kmt, distance_group_id
    , depart_time_block, arrive_time_block
    , report_depart_tmstz_lcl, report_depart_tmstz_utc, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
    , report_elapsed_time_min, flight_status
    , created_by, created_ts, updated_by, updated_ts
FROM aviation.air_oai_facts.airline_flights_cancelled; -- 153,861 --limit 100


INSERT INTO aviation.air_oai_facts.airline_flights_scheduled_new
( flight_key, flight_date, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
, flight_nbr, flight_count, tail_nbr
, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
, distance_smi, distance_nmi, distance_kmt, distance_group_id, depart_time_block, arrive_time_block
, report_depart_tmstz_lcl, report_depart_tmstz_utc, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
, report_elapsed_time_min, flight_status
, created_by, created_ts, updated_by, updated_ts)
SELECT flight_key, flight_date, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
    , flight_nbr, flight_count, tail_nbr
    , depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
    , arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
    , distance_smi, distance_nmi, distance_kmt, distance_group_id
    , depart_time_block, arrive_time_block
    , report_depart_tmstz_lcl, report_depart_tmstz_utc, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
    , report_elapsed_time_min, flight_status
    , created_by, created_ts, updated_by, updated_ts
FROM aviation.air_oai_facts.airline_flights_diverted; -- 17,791

--- 

alter table air_oai_facts.airline_flights_scheduled_new drop constraint airline_flights_scheduled_new_pkey;
alter table air_oai_facts.airline_flights_scheduled_bak2 drop constraint airline_flights_scheduled_pk;

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled_bak2
	ADD CONSTRAINT airline_flights_scheduled_bak2_pk
	PRIMARY KEY (flight_key);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_pk
	PRIMARY KEY (flight_key);

CREATE UNIQUE INDEX airline_flights_scheduled_nk
	ON air_oai_facts.airline_flights_scheduled (airline_oai_code, flight_nbr, flight_date, depart_airport_oai_code);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_depart_airport_history_id_fk
	FOREIGN KEY (depart_airport_history_id) 
	REFERENCES air_oai_dims.airport_history (airport_history_id);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_depart_airport_history_key_fk
	FOREIGN KEY (depart_airport_history_key) 
	REFERENCES air_oai_dims.airport_history (airport_history_key);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_arrive_airport_history_id_fk
	FOREIGN KEY (arrive_airport_history_id) 
	REFERENCES air_oai_dims.airport_history (airport_history_id);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_arrive_airport_history_key_fk
	FOREIGN KEY (arrive_airport_history_key) 
	REFERENCES air_oai_dims.airport_history (airport_history_key);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_airline_entity_id_fk
	FOREIGN KEY (airline_entity_id) 
	REFERENCES air_oai_dims.airline_entities (airline_entity_id);

ALTER TABLE aviation.air_oai_facts.airline_flights_scheduled
	ADD CONSTRAINT airline_flights_scheduled_airline_entity_key_fk
	FOREIGN KEY (airline_entity_key) 
	REFERENCES air_oai_dims.airline_entities (airline_entity_key);
