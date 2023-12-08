
select * from citus_tables;
select * from citus_shards;

select table_name
     , max(citus_table_type) as citus_table_type
     , max(colocation_id) as colocation_id
     , min(shardid) as min_shardid
     , max(shardid) as max_shardid
     , count(distinct shardid) as shard_qty
     , min(nodeport) as min_node_port
     , max(nodeport) as max_node_port
     --, sum(shard_size) as sum_shard_size_bytes
     , sum(shard_size/((1000^2)::float))::numeric(5,2) as sum_shard_size_mb
from citus_shards
group by 1 order by 1;

select * from pg_dist_partition;

----
alter schema air_oai_parts rename to air_oai_facts;
alter schema air_oai rename to air_oai_dims;

alter table air_oai.airfare_survey_coupon set schema air_oai_facts;
alter table air_oai.airfare_survey_itinerary set schema air_oai_facts;
alter table air_oai.airfare_survey_market set schema air_oai_facts;

drop foreign table if exists air_oai.master_cord_fdw;
drop foreign table if exists air_oai.airfare_survey_ticket_fdw;

--- vacuum analyze air_oai_facts.airfare_survey_itinerary; 600+ secs
vacuum verbose air_oai_facts.airfare_survey_market;

vacuum verbose air_oai_facts.airfare_survey_market_2023q1;
analyze air_oai_facts.airfare_survey_market_2023q1;
analyze air_oai_facts.airfare_survey_market_2022q4;
analyze air_oai_facts.airfare_survey_market_2022q3;
analyze air_oai_facts.airfare_survey_market_2022q2;
analyze air_oai_facts.airfare_survey_market_2022q1;
analyze air_oai_facts.airfare_survey_market;

SELECT * from master_get_table_metadata('air_oai_facts.airfare_survey_market_2023q1');
SELECT pg_size_pretty(citus_relation_size('air_oai_facts.airfare_survey_market_2023q1'));
SELECT pg_size_pretty(citus_table_size('air_oai_facts.airfare_survey_market_2023q1'));
SELECT pg_size_pretty(citus_table_size('air_oai_facts.airfare_survey_coupon_2023q1'));
SELECT pg_size_pretty(citus_table_size('air_oai_facts.airfare_survey_itinerary_2023q1')); -- 75MB

SELECT year_quarter_start_date, count(*) AS count FROM air_oai_facts.airfare_survey_market_103100 airfare_survey_market WHERE true GROUP BY year_quarter_start_date;

SELECT pg_size_pretty(citus_total_relation_size('air_oai_facts.airfare_survey_coupon_2023q1'));

select year_quarter_start_date, count(*) from air_oai_facts.airfare_survey_market group by 1 order by 1 desc;

explain verbose
select year_quarter_start_date, count(*) from air_oai_facts.airfare_survey_market group by 1 order by 1 desc;

-----
explain verbose
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
JOIN air_oai_dims.airline_entities ae on asi.reporting_airline_entity_id = ae.airline_entity_id
JOIN air_oai_dims.airline_entities aeo on afsc.operating_airline_entity_id = aeo.airline_entity_id
where asi.reporting_airline_entity_id != afsc.operating_airline_entity_id
group by asi.year_quarter_start_date, asi.reporting_airline_entity_id, afsc.operating_airline_entity_id
order by 1 desc, sum(asi.passenger_qty) desc;