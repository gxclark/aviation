
create schema aviation;

-- drop view if exists aviation.airline_flights_scheduled_v:
create or replace view aviation.airline_flights_scheduled_v as
SELECT flight_key, flight_date
	, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
	, flight_nbr, flight_count, tail_nbr
	, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
	, distance_smi, distance_nmi, distance_kmt, distance_group_id
	, depart_time_block, arrive_time_block
	, report_depart_tmstz_lcl, report_depart_tmstz_utc
	, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
	, report_elapsed_time_min
FROM air_oai_facts.airline_flights_scheduled;

-- drop view if exists aviation.airline_flights_completed_v:
create or replace view aviation.airline_flights_completed_v as
SELECT flight_key, flight_date
	, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
	, flight_nbr, flight_count, tail_nbr
	, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
	, distance_smi, distance_nmi, distance_kmt, distance_group_id
	, depart_time_block, arrive_time_block
	, report_depart_tmstz_lcl, report_depart_tmstz_utc
	, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
	, report_elapsed_time_min, flight_status
	, actual_depart_tmstz_lcl, actual_depart_tmstz_utc, actual_arrive_tmstz_lcl, actual_arrive_tmstz_utc
	, actual_elapsed_time_min
	, wheels_off_tmstz_lcl, wheels_off_tmstz_utc, wheels_on_tmstz_lcl, wheels_on_tmstz_utc
	, airborne_time_min, taxi_out_min, taxi_in_min
	, first_gate_depart_tmstz_lcl, first_gate_depart_tmstz_utc
	, total_ground_time, longest_ground_time
	, airline_delay_min, weather_delay_min, nas_delay_min, security_delay_min, late_aircraft_delay_min
FROM air_oai_facts.airline_flights_completed;

-- drop view if exists aviation.airline_flights_cancelled_v:
create or replace view aviation.airline_flights_cancelled_v as
SELECT flight_key, flight_date
	, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
	, flight_nbr, flight_count, tail_nbr
	, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
	, distance_smi, distance_nmi, distance_kmt, distance_group_id
	, depart_time_block, arrive_time_block
	, report_depart_tmstz_lcl, report_depart_tmstz_utc
	, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
	, report_elapsed_time_min, flight_status
	, actual_depart_tmstz_lcl, actual_depart_tmstz_utc
	, wheels_off_tmstz_lcl, wheels_off_tmstz_utc, taxi_out_min
	, first_gate_depart_tmstz_lcl, first_gate_depart_tmstz_utc
	, total_ground_time, longest_ground_time
FROM air_oai_facts.airline_flights_cancelled;

-- drop view if exists aviation.airline_flights_diverted_v:
create or replace view aviation.airline_flights_diverted_v as
SELECT flight_key, flight_date
	, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
	, flight_nbr, flight_count, tail_nbr
	, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_oai_code, arrive_airport_from_date, arrive_airport_history_id, arrive_airport_history_key
	, distance_smi, distance_nmi, distance_kmt, distance_group_id
	, depart_time_block, arrive_time_block
	, report_depart_tmstz_lcl, report_depart_tmstz_utc
	, report_arrive_tmstz_lcl, report_arrive_tmstz_utc
	, report_elapsed_time_min, flight_status
	, actual_depart_tmstz_lcl, actual_depart_tmstz_utc
	, actual_arrive_tmstz_lcl, actual_arrive_tmstz_utc
	, actual_elapsed_time_min
	, wheels_off_tmstz_lcl, wheels_off_tmstz_utc
	, wheels_on_tmstz_lcl, wheels_on_tmstz_utc
	, airborne_time_min, taxi_out_min, taxi_in_min
	, first_gate_depart_tmstz_lcl, first_gate_depart_tmstz_utc
	, total_ground_time, longest_ground_time
FROM aviation.air_oai_facts.airline_flights_diverted;

-- drop view if exists aviation.airline_flights_diverted_legs_v:
create or replace view aviation.airline_flights_diverted_legs_v as
SELECT flight_key, diversion_nbr, flight_date
	, airline_oai_code, airline_entity_from_date, airline_entity_id, airline_entity_key
	, flight_nbr, flight_count, tail_nbr
	, depart_airport_oai_code, depart_airport_from_date, depart_airport_history_id, depart_airport_history_key
	, original_arrive_airport_oai_code, original_arrive_airport_from_date, original_arrive_airport_history_id, original_arrive_airport_history_key
	, diverted_airport_oai_code, diverted_airport_from_date, diverted_airport_history_id, diverted_airport_history_key
	, diverted_tail_nbr
	, diverted_wheels_on_tmstz_lcl, diverted_wheels_on_tmstz_utc
	, diverted_wheels_off_tmstz_lcl, diverted_wheels_off_tmstz_utc
	, diverted_total_ground_time_min, diverted_longest_ground_time_min
FROM aviation.air_oai_facts.airline_flights_diverted_legs;

-- drop view if exists aviation.airline_traffic_market_v:
create or replace view aviation.airline_traffic_market_v as
SELECT airline_traffic_market_key, year_month_nbr
	, airline_oai_code, airline_effective_date, airline_entity_id, airline_entity_key
	, depart_airport_oai_code, depart_airport_effective_date, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_oai_code, arrive_airport_effective_date, arrive_airport_history_id, arrive_airport_history_key
	, service_class_code, data_source_code
	, passengers_qty, freight_kgm, mail_kgm
	--, t100_records_qty
FROM air_oai_facts.airline_traffic_market;

-- drop view if exists aviation.airline_traffic_segment_v:
create or replace view aviation.airline_traffic_segment_v as
SELECT airline_traffic_segment_key, year_month_nbr, service_class_code
	, airline_oai_code, airline_effective_date, airline_entity_id, airline_entity_key
	, depart_airport_oai_code, depart_airport_effective_date, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_oai_code, arrive_airport_effective_date, arrive_airport_history_id, arrive_airport_history_key
	, aircraft_type_oai_nbr, aircraft_configuration_ref
	, data_source_code
	, scheduled_departures_qty, performed_departures_qty
	, available_seat_qty, passengers_qty, freight_kgm, mail_kgm
	, ramp_to_ramp_min, air_time_min
	--, t100_records_qty
FROM air_oai_facts.airline_traffic_segment;

-- drop view if exists aviation.airfare_survey_itinerary_v:
create or replace view aviation.airfare_survey_itinerary_v as
SELECT itinerary_oai_id, year_quarter_start_date
	, reporting_airline_entity_id, reporting_airline_entity_key
	, depart_airport_history_id, depart_airport_history_key
	, round_trip_fare_ind, online_purchase_ind, bulk_fare_ind, fare_credibility_ind
	, distance_group_oai_id, geographic_type_oai_id
	, coupon_qty, passenger_qty, distance_smi
	, flown_distance_smi, fare_per_person_usd, fare_per_mile_usd
FROM air_oai_facts.airfare_survey_itinerary;

-- drop view if exists aviation.airfare_survey_coupon_v:
create or replace view aviation.airfare_survey_coupon_v as
SELECT itinerary_oai_id, flight_pass_seq, year_quarter_start_date, market_oai_id
	, ticketing_airline_entity_id, ticketing_airline_entity_key
	, operating_airline_entity_id, operating_airline_entity_key
	, reporting_airline_entity_id, reporting_airline_entity_key
	, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_history_id, arrive_airport_history_key
	, trip_break_code, gateway_ind
	, distance_group_oai_id, airfare_class_code
	, itinerary_geographic_type_oai_id, coupon_geographic_type_oai_id
	, flight_pass_type, flight_pass_qty
	, passengers_qty, distance_smi
FROM air_oai_facts.airfare_survey_coupon;

-- drop view if exists aviation.airfare_survey_market_v:
create or replace view aviation.airfare_survey_market_v as
SELECT itinerary_oai_id, market_oai_id, year_quarter_start_date
	, ticketing_airline_entity_id, ticketing_airline_entity_key
	, ticketing_airline_change_ind, ticketing_airlines_group_code
	, operating_airline_entity_id, operating_airline_entity_key
	, operating_airline_change_ind, operating_airlines_group_code
	, reporting_airline_entity_id, reporting_airline_entity_key
	, depart_airport_history_id, depart_airport_history_key
	, arrive_airport_history_id, arrive_airport_history_key
	, airports_group_oai_code, world_areas_group_oai_code
	, itinerary_geograhic_type_oai_id, market_geograhic_type_oai_id, market_distance_group_oai_id
	, bulk_fare_ind, market_coupon_qty
	, passenger_qty, market_fare_amount_usd, market_distance_smi
	, market_flown_distance_smi, non_stop_distance_smi
FROM aviation.air_oai_facts.airfare_survey_market;

----------
-- DIMS --
----------

-- drop view if exists aviation.aircraft_configurations_v:
create or replace view aviation.aircraft_configurations_v as
SELECT aircraft_configuration_ref
	, aircraft_configuration_descr
FROM air_oai_dims.aircraft_configurations;

-- drop view if exists aviation.aircraft_types_v:
create or replace view aviation.aircraft_types_v as
SELECT aircraft_type_oai_nbr
	, aircraft_group_oai_nbr
	, aircraft_oai_type
	, manufacturer_name
	, aircraft_type_long_name
	, aircraft_type_brief_name
	, aircraft_type_from_date
	, aircraft_type_thru_date
FROM air_oai_dims.aircraft_types;

-- drop view if exists aviation.airline_entities_v:
create or replace view aviation.airline_entities_v as
SELECT airline_entity_id
	, airline_entity_key
	, airline_usdot_id
	, airline_oai_code
	, entity_oai_code
	, airline_name
	, airline_unique_oai_code
	, entity_unique_oai_code
	, airline_unique_name
	, world_area_oai_id
	, world_area_oai_seq_id
	, airline_old_group_nbr
	, airline_new_group_nbr
	, operating_region_code
	, source_from_date
	, source_thru_date
FROM aviation.air_oai_dims.airline_entities;

-- drop view if exists aviation.airline_service_classes_v:
create or replace view aviation.airline_service_classes_v as
SELECT service_class_code
	, scheduled_ind
	, chartered_ind
	, service_class_descr
FROM aviation.air_oai_dims.airline_service_classes;

-- drop view if exists aviation.airport_history_v:
create or replace view aviation.airport_history_v as
SELECT airport_history_id, airport_history_key, airport_oai_code, effective_from_date, effective_thru_date
	, airport_closed_ind, airport_latest_ind, airport_oai_seq_id, airport_oai_id, airport_display_name
	, city_full_display_name
	, airport_world_area_oai_seq_id, airport_world_area_oai_id, airport_world_area_key
	, utc_local_time_variation, time_zone_name
	, market_city_oai_seq_id, market_city_oai_id, market_city_full_display_name
	, market_city_world_area_oai_seq_id, market_city_world_area_oai_id, market_city_world_area_key
	, subdivision_iso_code, subdivision_fips_code, subdivision_name
	, country_iso_code, country_name
	, latitude_decimal_nbr, longitude_decimal_nbr
FROM aviation.air_oai_dims.airport_history;

-- drop view if exists aviation.world_areas_v:
create or replace view aviation.world_areas_v as
SELECT world_area_oai_seq_id, world_area_key
	, world_area_oai_id, effective_from_date, effective_thru_date, world_area_latest_ind
	, world_area_name, world_region_name
	, subdivision_iso_code, subdivision_fips_code, subdivision_name
	, country_iso_code, country_short_name, country_type_descr
	, sovereign_country_name, capital_city_name, world_area_comments_text
FROM aviation.air_oai_dims.world_areas;

-- drop view if exists aviation.aircraft_registry_v;
-- create or replace view aviation.aircraft_registry_v as
SELECT 'N' || ltrim(rtrim(n_nbr))::varchar(6) as tail_nbr
	 , ltrim(rtrim(serial_nbr))::varchar(30) as serial_nbr
	 , ltrim(rtrim(aircraft_reference_code))::varchar(7) as aircraft_reference_code
	 , ltrim(rtrim(engine_reference_code))::varchar(5) as engine_reference_code
	 , case when length(ltrim(rtrim(manufactured_year_nbr))) < 4 then null
	        else ltrim(rtrim(manufactured_year_nbr)) end::smallint as manufactured_year_nbr
	 , ltrim(rtrim(registrant_type_code))::char(1) as registrant_type_code
	 , ltrim(rtrim(registrant_name))::varchar(50) as registrant_name
	 , ltrim(rtrim(registrant_address_line1_text))::varchar(33) as registrant_address_line1_text
	 , ltrim(rtrim(registrant_address_line2_text))::varchar(33) as registrant_address_line2_text
	 , ltrim(rtrim(registrant_address_city_name))::varchar(18) as registrant_address_city_name
	 , ltrim(rtrim(registrant_address_state_code))::char(2) as registrant_address_state_code
	 , ltrim(rtrim(registrant_address_zip_code))::varchar(10) as registrant_address_zip_code
	 , ltrim(rtrim(registrant_region_code))::char(1) as registrant_region_code
	 , ltrim(rtrim(registrant_county_code))::varchar(3) as registrant_county_code
	 , ltrim(rtrim(registrant_country_code))::char(2) as registrant_country_code
	 , case when length(ltrim(rtrim(last_action_date))) < 8 then null
	        else ltrim(rtrim(last_action_date)) end::date as last_action_date
	 , case when length(ltrim(rtrim(certification_issue_date))) < 8 then null
	        else ltrim(rtrim(certification_issue_date)) end::date as certification_issue_date
	 , ltrim(rtrim(airworthiness_classification_code))::varchar(10) as airworthiness_classification_code
	 , ltrim(rtrim(aircraft_type_code))::char(1) as aircraft_type_code
	 , ltrim(rtrim(engine_type_code))::varchar(2) as engine_type_code
	 , ltrim(rtrim(registrant_status_code))::varchar(2) as registrant_status_code
	 , ltrim(rtrim(aircraft_transponder_code))::varchar(8) as aircraft_transponder_code
	 , ltrim(rtrim(fractional_ownership_code))::char(1) as fractional_ownership_code
	 , case when length(ltrim(rtrim(airworthiness_date))) < 8 then null
	        else ltrim(rtrim(airworthiness_date)) end::date as airworthiness_date
	 , ltrim(rtrim(owner1_name))::varchar(50) as owner1_name
	 , ltrim(rtrim(owner2_name))::varchar(50) as owner2_name
	 , ltrim(rtrim(owner3_name))::varchar(50) as owner3_name
	 , ltrim(rtrim(owner4_name))::varchar(50) as owner4_name
	 , ltrim(rtrim(owner5_name))::varchar(50) as owner5_name
	 , case when length(ltrim(rtrim(expiration_date))) < 8 then null
	        else ltrim(rtrim(expiration_date)) end::date as expiration_date
	 , ltrim(rtrim(unique_identification_nbr))::varchar(8) as unique_identification_nbr
	 , ltrim(rtrim(kit_manufacturer_name))::varchar(30) as kit_manufacturer_name
	 , ltrim(rtrim(kit_model_name))::varchar(20) as kit_model_name
	 , ltrim(rtrim(mode_s_hexidecimal_code))::varchar(10) as mode_s_hexidecimal_code
FROM air_faa_reg.aircraft_registry;

-- drop view if exists aviation.aircraft_reference_v;
-- create or replace view aviation.aircraft_reference_v as
SELECT ltrim(rtrim(aircraft_reference_code))::varchar(7) as aircraft_reference_code
	 , ltrim(rtrim(aircraft_manufacturer_name))::varchar(30) as aircraft_manufacturer_name
	 , ltrim(rtrim(aircraft_model_name))::varchar(20) as aircraft_model_name
	 , ltrim(rtrim(aircraft_type_code))::char(1) as aircraft_type_code
	 , ltrim(rtrim(engine_type_code))::varchar(2) as engine_type_code
	 , ltrim(rtrim(aircraft_category_code))::char(1) as aircraft_category_code
	 , ltrim(rtrim(builder_certification_code))::char(1) as builder_certification_code
	 , ltrim(rtrim(engines_count))::smallint as engines_count
	 , ltrim(rtrim(seats_count))::smallint as seats_count
	 , ltrim(rtrim(aircraft_weight_lbr))::varchar(7) as aircraft_weight_class
	 , ltrim(rtrim(aircraft_cruising_speed_mph))::integer as aircraft_cruising_speed_mph
	 , ltrim(rtrim(type_certificate_code))::varchar(15) as type_certificate_code
	 , ltrim(rtrim(type_certificate_holder_name))::varchar(50) as type_certificate_holder_name
FROM air_faa_reg.aircraft_reference;

--select ltrim(rtrim(aircraft_weight_lbr)) as aircraft_weight_lbr, count(*)
--from air_faa_reg.aircraft_reference group by 1 order by count(*) desc;

-- drop view if exists aviation.engine_reference_v;
-- create or replace view aviation.engine_reference_v as
SELECT ltrim(rtrim(engine_reference_code))::varchar(5) as engine_reference_code
	 , ltrim(rtrim(engine_manufacturer_name))::varchar(10) as engine_manufacturer_name
	 , ltrim(rtrim(engine_model_name))::varchar(13) as engine_model_name
	 , ltrim(rtrim(engine_type_code))::varchar(2) as engine_type_code
	 , ltrim(rtrim(engine_horsepower))::integer as engine_horsepower
	 , ltrim(rtrim(engine_thrust_lbs))::integer as engine_thrust_lbs
FROM air_faa_reg.engine_reference;

