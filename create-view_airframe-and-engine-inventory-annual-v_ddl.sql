
CREATE OR REPALCE VIEW aviation.airlines_pg.airframe_and_engine_inventory_annual_v AS  
SELECT inventory_key
     , airline_entity_id, airline_entity_key, airline_oai_code
     , year_nbr, tail_nbr, serial_nbr
     , manufacturer_name, model_ref
     , aircraft_oai_type, aircraft_icao_type, aircraft_iata_type
     , manufacture_year_nbr
     , acquisition_date
     , aircraft_status_code
     , operating_status_ind
     , seats_qty
     , capacity_lbr
FROM air_oai_fin.airframe_and_engine_inventory_annual;

