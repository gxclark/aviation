
-- create schema air_faa_reg;

-- drop table air_faa_reg.aircraft_registry;
create table air_faa_reg.aircraft_registry
	( n_nbr									varchar(5)  not null -- N-NUMBER
	, serial_nbr							varchar(30) null -- SERIAL NUMBER
	, aircraft_reference_code				varchar(7)  null -- MFR MDL CODE
	, engine_reference_code					varchar(5)  null -- ENG MFR MDL
	, manufactured_year_nbr					char(4)		null -- YEAR MFR
	, registrant_type_code					char(1)     null -- TYPE REGISTRANT
	, registrant_name						varchar(50) null -- NAME
	, registrant_address_line1_text  		varchar(33) null -- STREET
	, registrant_address_line2_text  		varchar(33) null -- STREET2
	, registrant_address_city_name			varchar(18) null -- CITY
	, registrant_address_state_code			char(2)		null -- STATE
	, registrant_address_zip_code			varchar(10) null -- ZIP CODE
	, registrant_region_code				char(1)		null -- REGION
	, registrant_county_code				varchar(3)  null -- COUNTY
	, registrant_country_code				char(2)     null -- COUNTRY
	, last_action_date						char(8)		null -- LAST ACTION DATE
	, certification_issue_date				char(8)		null -- CERT ISSUE DATE
	, airworthiness_classification_code		varchar(10)	null -- CERTIFICATION
	, aircraft_type_code					char(1)		null -- TYPE AIRCRAFT
	, engine_type_code						varchar(2)	null -- TYPE ENGINE
	, registrant_status_code				varchar(2)  null -- STATUS CODE
	, aircraft_transponder_code				varchar(8)  null -- MODE S CODE
	, fractional_ownership_code				char(1)     null -- FRACT OWNER
	, airworthiness_date					char(8)     null -- AIR WORTH DATE
	, owner1_name							varchar(50) null -- OTHER NAMES(1)
	, owner2_name							varchar(50) null -- OTHER NAMES(2)
	, owner3_name							varchar(50) null -- OTHER NAMES(3)
	, owner4_name							varchar(50) null -- OTHER NAMES(4)
	, owner5_name							varchar(50) null -- OTHER NAMES(5)
	, expiration_date						char(8)		null -- EXPIRATION DATE
	, unique_identification_nbr				varchar(8)  null -- UNIQUE ID
	, kit_manufacturer_name					varchar(30) null -- KIT MFR
	, kit_model_name						varchar(20) null -- KIT MODEL
	, mode_s_hexidecimal_code				varchar(10) null -- MODE S CODE HEX
	, filler01								varchar(255) null
	, constraint aircraft_registry_pk primary key (n_nbr)
	) -- diststyle even
	;

copy air_faa_reg.aircraft_registry
from '/opt/_data/_air/_faa/_reg/MASTER.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5 removequotes blanksasnull emptyasnull --noload
;
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.aircraft_registry;


select a.airline_oai_code
     , max(a.airline_name) as airline_name
     , case when a.aircraft_manufacturer_name like 'AIRBUS%' then 'AIRBUS'
            when a.aircraft_manufacturer_name like 'BOEING%' then 'BOEING'
            when a.aircraft_manufacturer_name like 'BOMBARDIER%' then 'BOMBARDIER'
            when a.aircraft_manufacturer_name like 'C SERIES%' then 'C-SERIES'
            when a.aircraft_manufacturer_name like 'DIAMOND%' then 'DIAMOND'
            when a.aircraft_manufacturer_name like 'EMBRAER%' then 'EMBRAER'
            when a.aircraft_manufacturer_name like 'YABORA%' then 'YABORA'
            else '___' end::varchar(55) as ac_mfg_name
     , split_part(a.aircraft_model_name,'-',1) as aircraft_gen_model_name
     , min(case when a.mfg_year_nbr is not null and a.mfg_year_nbr != '    '
                then a.mfg_year_nbr else '0000'end::smallint) as min_mfg_year_nbr
     , max(case when a.mfg_year_nbr is not null and a.mfg_year_nbr != '    '
                then a.mfg_year_nbr else '0000'end::smallint) as max_mfg_year_nbr
     , count(distinct a.aircraft_model_name) as ac_model_count
     , count(distinct a.tail_nbr) as tail_count
     , count(distinct a.n_nbr) as n_nbr_count
     , sum(a.count_flight_date) as sum_flight_dates
     , sum(a.count_flight_nbr) as sum_flight_nbrs
     , (sum(a.count_flight_date)::float/count(distinct a.tail_nbr))::numeric(5,1) as days_per_tail
     , (sum(a.count_flight_nbr)::float/count(distinct a.tail_nbr))::numeric(5,1) as flights_per_tail
from (
SELECT f.airline_oai_code
     , max(ae.airline_name) as airline_name
     , f.tail_nbr
     , min(f.flight_date) as min_flight_date
     , max(f.flight_date) as max_flight_date
     , count(distinct f.flight_date) as count_flight_date
     , count(distinct f.flight_nbr) as count_flight_nbr
     --, min(f.flight_nbr) as min_flight_nbr
     --, max(f.flight_nbr) as max_flight_nbr
	 , max(r.n_nbr) as n_nbr
	 --, max(r.aircraft_reference_code) as aircraft_ref_code
	 , max(r.manufactured_year_nbr) as mfg_year_nbr
	 --, max(r.registrant_type_code) as registrant_type
	 --, max(r.registrant_name) as registrant_name
	 --, max(r.aircraft_type_code) as aircraft_type_code
	 , max(ar.aircraft_manufacturer_name) as aircraft_manufacturer_name
	 , max(ar.aircraft_model_name) as aircraft_model_name
FROM air_oai_facts.airline_flights_scheduled f
join air_oai_dims.airline_entities ae on f.airline_entity_id = ae.airline_entity_id
left outer join air_faa_reg.aircraft_registry r 
  on case when f.tail_nbr like 'N%' then substring(f.tail_nbr,2,99) else f.tail_nbr end = r.n_nbr
left outer join air_faa_reg.aircraft_reference ar
  on r.aircraft_reference_code = ar.aircraft_reference_code
group by f.airline_oai_code, f.tail_nbr
order by f.airline_oai_code, f.tail_nbr
) a group by a.airline_oai_code, 3, split_part(a.aircraft_model_name,'-',1) 
order by 13 desc -- a.airline_oai_code, 3, split_part(a.aircraft_model_name,'-',1) 
;


-- drop table air_faa_reg.aircraft_reference;
create table air_faa_reg.aircraft_reference
	( aircraft_reference_code				varchar(7)  not null -- CODE
	, aircraft_manufacturer_name			varchar(30) null -- MFR
	, aircraft_model_name					varchar(20) null -- MODEL
	, aircraft_type_code					char(1)     null -- TYPE-ACFT
	, engine_type_code						varchar(2)  null -- TYPE-ENG
	, aircraft_category_code				char(1)     null -- AC-CAT
	, builder_certification_code			char(1)     null -- BUILD-CERT-IND
	, engines_count							varchar(2)  null -- NO-ENG
	, seats_count							varchar(3)  null -- NO-SEATS
	, aircraft_weight_lbr					varchar(7)  null -- AC-WEIGHT
	, aircraft_cruising_speed_mph			varchar(4)  null -- SPEED
	, type_certificate_code					varchar(15)	null -- TC Data Sheet
	, type_certificate_holder_name			varchar(50)	null -- TC Data Holder
	, filler03								varchar(10)	null
	, constraint aircraft_reference_pk primary key (aircraft_reference_code)
	) -- diststyle even
	;

copy air_faa_reg.aircraft_reference
from '/opt/_data/_air/_faa/_reg/ACFTREF.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5  blanksasnull emptyasnull --noload
; -- removequotes
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.aircraft_reference;
-- select count(*) from air_faa_reg.aircraft_reference; -- 91028

-- drop table air_faa_reg.engine_reference;
create table air_faa_reg.engine_reference
	( engine_reference_code					varchar(5)	not null -- CODE
	, engine_manufacturer_name				varchar(10)	null	 -- MFR
	, engine_model_name						varchar(13)	null	 -- MODEL
	, engine_type_code						varchar(2)  null	 -- TYPE
	, engine_horsepower						varchar(5)  null	 -- HORSEPOWER
	, engine_thrust_lbs						varchar(6)  null	 -- THRUST
	, filler01								varchar(10) null
	, constraint engine_reference_pk primary key (engine_reference_code)
	) -- diststyle even
	;
	
copy air_faa_reg.engine_reference
from '/opt/_data/_air/_faa/_reg/ENGINE.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5 removequotes blanksasnull emptyasnull --noload
;
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.engine_reference;
-- select count(*) from air_faa_reg.engine_reference; -- 4645


-- drop table air_faa_reg.aircraft_document_index;
create table air_faa_reg.aircraft_document_index
	( collateral_type_cede					char(1)		not null -- TYPE-COLLATERAL
	, collateral_value_text					varchar(37)	null	 -- COLLATERAL
	, party_name							varchar(50) null	 -- PARTY
	, document_id							varchar(12) null	 -- DOC-ID
	, document_receipt_date					varchar(8)	null	 -- DRDATE
	, processing_date						varchar(8)	null	 -- PROCESSING-DATE
	, correction_date						varchar(8)	null	 -- CORR-DATE
	, correction_code						char(1)		null	 -- CORR-ID
	, serial_id								varchar(30)	null	 -- SERIAL-ID
	, filler01								varchar(10)	null
	) -- diststyle even
	;
	
copy air_faa_reg.aircraft_document_index
from '/opt/_data/_air/_faa/_reg/DOCINDEX.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5 removequotes blanksasnull emptyasnull --noload
;
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.aircraft_document_index;
-- select count(*) from air_faa_reg.aircraft_document_index; -- 24212

-- drop table air_faa_reg.aircraft_reserved_registry;
create table air_faa_reg.aircraft_reserved_registry
	( n_nbr									varchar(5)	not null -- N-NUMBER
	, reserving_party_name					varchar(50) null	 -- REGISTRANT
	, reserving_party_address_line1_text	varchar(33) null	 -- STREET
	, reserving_party_address_line2_text	varchar(33) null	 -- STREET2
	, reserving_party_address_city_name		varchar(18) null	 -- CITY
	, reserving_party_address_state_code	char(2)		null	 -- STATE
	, reserving_party_address_zip_code		varchar(10)	null	 -- ZIP CODE
	, reservation_date						char(8)		null	 -- RSV DATE
	, reservation_type_code					varchar(2)	null	 -- TR
	, expiration_notice_date				char(8)		null	 -- EXP DATE
	, changed_n_nbr							varchar(5)	null	 -- N-NUM-CHG
	, purge_date							char(8)		null	 -- PURGE DATE
	, filler01								varchar(10)	null
	) -- diststyle even
	;

copy air_faa_reg.aircraft_reserved_registry
from '/opt/_data/_air/_faa/_reg/RESERVED.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5 removequotes blanksasnull emptyasnull --noload
;
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.aircraft_reserved_registry;
-- select count(*) from air_faa_reg.aircraft_reserved_registry; -- 137409

-- drop table air_faa_reg.aircraft_dealer_applicant;
create table air_faa_reg.aircraft_dealer_applicant
	( certificate_nbr						varchar(7)	not null -- CERTIFICATE-NUMBER
	, ownership_type_code					char(1)		null	 -- OWNERSHIP
	, certificate_issue_date				char(8)		null	 -- CERTIFICATE-DATE
	, expiration_date						char(8)		null	 -- EXPIRATION-DATE
	, expiration_ind						char(1)		null	 -- EXPIRATION-FLAG
	, cumulative_certificate_issued_count	varchar(4)	null	 -- CERTIFICATE-ISSUE-COUNT
	, aircraft_dealer_applicant_name		varchar(50)	null	 -- NAME
	, applicant_address_line1_text			varchar(33)	null	 -- STREET
	, applicant_address_line2_text			varchar(33)	null	 -- STREET2
	, applicant_address_city_name			varchar(18) null	 -- CITY
	, applicant_address_state_code			char(2)		null	 -- STATE-ABBREV
	, applicant_address_zip_code			varchar(10)	null	 -- ZIP-CODE
	, applicant_other_names_count			varchar(2)	null	 -- OTHER-NAMES-COUNT
	, applicant_other01_name				varchar(50)	null	 -- OTHER-NAMES-1
	, applicant_other02_name				varchar(50)	null	 -- OTHER-NAMES-2
	, applicant_other03_name				varchar(50)	null	 -- OTHER-NAMES-3
	, applicant_other04_name				varchar(50)	null	 -- OTHER-NAMES-4
	, applicant_other05_name				varchar(50)	null	 -- OTHER-NAMES-5
	, applicant_other06_name				varchar(50)	null	 -- OTHER-NAMES-6
	, applicant_other07_name				varchar(50)	null	 -- OTHER-NAMES-7
	, applicant_other08_name				varchar(50)	null	 -- OTHER-NAMES-8
	, applicant_other09_name				varchar(50)	null	 -- OTHER-NAMES-9
	, applicant_other10_name				varchar(50)	null	 -- OTHER-NAMES-10
	, applicant_other11_name				varchar(50)	null	 -- OTHER-NAMES-11
	, applicant_other12_name				varchar(50)	null	 -- OTHER-NAMES-12
	, applicant_other13_name				varchar(50)	null	 -- OTHER-NAMES-13
	, applicant_other14_name				varchar(50)	null	 -- OTHER-NAMES-14
	, applicant_other15_name				varchar(50)	null	 -- OTHER-NAMES-15
	, applicant_other16_name				varchar(50)	null	 -- OTHER-NAMES-16
	, applicant_other17_name				varchar(50)	null	 -- OTHER-NAMES-17
	, applicant_other18_name				varchar(50)	null	 -- OTHER-NAMES-18
	, applicant_other19_name				varchar(50)	null	 -- OTHER-NAMES-19
	, applicant_other20_name				varchar(50)	null	 -- OTHER-NAMES-20
	, applicant_other21_name				varchar(50)	null	 -- OTHER-NAMES-21
	, applicant_other22_name				varchar(50)	null	 -- OTHER-NAMES-22
	, applicant_other23_name				varchar(50)	null	 -- OTHER-NAMES-23
	, applicant_other24_name				varchar(50)	null	 -- OTHER-NAMES-24
	, applicant_other25_name				varchar(50)	null	 -- OTHER-NAMES-25
	, filler01								varchar(19) null
	) -- diststyle even
	;

copy air_faa_reg.aircraft_dealer_applicant
from '/opt/_data/_air/_faa/_reg/DEALER.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5 removequotes blanksasnull emptyasnull --noload
;
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.aircraft_dealer_applicant;
-- select count(*) from air_faa_reg.aircraft_dealer_applicant; -- 11509

-- drop table air_faa_reg.aircraft_deregistered;
create table air_faa_reg.aircraft_deregistered
	( n_nbr										varchar(5)  not null -- N-NUMBER
	, serial_nbr								varchar(30) null -- SERIAL NUMBER
	, aircraft_reference_code					varchar(7)  null -- MFR MDL CODE
	, registry_status_code						varchar(2)	null -- STATUS-CODE
	, registrant_name							varchar(50) null -- NAME
	, registrant_mail_address_line1_text		varchar(33) null -- STREET-MAIL
	, registrant_mail_address_line2_text		varchar(33) null -- STREET2-MAIL
	, registrant_mail_address_city_name			varchar(18) null -- CITY-MAIL
	, registrant_mail_address_state_code		char(2)		null -- STATE-ABBREV-MAIL
	, registrant_mail_address_zip_code			varchar(10) null -- ZIP-CODE-MAIL
	, engine_reference_code						varchar(5)  null -- ENG-MFR-MDL
	, manufactured_year_nbr						char(4)		null -- YEAR-MFR
	, certification_codes						varchar(10) null -- CERTIFICATION
	, registrant_region_code					char(1)		null -- REGION
	, registrant_mail_address_county_code		varchar(3)  null -- COUNTY-MAIL
	, registrant_mail_address_country_code		char(2)		null -- COUNTRY-MAIL
	, airworthiness_date						char(8)		null -- AIR-WORTH-DATE
	, cancelation_date							char(8)		null -- CANCEL-DATE
	, aircraft_transponder_code					varchar(8)  null -- MODE-S-CODE
	, registration_type_code					char(1)		null -- INDICATOR-GROUP
	, export_country_name						varchar(18) null -- EXP-COUNTRY
	, last_active_date							char(8)		null -- LAST-ACT-DATE
	, certificate_issued_date					char(8)		null -- CERT-ISSUE-DATE
	, registrant_physical_address_line1_text	varchar(33) null -- STREET-PHYSICAL
	, registrant_physical_address_line2_text	varchar(33) null -- STREET2-PHYSICAL
	, registrant_physical_address_city_name		varchar(18) null -- CITY-PHYSICAL
	, registrant_physical_address_state_code	char(2)		null -- STATE-ABBREV-PHYSICAL
	, registrant_physical_address_zip_code		varchar(10) null -- ZIP-CODE-PHYSICAL
	, registrant_physical_address_county_code	varchar(3)  null -- COUNTY-PHYSICAL
	, registrant_physical_address_country_code	char(2)		null -- COUNTRY-PHYSICAL
	, registrant_other1_name					varchar(50) null -- OTHER-NAMES(1)
	, registrant_other2_name					varchar(50) null -- OTHER-NAMES(2)
	, registrant_other3_name					varchar(50) null -- OTHER-NAMES(3)
	, registrant_other4_name					varchar(50) null -- OTHER-NAMES(4)
	, registrant_other5_name					varchar(50) null -- OTHER-NAMES(5)
	, kit_maufacturer_name						varchar(30) null -- KIT MFR
	, kit_model_name							varchar(20) null -- KIT MODEL
	, mode_s_hexidecimal_code					varchar(10) null -- MODE S CODE HEX
	, filler01									varchar(10) null
	--, filler02									varchar(10) null
	) -- diststyle even
	;
	
copy air_faa_reg.aircraft_deregistered
from '/opt/_data/_air/_faa/_reg/DEREG.txt'
delimiter ',' header csv
-- ignoreheader 1 maxerror 5 removequotes blanksasnull emptyasnull --noload
;
-- select * from stl_load_errors order by starttime desc;
-- select * from air_faa_reg.aircraft_deregistered;
-- select count(*) from air_faa_reg.aircraft_deregistered; -- 372573

-- ERROR: extra data after last expected column Where: COPY aircraft_deregistered, line 194232: "54096,27-7405407 ,7102308,V ,RUSSELL INC ,P..." 
-- Query = copy air_faa_reg.aircraft_deregistered from '/opt/_data/_air/_faa/_reg/DEREG.txt' delimiter ',' header csv;

--select n_nbr, cancelation_dt, count(*) from stg.us_faa_aircraft_deregistered group by 1,2 order by count(*) desc;
--select * from stg.us_faa_aircraft_deregistered where n_nbr = '1237N';

------------------------------------------------
--- Small Reference Tables from the metadata ---
------------------------------------------------

-- drop table air_faa_reg.registrant_region_codes;
create table air_faa_reg.registrant_region_codes
	( registrant_region_code					char(1) not null
	, registrant_region_name					varchar(55) not null
	, created_by								varchar(32) not null default current_user
	, created_ts								timestamp(0) not null default current_timestamp		
	, constraint registrant_region_codes_pk primary key (registrant_region_code)
	) -- distkey(registrant_region_cd) sortkey(registrant_region_cd)
	;

insert into air_faa_reg.registrant_region_codes
(registrant_region_code, registrant_region_name)
values
  ('1','Eastern')
, ('2','Southwestern')
, ('3','Central')
, ('4','Western-Pacific')
, ('5','Alaskan')
, ('7','Southern')
, ('8','European')
, ('C','Great Lakes')
, ('E','New England')
, ('S','Northwest Mountain')
;

-- select * from air_faa_reg.registrant_region_codes;

-- drop table air_faa_reg.registrant_type_codes;
create table air_faa_reg.registrant_type_codes
	( registrant_type_code						char(1) not null
	, registrant_type_name						varchar(55) not null
	, created_by								varchar(32) not null default current_user
	, created_ts								timestamp(0) not null default current_timestamp	
	, constraint registrant_type_codes_pk primary key (registrant_type_code)
	) -- distkey(registrant_type_cd) sortkey(registrant_type_cd)
	;

insert into air_faa_reg.registrant_type_codes
(registrant_type_code, registrant_type_name)
values
  ('1','Individual')
, ('2','Partnership')
, ('3','Corporation')
, ('4','Co-Owned')
, ('5','Government')
, ('8','Non Citizen Corporation')
, ('9','Non Citizen Co-Owned')
;

-- select * from air_faa_reg.registrant_type_codes;

-- drop table air_faa_reg.aircraft_type_codes;
create table air_faa_reg.aircraft_type_codes
	( aircraft_type_code					char(1) not null
	, aircraft_type_name					varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp	
	, constraint aircraft_type_codes_pk primary key (aircraft_type_code)
	) -- distkey(aircraft_type_cd) sortkey(aircraft_type_cd)
	;

insert into air_faa_reg.aircraft_type_codes
(aircraft_type_code, aircraft_type_name)
values
  ('1','Glider')
, ('2','Balloon')
, ('3','Blimp/Dirigible')
, ('4','Fixed wing single engine')
, ('5','Fixed wing multi engine')
, ('6','Rotorcraft')
, ('7','Weight-shift-control')
, ('8','Powered Parachute')
, ('9','Gyroplane')
;

-- select * from air_faa_reg.aircraft_type_codes;

--drop table air_faa_reg.engine_type_codes;
create table air_faa_reg.engine_type_codes
	( engine_type_code						varchar(2) not null
	, engine_type_name						varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp	
	, constraint engine_type_code_pk primary key (engine_type_code)
	) -- distkey(engine_type_cd) sortkey(engine_type_cd)
	;

insert into air_faa_reg.engine_type_codes
(engine_type_code, engine_type_name)
values
  ('0','None')
, ('1','Reciprocating')
, ('2','Turbo-prop')
, ('3','Turbo-shaft')
, ('4','Turbo-jet')
, ('5','Turbo-fan')
, ('6','Ramjet')
, ('7','2 Cycle')
, ('8','4 Cycle')
, ('9','Unknown')
, ('10','Electric')
, ('11','Rotary')
;

-- select * from air_faa_reg.engine_type_codes;

-- drop table air_faa_reg.aircraft_category_codes;
create table air_faa_reg.aircraft_category_codes
	( aircraft_category_code				varchar(2) not null
	, aircraft_category_name				varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp	
	, constraint aircraft_category_codes_pk primary key (aircraft_category_code)
	) -- distkey(aircraft_category_cd) sortkey(aircraft_category_cd)
	;

insert into air_faa_reg.aircraft_category_codes
(aircraft_category_code, aircraft_category_name)
values
  ('1','Land')
, ('2','Sea')
, ('3','Amphibious')
;

-- select * from air_faa_reg.aircraft_category_codes;

-- drop table air_faa_reg.builder_certification_codes;
create table air_faa_reg.builder_certification_codes
	( builder_certification_code			varchar(2) not null
	, builder_certification_name			varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp	
	, constraint builder_certification_codes_pk primary key (builder_certification_code)
	) -- distkey(builder_certification_cd) sortkey(builder_certification_cd)
	;

insert into air_faa_reg.builder_certification_codes
(builder_certification_code, builder_certification_name)
values
  ('0','Type Certificated')
, ('1','Not Type Certificated')
, ('2','Light Sport')
;

-- select * from air_faa_reg.builder_certification_codes;

-- drop table air_faa_reg.aircraft_weight_codes;
create table air_faa_reg.aircraft_weight_codes
	( aircraft_weight_code					varchar(2) not null
	, lower_bound_weight_lbr				integer not null
	, upper_bound_weight_lbr				integer null
	, aircraft_weight_descr					varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp
	, constraint aircraft_weight_codes_pk primary key (aircraft_weight_code)
	) -- distkey(aircraft_weight_cd) sortkey(aircraft_weight_cd)
	;

insert into air_faa_reg.aircraft_weight_codes
(aircraft_weight_code, lower_bound_weight_lbr, upper_bound_weight_lbr, aircraft_weight_descr)
values
  ('1',0,12499,'Up to 12,499')
, ('2',12500,19999,'12,500 - 19,999')
, ('3',20000,9999999,'20,000 and over')
;

-- select * from air_faa_reg.aircraft_weight_codes;

-- drop table air_faa_reg.aircraft_document_collateral_codes;
create table air_faa_reg.aircraft_document_collateral_codes
	( aircraft_weight_code					varchar(2) not null
	, lower_bound_weight_lbr				integer not null
	, upper_bound_weight_lbr				integer null
	, aircraft_weight_descr					varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp
	, constraint aircraft_weight_code_pk primary key (aircraft_weight_code)
	) -- distkey(aircraft_weight_cd) sortkey(aircraft_weight_cd)
	;

insert into air_faa_reg.aircraft_document_collateral_codes
(aircraft_weight_code, lower_bound_weight_lbr, upper_bound_weight_lbr, aircraft_weight_descr)
values
  ('1',0,12499,'Up to 12,499')
, ('2',12500,19999,'12,500 - 19,999')
, ('3',20000,9999999,'20,000 and over')
;

-----------------------------
--- Still being developed ---
-----------------------------

--drop table air_faa_reg.airworthiness_classification_codes;
create table air_faa_reg.airworthiness_classification_codes
	( airworthiness_classification_code		char(1) not null
	, airworthiness_classification_descr	varchar(55) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp
	, constraint airworthiness_classification_codes_pk primary key (airworthiness_classification_code)
	) -- distkey(airworthiness_classification_cd) sortkey(airworthiness_classification_cd)
	;

insert into air_faa_reg.airworthiness_classification_codes
(airworthiness_classification_code, airworthiness_classification_descr)
values
  ('1','Standard')
, ('2','Limited')
, ('3','Restricted')
, ('4','Experimental')
, ('5','Provisional')
, ('6','Multiple')
, ('7','Primary')
, ('8','Special Flight Permit')
, ('9','Light Sport')
;

-- select * from air_faa_reg.airworthiness_classification_codes;

-- drop table air_faa_reg.approved_operations_codes;
create table air_faa_reg.approved_operations_codes
	( airworthiness_classification_code		char(1) not null
	, approved_operations_code				varchar(2) not null
	, approved_operations_descr				varchar(125) not null
	, created_by							varchar(32) not null default current_user
	, created_ts							timestamp(0) not null default current_timestamp
	, constraint approved_operations_codes_pk primary key (airworthiness_classification_code, approved_operations_code)
	) -- distkey(airworthiness_classification_cd) sortkey(airworthiness_classification_cd)
	;

insert into air_faa_reg.approved_operations_codes
(airworthiness_classification_code, approved_operations_code, approved_operations_descr)
values
  ('1','N','Normal')
, ('1','U','Utility')
, ('1','A','Acrobatic')
, ('1','T','Transport')
, ('1','G','Glider')
, ('1','B','Balloon')
, ('1','C','Commuter')
, ('1','O','Other')
, ('3','0','Other')
, ('3','1','Agriculture and Pest Control')
, ('3','2','Aerial Surveying')
, ('3','3','Aerial Advertising')
, ('3','4','Forest')
, ('3','5','Patrolling')
, ('3','6','Weather Control')
, ('3','7','Carriage of Cargo')
, ('4','0','To Show compliance with FAR')
, ('4','1','Research and Development')
, ('4','2','Amateur Built')
, ('4','3','Exhibition')
, ('4','4','Racing')
, ('4','5','Crew Training')
, ('4','6','Market Survey')
, ('4','7','Operating Kit Built Aircraft')
, ('4','8A','Reg. Prior to 01/31/08')
, ('4','8B','Operating Light-Sport Kit-Built')
, ('4','8C','Operating Light-Sport Previously issued cert under 21.190')
, ('4','9A','Unmanned Aircraft - Research and Development')
, ('4','9B','Unmanned Aircraft - Market Survey')
, ('4','9C','Unmanned Aircraft - Crew Training')
, ('5','1','Class I')
, ('5','2','Class II')
--, ('6','1','Standard')
, ('6','2','Limited')
, ('6','3','Restricted')
, ('6','0','Other')
--, ('6','1','Agriculture and Pest Control')
, ('6','2','Aerial Surveying')
, ('6','3','Aerial Advertising')
, ('6','4','Forest')
, ('6','5','Patrolling')
, ('6','6','Weather Control')
, ('6','7','Carriage of Cargo')
;
