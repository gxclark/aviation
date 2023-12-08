USE ROLE SYSADMIN;
use database air_oai;
create schema air_oai_dims;

use schema air_oai.air_oai_dims;

create OR REPLACE table air_oai_dims.aircraft_types_fdw
	( aircraft_type_oai_nbr			smallint	not null
	, aircraft_group_oai_nbr		smallint	not null
	, aircraft_oai_type				varchar(55)	not null
	, manufacturer_name				varchar(55)	
	, aircraft_type_long_name		varchar(55)	not null
	, aircraft_type_brief_name		varchar(55)	not null
	, aircraft_type_from_date		date		not null
	, aircraft_type_thru_date		date
	--, filler01_txt					varchar(10)
	)
/*server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_dims/T_AIRCRAFT_TYPES.csv'
	-- '/Volumes/24tbRaid5/opt/_data/_air/_oai/_dims/16922659_T_AIRCRAFT_TYPES.csv'
	, delimiter ','
	, null ''
	)*/
    ;
	
--select aircraft_type_oai_nbr, count(*) from air_oai_dims.aircraft_types_fdw group by 1 having count(*) > 1 order by count(*) desc; -- unique!

SELECT * FROM air_oai_dims.aircraft_types_fdw;

--drop table if exists air_oai_dims.aircraft_types;
create table air_oai_dims.aircraft_types
	( aircraft_type_oai_nbr			smallint	not null
	, aircraft_group_oai_nbr		smallint	not null
	, aircraft_oai_type				varchar(55)	not null
	, manufacturer_name				varchar(55)	not null
	, aircraft_type_long_name		varchar(55)	not null
	, aircraft_type_brief_name		varchar(55)	not null
	, aircraft_type_from_date		date		not null
	, aircraft_type_thru_date		date
	, created_by					varchar(32) not null default current_user
	, created_tmst					timestamp not null default current_timestamp
	, updated_by					varchar(32)
	, updated_tsmt					timestamp(0)
	, constraint aircraft_types_pk primary key (aircraft_type_oai_nbr)
    
	);

/*
comment on column air_oai_dims.aircraft_types.aircraft_type_oai_nbr is 'AC_TYPEID = Aircraft Type Identification Number. This Number Is Related To The Aircraft Group Number And Falls Within The Range Of A Group Number.';
comment on column air_oai_dims.aircraft_types.aircraft_group_oai_nbr is 'AC_GROUP = Aircraft Type Group - This Number Gives The Group Or Classification Of Aircraft Engine And Type Of Aircraft.';
comment on column air_oai_dims.aircraft_types.aircraft_oai_type is 'SSD_NAME = Aircraft Name.';
comment on column air_oai_dims.aircraft_types.manufacturer_name is 'MANUFACTURER = Manufacturing Company Name.';
comment on column air_oai_dims.aircraft_types.aircraft_type_long_name is 'LONG_NAME = Complete Name Of The Aircraft.';
comment on column air_oai_dims.aircraft_types.aircraft_type_brief_name is 'SHORT_NAME = Abbreviated Name Of The Aircraft.';
comment on column air_oai_dims.aircraft_types.aircraft_type_from_date is 'BEGIN_DATE = The Date When The Aircraft Was Added To The Database.';
comment on column air_oai_dims.aircraft_types.aircraft_type_thru_date is 'END_DATE = The Date Through Which Aircraft Type Remains In Effect.';
*/

insert into air_oai_dims.aircraft_types
( aircraft_type_oai_nbr, aircraft_group_oai_nbr, aircraft_oai_type, manufacturer_name
, aircraft_type_long_name, aircraft_type_brief_name, aircraft_type_from_date, aircraft_type_thru_date
, created_by, created_tmst)
select f.aircraft_type_oai_nbr
	 , f.aircraft_group_oai_nbr
	 , f.aircraft_oai_type
	 , case when f.manufacturer_name is null then 'GENERIC' else f.manufacturer_name end as manufacturer_name
	 , f.aircraft_type_long_name
	 , f.aircraft_type_brief_name
	 , f.aircraft_type_from_date
	 , f.aircraft_type_thru_date
	 , 'amadia' as created_by
	 ,  current_timestamp as created_tmst
from air_oai_dims.aircraft_types_fdw f
left outer join air_oai_dims.aircraft_types t on f.aircraft_type_oai_nbr = t.aircraft_type_oai_nbr
where t.aircraft_type_oai_nbr is null
--and f.manufacturer_name is null
;

 select * from air_oai_dims.aircraft_types; -- 433
-- select * from air_oai_dims.aircraft_types_fdw; -- 433
-- drop foreign table if exists air_oai_dims.aircraft_types_fdw;




-----------------
-- World Areas --
-----------------

-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_dims/T_WAC_COUNTRY_STATE.csv'

-- DROP FOREIGN TABLE air_oai_dims.wac_country_state_fdw;
--TRUNCATE TABLE air_oai_dims.wac_country_state_fdw;
CREATE  TABLE air_oai_dims.wac_country_state_fdw
	( world_area_oai_id					integer
	, world_area_oai_seq_id				integer
	, world_area_name					varchar(125)
	, world_region_name					varchar(125)
	, country_short_name				varchar(75)
	, country_type_descr				varchar(75)
	, capital_city_name					varchar(75)
	, sovereign_country_name			varchar(75)
	, country_iso_code					char(2)
	, subdivision_iso_code				varchar(10)
	, subdivision_name					varchar(75)
	, subdivision_fips_code				varchar(10)
	, effective_from_date				date
	, effective_thru_date				date
	, comments_text						varchar(555)
	, world_area_latest_ind				smallint
	--, filler01_txt						varchar(10)
	) ;
/*server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_dims/T_WAC_COUNTRY_STATE.csv'
	-- '/Volumes/24tbRaid5/opt/_data/_air/_oai/_dims/16922659_T_WAC_COUNTRY_STATE.csv'
	, delimiter ','
	, null ''
	); */

-- select * from air_oai_dims.wac_country_state_fdw limit 100;

select world_area_oai_id, effective_from_date, count(*) 
from air_oai_dims.wac_country_state_fdw group by 1,2 having count(*) > 1 order by count(*) desc;

select world_area_oai_seq_id, count(*) 
from air_oai_dims.wac_country_state_fdw group by 1 having count(*) > 1 order by count(*) desc;

select world_area_name, effective_from_date, count(*) 
from air_oai_dims.wac_country_state_fdw group by 1,2 having count(*) > 1 order by count(*) desc;

-- SELECT * FROM air_oai_dims.airline_entities limit 100;
-- select sovereign_country_name, count(*) from air_oai_dims.wac_country_state_fdw group by 1 order by count(*) desc; 

-- select * from air_oai_dims.wac_country_state_fdw;
-- drop table if exists air_oai_dims.world_areas;
create table air_oai_dims.world_areas
	( world_area_oai_seq_id				integer			not null
	, world_area_key					char(32)		not null
	, world_area_oai_id					smallint		not null
	, effective_from_date				date			not null
	, effective_thru_date				date
	, world_area_latest_ind				smallint
	, world_area_name					varchar(125)
	, world_region_name					varchar(125)
	, subdivision_iso_code				varchar(10)
	, subdivision_fips_code				varchar(10)
	, subdivision_name					varchar(75)
	, country_iso_code					char(2)
	, country_short_name				varchar(75)
	, country_type_descr				varchar(75)
	, sovereign_country_name			varchar(75)
	, capital_city_name					varchar(75)
	, world_area_comments_text			varchar(555)
	, created_by						varchar(32) not null default current_user
	, created_tmst						timestamp 	not null default current_timestamp
	, updated_by						varchar(32)
	, updated_tsmt						timestamp(0)
	, constraint world_areas_pk primary key (world_area_oai_seq_id)
	, constraint world_areas_ak unique (world_area_key)
	, constraint world_areas_nk unique (world_area_oai_id, effective_from_date)
    , world_areas_pk                    varchar
    , world_areas_ak                     varchar
    ,world_areas_nk                    varchar
	);

/*
comment on column air_oai_dims.world_areas.world_area_key is 'MD5-hashed unique key of [world_area_oai_id & ~ & effective_from_date]';
comment on column air_oai_dims.world_areas.world_area_oai_id is 'WAC = World Area Code.';
comment on column air_oai_dims.world_areas.world_area_oai_seq_id is 'WAC_SEQ_ID2 = Unique Identifier for a World Area Code (WAC) at a given point of time.  WAC attributes may change over time.  For example the country name associated with the WAC can change, but the WAC code stays the same.';
comment on column air_oai_dims.world_areas.world_area_name is 'WAC_NAME = World Area Code Name.';
comment on column air_oai_dims.world_areas.world_region_name is 'WORLD_AREA_NAME = Geographic Region of World Area Code.';
comment on column air_oai_dims.world_areas.country_short_name is 'COUNTRY_SHORT_NAME = Country Name.';
comment on column air_oai_dims.world_areas.country_type_descr is 'COUNTRY_TYPE = Country Type.';
comment on column air_oai_dims.world_areas.capital_city_name is 'CAPITAL = Capital.';
comment on column air_oai_dims.world_areas.sovereign_country_name is 'SOVEREIGNTY = Sovereignty.';
comment on column air_oai_dims.world_areas.country_iso_code is 'COUNTRY_CODE_ISO = Two-Character ISO Country Code.';
comment on column air_oai_dims.world_areas.subdivision_iso_code is 'STATE_CODE = State Abbreviation.';
comment on column air_oai_dims.world_areas.subdivision_name is 'STATE_NAME = State Name.';
comment on column air_oai_dims.world_areas.subdivision_fips_code is 'STATE_FIPS = FIPS (Federal Information Processing Standard) State Code.';
comment on column air_oai_dims.world_areas.effective_from_date is 'START_DATE = Start Date of World Area Code Attributes.';
comment on column air_oai_dims.world_areas.effective_thru_date is 'THRU_DATE = End Date of World Area Code Attributes (Active = NULL).';
comment on column air_oai_dims.world_areas.world_area_comments_text is 'COMMENTS = Comments.';
comment on column air_oai_dims.world_areas.world_area_latest_ind is 'IS_LATEST = Indicates if this row contains the latest attributes for the World Area Code (1 = Yes).';
*/

INSERT INTO air_oai_dims.world_areas
	( world_area_oai_seq_id
	, world_area_key
	, world_area_oai_id
	, effective_from_date, effective_thru_date, world_area_latest_ind
	, world_area_name, world_region_name
	, subdivision_iso_code, subdivision_fips_code, subdivision_name
	, country_iso_code, country_short_name, country_type_descr, sovereign_country_name, capital_city_name
	, world_area_comments_text
	, created_by, created_tmst)
SELECT world_area_oai_seq_id
     , md5(world_area_oai_id ||'~'||(effective_from_date)) as world_area_key
     , world_area_oai_id
	 , effective_from_date
	 , effective_thru_date
	 , world_area_latest_ind
	 , world_area_name
	 , world_region_name
	 , subdivision_iso_code
	 , subdivision_fips_code
	 , subdivision_name
	 , country_iso_code
	 , country_short_name
	 , country_type_descr
	 , sovereign_country_name
	 , capital_city_name
     , comments_text
	 , current_user
	 , current_timestamp
FROM air_oai_dims.wac_country_state_fdw;

-- select count(*) from air_oai_dims.wac_country_state_fdw; -- 344
-- select count(*) from air_oai_dims.world_areas; -- 344
-- drop foreign table air_oai_dims.wac_country_state_fdw;

select * from air_oai_dims.world_areas;

--------------------
-- Carrier Decode --
--------------------

TRUNCATE TABLE air_oai_dims.carrier_decode_fdw;
--drop foreign table if exists air_oai_dims.carrier_decode_fdw;
CREATE TABLE air_oai_dims.carrier_decode_fdw
	( airline_usdot_id				smallint
	, airline_oai_code				varchar(10)
	, entity_oai_code				varchar(10)
	, airline_name					varchar(125)
	, airline_unique_oai_code		varchar(10)
	, entity_unique_oai_code		varchar(10)
	, airline_unique_name			varchar(125)
	, world_area_oai_id			smallint
	, airline_old_group_nbr			smallint
	, airline_new_group_nbr			smallint		
	, operating_region_code			varchar(25)			
	, source_from_date				date
	, source_thru_date				date
	--, filler01_txt					varchar(10)
	) ;
/*server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_dims/T_CARRIER_DECODE.csv'
	-- '/Volumes/24tbRaid5/opt/_data/_air/_oai/_dims/16922659_T_CARRIER_DECODE.csv'
	, delimiter ','
	, null ''
	);
    */

copy into AIR_OAI_DIMS.CARRIER_DECODE_FDW  FROM @csv_stage/T_CARRIER_DECODE.csv 
ON_ERROR=CONTINUE
FORCE = TRUE
;
    
select * from air_oai_dims.carrier_decode_fdw;

select airline_usdot_id, count(*) from air_oai_dims.carrier_decode_fdw group by 1 having count(*) > 1 order by count(*) desc;
select airline_oai_code, count(*) from air_oai_dims.carrier_decode_fdw group by 1 having count(*) > 1 order by count(*) desc;
select airline_oai_code, entity_oai_code, count(*) from air_oai_dims.carrier_decode_fdw group by 1,2 having count(*) > 1 order by count(*) desc;

select airline_usdot_id, airline_unique_oai_code, entity_unique_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw group by 1,2,3,4 having count(*) > 1 order by count(*) desc;

select airline_usdot_id, airline_oai_code, entity_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw group by 1,2,3,4 having count(*) > 1 order by count(*) desc;

select airline_usdot_id, airline_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw group by 1,2,3 having count(*) > 1 order by count(*) desc;

select airline_oai_code, entity_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw group by 1,2,3 having count(*) > 1 order by count(*) desc;

select airline_usdot_id, entity_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw group by 1,2,3 having count(*) > 1 order by count(*) desc;

select min(airline_usdot_id) as min_id, max(airline_usdot_id) as max_id, count(*) from air_oai_dims.carrier_decode_fdw;

/*
CREATE TABLE color 
( color_id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10)
, color_name VARCHAR NOT NULL);
*/ 

-- WHERE octet_length(col) > length(col);  -- any non-ASCII letter?
-- WHERE col ~ '\W';                       -- anything but digits & letters? 

select airline_oai_code, entity_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw group by 1,2,3 having count(*) > 1 order by count(*) desc;

select count(*) from air_oai_dims.carrier_decode_fdw 
--where airline_usdot_id is null
--where carrier_oai_code is null
--where entity_oai_code is null
--where carrier_name is null
--where unique_carrier_oai_code is null
--where unique_entity_oai_code is null
--where unique_carrier_name is null
--where world_area_oai_code is null
--where carrier_old_group_nbr is null
--where carrier_new_group_nbr is null
--where operating_region_code	is null	
--where source_from_date  is null
where source_thru_date  is null -- yes, many
;
select airline_oai_code, entity_oai_code, source_from_date, count(*) 
from air_oai_dims.carrier_decode_fdw 
--where ( octet_length(carrier_oai_code) > length(carrier_oai_code) or carrier_oai_code ~ '\W'
--or      octet_length(entity_oai_code) > length(entity_oai_code) or entity_oai_code ~ '\W') 
group by 1,2,3 having count(*) > 1 order by count(*) desc;
-- 3KQ	01267	2021-04-01	2

-- select ctid, * from air_oai_dims.carrier_decode_fdw where airline_oai_code = '3KQ';
-- delete 

-- select * from air_oai_dims.airline_entities;
-- drop table if exists air_oai_dims.airline_entities;

CREATE OR REPLACE SEQUENCE airline_entity_id
    START = 4000
   INCREMENT = 1
   ;

drop table air_oai_dims.airline_entities ;
create table air_oai_dims.airline_entities
	( airline_entity_id				int 	not null default airline_entity_id.nextval
	, airline_entity_key			VARCHAR 	not null -- md5 hash of natural key <'airline_oai_code'|'entity_oai_code'|'source_from_date'>
	, airline_usdot_id				smallint	not null -- airline_usdot_id
	, airline_oai_code				varchar(10)	not null -- carrier_oai_code
	, entity_oai_code				varchar(10) not null -- carrier_entity_code
	, airline_name					varchar(125) not null -- carrier_nm
	, airline_unique_oai_code		varchar(10)	not null -- unique_carrier_code
	, entity_unique_oai_code		varchar(10)	not null -- unique_carrier_entity_code
	, airline_unique_name			varchar(125) not null -- unique_carrier_name
	, world_area_oai_id				smallint	not null -- airline_world_area_oai_code
	, world_area_oai_seq_id			integer		null	 -- this must be the actual FK, since the ID is not unique
	, airline_old_group_nbr			smallint	not null -- carrier_old_group_nbr
	, airline_new_group_nbr			smallint	not null -- carrier_new_group_nbr
	, operating_region_code			varchar(25)	not null -- operating_region_code
	, source_from_date				date		not null -- source_from_date
	, source_thru_date				date			     -- source_thru_date
	, created_by					varchar(32) not null default current_user
	, created_tmst					timestamp not null default current_timestamp
	, updated_by					varchar(32)
	, updated_tsmt					timestamp(0)
	, constraint airline_entities_pk primary key (airline_entity_id)
	, constraint airline_entities_ak unique (airline_entity_key)
	, constraint airline_entities_nk unique (airline_oai_code, entity_oai_code, source_from_date)
    
	);

-- ALTER [ COLUMN ] column_name DROP IDENTITY [ IF EXISTS ]
/*alter table air_oai_dims.airline_entities alter column airline_entity_id drop identity;

comment on column air_oai_dims.airline_entities.airline_entity_id is 'PostgreSQL defined identity surrogate key for high performance joins. Start with 4000.';
comment on column air_oai_dims.airline_entities.airline_entity_key is 'md5 hash of natural key <carrier_oai_code|entity_oai_code|source_from_date>.';
comment on column air_oai_dims.airline_entities.airline_usdot_id is 'AIRLINE_ID = An identification number assigned by US DOT to identify a unique airline (carrier). A unique airline (carrier) is defined as one holding and reporting under the same DOT certificate regardless of its Code, Name, or holding company/corporation.';
comment on column air_oai_dims.airline_entities.airline_oai_code is 'CARRIER = Code assigned by IATA and commonly used to identify a carrier. As the same code may have been assigned to different carriers over time, the code is not always unique.';
comment on column air_oai_dims.airline_entities.entity_oai_code is 'CARRIER_ENTITY = Carrier Entity.';
comment on column air_oai_dims.airline_entities.airline_name is 'CARRIER_NAME = Carrier Name.';
comment on column air_oai_dims.airline_entities.airline_unique_oai_code is 'UNIQUE_CARRIER = Unique Carrier Code. When the same code has been used by multiple carriers, a numeric suffix is used for earlier users, for example, PA, PA(1), PA(2). Use this field for analysis across a range of years.';
comment on column air_oai_dims.airline_entities.entity_unique_oai_code is 'UNIQUE_CARRIER_ENTITY = Unique Entity for a Carrier''s Operation Region.';
comment on column air_oai_dims.airline_entities.airline_unique_name is 'UNIQUE_CARRIER_NAME = Unique Carrier Name. When the same name has been used by multiple carriers, a numeric suffix is used for earlier users, for example, Air Caribbean, Air Caribbean (1).';
comment on column air_oai_dims.airline_entities.world_area_oai_id is 'WAC = World Area Code, this is a non-unique ID that represents a WAC.';
comment on column air_oai_dims.airline_entities.world_area_oai_seq_id is 'WAC = World Area Code, this is actual FK, since it is unique over time.';
comment on column air_oai_dims.airline_entities.airline_old_group_nbr is 'CARRIER_GROUP = Carrier Group Code.  Used in Legacy Analysis.';
comment on column air_oai_dims.airline_entities.airline_new_group_nbr is 'CARRIER_GROUP_NEW = Carrier Group New.';
comment on column air_oai_dims.airline_entities.operating_region_code is 'REGION = Carrier''s Operation Region. Carriers Report Data by Operation Region.';
comment on column air_oai_dims.airline_entities.source_from_date is 'START_DATE_SOURCE = Starting Date of Carrier Code.';
comment on column air_oai_dims.airline_entities.source_thru_date is 'THRU_DATE_SOURCE = Ending Date of Carrier Code (Active = NULL).';

*/

insert into air_oai_dims.airline_entities
( airline_entity_key, airline_usdot_id, airline_oai_code, entity_oai_code, airline_name
, airline_unique_oai_code, entity_unique_oai_code, airline_unique_name
, world_area_oai_id, airline_old_group_nbr, airline_new_group_nbr, operating_region_code, source_from_date, source_thru_date
, created_by, created_tmst)
select md5(f.airline_oai_code||'~'||f.entity_oai_code||'~'||f.source_from_date::char(10)) as airline_entity_key
    , f.airline_usdot_id
	, f.airline_oai_code
	, f.entity_oai_code
	, f.airline_name
	, f.airline_unique_oai_code
	, f.entity_unique_oai_code
	, f.airline_unique_name
	, f.world_area_oai_id
	, f.airline_old_group_nbr
	, f.airline_new_group_nbr
	, f.operating_region_code
	, f.source_from_date
	, f.source_thru_date
    , current_user
	 ,current_timestamp
from air_oai_dims.carrier_decode_fdw f
left outer join air_oai_dims.airline_entities e 
  on f.airline_oai_code = e.airline_oai_code and f.entity_oai_code = e.entity_oai_code and f.source_from_date = e.source_from_date
where e.airline_oai_code is null
and f.airline_oai_code != '3KQ' -- this code or set of codes was found to be non-unique
order by f.airline_usdot_id, f.airline_oai_code, f.entity_oai_code, f.source_from_date;




insert into air_oai_dims.airline_entities
( airline_entity_key, airline_usdot_id, airline_oai_code, entity_oai_code, airline_name
, airline_unique_oai_code, entity_unique_oai_code, airline_unique_name
, world_area_oai_id, airline_old_group_nbr, airline_new_group_nbr, operating_region_code, source_from_date, source_thru_date
, created_by, created_tmst)
select md5(airline_oai_code||'~'||entity_oai_code||'~'||source_from_date::char(10)) as airline_entity_key
    , airline_usdot_id
	, airline_oai_code
	, entity_oai_code
	, airline_name
	, airline_unique_oai_code
	, entity_unique_oai_code
	, airline_unique_name
	, world_area_oai_id
	, airline_old_group_nbr
	, airline_new_group_nbr
	, operating_region_code
	, source_from_date
	, source_thru_date
    , current_user
	 , current_timestamp
from (
select max(f.airline_usdot_id) as airline_usdot_id
	, f.airline_oai_code
	, f.entity_oai_code
	, max(f.airline_name) as airline_name
	, max(f.airline_unique_oai_code) as airline_unique_oai_code
	, max(f.entity_unique_oai_code) as entity_unique_oai_code
	, max(f.airline_unique_name) as airline_unique_name
	, max(f.world_area_oai_id) as world_area_oai_id
	, max(f.airline_old_group_nbr) as airline_old_group_nbr
	, max(f.airline_new_group_nbr) as airline_new_group_nbr
	, max(f.operating_region_code) as operating_region_code
	, f.source_from_date
	, max(f.source_thru_date) as source_thru_date
from air_oai_dims.carrier_decode_fdw f
where f.airline_oai_code = '3KQ'
group by airline_oai_code, entity_oai_code, source_from_date
) x;


-- select count(*) from air_oai_dims.airline_entities; -- 2785
-- select count(*) from air_oai_dims.carrier_decode_fdw; -- 2786
-- drop foreign table if exists air_oai_dims.carrier_decode_fdw;

select ae.airline_entity_id, source_from_date, source_thru_date
     , ae.airline_name, ae.operating_region_code
     , ae.world_area_oai_id, ae.world_area_oai_seq_id
     , wa.world_area_oai_id, wa.world_area_oai_seq_id
     , wa.effective_from_date, wa.effective_thru_date
     , wa.world_area_name
from air_oai_dims.airline_entities ae
left join air_oai_dims.world_areas wa
  on ae.world_area_oai_id = wa.world_area_oai_id
 -- where source_from_date between wa.effective_from_date and coalesce(wa.effective_thru_date, now())
 -- and ae.airline_entity_id in (4542,4545)
;

-- select * from air_oai_dims.world_areas where world_area_oai_id = 10;
-- select * from air_oai_dims.world_areas where country_iso_code = 'US' order by world_area_oai_id;

-- select count(*) from air_oai_dims.airline_entities; -- 2785
-- select count(*) from air_oai_dims.carrier_decode_fdw; -- 2786
-- drop foreign table if exists air_oai_dims.carrier_decode_fdw;







---------------------
-- Airport History MASTER CORD-- 
---------------------

-- '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_dims/T_MASTER_CORD.csv'

-- DROP  TABLE air_oai_dims.master_cord_fdw;

CREATE TABLE air_oai_dims.master_cord_fdw
	( airport_oai_seq_id					integer
	, airport_oai_id						integer
	, airport_oai_code						varchar(3)
	, airport_display_name					varchar(125)
	, city_full_display_name				varchar(125)
	, airport_world_area_oai_seq_id			integer
	, airport_world_area_oai_id				integer
	, country_name							varchar(75)
	, country_iso_code						varchar(10)
	, subdivision_name						varchar(75)
	, subdivision_iso_code					varchar(10)
	, subdivision_fips_code					varchar(10)
	, market_city_oai_seq_id				integer
	, market_city_oai_id					integer
	, market_city_full_display_name			varchar(75)
	, market_city_world_area_oai_seq_id		integer
	, market_city_world_area_oai_id			integer
	, latitude_degrees						smallint
	, latitude_hemisphere_code				char(1)
	, latitude_minutes						smallint
	, latitude_seconds						smallint
	, latitude_decimal_nbr					numeric(9,7)
	, longitude_degrees						smallint
	, longitude_hemisphere_code				char(1)
	, longitude_minutes						smallint
	, longitude_seconds						smallint
	, longitude_decimal_nbr					numeric(10,7)
	, utc_local_time_variation				varchar(75)
	, airport_effective_from_date			date
	, airport_effective_thru_date			date
	, airport_closed_ind					smallint
	, airport_latest_ind					smallint
	--, filler01_txt							varchar(10)	
	);
    /*
server abrams_ssd8tb options 
	( format 'csv'
	, header 'true'
	, filename '/Volumes/ssd8tbRaid0/opt/_data/_air/_oai/_dims/T_MASTER_CORD.csv'
	-- '/Volumes/24tbRaid5/opt/_data/_air/_oai/_dims/16922659_T_MASTER_CORD.csv'
	, delimiter ','
	, null ''
	);
    */
    
	TRUNCATE TABLE  air_oai_dims.master_cord_fdw;
    SELECT * FROM air_oai_dims.master_cord_fdw ;

    USE DATABASE AIR_OAI;
    
copy into AIR_OAI_DIMS.MASTER_CORD_FDW  FROM @csv_stage/T_MASTER_CORD.csv  ON_ERROR=CONTINUE FORCE = TRUE ;

    
-- select * from air_oai_dims.master_cord_fdw;

select airport_oai_code, airport_effective_from_date, count(*)
from air_oai_dims.master_cord_fdw group by 1,2 having count(*) > 1 order by count(*)  desc; -- unique

select min(airport_oai_id) as min_id, max(airport_oai_id) as max_id, min(airport_oai_seq_id) as min_seq, max(airport_oai_seq_id) as max_seq, count(*)
from air_oai_dims.master_cord_fdw;

select distinct utc_local_time_variation from air_oai_dims.master_cord_fdw;

-- drop table if exists air_oai_dims.airport_history;

CREATE OR REPLACE SEQUENCE airport_history_id
    START = 10000
   INCREMENT = 1
   ;


drop table if exists air_oai_dims.airport_history ;

CREATE TABLE air_oai_dims.airport_history 
	( airport_history_id 					integer NOT NULL default airport_history_id.nextval
	, airport_history_key 					char(32) NOT NULL
	, airport_oai_code 						varchar(3) NOT NULL
	, effective_from_date 					date NOT NULL
	, effective_thru_date 					date
	, airport_closed_ind 					smallint NOT NULL
	, airport_latest_ind 					smallint NOT NULL
	, airport_oai_seq_id 					integer NOT NULL
	, airport_oai_id 						integer NOT NULL
	, airport_display_name 					varchar(125) NOT NULL
	, city_full_display_name 				varchar(125) NOT NULL
	, airport_world_area_oai_seq_id 		integer NOT NULL
	, airport_world_area_oai_id 			integer NOT NULL
	, airport_world_area_key				char(32)
	, utc_local_time_variation 				char(5)
	, time_zone_name 						varchar(100)
	, market_city_oai_seq_id 				integer NOT NULL
	, market_city_oai_id 					integer NOT NULL
	, market_city_full_display_name 		varchar(75) NOT NULL
	, market_city_world_area_oai_seq_id 	integer NOT NULL
	, market_city_world_area_oai_id 		integer NOT NULL
	, market_city_world_area_key			char(32)
	, subdivision_iso_code 					varchar(10)
	, subdivision_fips_code 				varchar(10)
	, subdivision_name 						varchar(75)
	, country_iso_code 						varchar(10)  -- NOT NULL
	, country_name 							varchar(75) NOT NULL
	, latitude_decimal_nbr 					numeric(9,7)
	, longitude_decimal_nbr 				numeric(10,7)
	, point_geom 							geometry
	, created_by 							varchar(32) DEFAULT CURRENT_USER NOT NULL
	, created_tmst 							timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	, updated_by 							varchar(32)
	, updated_tsmt 							timestamp
, constraint airport_history_pk primary key (airport_history_id)
	, constraint airport_history_ak unique (airport_history_key)
	, constraint airport_history_nk unique (airport_oai_code, effective_from_date)  
   
);

-- ALTER [ COLUMN ] column_name DROP IDENTITY [ IF EXISTS ]
alter table air_oai_dims.airport_history alter column airport_history_id drop identity;

--CREATE UNIQUE INDEX airport_history_ak ON air_oai_dims.airport_history USING btree (airport_history_key);
--CREATE UNIQUE INDEX airport_history_nk ON air_oai_dims.airport_history USING btree (airport_oai_code, effective_from_date);
--CREATE UNIQUE INDEX airport_history_pk ON air_oai_dims.airport_history USING btree (airport_history_id);

/*
comment on column air_oai_dims.airport_history.airport_oai_seq_id is 'AIRPORT_SEQ_ID = An identification number assigned by US DOT to identify a unique airport at a given point of time.  Airport attributes, such as airport name or coordinates, may change over time.';
comment on column air_oai_dims.airport_history.airport_oai_id is 'AIRPORT_ID = An identification number assigned by US DOT to identify a unique airport.  Use this field for airport analysis across a range of years because an airport can change its airport code and airport codes can be reused.';
comment on column air_oai_dims.airport_history.airport_oai_code is 'AIRPORT = A three character alpha-numeric code issued by the U.S. Department of Transportation which is the official designation of the airport.  The airport code is not always unique to a specific airport because airport codes can change or can be reused.';
comment on column air_oai_dims.airport_history.airport_display_name is 'DISPLAY_AIRPORT_NAME = Airport Name.';
comment on column air_oai_dims.airport_history.city_full_display_name is 'DISPLAY_AIRPORT_CITY_NAME_FULL = Airport City Name with either U.S. State or Country.';
comment on column air_oai_dims.airport_history.airport_world_area_oai_id is 'AIRPORT_WAC = World Area Code for the Physical Location of the Airport.';
comment on column air_oai_dims.airport_history.country_name is 'AIRPORT_COUNTRY_NAME = Country Name for the Physical Location of the Airport.';
comment on column air_oai_dims.airport_history.country_iso_code is 'AIRPORT_COUNTRY_CODE_ISO = Two-character ISO Country Code for the Physical Location of the Airport.';
comment on column air_oai_dims.airport_history.subdivision_name is 'AIRPORT_STATE_NAME = State Name for the Physical Location of the Airport.';
comment on column air_oai_dims.airport_history.subdivision_iso_code is 'AIRPORT_STATE_CODE = State Abbreviation for the Physical Location of the Airport.';
comment on column air_oai_dims.airport_history.subdivision_fips_code is 'AIRPORT_STATE_FIPS = FIPS (Federal Information Processing Standard) State Code for the Physical Location of the Airport.';
comment on column air_oai_dims.airport_history.market_city_oai_id is 'CITY_MARKET_ID = An identification number assigned by US DOT to identify a city market.  Use this field to consolidate airports serving the same city market.';
comment on column air_oai_dims.airport_history.market_city_full_display_name is 'DISPLAY_CITY_MARKET_NAME_FULL = City Market Name with either U.S. State or Country';
comment on column air_oai_dims.airport_history.market_city_world_area_oai_id is 'CITY_MARKET_WAC = World Area Code for the City Market';
comment on column air_oai_dims.airport_history.latitude_decimal_nbr is 'LATITUDE = Latitude';
comment on column air_oai_dims.airport_history.longitude_decimal_nbr is 'LONGITUDE = Longitude';
comment on column air_oai_dims.airport_history.effective_from_date is 'AIRPORT_START_DATE = Start Date of Airport Attributes';
comment on column air_oai_dims.airport_history.effective_thru_date is 'AIRPORT_THRU_DATE = End Date of Airport Attributes (Active = NULL)';
comment on column air_oai_dims.airport_history.airport_closed_ind is 'AIRPORT_IS_CLOSED = Indicates if the airport is closed (1 = Yes).  If yes, the airport is closed is on the AirportEndDate.';
comment on column air_oai_dims.airport_history.airport_latest_ind is 'AIRPORT_IS_LATEST = Indicates if this row contains the latest attributes for the Airport (1 = Yes)';
*/

INSERT INTO air_oai_dims.airport_history
( airport_history_key, airport_oai_code
, effective_from_date, effective_thru_date
, airport_closed_ind, airport_latest_ind
, airport_oai_seq_id, airport_oai_id
, airport_display_name, city_full_display_name
, airport_world_area_oai_seq_id, airport_world_area_oai_id
, utc_local_time_variation -- time_zone_name
, market_city_oai_seq_id, market_city_oai_id
, market_city_full_display_name, market_city_world_area_oai_seq_id, market_city_world_area_oai_id
, subdivision_iso_code, subdivision_fips_code, subdivision_name
, country_iso_code, country_name, latitude_decimal_nbr, longitude_decimal_nbr
, point_geom, created_by, created_tmst)
SELECT md5(upper(m.airport_oai_code)||'~'||m.airport_effective_from_date::char(10)) as airport_history_key
    , m.airport_oai_code
    , m.airport_effective_from_date
	, m.airport_effective_thru_date
	, m.airport_closed_ind
	, m.airport_latest_ind
    , m.airport_oai_seq_id
	, m.airport_oai_id
	, m.airport_display_name
	, m.city_full_display_name
	, m.airport_world_area_oai_seq_id
	, m.airport_world_area_oai_id
	, case when length(m.utc_local_time_variation) = 0 then null else m.utc_local_time_variation end as utc_local_time_variation
	, m.market_city_oai_seq_id
	, m.market_city_oai_id
	, m.market_city_full_display_name
	, m.market_city_world_area_oai_seq_id
	, m.market_city_world_area_oai_id
	, m.subdivision_iso_code
	, m.subdivision_fips_code
	, m.subdivision_name
	, m.country_iso_code
	, m.country_name
	, m.latitude_decimal_nbr
	, m.longitude_decimal_nbr
	, case when m.latitude_decimal_nbr is not null and m.longitude_decimal_nbr is not null 
	       then  ST_SETSRID(TO_GEOMETRY('POINT('||m.longitude_decimal_nbr || ' '|| m.latitude_decimal_nbr||')'), 4326)
  --ST_SetSRID(ST_MakePoint(m.longitude_decimal_nbr, m.latitude_decimal_nbr),4326)
	       else null end as point_geom
    , CURRENT_USER
    , CURRENT_TIMESTAMP
FROM air_oai_dims.master_cord_fdw m
left join air_oai_dims.airport_history h 
  on m.airport_oai_code = h.airport_oai_code 
 and m.airport_effective_from_date = h.effective_from_date
where h.airport_oai_code is null
-- and md5(upper(m.airport_oai_code)||m.airport_effective_from_date::char(10)) = 'daa5b84ed2fc2cb69a12e4c7049df12a'
;

-- select * from air_oai_dims.airport_history;
-- select count(*) from air_oai_dims.airport_history; -- 19132
-- select count(*) from air_oai_dims.master_cord_fdw; -- 19132
-- drop foreign table if exists air_oai_dims.master_cord_fdw;

----

select case when airport_world_area_oai_id is null then 'null'::char(4) else 'data'::char(4) end as airport_wac_oai_id_data
     , case when airport_world_area_key is null then 'null'::char(4) else 'data'::char(4) end as airport_wac_key_data
     , case when market_city_world_area_oai_id is null then 'null'::char(4) else 'data'::char(4) end as market_city_wac_oai_id_data
     , case when market_city_world_area_key is null then 'null'::char(4) else 'data'::char(4) end as market_city_wac_key_data
     , count(*)
from (
select a.airport_history_id, a.airport_oai_code, a.effective_from_date
     , a.subdivision_iso_code, a.country_iso_code
     , a.airport_world_area_oai_id as airport_wac_oai_id, a.airport_world_area_oai_seq_id as airport_wac_oai_seq_id
     , b.world_area_oai_id as airport_world_area_oai_id, b.world_area_key as airport_world_area_key
     , a.market_city_world_area_oai_id as market_city_wac_oai_id, a.market_city_world_area_oai_seq_id
     , c.world_area_oai_id as market_city_world_area_oai_id, c.world_area_key as market_city_world_area_key
from air_oai_dims.airport_history a 
left outer join air_oai_dims.world_areas b
  on a.airport_world_area_oai_id = b.world_area_oai_id
 and a.airport_world_area_oai_seq_id = b.world_area_oai_seq_id
left outer join air_oai_dims.world_areas c
  on a.market_city_world_area_oai_id = c.world_area_oai_id
 and a.market_city_world_area_oai_seq_id = c.world_area_oai_seq_id
) abc 
group by 1,2,3,4 order by count(*) desc;
--limit 100;

update air_oai_dims.airport_history
set  -- airport_world_area_id = abc.airport_world_area_id 
    airport_world_area_key = abc.airport_world_area_key
  --, market_city_world_area_id = abc.market_city_world_area_id
  , market_city_world_area_key = abc.market_city_world_area_key
from (
select a.airport_history_id, a.airport_history_key
     , a.airport_oai_code, a.effective_from_date
     , a.subdivision_iso_code, a.country_iso_code
     , a.airport_world_area_oai_id
     , a.airport_world_area_oai_seq_id
     --, b.world_area_id as airport_world_area_id
     , b.world_area_key as airport_world_area_key
     , a.market_city_world_area_oai_id
     , a.market_city_world_area_oai_seq_id
     --, c.world_area_id as market_city_world_area_id
     , c.world_area_key as market_city_world_area_key
from air_oai_dims.airport_history a 
left outer join air_oai_dims.world_areas b
  on a.airport_world_area_oai_id = b.world_area_oai_id
 and a.airport_world_area_oai_seq_id = b.world_area_oai_seq_id
left outer join air_oai_dims.world_areas c
  on a.market_city_world_area_oai_id = c.world_area_oai_id
 and a.market_city_world_area_oai_seq_id = c.world_area_oai_seq_id
) abc 
where air_oai_dims.airport_history.airport_history_id = abc.airport_history_id
and air_oai_dims.airport_history.airport_history_key = abc.airport_history_key;

select case when airport_world_area_key is null then 'null'::char(4) else 'data'::char(4) end as airport_wac_key_data
     , case when market_city_world_area_key is null then 'null'::char(4) else 'data'::char(4) end as market_city_wac_key_data
     , count(*)
from air_oai_dims.airport_history
group by 1,2 order by count(*) desc;

select country_iso_code
     , max(airport_country_code) as apt_iso 
     , max(market_country_code) as mkt_iso
     , max(airport_world_area_name) as world_area_name
     , max(airport_world_region_name) as world_region_name
     , max(airport_country_type_descr) as country_type_descr
     , max(airport_sovereign_country_name) as sovereign_country_name
     , count(*) as record_qty
from (
select a.airport_history_id, a.airport_history_key
     , a.airport_oai_code, a.effective_from_date
     , a.subdivision_iso_code
     , a.country_iso_code
     , b.country_iso_code as airport_country_code
     , b.country_type_descr as airport_country_type_descr
     , b.sovereign_country_name as airport_sovereign_country_name
     , b.world_area_name as airport_world_area_name
     , b.world_region_name as airport_world_region_name
     , c.country_iso_code as market_country_code
     , c.country_type_descr as market_country_type_descr
     , c.sovereign_country_name as market_sovereign_country_name
     , c.world_area_name as market_world_area_name
     , c.world_region_name as market_world_region_name
from air_oai_dims.airport_history a 
left outer join air_oai_dims.world_areas b
  on a.airport_world_area_key = b.world_area_key
left outer join air_oai_dims.world_areas c
  on a.market_city_world_area_key = c.world_area_key
) x --where country_iso_code != market_country_code
group by 1 order by world_region_name, count(*) desc;




show tables in air_oai.air_oai_dims;
