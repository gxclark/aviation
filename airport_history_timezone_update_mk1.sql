/*
In order to process the Flight Performance data, we need a valid time zone name for each airport.
The dimension table from OAI does not contain this data, so we have to load the time zone boundaries
, and then update the airport history dimension: 
*/

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
