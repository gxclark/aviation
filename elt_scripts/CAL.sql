-- SQL script for generating calendar (CAL) tables in Databricks SQL Warehouse
-- Code does not require any source data
-- Run on cluster with Databricks Runtime 14.1 or above

-- create schemas
CREATE SCHEMA IF NOT EXISTS cal_gen;
CREATE SCHEMA IF NOT EXISTS calendar_dbx;

comment on schema cal_gen is 'Gregorian and Julian calendar generation views to be able to adjust data time frame in calendar schema.';
comment on schema calendar_dbx is 'Gregorian and Julian calendar data as well as time transformation for ROLAP analysis.';

-- base views for creating CAL tables

-- cal_gen.make_gregorian_year_v
create or replace view cal_gen.make_gregorian_year_v as
select year_nbr::smallint as year_nbr
     , year_cd::char(4) as year_code
     , case when mod(year_nbr,400) = 0 then 1
            when mod(year_nbr,100) = 0 then 0
            when mod(year_nbr,4) = 0 then 1
            else 0 end::smallint as leap_year_ind
     , (year_cd || '-01-01')::date as year_from_date
     , (year_cd || '-12-31')::date as year_thru_date
     , (year_cd || '-12-31')::date - (year_cd || '-01-01')::date as day_qty
     , lag(year_nbr,1) over (order by year_nbr) as last_year_nbr
from (
select v1.col1::char(1) || v2.col1::char(1) || v3.col1::char(1) || v4.col1::char(1) as year_cd
     , cast(v1.col1::char(1) || v2.col1::char(1) || v3.col1::char(1) || v4.col1::char(1) as smallint) as year_nbr
from       (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v1
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v2
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v3
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v4
) yoe
where year_nbr between 1000 and 3000
order by 2;

-- cal_gen.make_hour_of_day_v
create or replace view cal_gen.make_hour_of_day_v as
select col1::integer as hour_of_day_nbr
      , col2::char(2) as hour_of_day_code
      , (col2::char(2)||':00'::char(3))::timestamp as hour_of_day_time
      , col3::char(2) as period_code
from (values 
 ( 0,'00','am'),( 1,'01','am'),( 2,'02','am'),( 3,'03','am'),( 4,'04','am'),( 5,'05','am')
,( 6,'06','am'),( 7,'07','am'),( 8,'08','am'),( 9,'09','am'),(10,'10','am'),(11,'11','am')
,(12,'12','pm'),(13,'13','pm'),(14,'14','pm'),(15,'15','pm'),(16,'16','pm'),(17,'17','pm')
,(18,'18','pm'),(19,'19','pm'),(20,'20','pm'),(21,'21','pm'),(22,'22','pm'),(23,'23','pm')
 ) hod;

-- al_gen.make_minute_of_hour_v
create or replace view cal_gen.make_minute_of_hour_v as
select cast(v1.col1::char(1) || v2.col1::char(1) as char(2)) as minute_of_hour_code
     , cast(v1.col1::char(1) || v2.col1::char(1) as smallint) as minute_of_hour_nbr
from       (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v1
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v2
where cast(v1.col1::char(1) || v2.col1::char(1) as smallint) < 60
order by 1,2;

-- cal_gen.make_day_of_month_v
create or replace view cal_gen.make_day_of_month_v as
select cast(v1.col1::char(1) || v2.col1::char(1) as smallint) as day_of_month_nbr
     , cast(v1.col1::char(1) || v2.col1::char(1) as char(2)) as day_of_month_code
from       (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v1
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v2
where cast(v1.col1::char(1) || v2.col1::char(1) as smallint) between 1 and 35
order by 1,2;

-- cal_gen.make_gregorian_month_of_year_v cascade
create or replace view cal_gen.make_gregorian_month_of_year_v as
select col1::smallint as month_of_year_nbr
     , col2::char(2) as month_of_year_code
     , col3::smallint as quarter_of_year_nbr
     , col4::smallint as standard_year_day_qty
     , col5::smallint as leap_year_day_qty
     , col6::char(3) as month_of_year_abbr
     , col7::varchar(10) as month_of_year_name
from (
values 
  ( 1,'01','1','31','31','Jan','Janurary')
, ( 2,'02','1','28','29','Feb','February')
, ( 3,'03','1','31','31','Mar','March')
, ( 4,'04','2','30','30','Apr','April')
, ( 5,'05','2','31','31','May','May')
, ( 6,'06','2','30','30','Jun','June')
, ( 7,'07','3','31','31','Jul','July')
, ( 8,'08','3','31','31','Aug','August')
, ( 9,'09','3','30','30','Sep','September')
, (10,'10','4','31','31','Oct','October')
, (11,'11','4','30','30','Nov','November')
, (12,'12','4','31','31','Dec','December')
) mo;

-- cal_gen.make_day_of_week_v
create or replace view cal_gen.make_day_of_week_v as
select col2::smallint as day_of_week_iso_nbr
     , col1::smallint as day_of_week_common_nbr
     , col3::smallint as day_of_week_pgsql_nbr
     , col4::char(3) as day_of_week_abbr
     , col5::varchar(10) as day_of_week_name_eng
from (
values
  (1,7,0,'Sun','Sunday')
, (2,1,1,'Mon','Monday')
, (3,2,2,'Tue','Tuesday')
, (4,3,3,'Wed','Wednesday')
, (5,4,4,'Thu','Thursday')
, (6,5,5,'Fri','Friday')
, (7,6,6,'Sat','Saturday')
) dow order by 1;

-- cal_gen.make_gregorian_quarter_of_year_v
create or replace view cal_gen.make_gregorian_quarter_of_year_v as
select col1::smallint as quarter_of_year_nbr
     , col2::char(1) as quarter_of_year_code
     , col3::char(2) as quarter_of_year_abbr
     , col4::varchar(15) as quarter_of_year_name
from (
values
  (1,'1','Q1','First Quarter')
, (2,'2','Q2','Second Quarter')
, (3,'3','Q3','Third Quarter')
, (4,'4','Q4','Fourth Quarter')
) qoy;

-- another set of base views for creating CAL tables (dependent on the ones created above)
-- cal_gen.make_gregorian_year_quarter_v
create or replace view cal_gen.make_gregorian_year_quarter_v as
select year_quarter_nbr::integer as year_quarter_nbr
     , year_quarter_standard_code::char(7) as year_quarter_standard_code
     , year_nbr::smallint as year_nbr
     , quarter_of_year_nbr::smallint as quarter_of_year_nbr
     , year_quarter_from_date::date as year_quarter_from_date
     , year_quarter_thru_date::date as year_quarter_thru_date
     , lag(year_quarter_nbr,1) over (order by year_quarter_nbr) as last_year_quarter_nbr
     , lag(year_quarter_nbr,4) over (order by year_quarter_nbr) as last_year_this_quarter_nbr
from (
select (y.year_code || q.quarter_of_year_code)::integer year_quarter_nbr
     ,  y.year_code || '-' || q.quarter_of_year_abbr::char(7) as year_quarter_standard_code
     ,  y.year_nbr::smallint as year_nbr
     ,  q.quarter_of_year_nbr::smallint as quarter_of_year_nbr
     , (y.year_code || case when q.quarter_of_year_nbr = 1 then '-01-01' 
                         when q.quarter_of_year_nbr = 2 then '-04-01'
                         when q.quarter_of_year_nbr = 3 then '-07-01'
                         when q.quarter_of_year_nbr = 4 then '-10-01'
                         else null end)::date as year_quarter_from_date
     , (y.year_code || case when q.quarter_of_year_nbr = 1 then '-03-31' 
                         when q.quarter_of_year_nbr = 2 then '-06-30'
                         when q.quarter_of_year_nbr = 3 then '-09-30'
                         when q.quarter_of_year_nbr = 4 then '-12-31'
                         else null end)::date as year_quarter_thru_date
from cal_gen.make_gregorian_year_v y
cross join cal_gen.make_gregorian_quarter_of_year_v q
where y.year_nbr between 1000 and 3000
) yq
order by 1; 

-- cal_gen.make_gregorian_year_month_v
create or replace view cal_gen.make_gregorian_year_month_v as
select year_month_nbr::integer as year_month_nbr
     , year_month_standard_code::char(7) as year_month_standard_code
     , month_of_year_nbr::smallint as month_of_year_nbr
     , year_quarter_nbr::integer as year_quarter_nbr
     , year_nbr::smallint as year_nbr
     , year_month_from_date:: date as year_month_from_date
     , year_month_thru_date::date as year_month_thru_date
     , lag(year_month_nbr,1) over (order by year_month_nbr) as last_year_month_nbr
     , lag(year_month_nbr,3) over (order by year_month_nbr) as last_quarter_this_month_nbr
     , lag(year_month_nbr,12) over (order by year_month_nbr) as last_year_this_month_nbr
from (
select (y.year_code || m.month_of_year_code)::integer year_month_nbr
     , y.year_code || '-' || m.month_of_year_code::char(7) as year_month_standard_code
     , m.month_of_year_nbr
     , (y.year_code || m.quarter_of_year_nbr::char(1))::integer as year_quarter_nbr
     , y.year_nbr
     , (y.year_code || '-' || m.month_of_year_code || '-01')::date as year_month_from_date
     , (y.year_code || '-' || m.month_of_year_code || '-'
        || case when y.leap_year_ind = 1 then leap_year_day_qty
                when y.leap_year_ind = 0 then standard_year_day_qty
                else '01' end)::date as year_month_thru_date
from cal_gen.make_gregorian_year_v y
cross join cal_gen.make_gregorian_month_of_year_v m
where y.year_nbr between 1000 and 3000
--order by 1
) ym
order by 1;

-- cal_gen.make_calendar_date_v
create or replace view cal_gen.make_calendar_date_v as 
select dt.calendar_date
     , extract(dow_iso from dt.calendar_date)::smallint as day_of_week_iso_nbr
     , extract(week from dt.calendar_date)::smallint as week_of_year_nbr
     , (extract(year from dt.calendar_date)::char(4)
     || case when length(extract(week from dt.calendar_date)::varchar(2)) < 2 
             then '0' || extract(week from dt.calendar_date)::char(1)
             else extract(week from dt.calendar_date)::char(2)
             end)::integer as year_week_nbr
     , m.year_month_nbr::integer as year_month_nbr
     , m.year_quarter_nbr::integer as year_quarter_nbr
     , dt.year_nbr::smallint as year_nbr
     , lag(dt.calendar_date, 1) over (order by dt.calendar_date) as yesterday_date
     , (calendar_date - interval '1 week')::date as this_day_last_week
     , (calendar_date - interval '1 month')::date as this_day_last_month
     , (calendar_date - interval '3 months')::date as this_day_last_quarter
     , (calendar_date - interval '1 year')::date as this_day_last_year
from (
select (y.year_code || '-' || moy.month_of_year_code || '-' || dom.day_of_month_code)::date as calendar_date
     , y.year_nbr
     , y.leap_year_ind
     , moy.month_of_year_nbr
     , moy.standard_year_day_qty::integer as standard_year_day_qty
     , moy.leap_year_day_qty::integer as leap_year_day_qty
from cal_gen.make_gregorian_year_v y
cross join cal_gen.make_gregorian_month_of_year_v moy
cross join cal_gen.make_day_of_month_v dom
where dom.day_of_month_nbr <= (case when y.leap_year_ind = 1 then leap_year_day_qty else standard_year_day_qty end::integer)
and y.year_nbr between 1000 and 3000 
) dt
left outer 
join cal_gen.make_gregorian_year_month_v m
  on dt.year_nbr = m.year_nbr
 and dt.month_of_year_nbr = m.month_of_year_nbr
order by 1;

-- cal_gen.make_year_week_v
create or replace view cal_gen.make_year_week_v as
select year_week_nbr
     , max(week_of_year_nbr) as week_of_year_nbr
     , max(substring(year_week_nbr::varchar(6),1,4))::smallint as year_nbr
     , max(year_nbr::varchar(4) || '-W' || substring((year_week_nbr::char(6)),5,2))::char(8) as year_week_std_cd
     , min(calendar_date) as week_from_dt
     , max(calendar_date) as week_thru_dt
from cal_gen.make_calendar_date_v
where year_nbr between 1000 and 3000
group by 1
--having max(substring(year_week_nbr::varchar(6),1,4))::smallint = 2006
--and max(week_of_year_nbr) > 50
order by 1;

-- create beta calendar tables based on base views and start/end year
-- set calendar start and end years as variables
DECLARE OR REPLACE VARIABLE start_year INT = 1900;
DECLARE OR REPLACE VARIABLE end_year INT = 2090;

create or replace table calendar_dbx.day_of_week_beta as select * from cal_gen.make_day_of_week_v;
create or replace table calendar_dbx.hour_of_day_beta as select * from cal_gen.make_hour_of_day_v;
create or replace table calendar_dbx.minute_of_hour_beta as select * from cal_gen.make_minute_of_hour_v;
create or replace table calendar_dbx.gregorian_month_of_year_beta as select * from cal_gen.make_gregorian_month_of_year_v;
create or replace table calendar_dbx.gregorian_quarter_of_year_beta as select * from cal_gen.make_gregorian_quarter_of_year_v;
create or replace table calendar_dbx.gregorian_year_beta as select * from cal_gen.make_gregorian_year_v where year_nbr between start_year and end_year; 
create or replace table calendar_dbx.gregorian_year_quarter_beta as select * from cal_gen.make_gregorian_year_quarter_v where year_nbr between start_year and end_year; 
create or replace table calendar_dbx.gregorian_year_month_beta as select * from cal_gen.make_gregorian_year_month_v where year_nbr between start_year and end_year; 
create or replace table calendar_dbx.year_week_beta as select * from cal_gen.make_year_week_v where year_nbr between start_year and end_year; 
create or replace table calendar_dbx.calendar_date_beta as select * from cal_gen.make_calendar_date_v where year_nbr between start_year and end_year; 

-- add comments to beta calendar tables 
-- calendar_dbx.calendar_date_beta
COMMENT ON TABLE calendar_dbx.calendar_date_beta IS 'A calendar day represents the spin of the earth on its axis, providing a day and night cycle.';
ALTER TABLE calendar_dbx.calendar_date_beta CHANGE COLUMN day_of_week_iso_nbr COMMENT 'ISO defines Monday as the first day of the week.';
ALTER TABLE calendar_dbx.calendar_date_beta CHANGE COLUMN year_week_nbr COMMENT 'Weeks always have seven days, and each is assigned a number within a Year; week 1 contains January 1 for that year.'; 

-- calendar_dbx.day_of_week_beta
COMMENT ON TABLE calendar_dbx.day_of_week_beta IS 'Monday is the first day of the working week, ISO 2105/8601.';
ALTER TABLE calendar_dbx.day_of_week_beta CHANGE COLUMN day_of_week_iso_nbr COMMENT 'ISO defines Monday as the first day of the week.';
ALTER TABLE calendar_dbx.day_of_week_beta CHANGE COLUMN day_of_week_common_nbr COMMENT 'This number begins with Sunday as 1, and is in common usage.';
ALTER TABLE calendar_dbx.day_of_week_beta CHANGE COLUMN day_of_week_pgsql_nbr COMMENT 'PostgreSQL functions list Sunday as 0, and Saturday as 6.';
ALTER TABLE calendar_dbx.day_of_week_beta CHANGE COLUMN day_of_week_abbr COMMENT 'Standard abbreviation of the day of week (in English).';
ALTER TABLE calendar_dbx.day_of_week_beta CHANGE COLUMN day_of_week_name_eng COMMENT 'The full name of the day of the week (in English).';

-- calendar_dbx.gregorian_month_of_year_beta
COMMENT ON TABLE calendar_dbx.gregorian_month_of_year_beta IS 'Gregorian Years have 12 months, and have since it evolved from Roman years.';
ALTER TABLE calendar_dbx.gregorian_month_of_year_beta CHANGE COLUMN standard_year_day_qty COMMENT 'The number of Days within this month for a Standard Year.';
ALTER TABLE calendar_dbx.gregorian_month_of_year_beta CHANGE COLUMN leap_year_day_qty COMMENT 'The number of days within this month during a Leap Year.';
ALTER TABLE calendar_dbx.gregorian_month_of_year_beta CHANGE COLUMN month_of_year_name COMMENT 'The word which identifies this month.';

-- calendar_dbx.gregorian_year_beta
COMMENT ON TABLE calendar_dbx.gregorian_year_beta IS 'A year represents the number of orbits by the earth around the sun within the Common Era (CE), defined by Pope Gregory XIII in October 1582.';
ALTER TABLE calendar_dbx.gregorian_year_beta CHANGE COLUMN year_nbr COMMENT 'A modern year is a four digit number.';

-- calendar_dbx.hour_of_day_beta
COMMENT ON TABLE calendar_dbx.hour_of_day_beta IS 'Our 24-hour day comes from the ancient Egyptians who divided day-time into 10 hours they measured with devices such as shadow clocks, and added a twilight hour at the beginning and another one at the end of the day-time.';
ALTER TABLE calendar_dbx.hour_of_day_beta CHANGE COLUMN period_code COMMENT 'This specifies a subdivision within a day, such as morning, afternoon, evening or night.';

-- calendar_dbx.minute_of_hour_beta
COMMENT ON TABLE calendar_dbx.minute_of_hour_beta IS 'The division of the hour into 60 minutes and of the minute into 60 seconds comes from ancient civilizations -  Babylonians, Sumerians and Egyptians - who had different numbering systems; base 12 (duodecimal) and base 60 (sexagesimal) for mathematics.';

-- calendar_dbx.gregorian_quarter_of_year_beta
COMMENT ON TABLE calendar_dbx.gregorian_quarter_of_year_beta IS 'A quarter is a standard calendar interval consisting of three calendar months, and generally analagous to a "season", which is in keeping with the agricultural purpose of the calendar_dbx.';

-- calendar_dbx.gregorian_year_month_beta
COMMENT ON TABLE calendar_dbx.gregorian_year_month_beta IS 'This is the natural list of months within a specific year.';

-- calendar_dbx.gregorian_year_quarter_beta
COMMENT ON TABLE calendar_dbx.gregorian_year_quarter_beta IS 'This is the natural list of quarters within a specific year.';
ALTER TABLE calendar_dbx.gregorian_year_quarter_beta CHANGE COLUMN year_nbr COMMENT 'The year containing this year-quarter.';

-- calendar_dbx.year_week_beta
COMMENT ON TABLE calendar_dbx.year_week_beta IS 'This is the nautral list of weeks within a specific year.';
ALTER TABLE calendar_dbx.year_week_beta CHANGE COLUMN year_week_nbr COMMENT 'The numbered weeks within a year.';
ALTER TABLE calendar_dbx.year_week_beta CHANGE COLUMN year_nbr COMMENT 'The year that contains this week.';

-- set primary key columns to NOT NULL - required in DBX
ALTER TABLE calendar_dbx.calendar_date_beta ALTER COLUMN calendar_date SET NOT NULL;
ALTER TABLE calendar_dbx.day_of_week_beta ALTER COLUMN day_of_week_iso_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.gregorian_month_of_year_beta ALTER COLUMN month_of_year_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.gregorian_year_beta ALTER COLUMN year_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.hour_of_day_beta ALTER COLUMN hour_of_day_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.minute_of_hour_beta ALTER COLUMN minute_of_hour_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.gregorian_quarter_of_year_beta ALTER COLUMN quarter_of_year_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.gregorian_year_month_beta ALTER COLUMN year_month_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.gregorian_year_quarter_beta ALTER COLUMN year_quarter_nbr SET NOT NULL;
ALTER TABLE calendar_dbx.year_week_beta ALTER COLUMN year_week_nbr SET NOT NULL;

-- add keys for beta calendar tables - UNITY CATALOG required
-- primary keys
ALTER TABLE calendar_dbx.calendar_date_beta ADD CONSTRAINT calendar_date_beta_pk  PRIMARY KEY (calendar_date);
ALTER TABLE calendar_dbx.day_of_week_beta ADD CONSTRAINT day_of_week_beta_pk  PRIMARY KEY (day_of_week_iso_nbr);
ALTER TABLE calendar_dbx.gregorian_month_of_year_beta ADD CONSTRAINT gregorian_month_of_year_beta_pk  PRIMARY KEY (month_of_year_nbr);
ALTER TABLE calendar_dbx.gregorian_year_beta ADD CONSTRAINT gregorian_year_beta_pk  PRIMARY KEY (year_nbr);
ALTER TABLE calendar_dbx.hour_of_day_beta ADD CONSTRAINT hour_of_day_beta_pk  PRIMARY KEY (hour_of_day_nbr);
ALTER TABLE calendar_dbx.minute_of_hour_beta ADD CONSTRAINT minute_of_hour_beta_pk  PRIMARY KEY (minute_of_hour_nbr);
ALTER TABLE calendar_dbx.gregorian_quarter_of_year_beta ADD CONSTRAINT gregorian_quarter_of_year_beta_pk  PRIMARY KEY (quarter_of_year_nbr);
ALTER TABLE calendar_dbx.gregorian_year_month_beta ADD CONSTRAINT gregorian_year_month_beta_pk  PRIMARY KEY (year_month_nbr);
ALTER TABLE calendar_dbx.gregorian_year_quarter_beta ADD CONSTRAINT gregorian_year_quarter_beta_pk  PRIMARY KEY (year_quarter_nbr);
ALTER TABLE calendar_dbx.year_week_beta ADD CONSTRAINT year_week_beta_pk  PRIMARY KEY (year_week_nbr);

-- foreign keys
ALTER TABLE calendar_dbx.calendar_date_beta ADD CONSTRAINT calendar_date_year_week_beta_fk 
FOREIGN KEY (year_week_nbr) REFERENCES calendar_dbx.year_week_beta (year_week_nbr);
ALTER TABLE calendar_dbx.calendar_date_beta ADD CONSTRAINT calendar_date_year_month_beta_fk 
FOREIGN KEY (year_month_nbr) REFERENCES calendar_dbx.gregorian_year_month_beta (year_month_nbr);
ALTER TABLE calendar_dbx.calendar_date_beta ADD CONSTRAINT calendar_date_day_of_week_beta_fk 
FOREIGN KEY (day_of_week_iso_nbr) REFERENCES calendar_dbx.day_of_week_beta (day_of_week_iso_nbr);
ALTER TABLE calendar_dbx.gregorian_month_of_year_beta ADD CONSTRAINT gregorian_month_of_year_quarter_of_year_beta_fk  
FOREIGN KEY (quarter_of_year_nbr) REFERENCES calendar_dbx.gregorian_quarter_of_year_beta (quarter_of_year_nbr);
ALTER TABLE calendar_dbx.gregorian_year_month_beta ADD CONSTRAINT gregorian_year_month_year_quarter_beta_fk 
FOREIGN KEY (year_quarter_nbr) REFERENCES calendar_dbx.gregorian_year_quarter_beta (year_quarter_nbr);
ALTER TABLE calendar_dbx.gregorian_year_month_beta ADD CONSTRAINT gregorian_year_month_month_of_year_beta_fk 
FOREIGN KEY (month_of_year_nbr) REFERENCES calendar_dbx.gregorian_month_of_year_beta (month_of_year_nbr);
ALTER TABLE calendar_dbx.gregorian_year_quarter_beta ADD CONSTRAINT gregorian_year_quarter_year_beta_fk 
FOREIGN KEY (year_nbr) REFERENCES calendar_dbx.gregorian_year_beta (year_nbr);
ALTER TABLE calendar_dbx.gregorian_year_quarter_beta ADD CONSTRAINT gregorian_year_quarter_quarter_of_year_beta_fk 
FOREIGN KEY (quarter_of_year_nbr) REFERENCES calendar_dbx.gregorian_quarter_of_year_beta (quarter_of_year_nbr);
ALTER TABLE calendar_dbx.year_week_beta ADD CONSTRAINT year_week_gregorian_year_beta_fk  
FOREIGN KEY (year_nbr) REFERENCES calendar_dbx.gregorian_year_beta (year_nbr);

-- generate beta transformation tables
-- MTD
create or replace table calendar_dbx.cumulative_month_to_dates_beta as
select d.calendar_date, x.calendar_date as cumulative_month_to_date
from calendar_dbx.calendar_date_beta d join calendar_dbx.calendar_date_beta x 
  on d.year_month_nbr = x.year_month_nbr
where x.calendar_date <= d.calendar_date
and x.calendar_date <= (select max(calendar_date) from calendar_dbx.calendar_date_beta)
order by d.calendar_date, x.calendar_date;

-- QTD 
create or replace table calendar_dbx.cumulative_quarter_to_dates_beta as
select d.calendar_date, x.calendar_date as cumulative_quarter_to_date
from calendar_dbx.calendar_date_beta d join calendar_dbx.calendar_date_beta x 
  on d.year_quarter_nbr = x.year_quarter_nbr
where x.calendar_date <= d.calendar_date
order by d.calendar_date, x.calendar_date;

-- YTD
create or replace table calendar_dbx.cumulative_year_to_dates_beta as
select d.calendar_date, x.calendar_date as cumulative_year_to_date
from calendar_dbx.calendar_date_beta d join calendar_dbx.calendar_date_beta x 
  on d.year_nbr = x.year_nbr
where x.calendar_date <= d.calendar_date
order by d.calendar_date, x.calendar_date;

-- WTD
create or replace table calendar_dbx.cumulative_week_to_dates_beta as
select d.calendar_date, x.calendar_date as cumulative_week_to_date
from calendar_dbx.calendar_date_beta d join calendar_dbx.calendar_date_beta x 
  on d.year_week_nbr = x.year_week_nbr
where x.calendar_date <= d.calendar_date
order by d.calendar_date, x.calendar_date;

-- set primary key columns in transformation tables to NOT NULL - required in DBX
alter table calendar_dbx.cumulative_month_to_dates_beta alter column calendar_date set not null;
alter table calendar_dbx.cumulative_month_to_dates_beta alter column cumulative_month_to_date set not null;
alter table calendar_dbx.cumulative_quarter_to_dates_beta alter column calendar_date set not null;
alter table calendar_dbx.cumulative_quarter_to_dates_beta alter column cumulative_quarter_to_date set not null;
alter table calendar_dbx.cumulative_year_to_dates_beta alter column calendar_date set not null;
alter table calendar_dbx.cumulative_year_to_dates_beta alter column cumulative_year_to_date set not null;
alter table calendar_dbx.cumulative_week_to_dates_beta alter column calendar_date set not null;
alter table calendar_dbx.cumulative_week_to_dates_beta alter column cumulative_week_to_date set not null;

-- create foreign keys for beta transformation tables - UNITY CATALOG required
-- add constraints for calendar_dbx.cumulative_month_to_dates_beta:
alter table calendar_dbx.cumulative_month_to_dates_beta 
  add constraint cumulative_month_to_dates_beta_pk primary key (calendar_date, cumulative_month_to_date);
  
alter table calendar_dbx.cumulative_month_to_dates_beta 
  add constraint cumulative_month_to_dates_beta_base_date_fk foreign key (calendar_date)
  references calendar_dbx.calendar_date_beta (calendar_date);
  
alter table calendar_dbx.cumulative_month_to_dates_beta 
  add constraint cumulative_month_to_dates_beta_mtd_date_fk foreign key (cumulative_month_to_date)
  references calendar_dbx.calendar_date_beta (calendar_date);

-- add constraints for calendar_dbx.cumulative_quarter_to_dates_beta:
alter table calendar_dbx.cumulative_quarter_to_dates_beta 
  add constraint cumulative_quarter_to_dates_beta_pk primary key (calendar_date, cumulative_quarter_to_date);
  
alter table calendar_dbx.cumulative_quarter_to_dates_beta 
  add constraint cumulative_quarter_to_dates_beta_base_date_fk foreign key (calendar_date)
  references calendar_dbx.calendar_date_beta (calendar_date);
  
alter table calendar_dbx.cumulative_quarter_to_dates_beta 
  add constraint cumulative_quarter_to_dates_beta_qtd_date_fk foreign key (cumulative_quarter_to_date)
  references calendar_dbx.calendar_date_beta (calendar_date);

-- add constraints for calendar_dbx.cumulative_year_to_dates_beta:
alter table calendar_dbx.cumulative_year_to_dates_beta 
  add constraint cumulative_year_to_dates_beta_pk primary key (calendar_date, cumulative_year_to_date);
  
alter table calendar_dbx.cumulative_year_to_dates_beta 
  add constraint cumulative_year_to_dates_beta_base_date_fk foreign key (calendar_date)
  references calendar_dbx.calendar_date_beta (calendar_date);
  
alter table calendar_dbx.cumulative_year_to_dates_beta 
  add constraint cumulative_year_to_dates_beta_qtd_date_fk foreign key (cumulative_year_to_date)
  references calendar_dbx.calendar_date_beta (calendar_date);

-- add constraints for calendar.cumulative_year_to_dates_beta:
alter table calendar_dbx.cumulative_week_to_dates_beta 
  add constraint cumulative_week_to_dates_beta_pk primary key (calendar_date, cumulative_week_to_date);
  
alter table calendar_dbx.cumulative_week_to_dates_beta 
  add constraint cumulative_week_to_dates_beta_base_date_fk foreign key (calendar_date)
  references calendar_dbx.calendar_date_beta (calendar_date);
  
alter table calendar_dbx.cumulative_week_to_dates_beta 
  add constraint cumulative_week_to_dates_beta_qtd_date_fk foreign key (cumulative_week_to_date)
  references calendar_dbx.calendar_date_beta (calendar_date);

-- create CAL views (MicroStrategy facing)
create or replace view calendar_dbx.calendar_date_v as select *, 1::integer as calendar_date_qty from calendar_dbx.calendar_date_beta;
create or replace view calendar_dbx.calendar_year_v as select *, 1::integer as calendar_year_qty from calendar_dbx.gregorian_year_beta;
create or replace view calendar_dbx.day_of_week_v as select *, 1::integer as day_of_week_qty from calendar_dbx.day_of_week_beta;
create or replace view calendar_dbx.hour_of_day_v as select *, 1::integer as hour_of_day_qty from calendar_dbx.hour_of_day_beta;
create or replace view calendar_dbx.minute_of_hour_v as select *, 1::integer as minute_of_hour_qty from calendar_dbx.minute_of_hour_beta;
create or replace view calendar_dbx.month_of_year_v as select *, 1::integer as month_of_year_qty from calendar_dbx.gregorian_month_of_year_beta;
create or replace view calendar_dbx.quarter_of_year_v as select *, 1::integer as quarter_of_year_qty from calendar_dbx.gregorian_quarter_of_year_beta;
create or replace view calendar_dbx.year_month_v as select *, 1::integer as year_month_qty from calendar_dbx.gregorian_year_month_beta;
create or replace view calendar_dbx.year_quarter_v as select *, 1::integer as year_quarter_qty from calendar_dbx.gregorian_year_quarter_beta;
create or replace view calendar_dbx.year_week_v as select *, 1::integer as year_week_qty from calendar_dbx.year_week_beta;
create or replace view calendar_dbx.cumulative_month_to_dates_v as select calendar_date, cumulative_month_to_date from calendar_dbx.cumulative_month_to_dates_beta;
create or replace view calendar_dbx.cumulative_quarter_to_dates_v as select calendar_date, cumulative_quarter_to_date from calendar_dbx.cumulative_quarter_to_dates_beta;
create or replace view calendar_dbx.cumulative_year_to_dates_v as select calendar_date, cumulative_year_to_date from calendar_dbx.cumulative_year_to_dates_beta;
create or replace view calendar_dbx.cumulative_week_to_dates_v as select calendar_date, cumulative_week_to_date from calendar_dbx.cumulative_week_to_dates_beta;

-- END of script
