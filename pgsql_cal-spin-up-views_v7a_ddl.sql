
create schema cal_gen;

/*
drop view if exists cal_gen.make_year_week_v;
drop view if exists cal_gen.make_calendar_date_v;

drop view if exists cal_gen.make_gregorian_year_week_v;
drop view if exists cal_gen.make_gregorian_year_quarter_v;
drop view if exists cal_gen.make_gregorian_quarter_of_year_v;
drop view if exists cal_gen.make_minute_of_hour_v;
drop view if exists cal_gen.make_hour_of_day_v;
drop view if exists cal_gen.make_day_of_week_v;
drop view if exists cal_gen.make_day_nbr_v;
drop view if exists cal_gen.make_gregorian_calendar_date_v;

drop view if exists cal_gen.make_gregorian_month_of_year_v cascade;
drop view if exists cal_gen.make_gregorian_year_month_v cascade;
drop view if exists cal_gen.make_gregorian_year_v cascade;
drop view if exists cal_gen.make_day_of_month_v cascade;
*/

-- PostgreSQL 9.6
-- select * from cal_gen.make_gregorian_year_v;
-- drop view cal_gen.make_gregorian_year_v;
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
select v1.column1::char(1) || v2.column1::char(1) || v3.column1::char(1) || v4.column1::char(1) as year_cd
     , cast(v1.column1::char(1) || v2.column1::char(1) || v3.column1::char(1) || v4.column1::char(1) as smallint) as year_nbr
from       (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v1
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v2
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v3
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v4
) yoe
where year_nbr between 1000 and 3000
order by 2;

--PostgreSQL 9.6
--drop view cal_gen.make_hour_of_day_v;
create or replace view cal_gen.make_hour_of_day_v as
select column1::integer as hour_of_day_nbr
      , column2::char(2) as hour_of_day_code
      , (column2::char(2)||':00'::char(3))::time as hour_of_day_time
      , column3::char(2) as period_code
from (values 
 ( 0,'00','am'),( 1,'01','am'),( 2,'02','am'),( 3,'03','am'),( 4,'04','am'),( 5,'05','am')
,( 6,'06','am'),( 7,'07','am'),( 8,'08','am'),( 9,'09','am'),(10,'10','am'),(11,'11','am')
,(12,'12','pm'),(13,'13','pm'),(14,'14','pm'),(15,'15','pm'),(16,'16','pm'),(17,'17','pm')
,(18,'18','pm'),(19,'19','pm'),(20,'20','pm'),(21,'21','pm'),(22,'22','pm'),(23,'23','pm')
 ) hod;

--PostgreSQL 9.6
--select * from cal_gen.make_minute_of_hour_v; 
--drop view cal_gen.make_minute_of_hour_v;
create or replace view cal_gen.make_minute_of_hour_v as
select cast(v1.column1::char(1) || v2.column1::char(1) as char(2)) as minute_of_hour_code
     , cast(v1.column1::char(1) || v2.column1::char(1) as smallint) as minute_of_hour_nbr
from       (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v1
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v2
where cast(v1.column1::char(1) || v2.column1::char(1) as smallint) < 60
order by 1,2;

--PostgreSQL 9.6
--drop view cal_gen.make_day_of_month_v;
create or replace view cal_gen.make_day_of_month_v as
select cast(v1.column1::char(1) || v2.column1::char(1) as smallint) as day_of_month_nbr
     , cast(v1.column1::char(1) || v2.column1::char(1) as char(2)) as day_of_month_code
from       (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v1
cross join (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v2
where cast(v1.column1::char(1) || v2.column1::char(1) as smallint) between 1 and 35
order by 1,2;

--PostgreSQL 9.6
--select * from cal_gen.make_gregorian_month_of_year_v;
--drop view if exists cal_gen.make_gregorian_month_of_year_v cascade;
create or replace view cal_gen.make_gregorian_month_of_year_v as
select column1::smallint as month_of_year_nbr
     , column2::char(2) as month_of_year_code
     , column3::smallint as quarter_of_year_nbr
     , column4::smallint as standard_year_day_qty
     , column5::smallint as leap_year_day_qty
     , column6::char(3) as month_of_year_abbr
     , column7::varchar(10) as month_of_year_name
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

--PostgreSQL 9.6
--select * from cal_gen.make_day_of_week_v;
--drop view if exists cal_gen.make_day_of_week_v;
create or replace view cal_gen.make_day_of_week_v as
select column2::smallint as day_of_week_iso_nbr
     , column1::smallint as day_of_week_common_nbr
     , column3::smallint as day_of_week_pgsql_nbr
     , column4::char(3) as day_of_week_abbr
     , column5::varchar(10) as day_of_week_name_eng
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

--PostgreSQL 9.6
--select * from cal_gen.make_gregorian_quarter_of_year_v;
--drop view if exists cal_gen.make_gregorian_quarter_of_year_v;
create or replace view cal_gen.make_gregorian_quarter_of_year_v as
select column1::smallint as quarter_of_year_nbr
     , column2::char(1) as quarter_of_year_code
     , column3::char(2) as quarter_of_year_abbr
     , column4::varchar(15) as quarter_of_year_name
from (
values
  (1,'1','Q1','First Quarter')
, (2,'2','Q2','Second Quarter')
, (3,'3','Q3','Third Quarter')
, (4,'4','Q4','Fourth Quarter')
) qoy;

--vacuum; -- 1.2 secs
--analyze; -- 1.9 secs

---------------------------------------------
--from here on down depends on views above --
---------------------------------------------

--PostgreSQL 9.6
--select * from cal_gen.make_gregorian_year_quarter_v;
--drop view if exists cal_gen.make_gregorian_year_quarter_v;
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
order by 1 --desc
--limit 100
;

--PostgreSQL 9.6
--select * from cal_gen.make_gregorian_year_month_v limit 100;
--drop view if exists cal_gen.make_gregorian_year_month_v;
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

--PostgreSQL 9.6
--select * from cal_gen.make_calendar_date_v; -- 17 secs!
--drop view if exists cal_gen.make_calendar_date_v;
create or replace view cal_gen.make_calendar_date_v as 
select dt.calendar_date
     , extract(isodow from dt.calendar_date)::smallint as day_of_week_iso_nbr
     , extract(week from dt.calendar_date)::smallint as week_of_year_nbr
     , (extract(isoyear from dt.calendar_date)::char(4)
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
order by 1 --desc
--limit 100
;

--PostgreSQL 9.6
--select * from cal_gen.make_year_week_v; -- 9 secs!
--drop view if exists cal_gen.make_year_week_v;
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
order by 1 --desc
--limit 100
;

select * from cal_gen.make_year_week_v where year_nbr = 2006 and week_of_year_nbr > 50;
