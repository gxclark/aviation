
create schema calendar;

/* -- how to decide which years should be part of the calendar?
select year_nbr, year_from_date, extract(dow from year_from_date) as dow_nbr
from cal_gen.make_gregorian_year_v
where year_nbr > 2050 and year_nbr < 2110;
-- Postgres = "The day of the week as Sunday (0) to Saturday (6)"
*/
--1989 starts on Sunday
--1995 starts on Sunday, 1996 starts on Monday ... 1900
--2051 starts on Sunday, 2051 starts on Monday ... 2090

/* -- switch calendar views over to alpha calendar tables
create or replace view calendar.calendar_date_v as select *, 1::integer as calendar_date_qty from calendar.calendar_date_alpha;
create or replace view calendar.calendar_year_v as select *, 1::integer as calendar_year_qty from calendar.gregorian_year_alpha;
create or replace view calendar.day_of_week_v as select *, 1::integer as day_of_week_qty from calendar.day_of_week_alpha;
create or replace view calendar.hour_of_day_v as select *, 1::integer as hour_of_day_qty from calendar.hour_of_day_alpha;
create or replace view calendar.minute_of_hour_v as select *, 1::integer as minute_of_hour_qty from calendar.minute_of_hour_alpha;
create or replace view calendar.month_of_year_v as select *, 1::integer as month_of_year_qty from calendar.gregorian_month_of_year_alpha;
create or replace view calendar.quarter_of_year_v as select *, 1::integer as quarter_of_year_qty from calendar.gregorian_quarter_of_year_alpha;
create or replace view calendar.year_month_v as select *, 1::integer as year_month_qty from calendar.gregorian_year_month_alpha;
create or replace view calendar.year_quarter_v as select *, 1::integer as year_quarter_qty from calendar.gregorian_year_quarter_alpha;
create or replace view calendar.year_week_v as select *, 1::integer as year_week_qty from calendar.year_week_alpha;
*/

/* -- switch calendar views over to beta calendar tables
create or replace view calendar.calendar_date_v as select *, 1::integer as calendar_date_qty from calendar.calendar_date_beta;
create or replace view calendar.calendar_year_v as select *, 1::integer as calendar_year_qty from calendar.gregorian_year_beta;
create or replace view calendar.day_of_week_v as select *, 1::integer as day_of_week_qty from calendar.day_of_week_beta;
create or replace view calendar.hour_of_day_v as select *, 1::integer as hour_of_day_qty from calendar.hour_of_day_beta;
create or replace view calendar.minute_of_hour_v as select *, 1::integer as minute_of_hour_qty from calendar.minute_of_hour_beta;
create or replace view calendar.month_of_year_v as select *, 1::integer as month_of_year_qty from calendar.gregorian_month_of_year_beta;
create or replace view calendar.quarter_of_year_v as select *, 1::integer as quarter_of_year_qty from calendar.gregorian_quarter_of_year_beta;
create or replace view calendar.year_month_v as select *, 1::integer as year_month_qty from calendar.gregorian_year_month_beta;
create or replace view calendar.year_quarter_v as select *, 1::integer as year_quarter_qty from calendar.gregorian_year_quarter_beta;
create or replace view calendar.year_week_v as select *, 1::integer as year_week_qty from calendar.year_week_beta;
*/

/* -- careful, dropping views removes them from users!
drop view if exists calendar.calendar_date_v;
drop view if exists calendar.calendar_year_v;
drop view if exists calendar.day_of_week_v;
drop view if exists calendar.hour_of_day_v;
drop view if exists calendar.minute_of_hour_v;
drop view if exists calendar.month_of_year_v;
drop view if exists calendar.quarter_of_year_v;
drop view if exists calendar.year_month_v;
drop view if exists calendar.year_quarter_v;
drop view if exists calendar.year_week_v;
*/

---------------------------
-- alpha calendar tables --
---------------------------

/* -- drop alpha calendar table foreign keys
ALTER TABLE calendar.calendar_date_alpha DROP CONSTRAINT calendar_date_year_week_alpha_fk;
ALTER TABLE calendar.calendar_date_alpha DROP CONSTRAINT calendar_date_year_month_alpha_fk;
ALTER TABLE calendar.calendar_date_alpha DROP CONSTRAINT calendar_date_day_of_week_alpha_fk;
ALTER TABLE calendar.gregorian_month_of_year_alpha DROP CONSTRAINT gregorian_month_of_year_quarter_of_year_alpha_fk;
ALTER TABLE calendar.gregorian_year_month_alpha DROP CONSTRAINT gregorian_year_month_year_quarter_alpha_fk;
ALTER TABLE calendar.gregorian_year_month_alpha DROP CONSTRAINT gregorian_year_month_month_of_year_alpha_fk;
ALTER TABLE calendar.gregorian_year_quarter_alpha DROP CONSTRAINT gregorian_year_quarter_year_alpha_fk;
ALTER TABLE calendar.gregorian_year_quarter_alpha DROP CONSTRAINT gregorian_year_quarter_quarter_of_year_alpha_fk;
ALTER TABLE calendar.year_week_alpha DROP CONSTRAINT year_week_gregorian_year_alpha_fk;
*/

/* -- drop alpha calendar tables
drop table if exists calendar.calendar_date_alpha;
drop table if exists calendar.day_of_week_alpha;
drop table if exists calendar.gregorian_month_of_year_alpha;
drop table if exists calendar.gregorian_quarter_of_year_alpha;
drop table if exists calendar.gregorian_year_alpha;
drop table if exists calendar.gregorian_year_month_alpha;
drop table if exists calendar.gregorian_year_quarter_alpha;
drop table if exists calendar.hour_of_day_alpha;
drop table if exists calendar.minute_of_hour_alpha;
drop table if exists calendar.year_week_alpha;
*/

/* -- generate alpha calendar tables
create table calendar.day_of_week_alpha as select * from cal_gen.make_day_of_week_v;
create table calendar.hour_of_day_alpha as select * from cal_gen.make_hour_of_day_v;
create table calendar.minute_of_hour_alpha as select * from cal_gen.make_minute_of_hour_v;
create table calendar.gregorian_month_of_year_alpha as select * from cal_gen.make_gregorian_month_of_year_v;
create table calendar.gregorian_quarter_of_year_alpha as select * from cal_gen.make_gregorian_quarter_of_year_v;
create table calendar.gregorian_year_alpha as select * from cal_gen.make_gregorian_year_v where year_nbr between 1900 and 2090;
create table calendar.gregorian_year_quarter_alpha as select * from cal_gen.make_gregorian_year_quarter_v where year_nbr between 1900 and 2090;
create table calendar.gregorian_year_month_alpha as select * from cal_gen.make_gregorian_year_month_v where year_nbr between 1900 and 2090;
create table calendar.year_week_alpha as select * from cal_gen.make_year_week_v where year_nbr between 1900 and 2090;
create table calendar.calendar_date_alpha as select * from cal_gen.make_calendar_date_v where year_nbr between 1900 and 2090;
*/

/* -- generate keys for alpha calendar tables
ALTER TABLE calendar.calendar_date_alpha ADD CONSTRAINT calendar_date_alpha_pk  PRIMARY KEY (calendar_date);
ALTER TABLE calendar.day_of_week_alpha ADD CONSTRAINT day_of_week_alpha_pk  PRIMARY KEY (day_of_week_iso_nbr);
ALTER TABLE calendar.gregorian_month_of_year_alpha ADD CONSTRAINT gregorian_month_of_year_alpha_pk  PRIMARY KEY (month_of_year_nbr);
ALTER TABLE calendar.gregorian_year_alpha ADD CONSTRAINT gregorian_year_alpha_pk  PRIMARY KEY (year_nbr);
ALTER TABLE calendar.hour_of_day_alpha ADD CONSTRAINT hour_of_day_alpha_pk  PRIMARY KEY (hour_of_day_nbr);
ALTER TABLE calendar.minute_of_hour_alpha ADD CONSTRAINT minute_of_hour_alpha_pk  PRIMARY KEY (minute_of_hour_nbr);
ALTER TABLE calendar.gregorian_quarter_of_year_alpha ADD CONSTRAINT gregorian_quarter_of_year_alpha_pk  PRIMARY KEY (quarter_of_year_nbr);
ALTER TABLE calendar.gregorian_year_month_alpha ADD CONSTRAINT gregorian_year_month_alpha_pk  PRIMARY KEY (year_month_nbr);
ALTER TABLE calendar.gregorian_year_quarter_alpha ADD CONSTRAINT gregorian_year_quarter_alpha_pk  PRIMARY KEY (year_quarter_nbr);
ALTER TABLE calendar.year_week_alpha ADD CONSTRAINT year_week_alpha_pk  PRIMARY KEY (year_week_nbr);
CREATE UNIQUE INDEX gregorian_month_of_year_alpha_ak1 ON calendar.gregorian_month_of_year_alpha (month_of_year_code);
CREATE UNIQUE INDEX gregorian_year_quarter_alpha_ak1 ON calendar.gregorian_year_quarter_alpha (year_quarter_standard_code);
CREATE UNIQUE INDEX year_week_alpha_ak1 ON calendar.year_week_alpha (year_nbr, week_of_year_nbr);
CREATE INDEX calendar_date_year_week_alpha_if1 ON calendar.calendar_date_alpha (year_week_nbr);
CREATE INDEX calendar_date_year_month_alpha_if2 ON calendar.calendar_date_alpha (year_month_nbr);
CREATE INDEX calendar_date_day_of_week_alpha_if3 ON calendar.calendar_date_alpha (day_of_week_iso_nbr);
CREATE INDEX gregorian_month_of_year_alpha_quarter_of_year_if1 ON calendar.gregorian_month_of_year_alpha (quarter_of_year_nbr);
CREATE INDEX gregorian_year_month_year_quarter_alpha_if1 ON calendar.gregorian_year_month_alpha (year_quarter_nbr);
CREATE INDEX gregorian_year_month_of_year_alpha_if1 ON calendar.gregorian_year_month_alpha (month_of_year_nbr);
CREATE INDEX gregorian_year_quarter_year_alpha_if1 ON calendar.gregorian_year_quarter_alpha (year_nbr);
CREATE INDEX gregorian_year_quarter_of_year_alpha_if2 ON calendar.gregorian_year_quarter_alpha (quarter_of_year_nbr);
CREATE INDEX year_week_alpha_if1 ON calendar.year_week_alpha (year_nbr);
*/

--ERROR: insert or update on table "calendar_date_alpha" 
--violates foreign key constraint 
--"calendar_date_year_week_alpha_fk" Detail: Key 
--(year_week_nbr)=(209201) is not present in table 
--"year_week_alpha". 

/* -- add foreign keys to alpha calendar tables
ALTER TABLE calendar.calendar_date_alpha ADD CONSTRAINT calendar_date_year_week_alpha_fk 
FOREIGN KEY (year_week_nbr) REFERENCES calendar.year_week_alpha (year_week_nbr);
ALTER TABLE calendar.calendar_date_alpha ADD CONSTRAINT calendar_date_year_month_alpha_fk 
FOREIGN KEY (year_month_nbr) REFERENCES calendar.gregorian_year_month_alpha (year_month_nbr);
ALTER TABLE calendar.calendar_date_alpha ADD CONSTRAINT calendar_date_day_of_week_alpha_fk 
FOREIGN KEY (day_of_week_iso_nbr) REFERENCES calendar.day_of_week_alpha (day_of_week_iso_nbr);
ALTER TABLE calendar.gregorian_month_of_year_alpha ADD CONSTRAINT gregorian_month_of_year_quarter_of_year_alpha_fk  
FOREIGN KEY (quarter_of_year_nbr) REFERENCES calendar.gregorian_quarter_of_year_alpha (quarter_of_year_nbr);
ALTER TABLE calendar.gregorian_year_month_alpha ADD CONSTRAINT gregorian_year_month_year_quarter_alpha_fk 
FOREIGN KEY (year_quarter_nbr) REFERENCES calendar.gregorian_year_quarter_alpha (year_quarter_nbr);
ALTER TABLE calendar.gregorian_year_month_alpha ADD CONSTRAINT gregorian_year_month_month_of_year_alpha_fk 
FOREIGN KEY (month_of_year_nbr) REFERENCES calendar.gregorian_month_of_year_alpha (month_of_year_nbr);
ALTER TABLE calendar.gregorian_year_quarter_alpha ADD CONSTRAINT gregorian_year_quarter_year_alpha_fk 
FOREIGN KEY (year_nbr) REFERENCES calendar.gregorian_year_alpha (year_nbr);
ALTER TABLE calendar.gregorian_year_quarter_alpha ADD CONSTRAINT gregorian_year_quarter_quarter_of_year_alpha_fk 
FOREIGN KEY (quarter_of_year_nbr) REFERENCES calendar.gregorian_quarter_of_year_alpha (quarter_of_year_nbr);
ALTER TABLE calendar.year_week_alpha ADD CONSTRAINT year_week_gregorian_year_alpha_fk  
FOREIGN KEY (year_nbr) REFERENCES calendar.gregorian_year_alpha (year_nbr);
*/

/* -- add comments for alpha calendar tables
COMMENT ON TABLE calendar.calendar_date_alpha IS 'A calendar day represents the spin of the earth on its axis, providing a day and night cycle.';
COMMENT ON COLUMN calendar.calendar_date_alpha.day_of_week_iso_nbr IS 'ISO defines Monday as the first day of the week.';
COMMENT ON COLUMN calendar.calendar_date_alpha.year_week_nbr IS 'Weeks always have seven days, and each is assigned a number within a Year; week 1 contains January 1 for that year.';
COMMENT ON TABLE calendar.day_of_week_alpha IS 'Monday is the first day of the working week, ISO 2105/8601.';
COMMENT ON COLUMN calendar.day_of_week_alpha.day_of_week_iso_nbr IS 'ISO defines Monday as the first day of the week.';
COMMENT ON COLUMN calendar.day_of_week_alpha.day_of_week_common_nbr IS 'This number begins with Sunday as 1, and is in common usage.';
COMMENT ON COLUMN calendar.day_of_week_alpha.day_of_week_pgsql_nbr IS 'PostgreSQL functions list Sunday as 0, and Saturday as 6.';
COMMENT ON COLUMN calendar.day_of_week_alpha.day_of_week_abbr IS 'Standard abbreviation of the day of week (in English).';
COMMENT ON COLUMN calendar.day_of_week_alpha.day_of_week_name_eng IS 'The full name of the day of the week (in English).';
COMMENT ON TABLE calendar.gregorian_month_of_year_alpha IS 'Gregorian Years have 12 months, and have since it evolved from Roman years.';
COMMENT ON COLUMN calendar.gregorian_month_of_year_alpha.standard_year_day_qty IS 'The number of Days within this month for a Standard Year.';
COMMENT ON COLUMN calendar.gregorian_month_of_year_alpha.leap_year_day_qty IS 'The number of days within this month during a Leap Year.';
COMMENT ON COLUMN calendar.gregorian_month_of_year_alpha.month_of_year_name IS 'The word which identifies this month.';
COMMENT ON TABLE calendar.gregorian_year_alpha IS 'A year represents the number of orbits by the earth around the sun within the Common Era (CE), defined by Pope Gregory XIII in October 1582.';
COMMENT ON COLUMN calendar.gregorian_year_alpha.year_nbr IS 'A modern year is a four digit number.';
COMMENT ON TABLE calendar.hour_of_day_alpha IS 'Our 24-hour day comes from the ancient Egyptians who divided day-time into 10 hours they measured with devices such as shadow clocks, and added a twilight hour at the beginning and another one at the end of the day-time.';
COMMENT ON COLUMN calendar.hour_of_day_alpha.period_code IS 'This specifies a subdivision within a day, such as morning, afternoon, evening or night.';
COMMENT ON TABLE calendar.minute_of_hour_alpha IS 'The division of the hour into 60 minutes and of the minute into 60 seconds comes from ancient civilizations -  Babylonians, Sumerians and Egyptians - who had different numbering systems; base 12 (duodecimal) and base 60 (sexagesimal) for mathematics.';
COMMENT ON TABLE calendar.gregorian_quarter_of_year_alpha IS 'A quarter is a standard calendar interval consisting of three calendar months, and generally analagous to a "season", which is in keeping with the agricultural purpose of the calendar.';
COMMENT ON TABLE calendar.gregorian_year_month_alpha IS 'This is the natural list of months within a specific year.';
COMMENT ON TABLE calendar.gregorian_year_quarter_alpha IS 'This is the natural list of quarters within a specific year.';
COMMENT ON COLUMN calendar.gregorian_year_quarter_alpha.year_nbr IS 'The year containing this year-quarter.';
COMMENT ON TABLE calendar.year_week_alpha IS 'This is the nautral list of weeks within a specific year.';
COMMENT ON COLUMN calendar.year_week_alpha.year_week_nbr IS 'The numbered weeks within a year.';
COMMENT ON COLUMN calendar.year_week_alpha.year_nbr IS 'The year that contains this week.';
*/

--------------------------
-- beta calendar tables --
--------------------------

/* -- drop beta calendar table foreign keys
ALTER TABLE calendar.calendar_date_beta DROP CONSTRAINT calendar_date_year_week_beta_fk;
ALTER TABLE calendar.calendar_date_beta DROP CONSTRAINT calendar_date_year_month_beta_fk;
ALTER TABLE calendar.calendar_date_beta DROP CONSTRAINT calendar_date_day_of_week_beta_fk;
ALTER TABLE calendar.gregorian_month_of_year_beta DROP CONSTRAINT gregorian_month_of_year_quarter_of_year_beta_fk;
ALTER TABLE calendar.gregorian_year_month_beta DROP CONSTRAINT gregorian_year_month_year_quarter_beta_fk;
ALTER TABLE calendar.gregorian_year_month_beta DROP CONSTRAINT gregorian_year_month_month_of_year_beta_fk;
ALTER TABLE calendar.gregorian_year_quarter_beta DROP CONSTRAINT gregorian_year_quarter_year_beta_fk;
ALTER TABLE calendar.gregorian_year_quarter_beta DROP CONSTRAINT gregorian_year_quarter_quarter_of_year_beta_fk;
ALTER TABLE calendar.year_week_beta DROP CONSTRAINT year_week_gregorian_year_beta_fk;
*/

/* -- drop beta calendar tables
drop table if exists calendar.calendar_date_beta;
drop table if exists calendar.day_of_week_beta;
drop table if exists calendar.gregorian_month_of_year_beta;
drop table if exists calendar.gregorian_quarter_of_year_beta;
drop table if exists calendar.gregorian_year_beta;
drop table if exists calendar.gregorian_year_month_beta;
drop table if exists calendar.gregorian_year_quarter_beta;
drop table if exists calendar.hour_of_day_beta;
drop table if exists calendar.minute_of_hour_beta;
drop table if exists calendar.year_week_beta;
*/

/* -- generate beta calendar tables
create table calendar.day_of_week_beta as select * from cal_gen.make_day_of_week_v;
create table calendar.hour_of_day_beta as select * from cal_gen.make_hour_of_day_v;
create table calendar.minute_of_hour_beta as select * from cal_gen.make_minute_of_hour_v;
create table calendar.gregorian_month_of_year_beta as select * from cal_gen.make_gregorian_month_of_year_v;
create table calendar.gregorian_quarter_of_year_beta as select * from cal_gen.make_gregorian_quarter_of_year_v;
create table calendar.gregorian_year_beta as select * from cal_gen.make_gregorian_year_v where year_nbr between 1900 and 2090;
create table calendar.gregorian_year_quarter_beta as select * from cal_gen.make_gregorian_year_quarter_v where year_nbr between 1900 and 2090;
create table calendar.gregorian_year_month_beta as select * from cal_gen.make_gregorian_year_month_v where year_nbr between 1900 and 2090;
create table calendar.year_week_beta as select * from cal_gen.make_year_week_v where year_nbr between 1900 and 2090;
create table calendar.calendar_date_beta as select * from cal_gen.make_calendar_date_v where year_nbr between 1900 and 2090;
*/

/* -- generate keys for beta calendar tables
ALTER TABLE calendar.calendar_date_beta ADD CONSTRAINT calendar_date_beta_pk  PRIMARY KEY (calendar_date);
ALTER TABLE calendar.day_of_week_beta ADD CONSTRAINT day_of_week_beta_pk  PRIMARY KEY (day_of_week_iso_nbr);
ALTER TABLE calendar.gregorian_month_of_year_beta ADD CONSTRAINT gregorian_month_of_year_beta_pk  PRIMARY KEY (month_of_year_nbr);
ALTER TABLE calendar.gregorian_year_beta ADD CONSTRAINT gregorian_year_beta_pk  PRIMARY KEY (year_nbr);
ALTER TABLE calendar.hour_of_day_beta ADD CONSTRAINT hour_of_day_beta_pk  PRIMARY KEY (hour_of_day_nbr);
ALTER TABLE calendar.minute_of_hour_beta ADD CONSTRAINT minute_of_hour_beta_pk  PRIMARY KEY (minute_of_hour_nbr);
ALTER TABLE calendar.gregorian_quarter_of_year_beta ADD CONSTRAINT gregorian_quarter_of_year_beta_pk  PRIMARY KEY (quarter_of_year_nbr);
ALTER TABLE calendar.gregorian_year_month_beta ADD CONSTRAINT gregorian_year_month_beta_pk  PRIMARY KEY (year_month_nbr);
ALTER TABLE calendar.gregorian_year_quarter_beta ADD CONSTRAINT gregorian_year_quarter_beta_pk  PRIMARY KEY (year_quarter_nbr);
ALTER TABLE calendar.year_week_beta ADD CONSTRAINT year_week_beta_pk  PRIMARY KEY (year_week_nbr);
CREATE UNIQUE INDEX gregorian_month_of_year_beta_ak1 ON calendar.gregorian_month_of_year_beta (month_of_year_code);
CREATE UNIQUE INDEX gregorian_year_quarter_beta_ak1 ON calendar.gregorian_year_quarter_beta (year_quarter_standard_code);
CREATE UNIQUE INDEX year_week_beta_ak1 ON calendar.year_week_beta (year_nbr, week_of_year_nbr);
CREATE INDEX calendar_date_year_week_beta_if1 ON calendar.calendar_date_beta (year_week_nbr);
CREATE INDEX calendar_date_year_month_beta_if2 ON calendar.calendar_date_beta (year_month_nbr);
CREATE INDEX calendar_date_day_of_week_beta_if3 ON calendar.calendar_date_beta (day_of_week_iso_nbr);
CREATE INDEX gregorian_month_of_year_beta_quarter_of_year_if1 ON calendar.gregorian_month_of_year_beta (quarter_of_year_nbr);
CREATE INDEX gregorian_year_month_year_quarter_beta_if1 ON calendar.gregorian_year_month_beta (year_quarter_nbr);
CREATE INDEX gregorian_year_month_of_year_beta_if1 ON calendar.gregorian_year_month_beta (month_of_year_nbr);
CREATE INDEX gregorian_year_quarter_year_beta_if1 ON calendar.gregorian_year_quarter_beta (year_nbr);
CREATE INDEX gregorian_year_quarter_of_year_beta_if2 ON calendar.gregorian_year_quarter_beta (quarter_of_year_nbr);
CREATE INDEX year_week_beta_if1 ON calendar.year_week_beta (year_nbr);
*/

/* -- add foreign keys to beta calendar tables
ALTER TABLE calendar.calendar_date_beta ADD CONSTRAINT calendar_date_year_week_beta_fk 
FOREIGN KEY (year_week_nbr) REFERENCES calendar.year_week_beta (year_week_nbr);

--ERROR: insert or update on table "calendar_date_beta" violates foreign key constraint "calendar_date_year_week_beta_fk" 
--Detail: Key (year_week_nbr)=(198852) is not present in table "year_week_beta". 

ALTER TABLE calendar.calendar_date_beta ADD CONSTRAINT calendar_date_year_month_beta_fk 
FOREIGN KEY (year_month_nbr) REFERENCES calendar.gregorian_year_month_beta (year_month_nbr);
ALTER TABLE calendar.calendar_date_beta ADD CONSTRAINT calendar_date_day_of_week_beta_fk 
FOREIGN KEY (day_of_week_iso_nbr) REFERENCES calendar.day_of_week_beta (day_of_week_iso_nbr);
ALTER TABLE calendar.gregorian_month_of_year_beta ADD CONSTRAINT gregorian_month_of_year_quarter_of_year_beta_fk  
FOREIGN KEY (quarter_of_year_nbr) REFERENCES calendar.gregorian_quarter_of_year_beta (quarter_of_year_nbr);
ALTER TABLE calendar.gregorian_year_month_beta ADD CONSTRAINT gregorian_year_month_year_quarter_beta_fk 
FOREIGN KEY (year_quarter_nbr) REFERENCES calendar.gregorian_year_quarter_beta (year_quarter_nbr);
ALTER TABLE calendar.gregorian_year_month_beta ADD CONSTRAINT gregorian_year_month_month_of_year_beta_fk 
FOREIGN KEY (month_of_year_nbr) REFERENCES calendar.gregorian_month_of_year_beta (month_of_year_nbr);
ALTER TABLE calendar.gregorian_year_quarter_beta ADD CONSTRAINT gregorian_year_quarter_year_beta_fk 
FOREIGN KEY (year_nbr) REFERENCES calendar.gregorian_year_beta (year_nbr);
ALTER TABLE calendar.gregorian_year_quarter_beta ADD CONSTRAINT gregorian_year_quarter_quarter_of_year_beta_fk 
FOREIGN KEY (quarter_of_year_nbr) REFERENCES calendar.gregorian_quarter_of_year_beta (quarter_of_year_nbr);
ALTER TABLE calendar.year_week_beta ADD CONSTRAINT year_week_gregorian_year_beta_fk  
FOREIGN KEY (year_nbr) REFERENCES calendar.gregorian_year_beta (year_nbr);
*/

/* add comments for for beta calendar tables
COMMENT ON TABLE calendar.calendar_date_beta IS 'A calendar day represents the spin of the earth on its axis, providing a day and night cycle.';
COMMENT ON COLUMN calendar.calendar_date_beta.day_of_week_iso_nbr IS 'ISO defines Monday as the first day of the week.';
COMMENT ON COLUMN calendar.calendar_date_beta.year_week_nbr IS 'Weeks always have seven days, and each is assigned a number within a Year; week 1 contains January 1 for that year.';
COMMENT ON TABLE calendar.day_of_week_beta IS 'Monday is the first day of the working week, ISO 2105/8601.';
COMMENT ON COLUMN calendar.day_of_week_beta.day_of_week_iso_nbr IS 'ISO defines Monday as the first day of the week.';
COMMENT ON COLUMN calendar.day_of_week_beta.day_of_week_common_nbr IS 'This number begins with Sunday as 1, and is in common usage.';
COMMENT ON COLUMN calendar.day_of_week_beta.day_of_week_pgsql_nbr IS 'PostgreSQL functions list Sunday as 0, and Saturday as 6.';
COMMENT ON COLUMN calendar.day_of_week_beta.day_of_week_abbr IS 'Standard abbreviation of the day of week (in English).';
COMMENT ON COLUMN calendar.day_of_week_beta.day_of_week_name_eng IS 'The full name of the day of the week (in English).';
COMMENT ON TABLE calendar.gregorian_month_of_year_beta IS 'Gregorian Years have 12 months, and have since it evolved from Roman years.';
COMMENT ON COLUMN calendar.gregorian_month_of_year_beta.standard_year_day_qty IS 'The number of Days within this month for a Standard Year.';
COMMENT ON COLUMN calendar.gregorian_month_of_year_beta.leap_year_day_qty IS 'The number of days within this month during a Leap Year.';
COMMENT ON COLUMN calendar.gregorian_month_of_year_beta.month_of_year_name IS 'The word which identifies this month.';
COMMENT ON TABLE calendar.gregorian_year_beta IS 'A year represents the number of orbits by the earth around the sun within the Common Era (CE), defined by Pope Gregory XIII in October 1582.';
COMMENT ON COLUMN calendar.gregorian_year_beta.year_nbr IS 'A modern year is a four digit number.';
COMMENT ON TABLE calendar.hour_of_day_beta IS 'Our 24-hour day comes from the ancient Egyptians who divided day-time into 10 hours they measured with devices such as shadow clocks, and added a twilight hour at the beginning and another one at the end of the day-time.';
COMMENT ON COLUMN calendar.hour_of_day_beta.period_code IS 'This specifies a subdivision within a day, such as morning, afternoon, evening or night.';
COMMENT ON TABLE calendar.minute_of_hour_beta IS 'The division of the hour into 60 minutes and of the minute into 60 seconds comes from ancient civilizations -  Babylonians, Sumerians and Egyptians - who had different numbering systems; base 12 (duodecimal) and base 60 (sexagesimal) for mathematics.';
COMMENT ON TABLE calendar.gregorian_quarter_of_year_beta IS 'A quarter is a standard calendar interval consisting of three calendar months, and generally analagous to a "season", which is in keeping with the agricultural purpose of the calendar.';
COMMENT ON TABLE calendar.gregorian_year_month_beta IS 'This is the natural list of months within a specific year.';
COMMENT ON TABLE calendar.gregorian_year_quarter_beta IS 'This is the natural list of quarters within a specific year.';
COMMENT ON COLUMN calendar.gregorian_year_quarter_beta.year_nbr IS 'The year containing this year-quarter.';
COMMENT ON TABLE calendar.year_week_beta IS 'This is the nautral list of weeks within a specific year.';
COMMENT ON COLUMN calendar.year_week_beta.year_week_nbr IS 'The numbered weeks within a year.';
COMMENT ON COLUMN calendar.year_week_beta.year_nbr IS 'The year that contains this week.';
*/
