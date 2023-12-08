-- TO BE EXECUTED WITH SYSADMIN ROLE PRIVILEGE ----

USE ROLE SYSADMIN;
use database air_oai;

/* CREATE CSV FILE FORMAT */

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  --FIELD_DELIMITER = '|'
  ESCAPE_UNENCLOSED_FIELD = 'NONE' 
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true
  COMPRESSION = AUTO
  ;

  alter file format csv_format
   set   ESCAPE_UNENCLOSED_FIELD ='/';
   
alter file format csv_format
   set   ESCAPE ='/' ;

   alter file format csv_format
   set   ESCAPE=NONE ;

   alter file format csv_format
   set   FIELD_DELIMITER = ',';


   

/* CREATE CSV FILE FORMAT STAGE */
  CREATE OR REPLACE STAGE csv_stage
  FILE_FORMAT = csv_format;
  ;

  

  