----PUT TO BE EXECUTED FROM SNOWSQL CLI ----------------

snowsql: https://olverol-bn92760.snowflakecomputing.com/
snowsql -a xy59855.ap-northeast-1.aws 

snowsql -c example

--go to the script folder and exeucte the below:
snowsql -c my_dev -f 8_aviation_views_mk4.sql

list @csv_stage;

USE ROLE SYSADMIN;
use database air_oai;

----------------------------

PUT file://C:\mstr\dwh_benchmarking_data\dimensions\T_AIRCRAFT_TYPES.csv @csv_stage  OVERWRITE = TRUE;

ALTER WAREHOUSE COMPUTE_WH RESUME;

copy into AIR_OAI_DIMS.AIRCRAFT_TYPES_FDW FROM @csv_stage/T_AIRCRAFT_TYPES.csv 
ON_ERROR=CONTINUE;

REMOVE @csv_stage PATTERN='.*.csv.gz';


------------------

REMOVE @csv_stage pattern='.*CARR.*';

PUT file://C:\mstr\dwh_benchmarking_data\dimensions\T_CARRIER_DECODE.csv @csv_stage OVERWRITE = TRUE;


copy into AIR_OAI_DIMS.CARRIER_DECODE_FDW  FROM @csv_stage/T_CARRIER_DECODE.csv 
ON_ERROR=CONTINUE
FORCE = TRUE
;

-----------------------------------------------------------
REMOVE @csv_stage pattern='.*COUNT.*';

PUT file://C:\mstr\dwh_benchmarking_data\dimensions\T_WAC_COUNTRY_STATE.csv @csv_stage OVERWRITE = TRUE;


copy into AIR_OAI_DIMS.WAC_COUNTRY_STATE_FDW  FROM @csv_stage/T_WAC_COUNTRY_STATE.csv  ON_ERROR=CONTINUE FORCE = TRUE ;


-------------------------------------------------------


REMOVE @csv_stage pattern='.*CORD.*';

PUT file://C:\mstr\dwh_benchmarking_data\dimensions\T_MASTER_CORD.csv @csv_stage OVERWRITE = TRUE;


copy into AIR_OAI_DIMS.MASTER_CORD_FDW  FROM @csv_stage/T_MASTER_CORD.csv  ON_ERROR=CONTINUE FORCE = TRUE ;



---------------------------------facts file upload to stage ----------------------


PUT file://C:\mstr\dwh_benchmarking_data\FACTS\ticket\ @csv_stage\ticket\*.csv OVERWRITE = TRUE AUTO_COMPRESS=TRUE;
PUT file://C:\mstr\dwh_benchmarking_data\FACTS\ticket\ @csv_stage\coupon\*.csv OVERWRITE = TRUE AUTO_COMPRESS=TRUE;
PUT file://C:\mstr\dwh_benchmarking_data\FACTS\ticket\ @csv_stage\market\*.csv OVERWRITE = TRUE AUTO_COMPRESS=TRUE;

PUT file://C:\mstr\dwh_benchmarking_data\FACTS\market\T_T100_MARKET_ALL_CARRIER.csv @csv_stage OVERWRITE = TRUE;

PUT file://C:\mstr\dwh_benchmarking_data\FACTS\market\T_T100_SEGMENT_ALL_CARRIER.csv @csv_stage OVERWRITE = TRUE;

PUT file://C:\mstr\dwh_benchmarking_data\FACTS\otp\*.csv @csv_stage OVERWRITE = TRUE AUTO_COMPRESS=TRUE;



------------

/* files for air_faa_reg schema tables */


PUT file://C:\mstr\dwh_benchmarking_data\USDOT_FAA\*.txt @csv_stage OVERWRITE = TRUE AUTO_COMPRESS=TRUE;
PUT file://C:\mstr\dwh_benchmarking_data\USDOT_FAA\*.csv @csv_stage OVERWRITE = TRUE AUTO_COMPRESS=TRUE;



