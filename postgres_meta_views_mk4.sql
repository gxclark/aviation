
create schema zed_meta;

-- select * from zed_meta.database_schema_descriptions_v;
-- CREATE VIEW zed_meta.database_schema_descriptions_v AS  
SELECT n.oid as schema_oid
     , max(n.nspname) AS schema_name
     , sum(sum_object_size_mb)::numeric(12,4) as sum_object_size_mb
     , sum(sum_total_size_mb - sum_object_size_mb)::numeric(12,4) as sum_index_size_mb
     , sum(sum_total_size_mb)::numeric(12,4) as sum_total_size_mb
     , max(d.description) as schema_descr
FROM pg_namespace 			n
LEFT JOIN pg_description 	d ON n.oid = d.objoid
LEFT JOIN (
	SELECT relnamespace
	     , sum((pg_relation_size(oid)::float / (1000)^2))::numeric(12,4) as sum_object_size_mb
	     , sum((pg_total_relation_size(oid)::float / (1000)^2))::numeric(12,4) as sum_total_size_mb
	FROM pg_class GROUP BY relnamespace
	) 						c ON n.oid = c.relnamespace
WHERE n.nspname not in ('pg_catalog','information_schema','pg_toast')
GROUP BY n.oid
ORDER BY n.nspname;

comment on schema air_faa_reg is 'Dimension and spatial data tables and associated objects for processing FAA data.';
comment on schema air_oai_dims is 'Dimension data tables and associated foreign tables for processing OAI dimension data.';
comment on schema air_oai_facts is 'Fact data and associated foreign tables and materialized views for processing OAI fact data.';
comment on schema air_oai_parts is 'Holds partition tables for data tables in schema air_oai_facts.';
comment on schema aviation is 'Views that simplify the presentation of schemata like air_ for analysis tools, such as MicroStrategy.';
comment on schema cal_gen is 'Gregorian and Julian calendar generation views to be able to adjust data time frame in calendar schema.';
comment on schema calendar is 'Gregorian and Julian calendar data as well as time transformation for ROLAP analysis.';
comment on schema geography is 'geo-political dimension and spatial data in support of aviation analysis.';
comment on schema zed_meta is 'Metadata and examples from documentation to assist with Postgres design and analysis.';
-- COMMENT ON TABLE mytable IS 'This is my table.'; 

-- select * from zed_meta.database_objects_v;
-- CREATE VIEW zed_meta.database_objects_v AS  
SELECT current_database() AS database_name
     , n.nspname AS schema_name
     , c.relname AS object_name
     , u.rolname AS owner_name
     , c.relkind
     , CASE WHEN (c.relkind = 'r'::char(1)) THEN 'table'
            WHEN (c.relkind = 'v'::char(1)) THEN 'view'
            WHEN (c.relkind = 'f'::char(1)) THEN 'file'
            WHEN (c.relkind = 'i'::char(1)) THEN 'index'
            WHEN (c.relkind = 'm'::char(1)) THEN 'mview'
            ELSE '__' END::varchar(10) AS object_type
     , (pg_relation_size(c.oid)::float / (1000)^2)::numeric(12,4) AS object_size_mb
     , ((pg_total_relation_size(c.oid)::float / (1000)^2) - (pg_relation_size(c.oid)::float / (1000)^2))::numeric(12,4) as index_size_mb
     , (pg_total_relation_size(c.oid)::float / (1000)^2)::numeric(12,4) AS total_size_mb
     , d.description AS object_descr
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_authid u ON c.relowner = u.oid
LEFT JOIN (
	SELECT pg_description.objoid, pg_description.classoid, pg_description.objsubid, pg_description.description
	FROM  pg_description WHERE pg_description.objsubid = 0
	) d ON c.oid = d.objoid
LEFT JOIN (
	SELECT pg_attribute.attrelid, max(pg_attribute.attnum) AS column_count
	FROM pg_attribute
	GROUP BY pg_attribute.attrelid
	) a ON c.oid = a.attrelid
WHERE n.nspname not in ('pg_catalog','information_schema','pg_toast')
and c.relkind = 'r'
ORDER BY 3,7 desc;

-- drop table air_oai_facts.airfare_survey_coupon_bak;
-- drop table air_oai_facts.airfare_survey_market_bak;
-- drop table air_oai_facts.airfare_survey_itinerary_bak;



