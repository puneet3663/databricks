--unity catalog
SHOW CATALOGS;

--schemas in database
show schemas in samples;

--table details
DESCRIBE DETAIL samples.accuweather.forecast_hourly_metric;
s3://system-tables-prod-ap-southeast-2-uc-metastore-bucket/metastore/a8838226-a493-4ac5-82da-4bf6fddd823a/tables/933c7cd4-39f5-44fb-ac7c-37a5801e37cf

COPY INTO 's3://your-bucket/' FROM `<shared_table>`;
  
SELECT * FROM information_schema.query_history 
ORDER BY start_time DESC 
LIMIT 50;

---------------------------------------------------------------------
SELECT
  `review`,
  `review_date`,
  `franchiseID`
FROM
  `samples`.`bakehouse`.`media_customer_reviews`
ORDER BY
  `review_date` DESC
LIMIT 1

---------------------------------------------------------------------

`<shared_catalog_name>.<schema_name>.<table_name>`;



select * from samples.accuweather.forecast_hourly_metric where datetime_valid_local = '2024-07-15T15:00:00.000'
and city_name = 'hong kong'

update samples.accuweather.forecast_hourly_metric 
set country_code='hkg'
where datetime_valid_local = '2024-07-15T15:00:00.000'
and city_name = 'hong kong'

PERMISSION_DENIED: User does not have MODIFY on Table 'samples.accuweather.forecast_hourly_metric'.
