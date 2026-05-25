CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customername`              STRING,
  `email`                     STRING,
  `phone`                     STRING,
  `birthday`                  STRING,
  `serialnumber`              STRING,
  `registrationdate`          BIGINT,
  `lastupdatedate`            BIGINT,
  `sharewithresearchasofdate` BIGINT,
  `sharewithpublicasofdate`   BIGINT,
  `sharewithfriendsasofdate`  BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://satyam-stedi-lakehouse/customer_landing/'
TBLPROPERTIES ('has_encrypted_data' = 'false');
