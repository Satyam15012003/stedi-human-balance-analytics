CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` (
  `sensorreadingtime` BIGINT,
  `serialnumber`      STRING,
  `distancefromobject` INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://satyam-stedi-lakehouse/step_trainer/landing/'
TBLPROPERTIES ('has_encrypted_data' = 'false');
