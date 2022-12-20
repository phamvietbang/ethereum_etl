CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`lp_tokens` (
  `lp_token` string,
  `token_a` string,
  `token_b` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://bangbich123/onus/lp_tokens/'
TBLPROPERTIES ('classification' = 'json');