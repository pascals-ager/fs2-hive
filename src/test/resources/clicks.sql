CREATE TABLE `clicks`(
  `type` string,
  `id` string,
  `isp` string,
  `service_id` string,
  `campaign_id` string,
  `target_url` string,
  `traffic_id` string,
  `external_id` string,
  `happened` timestamp,
  `processed` timestamp,
  `processing_score` int,
  `processing_fingerprint` string,
  `processing_redirect_url` string,
  `processing_token` string,
  `processing_strategies` array<struct<strategy:string,score:int,decision:string,reason:string>>,
  `processing_decision` string,
  `request` struct<url: string, headers: map<string, string>, parameters: map<string, array<string>>, remote_address: string>,
  `device` map<string, string>,
  `country_iso_name` string,
  `city` string,
  `kafka_offset` bigint,
  `kafka_partition` int,
  `kafka_topic` string,
  `kafka_sql_ts` timestamp)
PARTITIONED BY (
  `year` int,
  `month` int,
  `day` int)
CLUSTERED BY (
  id)
INTO 6 BUCKETS
STORED AS ORC
TBLPROPERTIES (
  'orc.compress'='ZLIB',
  'orc.compression.strategy'='SPEED',
  'orc.create.index'='true',
  'orc.encoding.strategy'='SPEED',
  'transactional'='true');