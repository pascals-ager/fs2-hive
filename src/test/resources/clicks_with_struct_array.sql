CREATE TABLE `clicks_with_struct_array`(
  `type` string,
  `id` string,
  `referenced_event_id` string,
  `happened` timestamp,
  `processed` timestamp,
  `tracking_id` string,
  `processing_score` int,
  `source_attributes` struct<id:string, origin:string, data:struct<remote_address:string, url:string, headers:map<string, string>, parameters:map<string, array<string>>>, external_data: map<string, string>>,
  `event_data_strategies` array<struct<strategy:string,score:int,decision:string,reason:string>>)
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