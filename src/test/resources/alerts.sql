create table `alerts`(
     `id` int,
     `tracking_id` string,
     `msg` string,
     `continent` string,
     `country` string,
     `event_time` timestamp)
    partitioned by (
        `year` int,
        `month` int,
        `day` int)
    clustered by (
        `tracking_id`)
    into 5 buckets
    stored as orc
    tblproperties(
        "transactional"="true");
