create table `simple_alerts` (
    `id` string,
    `msg` string )
    partitioned by (
        `continent` string,
        `country` string)
    clustered by (`id`)
    into 5 buckets 
    stored as orc 
    tblproperties("transactional"="true");