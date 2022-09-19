create database logs_analytics;

create table agg_fhv_trip (
    date_partition date,
    dispatching_base_num varchar,
    PUlocationID varchar,
    DOlocationID varchar,
    count_trip int,
    primary key(date_partition, dispatching_base_num, PUlocationID, DOlocationID)
);