drop table if exists `trips_data_all.production`;
create table `trips_data_all.production`
as
select * from `trips_data_all.stage_rides_yellow`
union all
select * from `trips_data_all.stage_rides_green`;
