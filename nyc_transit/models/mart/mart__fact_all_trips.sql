with all_trip_data as 
(
 
  select 'bike' as type, 
        started_at_ts,
        ended_at_ts,
        datediff('second', started_at_ts, ended_at_ts) as duration_sec,
        datediff('minute', started_at_ts, ended_at_ts) as duration_min
        from {{ ref("stg__bike_data") }}
 union all 
 select 'fhv' as type, 
        pickup_datetime as started_at_ts,
        dropoff_datetime as ended_at_ts,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min
        from {{ ref("stg__fhv_tripdata") }}
 union all
 select 'fhvhv' as type, 
        pickup_datetime as started_at_ts,
        dropoff_datetime as ended_at_ts,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min

  from {{ ref("stg__fhvhv_tripdata") }}
  union all
  select 'green' as type, 
        lpep_pickup_datetime as started_at_ts ,
        lpep_dropoff_datetime as ended_at_ts ,
        datediff('second', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_sec,
        datediff('minute', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_min
        from {{  ref("stg__green_tripdata") }}
  union all
   select 'yellow' as type, 
        tpep_pickup_datetime as started_at_ts,
        tpep_dropoff_datetime as ended_at_ts,
        datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_sec,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_min
        from {{ ref("stg__yellow_tripdata") }}
)

select * from all_trip_data 