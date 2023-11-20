with taxi_data as 
(
 
 select 'fhv' as type, 
        pickup_datetime,
        dropoff_datetime,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
        pulocationid,
        dolocationid 
        from {{  ref("stg__fhv_tripdata") }}
 union all
 select 'fhvhv' as type, 
        pickup_datetime,
        dropoff_datetime,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
        pulocationid,
        dolocationid
  from   {{ ref("stg__fhvhv_tripdata") }}
  union all
  select 'green' as type, 
        lpep_pickup_datetime as pickup_datetime ,
        lpep_dropoff_datetime as dropoff_datetime ,
        datediff('second', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_sec,
        datediff('minute', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_min,
        pulocationid,
        dolocationid 
        from    {{ ref("stg__green_tripdata") }}
  union all
   select 'yellow' as type, 
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_sec,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_min,
        pulocationid,
        dolocationid 
        from   {{ ref("stg__yellow_tripdata") }}
)

select * from taxi_data 