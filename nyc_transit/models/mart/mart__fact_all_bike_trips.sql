with bike_trips as (
   select
      started_at_ts,
      ended_at_ts,
      datediff('second', started_at_ts, ended_at_ts) as duration_sec,
      datediff('minute', started_at_ts, ended_at_ts) as duration_min,
      start_station_id,
      end_station_id
    from 
    {{ ref('stg__bike_data') }}

)

select * from bike_trips