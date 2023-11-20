  select 
        weekday(started_at_ts) as week_day,
        count(*) as trips,
        sum(duration_min) as total_sum_duration 
        from {{ ref("mart__fact_all_bike_trips") }}
        group by week_day
