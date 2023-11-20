
with taxi_trip_by_weekday as 
(
  select 
         weekday(pickup_datetime) as week_day,
         count(*) as total_trips_count,   
         from {{ ref("mart__fact_all_taxi_trips") }} 
         group by week_day

),

taxi_trip_inter_borough_weekday as 
(
   select 
            weekday(pickup_datetime) as week_day,
            count(*) as trips_inter_borough_count,   
            from {{ ref("mart__fact_all_taxi_trips") }}  taxi_trip join {{ ref("mart__dim_locations") }} loc
            on taxi_trip.dolocationid =  loc.locationid 
            join {{ ref("mart__dim_locations") }} loc2
             on taxi_trip.pulocationid =  loc2.locationid         
            where   loc.borough <>  loc2.borough   
            group by  week_day     

),

final as 
(
  select 
     trip_inter_borough.week_day, trips_inter_borough_count, total_trips_count
     (trips_inter_borough_count /  total_trips_count) * 100 as percentage_inter_borough_with_respect_total
     from taxi_trip_by_weekday trip join
      taxi_trip_inter_borough_weekday trip_inter_borough
       on trip.week_day = trip_inter_borough.week_day

)

select * from final 




      
