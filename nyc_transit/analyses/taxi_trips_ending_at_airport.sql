select 
        count(*) as trips_ending_at_airport
        from {{ ref("mart__fact_all_taxi_trips") }} taxi_trip 
        join {{ ref("mart__dim_locations") }} loc
        on taxi_trip.dolocationid =  loc.locationid           
        where service_zone in ('Airports', 'EWR')