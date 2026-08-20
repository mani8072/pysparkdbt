{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'merge',
        file_format = 'delta',
        on_schema_change = 'append_new_columns'
    )
}}

with trips as (
 
    select *
    from {{ ref('trips') }}
 
    {% if is_incremental() %}
    where last_updated_timestamp > (
        select coalesce(max(last_updated_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
 
),
enriched as (
 
    select
        t.trip_id,
        c.dbt_scd_id as customer_key,
        d.dbt_scd_id as driver_key,
        v.dbt_scd_id as vehicle_key,

        t.customer_id,
        t.driver_id,
        t.vehicle_id,

        t.trip_start_time,
        t.trip_end_time,
        to_date(t.trip_start_time)  as trip_date,
 
        t.distance_km,
        t.fare_amount,
        round(
            (unix_timestamp(t.trip_end_time) - unix_timestamp(t.trip_start_time)) / 60.0, 2) as trip_duration_minutes,
        case
            when t.distance_km > 0 then round(t.fare_amount / t.distance_km, 2)
        end as fare_per_km,
        t.last_updated_timestamp,
        current_timestamp() as dbt_loaded_at
    from trips t
    
    left join {{ ref('DimCustomers') }} c
        on  t.customer_id = c.customer_id
        and t.trip_start_time >= c.dbt_valid_from
        and t.trip_start_time <  c.dbt_valid_to
 
    left join {{ ref('DimDrivers') }} d
        on  t.driver_id = d.driver_id
        and t.trip_start_time >= d.dbt_valid_from
        and t.trip_start_time <  d.dbt_valid_to
 
    left join {{ ref('DimVehicles') }} v
        on  t.vehicle_id = v.vehicle_id
        and t.trip_start_time >= v.dbt_valid_from
        and t.trip_start_time <  v.dbt_valid_to
 
)
 
select * from enriched