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
        select coalesce(max(last_updated_timestamp), cast('1900-01-01' as timestamp))
        from {{ this }}
    )
    {% endif %}

),

customers as (
    select
        dbt_scd_id,
        customer_id,
        dbt_valid_to,
        case
            when dbt_valid_from = min(dbt_valid_from) over (partition by customer_id)
                then cast('1900-01-01' as timestamp)
            else dbt_valid_from
        end as valid_from_adj
    from {{ ref('DimCustomers') }}
),

drivers as (
    select
        dbt_scd_id,
        driver_id,
        dbt_valid_to,
        case
            when dbt_valid_from = min(dbt_valid_from) over (partition by driver_id)
                then cast('1900-01-01' as timestamp)
            else dbt_valid_from
        end as valid_from_adj
    from {{ ref('DimDrivers') }}
),

vehicles as (
    select
        dbt_scd_id,
        vehicle_id,
        dbt_valid_to,
        case
            when dbt_valid_from = min(dbt_valid_from) over (partition by vehicle_id)
                then cast('1900-01-01' as timestamp)
            else dbt_valid_from
        end as valid_from_adj
    from {{ ref('DimVehicles') }}
),

enriched as (

    select
        t.trip_id,

        c.dbt_scd_id                as customer_key,
        d.dbt_scd_id                as driver_key,
        v.dbt_scd_id                as vehicle_key,

        t.customer_id,
        t.driver_id,
        t.vehicle_id,

        t.trip_start_time,
        t.trip_end_time,
        to_date(t.trip_start_time)  as trip_date,

        t.distance_km,
        t.fare_amount,
        round(
            (unix_timestamp(t.trip_end_time) - unix_timestamp(t.trip_start_time)) / 60.0
        , 2)                        as trip_duration_minutes,
        case
            when t.distance_km > 0 then round(t.fare_amount / t.distance_km, 2)
        end                         as fare_per_km,

        t.last_updated_timestamp,
        current_timestamp()         as dbt_loaded_at

    from trips t

    left join customers c
        on  t.customer_id = c.customer_id
        and t.trip_start_time >= c.valid_from_adj
        and t.trip_start_time <  c.dbt_valid_to

    left join drivers d
        on  t.driver_id = d.driver_id
        and t.trip_start_time >= d.valid_from_adj
        and t.trip_start_time <  d.dbt_valid_to

    left join vehicles v
        on  t.vehicle_id = v.vehicle_id
        and t.trip_start_time >= v.valid_from_adj
        and t.trip_start_time <  v.dbt_valid_to

)

select * from enriched