{{ config(materialized='table') }}

with date_spine as (
    -- TODO, we arbitrarily start and end, update to be relative to current date:
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=dbt_utils.dateadd('day', 3 * -365 , dbt_utils.current_timestamp()),
        end_date=dbt_utils.dateadd('day', 3 * 365 , dbt_utils.current_timestamp())
       )
    }}
),

base_dates as (
    select
        date(d.date_day) as date_day
    from
        date_spine d
),

dates_with_prior_year_dates as (
    select
        cast(d.date_day as date) as date_day,
        cast({{ dbt_utils.dateadd('year', -1 , 'd.date_day') }} as date) as prior_year_date_day,
        cast({{ dbt_utils.dateadd('day', -364 , 'd.date_day') }} as date) as prior_year_over_year_date_day,
        cast(d.date_day as date) as mon_sun_date_offset,
        cast({{ week_trunc('d.date_day') }} as date) as mon_sun_week_start,
        cast(d.date_day + 1 as date) as sun_sat_date_offset,
        cast({{ week_trunc('d.date_day', 1) }} as date) as sun_sat_week_start
    from
    	base_dates d
)
select
    d.date_day,
    EXTRACT(YEAR FROM d.date_day)::integer as date_year,
    EXTRACT(MONTH FROM d.date_day)::integer as date_month_of_year,
    EXTRACT(DAY FROM d.date_day)::integer as date_day_of_month,
    cast({{ dbt_utils.dateadd('day', -1 , 'd.date_day') }} as date) as prior_date_day,
    cast({{ dbt_utils.dateadd('day', 1 , 'd.date_day') }} as date) as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,
    cast(
            case
                when {{ date_part('dow', 'd.date_day') }} = 0 then 7
                else {{ date_part('dow', 'd.date_day') }}
            end
        as {{ dbt_utils.type_int() }}
    ) as day_of_week,

    {{ day_name('d.date_day', short=false) }} as day_of_week_name,
    {{ day_name('d.date_day', short=true) }} as day_of_week_name_short,
    cast({{ date_part('day', 'd.date_day') }} as {{ dbt_utils.type_int() }}) as day_of_month,
    cast({{ date_part('doy', 'd.date_day') }} as {{ dbt_utils.type_int() }}) as day_of_year,

    -- Default week (Mon - Sun) number, and a week key to sort
    mon_sun_week_start,
    cast((to_char(d.mon_sun_week_start, 'YYYY') 
            || right('0' || {{ date_part('week', 'd.mon_sun_date_offset') }}, 2)) 
        as {{ dbt_utils.type_int() }}) as week_key,
    EXTRACT(YEAR FROM d.mon_sun_week_start)::integer as week_year,
    cast({{ date_part('week', 'd.mon_sun_date_offset') }} as {{ dbt_utils.type_int() }}) as week_of_year,

    -- Retail week (Sun - Sat) number, and a week key to sort
    sun_sat_week_start,
    cast((to_char(d.sun_sat_week_start, 'YYYY') 
            || right('0' || {{ date_part('week', 'd.sun_sat_date_offset') }}, 2)) 
        as {{ dbt_utils.type_int() }}) as sun_sat_week_key,
    EXTRACT(YEAR FROM d.sun_sat_week_start)::integer as sun_sat_week_year,
    cast({{ date_part('week', 'd.sun_sat_date_offset') }} as {{ dbt_utils.type_int() }}) as sun_sat_week_of_year

from
    dates_with_prior_year_dates d
order by 1


