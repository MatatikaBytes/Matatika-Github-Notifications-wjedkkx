{{ config(materialized='table') }}

with repositories as (
    select 
        * 
    from {{ ref('dim_repositories_history') }}
),
joined as (
    select
        *
        , stargazers_count - (select stargazers_count from repositories r1
                                where r1.full_name = r.full_name
                                and r1.prior_date_day = r.date_day) "daily_stars_gained"
    from repositories r
),
final as (
    select * from joined
)

select * from final