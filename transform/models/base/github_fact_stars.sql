{{ config(materialized='table') }}

with repositories as (
    select 
        * 
    from {{ ref('github_dim_repositories_history') }}
),
joined as (
    select
        *
        , coalesce(stargazers_count - (select stargazers_count from repositories r1
                                where r1.full_name = r.full_name
                                and r1.prior_date_day = r.date_day),0) "daily_stars_gained"
    from repositories r
),
final as (
    select * from joined
)

select * from final