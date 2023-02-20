{{ config(materialized='table') }}

with repositories as (
    select * from {{ ref('github_dim_repositories_snapshot') }}
),
most_recent_daily_repositories as (
    select
        *
    from repositories r1
    where not exists (select * from repositories r2
                    where r2."_sdc_batched_at"::date = r1."_sdc_batched_at"::date
                    and r2."_sdc_batched_at" > r1."_sdc_batched_at"
                    and r2.id = r1.id)
),
dates as (
    select
        *
    from {{ ref('dim_date') }}
),
final as (
    select
        *
    from dates d
    left join most_recent_daily_repositories mrdr on mrdr._sdc_batched_at::date = d.date_day
)

select * from final