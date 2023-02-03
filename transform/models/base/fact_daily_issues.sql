{{ config(materialized='table') }}

with issues as (
    select * from {{ source('github_source', 'issues') }}
),
dates as (
    select
        *
    from {{ ref('dim_date') }}
),
daily_created_issues as (
    select
    	created_at::date
    	, count(*) as "created_issues"
    from issues
    group by created_at::date
),
daily_closed_issues as (
    select
    	closed_at::date
    	, count(*) as "closed_issues"
    from issues
    group by closed_at::date
),
joined as (
    select
        *
    from dates
    left join daily_created_issues on created_at::date = date_day
    left join daily_closed_issues on closed_at::date = date_day
),
final as (
    select
        date_day
        , sum(created_issues) as "created_issues"
        , sum(closed_issues) as "closed_issues"
    from joined
    group by date_day
)
select * from final