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
    where "type" = 'issue'
    group by created_at::date
),
daily_closed_issues as (
    select
    	closed_at::date
    	, count(*) as "closed_issues"
    from issues
    where "type" = 'issue'
    group by closed_at::date
),
daily_created_prs as (
    select
    	created_at::date
    	, count(*) as "created_prs"
    from issues
    where "type" = 'pull_request'
    group by created_at::date
),
daily_closed_prs as (
    select
    	closed_at::date
    	, count(*) as "closed_prs"
    from issues
    where "type" = 'pull_request'
    group by closed_at::date
),
joined as (
    select
        *
    from dates
    left join daily_created_issues on daily_created_issues.created_at::date = date_day
    left join daily_closed_issues on daily_closed_issues.closed_at::date = date_day
    left join daily_created_prs on daily_created_prs.created_at::date = date_day
    left join daily_closed_prs on daily_closed_prs.closed_at::date = date_day
),
final as (
    select
        date_day
        , sum(created_issues) as "created_issues"
        , sum(closed_issues) as "closed_issues"
        , sum(created_prs) as "created_prs"
        , sum(closed_prs) as "closed_prs"
    from joined
    group by date_day
)
select * from final