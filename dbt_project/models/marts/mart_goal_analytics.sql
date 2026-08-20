with goals as (
    select
        team,
        scorer,
        minute,
        own_goal,
        penalty,
        case
            when TRY_CAST(minute AS INTEGER) between 0 and 15 then '0-15'
            when TRY_CAST(minute AS INTEGER) between 16 and 30 then '16-30'
            when TRY_CAST(minute AS INTEGER) between 31 and 45 then '31-45'
            when TRY_CAST(minute AS INTEGER) between 46 and 60 then '46-60'
            when TRY_CAST(minute AS INTEGER) between 61 and 75 then '61-75'
            when TRY_CAST(minute AS INTEGER) between 76 and 90 then '76-90'
            when TRY_CAST(minute AS INTEGER) > 90 then '90+'
            else 'unknown'
        end as minute_bucket
    from {{ ref('stg_historical_goals') }}
)
select
    team,
    count(*) as total_goals,
    sum(case when penalty then 1 else 0 end) as penalty_goals,
    round(100.0 * sum(case when penalty then 1 else 0 end) / count(*), 1) as penalty_goal_pct,
    sum(case when own_goal then 1 else 0 end) as own_goals_against,
    sum(case when minute_bucket in ('0-15','16-30','31-45') then 1 else 0 end) as first_half_goals,
    sum(case when minute_bucket in ('46-60','61-75','76-90','90+') then 1 else 0 end) as second_half_goals,
    sum(case when minute_bucket = '90+' then 1 else 0 end) as stoppage_time_goals,
    count(distinct scorer) as unique_scorers
from goals
group by team
