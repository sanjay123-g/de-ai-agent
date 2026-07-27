with home_perspective as (
    select
        home_team as team,
        tournament,
        date,
        home_score as goals_for,
        away_score as goals_against,
        'home' as venue,
        case
            when home_score > away_score then 'win'
            when home_score < away_score then 'loss'
            else 'draw'
        end as result
    from {{ ref('stg_historical_results') }}
),
away_perspective as (
    select
        away_team as team,
        tournament,
        date,
        away_score as goals_for,
        home_score as goals_against,
        'away' as venue,
        case
            when away_score > home_score then 'win'
            when away_score < home_score then 'loss'
            else 'draw'
        end as result
    from {{ ref('stg_historical_results') }}
),
unioned as (
    select * from home_perspective
    union all
    select * from away_perspective
)
select
    team,
    tournament,
    count(*) as matches_played,
    sum(case when result = 'win' then 1 else 0 end) as wins,
    sum(case when result = 'draw' then 1 else 0 end) as draws,
    sum(case when result = 'loss' then 1 else 0 end) as losses,
    round(100.0 * sum(case when result = 'win' then 1 else 0 end) / count(*), 1) as win_pct,
    sum(case when result = 'win' then 3 when result = 'draw' then 1 else 0 end) as points,
    sum(goals_for) as goals_for,
    sum(goals_against) as goals_against,
    sum(goals_for) - sum(goals_against) as goal_difference,
    round(sum(goals_for) * 1.0 / count(*), 2) as avg_goals_scored_per_match,
    round(sum(goals_against) * 1.0 / count(*), 2) as avg_goals_conceded_per_match,
    sum(case when goals_against = 0 then 1 else 0 end) as clean_sheets,
    sum(case when goals_for = 0 then 1 else 0 end) as failed_to_score,
    max(goals_for - goals_against) as biggest_win_margin,
    sum(case when venue = 'home' then 1 else 0 end) as home_matches,
    sum(case when venue = 'away' then 1 else 0 end) as away_matches,
    sum(case when venue = 'home' and result = 'win' then 1 else 0 end) as home_wins,
    sum(case when venue = 'away' and result = 'win' then 1 else 0 end) as away_wins
from unioned
group by team, tournament
