-- Singular test: assert no negative scores in historical results
-- Any row returned by this query = test failure
-- Applies universal data quality rule: scores cannot be negative

select
    date,
    home_team,
    away_team,
    home_score,
    away_score
from {{ ref('stg_historical_results') }}
where
    home_score < 0
    or away_score < 0
