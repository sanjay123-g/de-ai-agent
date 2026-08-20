-- Staging model for historical_goals
WITH source AS (
    SELECT
        date,
        home_team,
        away_team,
        team,
        scorer,
        minute,
        own_goal,
        penalty,
        _ingested_at,
        _source_name,
        _pipeline_run_id
    FROM {{ source('bronze', 'RAW_HISTORICAL_GOALS') }}
),
renamed AS (
    SELECT
        TRY_CAST(date AS DATE) AS date,
        LOWER(home_team) AS home_team,
        LOWER(away_team) AS away_team,
        LOWER(team) AS team,
        LOWER(scorer) AS scorer,
        minute::BIGINT AS minute,
        own_goal::BOOLEAN AS own_goal,
        penalty::BOOLEAN AS penalty,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
)
SELECT
    date,
    home_team,
    away_team,
    team,
    scorer,
    minute,
    own_goal,
    penalty,
    _loaded_at
FROM renamed