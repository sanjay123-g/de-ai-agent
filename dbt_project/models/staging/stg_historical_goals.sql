-- Staging model for historical_goals
WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'RAW_HISTORICAL_GOALS') }}
),
renamed AS (
    SELECT
        TRY_CAST(date AS DATE) AS game_date,
        LOWER(home_team) AS home_team,
        LOWER(away_team) AS away_team,
        LOWER(team) AS team,
        LOWER(scorer) AS scorer,
        minute::NUMBER AS minute,
        own_goal::BOOLEAN AS own_goal,
        penalty::BOOLEAN AS penalty,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)
SELECT * FROM renamed
WHERE game_date IS NOT NULL AND home_team IS NOT NULL AND away_team IS NOT NULL AND team IS NOT NULL AND scorer IS NOT NULL AND minute IS NOT NULL AND own_goal IS NOT NULL AND penalty IS NOT NULL