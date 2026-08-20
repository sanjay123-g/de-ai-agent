-- Staging model for historical shootouts
WITH source AS (
    SELECT
        date,
        home_team,
        away_team,
        winner,
        first_shooter
    FROM {{ source('bronze', 'RAW_HISTORICAL_SHOOTOUTS') }}
),
renamed AS (
    SELECT
        TRY_CAST(date AS DATE) AS date,
        LOWER(home_team) AS home_team,
        LOWER(away_team) AS away_team,
        LOWER(winner) AS winner,
        LOWER(first_shooter) AS first_shooter,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
)
SELECT * FROM renamed