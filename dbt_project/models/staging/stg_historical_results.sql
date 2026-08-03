-- Staging model for historical_results
WITH source AS (
    SELECT
        date,
        home_team,
        away_team,
        home_score,
        away_score,
        tournament,
        city,
        country,
        neutral,
        _ingested_at,
        _source_name,
        _pipeline_run_id
    FROM {{ source('bronze', 'RAW_HISTORICAL_RESULTS') }}
),
renamed AS (
    SELECT
        TRY_CAST(date AS DATE) AS date,
        LOWER(home_team) AS home_team,
        LOWER(away_team) AS away_team,
        home_score::NUMBER AS home_score,
        away_score::NUMBER AS away_score,
        LOWER(tournament) AS tournament,
        LOWER(city) AS city,
        LOWER(country) AS country,
        neutral::BOOLEAN AS neutral,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)
SELECT
    date,
    home_team,
    away_team,
    home_score,
    away_score,
    tournament,
    city,
    country,
    neutral,
    _loaded_at
FROM renamed