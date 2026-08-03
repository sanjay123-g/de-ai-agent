-- Staging model for RAW_WC2026_GOALS
WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'RAW_WC2026_GOALS') }}
),
renamed AS (
    SELECT
        match_num,
        TRY_CAST(match_date AS DATE) AS match_date,
        LOWER(round_name) AS round_name,
        LOWER(team1_name) AS team1_name,
        LOWER(team2_name) AS team2_name,
        LOWER(scoring_team) AS scoring_team,
        LOWER(scorer_name) AS scorer_name,
        minute,
        is_team1_goal::BOOLEAN AS is_team1_goal,
        is_own_goal::BOOLEAN AS is_own_goal,
        is_penalty::BOOLEAN AS is_penalty,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)
SELECT * FROM renamed