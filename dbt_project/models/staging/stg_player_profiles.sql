-- Staging model for player_profiles
WITH source AS (
    SELECT
        *
    FROM {{ source('bronze', 'RAW_PLAYER_PROFILES') }}
),
renamed AS (
    SELECT
        TRY_CAST(player_id AS VARCHAR) AS player_id,
        LOWER(player_name) AS player_name,
        LOWER(team_name) AS team_name,
        LOWER(position) AS position,
        jersey_number::NUMBER AS jersey_number,
        LOWER(club_name) AS club_name,
        LOWER(club_country) AS club_country,
        TRY_CAST(date_of_birth AS DATE) AS date_of_birth,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)
SELECT
    player_id,
    player_name,
    team_name,
    position,
    jersey_number,
    club_name,
    club_country,
    date_of_birth,
    _loaded_at
FROM renamed