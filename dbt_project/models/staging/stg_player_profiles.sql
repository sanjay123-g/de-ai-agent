-- Staging model for player_profiles
WITH source AS (
    SELECT
        *
    FROM {{ source('bronze', 'RAW_PLAYER_PROFILES') }}
),
renamed AS (
    SELECT
        TRY_CAST(player_id AS VARCHAR) AS player_id,
        LOWER(TRY_CAST(player_name AS VARCHAR)) AS player_name,
        LOWER(TRY_CAST(team_name AS VARCHAR)) AS team_name,
        LOWER(TRY_CAST(position AS VARCHAR)) AS position,
        TRY_CAST(jersey_number AS BIGINT) AS jersey_number,
        LOWER(TRY_CAST(club_name AS VARCHAR)) AS club_name,
        LOWER(TRY_CAST(club_country AS VARCHAR)) AS club_country,
        TRY_CAST(date_of_birth AS DATE) AS date_of_birth,
        CURRENT_TIMESTAMP AS _loaded_at
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