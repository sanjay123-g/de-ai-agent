-- Staging model for national_teams
WITH source AS (
    SELECT
        team_name,
        fifa_code,
        group_name,
        _ingested_at,
        _source_name,
        _pipeline_run_id
    FROM {{ source('bronze', 'RAW_NATIONAL_TEAMS') }}
),
renamed AS (
    SELECT
        LOWER(team_name) AS team_name,
        LOWER(fifa_code) AS fifa_code,
        LOWER(group_name) AS group_name,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)
SELECT
    team_name,
    fifa_code,
    group_name,
    _loaded_at
FROM renamed