-- Staging model for RAW_WC2026_MATCHES
WITH source AS (
    SELECT
        match_num,
        match_date,
        match_time,
        round_name,
        group_name,
        ground,
        team1_name,
        team2_name,
        score_ft_team1,
        score_ft_team2,
        score_ht_team1,
        score_ht_team2
    FROM {{ source('bronze', 'RAW_WC2026_MATCHES') }}
),
renamed AS (
    SELECT
        match_num::NUMBER AS match_num,
        TRY_CAST(match_date AS DATE) AS match_date,
        TRY_CAST(match_time AS TIME) AS match_time,
        LOWER(round_name) AS round_name,
        LOWER(group_name) AS group_name,
        LOWER(ground) AS ground,
        LOWER(team1_name) AS team1_name,
        LOWER(team2_name) AS team2_name,
        score_ft_team1::NUMBER AS score_ft_team1,
        score_ft_team2::NUMBER AS score_ft_team2,
        score_ht_team1::NUMBER AS score_ht_team1,
        score_ht_team2::NUMBER AS score_ht_team2,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
)
SELECT * FROM renamed