with source as (
    select * from {{ source('bronze', 'RAW_WC2026_MATCHES') }}
),

renamed as (
    select
    MATCH_NUM AS match_num,
    MATCH_DATE AS match_date,
    MATCH_TIME AS match_time,
    ROUND_NAME AS round_name,
    GROUP_NAME AS group_name,
    GROUND AS ground,
    TEAM1_NAME AS team1_name,
    TEAM2_NAME AS team2_name,
    SCORE_FT_TEAM1 AS score_ft_team1,
    SCORE_FT_TEAM2 AS score_ft_team2,
    SCORE_HT_TEAM1 AS score_ht_team1,
    SCORE_HT_TEAM2 AS score_ht_team2,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed