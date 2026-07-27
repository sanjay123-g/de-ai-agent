with source as (
    select * from {{ source('bronze', 'RAW_WC2026_GOALS') }}
),

renamed as (
    select
    MATCH_NUM AS match_num,
    MATCH_DATE AS match_date,
    ROUND_NAME AS round_name,
    TEAM1_NAME AS team1_name,
    TEAM2_NAME AS team2_name,
    SCORING_TEAM AS scoring_team,
    SCORER_NAME AS scorer_name,
    MINUTE AS minute,
    IS_TEAM1_GOAL AS is_team1_goal,
    IS_OWN_GOAL AS is_own_goal,
    IS_PENALTY AS is_penalty,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed