with source as (
    select * from {{ source('bronze', 'RAW_HISTORICAL_GOALS') }}
),

renamed as (
    select
    DATE AS date,
    HOME_TEAM AS home_team,
    AWAY_TEAM AS away_team,
    TEAM AS team,
    SCORER AS scorer,
    MINUTE AS minute,
    OWN_GOAL AS own_goal,
    PENALTY AS penalty,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed