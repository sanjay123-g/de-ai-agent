with source as (
    select * from {{ source('bronze', 'RAW_HISTORICAL_RESULTS') }}
),

renamed as (
    select
    DATE AS date,
    HOME_TEAM AS home_team,
    AWAY_TEAM AS away_team,
    HOME_SCORE AS home_score,
    AWAY_SCORE AS away_score,
    TOURNAMENT AS tournament,
    CITY AS city,
    COUNTRY AS country,
    NEUTRAL AS neutral,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed