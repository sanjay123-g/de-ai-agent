with source as (
    select * from {{ source('bronze', 'RAW_HISTORICAL_SHOOTOUTS') }}
),

renamed as (
    select
    DATE AS date,
    HOME_TEAM AS home_team,
    AWAY_TEAM AS away_team,
    WINNER AS winner,
    FIRST_SHOOTER AS first_shooter,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed