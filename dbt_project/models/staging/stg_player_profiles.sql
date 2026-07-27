with source as (
    select * from {{ source('bronze', 'RAW_PLAYER_PROFILES') }}
),

renamed as (
    select
    PLAYER_ID AS player_id,
    PLAYER_NAME AS player_name,
    TEAM_NAME AS team_name,
    POSITION AS position,
    JERSEY_NUMBER AS jersey_number,
    CLUB_NAME AS club_name,
    CLUB_COUNTRY AS club_country,
    DATE_OF_BIRTH AS date_of_birth,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed