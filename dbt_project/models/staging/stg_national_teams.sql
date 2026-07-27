with source as (
    select * from {{ source('bronze', 'RAW_NATIONAL_TEAMS') }}
),

renamed as (
    select
    TEAM_NAME AS team_name,
    FIFA_CODE AS fifa_code,
    GROUP_NAME AS group_name,
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed