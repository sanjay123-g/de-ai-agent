with players as (
    select
        p.team_name,
        p.position,
        datediff('year', p.date_of_birth, current_date()) as age,
        case when p.club_country != nt.fifa_code then 1 else 0 end as plays_abroad
    from {{ ref('stg_player_profiles') }} p
    left join {{ ref('stg_national_teams') }} nt
        on p.team_name = nt.team_name
)
select
    team_name,
    count(*) as squad_size,
    round(avg(age), 1) as avg_age,
    min(age) as youngest_player_age,
    max(age) as oldest_player_age,
    sum(case when position = 'GK' then 1 else 0 end) as goalkeepers,
    sum(case when position = 'DF' then 1 else 0 end) as defenders,
    sum(case when position = 'MF' then 1 else 0 end) as midfielders,
    sum(case when position = 'FW' then 1 else 0 end) as forwards,
    sum(plays_abroad) as players_at_foreign_clubs,
    round(100.0 * sum(plays_abroad) / count(*), 1) as legionnaire_pct
from players
group by team_name
