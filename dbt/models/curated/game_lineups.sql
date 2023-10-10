with game_lineups_cte as (

    select * from {{ ref('base_game_lineups') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'game_id',
        'player_id',
        'club_id',
        'type',
        'player_name',
        'position',
        'team_captain',
        'number'
    ]) }} as game_lineups_id,
    game_lineups_cte.* 

from game_lineups_cte

order by game_id, club_id, "type"
