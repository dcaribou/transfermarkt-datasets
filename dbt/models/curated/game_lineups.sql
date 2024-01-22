with game_lineups_cte as (

    select * from {{ ref('base_game_lineups') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'game_id',
        'player_id',
        'type',
        'position',
        'number',
        'team_captain'
    ]) }} as game_lineups_id,
    game_lineups_cte.* 

from game_lineups_cte

order by game_id, player_id, position, "number", team_captain, "type"
