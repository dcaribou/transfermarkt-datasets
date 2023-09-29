with game_events_cte as (

    select * from {{ ref('base_game_events') }}

),
games_cte as (

    select * from {{ ref('base_games') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'game_id',
        'player_id',
        'club_id',
        'type',
        'minute',
        'description',
        'player_in_id',
        'player_assist_id'
    ]) }} as game_event_id,
    games_cte."date",
    game_events_cte.* 

from game_events_cte

left join games_cte using(game_id)

order by game_id, player_id, club_id, "type", "minute", coalesce("description", ''), player_in_id, player_assist_id
