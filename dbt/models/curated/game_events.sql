with game_events_cte as (

    select * from {{ ref('base_game_events') }}

)

select * from game_events_cte

order by game_id, player_id, club_id, "type", "minute", coalesce("description", ''), player_in_id
