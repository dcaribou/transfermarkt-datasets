with game_events_cte as (

    select * from {{ ref('base_game_events') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['game_id', 'player_id', 'club_id', 'type', 'minute', 'description', 'player_in_id']) }} as game_event_id,
    * 

from game_events_cte

order by game_id, player_id, club_id, "type", "minute", coalesce("description", ''), player_in_id
