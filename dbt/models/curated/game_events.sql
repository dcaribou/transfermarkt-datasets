with game_events_cte as (

    select * from {{ ref('base_game_events') }}

)

select * from game_events_cte
