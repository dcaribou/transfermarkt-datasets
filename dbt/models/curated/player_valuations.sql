with market_value_development_cte as (

    select * from {{ ref('base_market_value_development') }}

),
players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

),
transfers_cte as (

    select * from {{ ref('base_transfers') }}

),
transfer_club_resolved as (

    select
        mv.player_id,
        mv."date",
        t.to_club_id,
        row_number() over (
            partition by mv.player_id, mv."date"
            order by t.transfer_date desc
        ) as n

    from market_value_development_cte mv

    inner join transfers_cte t
        on mv.player_id = t.player_id
        and t.transfer_date <= mv."date"

)

select
    market_value_development_cte.player_id,
    market_value_development_cte."date",
    market_value_development_cte.market_value_in_eur,
    market_value_development_cte.current_club_name,
    coalesce(
        transfer_club_resolved.to_club_id,
        players_cte.current_club_id::integer
    ) as current_club_id,
    clubs_cte.domestic_competition_id as player_club_domestic_competition_id

from market_value_development_cte

left join players_cte using(player_id)

left join transfer_club_resolved
    on market_value_development_cte.player_id = transfer_club_resolved.player_id
    and market_value_development_cte."date" = transfer_club_resolved."date"
    and transfer_club_resolved.n = 1

left join clubs_cte
    on coalesce(
        transfer_club_resolved.to_club_id,
        players_cte.current_club_id::integer
    ) = clubs_cte.club_id

order by market_value_development_cte."date", market_value_development_cte.player_id
