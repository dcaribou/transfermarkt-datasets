with transfers_cte as (

    select * from {{ ref('base_transfers') }}

),
players_cte as (

    select
        player_id,
        name as player_name
    from {{ ref('players') }}

)

select
    transfers_cte.player_id,
    transfers_cte.transfer_date,
    transfers_cte.transfer_season,
    transfers_cte.from_club_id,
    transfers_cte.to_club_id,
    transfers_cte.from_club_name,
    transfers_cte.to_club_name,
    transfers_cte.transfer_fee,
    transfers_cte.market_value_in_eur,
    players_cte.player_name

from transfers_cte

left join players_cte on transfers_cte.player_id = players_cte.player_id

order by transfers_cte.transfer_date desc, transfers_cte.player_id