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
    t.player_id,
    t.transfer_date,
    t.transfer_season,
    t.from_club_id,
    t.to_club_id,
    t.from_club_name,
    t.to_club_name,
    t.transfer_fee,
    t.market_value_in_eur,
    p.player_name

from transfers_cte t

left join players_cte p on t.player_id = p.player_id

order by t.transfer_date desc, t.player_id