with transfers_cte as (

    select * from {{ ref('base_transfers') }}

),
clubs_cte as (

    select
        club_id,
        name as club_name,
        domestic_competition_id
    from {{ ref('clubs') }}

),
players_cte as (

    select
        player_id,
        name as player_name
    from {{ ref('players') }}

)

select
    transfers_cte.*,
    players_cte.player_name,
    from_club.club_name as from_club_name,
    from_club.domestic_competition_id as from_club_domestic_competition_id,
    to_club.club_name as to_club_name,
    to_club.domestic_competition_id as to_club_domestic_competition_id

from transfers_cte

left join players_cte on transfers_cte.player_id = players_cte.player_id
left join clubs_cte as from_club on transfers_cte.from_club_id = from_club.club_id
left join clubs_cte as to_club on transfers_cte.to_club_id = to_club.club_id

order by transfers_cte.transfer_date desc, transfers_cte.player_id