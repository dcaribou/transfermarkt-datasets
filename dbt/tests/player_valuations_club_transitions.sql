{#
    Check that player valuations reflect club transitions over time.
    Player 8246 has multiple transfers in the dataset, so their valuations
    should show more than one distinct current_club_id.
    See https://github.com/dcaribou/transfermarkt-datasets/issues/137.

#}

with valuations as (

    select distinct current_club_id
    from {{ ref('player_valuations') }}
    where player_id = 8246

)

-- fail if the player does not have at least 2 distinct clubs
select 1
where (select count(*) from valuations) < 2
