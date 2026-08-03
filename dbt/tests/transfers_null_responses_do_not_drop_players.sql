{#
    A player with usable transfer history in ANY season must appear in
    base_transfers.

    base_transfers keeps only the latest season per player
    (row_number() ... order by season desc, where n = 1). A failed acquisition
    run still persists one {"response": null} row per player, and because those
    rows carry the latest season they win that ranking, shadow every earlier
    season, and erase the player's entire transfer history from the dataset.

    This is not hypothetical: the 2026-07-11 run was blocked by the API and
    wrote 22,324 null responses for season 2025, dropping transfers from
    175,120 rows to 35,139 and removing players such as Alexander Isak and
    Florian Wirtz completely, even though seasons 2023/2024 held their full
    history.

    base_market_value_development already filters null responses before
    ranking, which is why player_valuations survived the same wipe intact.
#}

with players_with_history as (

    select distinct
        json_extract_string(json(value), '$.player_id')::integer as player_id

    from {{ source("transfermarkt_api", "transfers") }},
        unnest(
            json_transform(
                json_extract(json(value), '$.response.transfers'), '["JSON"]'
            )
        ) as u(transfer)

    -- only responses that actually carry data
    where json_extract(json(value), '$.response') is not null
      and json_extract_string(json(value), '$.response') != 'null'
      -- mirror the date filter in base_transfers so that transfers the model
      -- legitimately discards are never counted as history
      and (transfer ->> 'dateUnformatted') is not null
      and (transfer ->> 'dateUnformatted') != '0000-00-00'
      and (transfer ->> 'dateUnformatted') != ''

)

select
    players_with_history.player_id

from players_with_history

left join {{ ref('base_transfers') }} as base_transfers
    on players_with_history.player_id = base_transfers.player_id

where base_transfers.player_id is null
