{#
    Catch high null response rates in the latest season's market value data.
    The API sometimes returns null responses for players, which causes them to
    be silently dropped from player_valuations when the latest season is selected.
    This test warns if more than 10% of responses in any season are null.
#}

with raw as (

    select
        str_split(filename, '/')[5] as season,
        json_extract_string(json(value), '$.response') as response

    from {{ source("transfermarkt_api", "market_value_development") }}

),
season_stats as (

    select
        season,
        count(*) as total,
        count(*) filter (where response is null or response = 'null') as null_count,
        round(100.0 * count(*) filter (where response is null or response = 'null') / count(*), 1) as null_pct

    from raw
    group by season

)

select *
from season_stats
where null_pct > 10
