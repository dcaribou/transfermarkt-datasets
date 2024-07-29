with
    json_transfers as (
        select
            str_split(filename, '/')[5] as season,
            json(value) as json_row,
            json_extract_string(json_row, '$.player_id')::integer as player_id,
            row_number() over (partition by player_id order by season desc) as n,
            filename
        from {{ source("transfermarkt_api", "transfers") }}
    ),
    unnested as (
        select
            player_id,
            season,
            filename,
            unnest(json_transform(json_extract(json_row, '$.response.transfers'), '["JSON"]')) as transfer
        from json_transfers
        where n = 1  -- take the last season extraction for each player
    ),
    transfers_data as (
        select
            player_id,
            transfer ->> 'dateUnformatted' as date_unformatted,
            transfer ->> 'season' as transfer_season,
            json_extract_string(transfer, '$.from.href') as from_club_url,
            json_extract_string(transfer, '$.to.href') as to_club_url,
            json_extract_string(transfer, '$.from.clubName') as from_club_name,
            json_extract_string(transfer, '$.to.clubName') as to_club_name,
            transfer ->> 'fee' as transfer_fee,
            transfer ->> 'marketValue' as market_value,
            filename as source_filename
        from unnested
    ),
    processed_transfers as (
        select
            player_id,
            try_cast(date_unformatted as date) as transfer_date,
            transfer_season,
            split_part(from_club_url, '/', 5)::integer as from_club_id,
            split_part(to_club_url, '/', 5)::integer as to_club_id,
            from_club_name,
            to_club_name,
            case
                when transfer_fee in ('-', '?', '') or transfer_fee is null then null
                when transfer_fee = 'free transfer' then 0
                when left(transfer_fee, 1) = '€' then
                    case
                        when right(transfer_fee, 1) = 'm' then cast(substring(transfer_fee, 2, length(transfer_fee) - 2) as decimal) * 1000000
                        when right(transfer_fee, 1) = 'k' then cast(substring(transfer_fee, 2, length(transfer_fee) - 2) as decimal) * 1000
                        else cast(substring(transfer_fee, 2) as decimal)
                    end
                else 0
            end as transfer_fee,
            case
                when market_value in ('-', '?', '') or market_value is null then null
                when left(market_value, 1) = '€' then
                    case
                        when right(market_value, 1) = 'm' then cast(substring(market_value, 2, length(market_value) - 2) as decimal) * 1000000
                        when right(market_value, 1) = 'k' then cast(substring(market_value, 2, length(market_value) - 2) as decimal) * 1000
                        else cast(substring(market_value, 2) as decimal)
                    end
                else 0
            end as market_value_in_eur,
            source_filename,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, date_unformatted, from_club_id, to_club_id
                ORDER BY source_filename DESC
            ) as row_num
        from transfers_data
        where date_unformatted != '0000-00-00' and date_unformatted is not null and date_unformatted != ''
    )

select
    player_id,
    transfer_date,
    transfer_season,
    from_club_id,
    to_club_id,
    from_club_name,
    to_club_name,
    transfer_fee,
    market_value_in_eur,
    source_filename
from processed_transfers
where row_num = 1
order by player_id, transfer_date