SELECT
    t.transfer_date,
    t.transfer_season,
    t.player_id,
    t.player_name,
    t.from_club_name,
    t.to_club_name,
    t.transfer_fee,
    t.market_value_in_eur,
    p.position,
    p.country_of_citizenship,
    c_from.domestic_competition_id AS from_league,
    c_to.domestic_competition_id AS to_league
FROM dev.transfers t
LEFT JOIN dev.players p ON t.player_id = p.player_id
LEFT JOIN dev.clubs c_from ON t.from_club_id = c_from.club_id
LEFT JOIN dev.clubs c_to ON t.to_club_id = c_to.club_id
WHERE t.transfer_fee > 0
