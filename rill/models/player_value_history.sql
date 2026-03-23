SELECT
    pv.player_id,
    p.name AS player_name,
    pv.date,
    pv.market_value_in_eur,
    p.position,
    p.sub_position,
    p.country_of_citizenship,
    pv.current_club_name,
    pv.player_club_domestic_competition_id AS domestic_competition_id
FROM dev.player_valuations pv
LEFT JOIN dev.players p ON pv.player_id = p.player_id
