SELECT
    g.game_id,
    g.competition_id,
    comp.name AS competition_name,
    g.competition_type,
    g.season,
    g.round,
    g.date,
    g.home_club_name,
    g.away_club_name,
    g.home_club_goals,
    g.away_club_goals,
    g.home_club_goals + g.away_club_goals AS total_goals,
    g.attendance,
    g.stadium,
    g.referee,
    g.home_club_formation,
    g.away_club_formation,
    CASE
        WHEN g.home_club_goals > g.away_club_goals THEN 'Home Win'
        WHEN g.home_club_goals < g.away_club_goals THEN 'Away Win'
        ELSE 'Draw'
    END AS result
FROM dev.games g
LEFT JOIN dev.competitions comp ON g.competition_id = comp.competition_id
