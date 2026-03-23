SELECT
    cg.game_id,
    cg.club_id,
    cl.name AS club_name,
    cg.own_manager_name,
    g.season,
    g.date,
    g.competition_id,
    g.competition_type,
    comp.name AS competition_name,
    cl.domestic_competition_id,
    cg.is_win,
    cg.own_goals,
    cg.opponent_goals,
    cg.hosting
FROM dev.club_games cg
LEFT JOIN dev.games g ON cg.game_id = g.game_id
LEFT JOIN dev.clubs cl ON cg.club_id = cl.club_id
LEFT JOIN dev.competitions comp ON g.competition_id = comp.competition_id
