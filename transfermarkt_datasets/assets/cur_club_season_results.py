from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

from datetime import datetime

import pandas as pd
import numpy as np
from transfermarkt_datasets.assets.cur_clubs import CurClubsAsset

from transfermarkt_datasets.assets.cur_games import CurGamesAsset

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.assets.cur_club_games import CurClubGamesAsset

class CurClubSeasonResultsAsset(Asset):

  name = "cur_club_season_results"
  description = """
  The `club_season_results` asset contains one row per club **per season**.
  Each row shows the club performance on that season across the different competitions:
    national league (`l_`), national cup (`c_`) and international competition (`i_`).
  """
  file_name = "club_season_results.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='pretty_name', type='string'))
    self.schema.add_field(Field(name='season', type='integer'))
    self.schema.add_field(Field(
        name='l_points',
        type='integer',
        description="Clubs performance in the national league."
      )
    )

    self.schema.primary_key = ["club_id", "season"]

    self.checks = [
      checks.table_dimensions(min_rows=400)
    ]

  def build(
    self,
    cur_games: CurGamesAsset,
    cur_clubs: CurClubsAsset,
    cur_club_games: CurClubGamesAsset
  ):
    
    games = cur_games.prep_df
    clubs = cur_clubs.prep_df
    club_games = cur_club_games.prep_df

    with_game_attributes = club_games.merge(
      games[["game_id", "season"]],
      how="left",
      on="game_id"
    )

    points = np.select(
      condlist=[
        with_game_attributes["own_goals"] > with_game_attributes["opponent_goals"],
        with_game_attributes["own_goals"] < with_game_attributes["opponent_goals"]
      ],
      choicelist=[
        3,
        0
      ],
      default=1
    )

    with_game_attributes["l_points"] = points

    club_season_results = (
      with_game_attributes
        .groupby(by=["club_id", "season"])["l_points"]
        .sum()
        .reset_index()
    )

    with_club_attributes = club_season_results.merge(
      clubs[["club_id", "pretty_name"]],
      how="left",
      on="club_id"
    )

    self.prep_df = with_club_attributes
