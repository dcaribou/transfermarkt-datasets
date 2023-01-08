from typing import List
from frictionless import checks

from datetime import datetime

import pandas as pd
import numpy as np

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.assets.base_games import BaseGamesAsset

class CurClubGamesAsset(Asset):

  name = "cur_club_games"
  description = """
  The `club_games` asset is an alternative representation of a season games from the clubs point of view.
  For each game in the `games` asset the `club_games` contains two rows, one for the home club and another for the away club.

  game_id | home_club_id   | home_club_name | away_club_id | away_club_name | aggregate
  --------|----------------|----------------|--------------|----------------|---------
  1       | 1              | Real Madrid    | 100          | Barcelona      | 1:2
  
  _A game as represented in the `games` asset_
  
  game_id | club_id | own_goals | opponent_id
  --------|---------|-----------|------------
  1       | 1       | 1         | 100
  2       | 100     | 2         | 1
  
  _The same game represented in the `club_games` asset_

  """
  file_name = "club_games.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='game_id', type='integer'))
    self.schema.add_field(Field(name='own_goals', type='integer'))
    self.schema.add_field(Field(name='own_position', type='integer'))
    self.schema.add_field(Field(name='own_manager_name', type="string", tags=["explore"]))
    self.schema.add_field(Field(name='opponent_id', type='integer'))
    self.schema.add_field(Field(name='opponent_goals', type='integer'))
    self.schema.add_field(Field(name='opponent_position', type='integer'))
    self.schema.add_field(Field(name='opponent_manager_name', type='string'))
    self.schema.add_field(Field(
      name="hosting",
      type="string",
      description="'Home' if the game took place at the club home stadium and 'Away' if at its opponent stadium"
    ))
    self.schema.add_field(Field(
      name="is_win",
      type="integer",
      description="'1' if the club won the game and '0' otherwise."
    ))

    self.schema.primary_key = ["club_id", "game_id"]

    self.schema.foreign_keys = [
      {"fields": "club_id", "reference": {"resource": "cur_clubs", "fields": "club_id"}},
      {"fields": "game_id", "reference": {"resource": "cur_games", "fields": "game_id"}}
    ]

    self.checks = [
      checks.table_dimensions(min_rows=58000*2)
    ]

  def build(self, base_games: BaseGamesAsset):

    games = base_games.prep_df

    home_columns_set = ["home_club_id", "home_club_goals", "home_club_position", "home_club_manager_name"]
    away_columns_set = ["away_club_id", "away_club_goals", "away_club_position", "away_club_manager_name"]

    games_home = games[
      ["game_id"] + home_columns_set
    ]
    games_away = games[
      ["game_id"] + away_columns_set
    ]

    game_home_full = games_home.rename(columns=lambda col: col.replace("home_club_", "own_")).merge(
      games_away.rename(columns=lambda col: col.replace("away_club_", "opponent_"))
    )

    games_away_full = games_away.rename(columns=lambda col: col.replace("away_club_", "own_")).merge(
      games_home.rename(columns=lambda col: col.replace("home_club_", "opponent_"))
    )
    
    game_home_full["hosting"] = "Home"
    games_away_full["hosting"] = "Away"

    club_games = pd.concat(
      [game_home_full, games_away_full]
    )

    is_win = np.select(
      condlist=[
        club_games["own_goals"] > club_games["opponent_goals"],
        club_games["own_goals"] < club_games["opponent_goals"]
      ],
      choicelist=[
        1,
        0
      ],
      default=0
    )

    club_games["is_win"] = is_win

    self.prep_df = club_games.rename(columns={"own_id": "club_id"})
