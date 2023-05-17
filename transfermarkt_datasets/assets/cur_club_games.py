from typing import List
from frictionless import checks

from datetime import datetime

import pandas as pd
import numpy as np

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field

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
  file_name = "club_games.csv.gz"

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
