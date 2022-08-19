from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

from datetime import datetime

import pandas as pd

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.assets.base_games import BaseGamesAsset
from transfermarkt_datasets.assets.base_clubs import BaseClubsAsset

class CurGamesAsset(Asset):

  name = "cur_games"
  description = """
  The `games` asset contains one row per game in the dataset.
  All games are tied to one particular `competition`.
  """
  file_name = "games.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='game_id', type='integer'))
    self.schema.add_field(Field(name='competition_code', type='string'))
    self.schema.add_field(Field(name='season', type='integer'))
    self.schema.add_field(Field(name='round', type='string'))
    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='home_club_id', type='integer'))
    self.schema.add_field(Field(name='away_club_id', type='integer'))
    self.schema.add_field(Field(name='home_club_goals', type='integer'))
    self.schema.add_field(Field(name='away_club_goals', type='integer'))
    self.schema.add_field(Field(name='aggregate', type='string'))
    self.schema.add_field(Field(name='home_club_position', type='integer'))
    self.schema.add_field(Field(name='away_club_position', type='integer'))
    self.schema.add_field(Field(name='stadium', type='string'))
    self.schema.add_field(Field(name='attendance', type='integer'))
    self.schema.add_field(Field(name='referee', type='string'))
    self.schema.add_field(Field(name='club_home_pretty_name', type='string'))
    self.schema.add_field(Field(name='club_away_pretty_name', type='string'))
    self.schema.add_field(Field(
        name='url',
        type='string',
        format='uri'
      )
    )

    self.schema.primary_key = ['game_id']
    
    self.checks = [
      checks.table_dimensions(min_rows=55000)
    ]

  def build(self, base_games: BaseGamesAsset, base_clubs: BaseClubsAsset):

    games = base_games.prep_df

    clubs = base_clubs.prep_df[
      ["club_id", "pretty_name"]
    ]

    with_home_attributes = games.merge(
      clubs.rename(columns={"pretty_name": "club_home_pretty_name"}, errors="raise"),
      how="left",
      left_on="home_club_id",
      right_on="club_id"
    )
    del with_home_attributes["club_id"]

    with_away_attributes = with_home_attributes.merge(
      clubs.rename(columns={"pretty_name": "club_away_pretty_name"}, errors="raise"),
      how="left",
      left_on="away_club_id",
      right_on="club_id"
    )
    del with_away_attributes["club_id"]

    with_away_attributes["aggregate"] = (
      with_away_attributes["home_club_goals"].astype("string") + 
      ":" + 
      with_away_attributes["away_club_goals"].astype("string")
    )

    self.prep_df = with_away_attributes
