from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

from datetime import datetime

import pandas

from transfermarkt_datasets.core.asset import Asset

class CurGamesAsset(Asset):

  name = 'cur_games'
  description = "Games in `competitions`. One row per game."

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
    self.schema.add_field(Field(name='home_club_position', type='integer'))
    self.schema.add_field(Field(name='away_club_position', type='integer'))
    self.schema.add_field(Field(name='stadium', type='string'))
    self.schema.add_field(Field(name='attendance', type='integer'))
    self.schema.add_field(Field(name='referee', type='string'))
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

  def build(self, context, base_games, base_clubs):

    games = base_games.merge(
        base_clubs,
        how="left",
        left_on="home_club_id",
        right_on="club_id",
        suffixes=[None, "_home"]
    )

    return games
