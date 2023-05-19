from frictionless import checks

import pandas as pd

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field

class CurPlayerValuationsAsset(Asset):

  name = "cur_player_valuations"
  description = """
  The `player_valuations` asset contains one row per player value record.
  Player value records appear as a result of a change in the player market value.
  """
  file_name = "player_valuations.csv.gz"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema(
      fields=[
        Field(name='date', type='date'),
        Field(name='datetime', type='date'),
        Field(name='dateweek', type='date'),
        Field(
          name='last_season',
          description='Last season where the player was seen in the dataset.',
          type='date'
        ),
        Field(name='player_id', type='integer'),
        Field(name='current_club_id', type='integer'),
        Field(name='market_value_in_eur', type='number'),
        Field(
          name='player_club_domestic_competition_id',
          type='string',
          tags=["explore"]
        ),
        Field(name='n', type='number')
      ]
    )

    self.schema.primary_key = ['player_id', 'date']
    self.schema.foreign_keys = [
      {"fields": "player_id", "reference": {"resource": "cur_players", "fields": "player_id"}}
    ]
