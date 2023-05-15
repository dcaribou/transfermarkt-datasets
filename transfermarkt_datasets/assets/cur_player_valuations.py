from frictionless import checks

import pandas as pd

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.assets.base_player_valuations import BasePlayerValuationsAsset
from transfermarkt_datasets.assets.base_players import BasePlayersAsset
from transfermarkt_datasets.assets.base_clubs import BaseClubsAsset

class CurPlayerValuationsAsset(Asset):

  name = "cur_player_valuations"
  description = """
  The `player_valuations` asset contains one row per player value record.
  Player value records appear as a result of a change in the player market value.
  """
  file_name = "player_valuations.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema(
      fields=[
        Field(name='date', type='date'),
        Field(name='datetime', type='date'),
        Field(name='dateweek', type='date'),
        Field(name='player_id', type='integer'),
        Field(name='current_club_id', type='integer'),
        Field(name='market_value_in_eur', type='number'),
        Field(
          name='player_club_domestic_competition_id',
          type='string',
          tags=["explore"]
        )
      ]
    )

    self.schema.primary_key = ['player_id', 'date']
    self.schema.foreign_keys = [
      {"fields": "player_id", "reference": {"resource": "cur_players", "fields": "player_id"}}
    ]
