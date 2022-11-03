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

    self.schema = Schema()

    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='datetime', type='date'))
    self.schema.add_field(Field(name='dateweek', type='date'))
    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='market_value', type='number'))
    self.schema.add_field(Field(
      name='player_club_domestic_competition_id',
      type='string',
      tags=["explore"]
    ))

    self.schema.primary_key = ['player_id', 'date']
    self.schema.foreign_keys = [
      {"fields": "player_id", "reference": {"resource": "cur_players", "fields": "player_id"}}
    ]

    self.checks = [
      checks.forbidden_value(field_name="market_value", values=[None]),
      checks.table_dimensions(min_rows=320000)
    ]

  def build(
    self,
    base_player_valuations: BasePlayerValuationsAsset,
    base_players: BasePlayersAsset,
    base_clubs: BaseClubsAsset
    ):

    player_valuations = base_player_valuations.prep_df
    player_valuations["datetime"] = pd.to_datetime(player_valuations["date"])
    player_valuations["dateweek"] = (
      player_valuations["datetime"] - pd.to_timedelta(player_valuations["datetime"].dt.dayofweek, unit='d')
    )

    player_attributes = base_players.prep_df[
      ["player_id", "current_club_id"]
    ]
    club_attributes = base_clubs.prep_df[
      ["club_id", "domestic_competition_id"]
    ]

    with_player_attributes = player_valuations.merge(
      player_attributes,
      how="left",
      on="player_id"
    )

    with_club_attributes = with_player_attributes.merge(
      club_attributes.rename(
        columns={"domestic_competition_id": "player_club_domestic_competition_id"}
      ),
      how="left",
      left_on="current_club_id",
      right_on="club_id"
    )
    del with_club_attributes["club_id"]

    self.prep_df = with_club_attributes
