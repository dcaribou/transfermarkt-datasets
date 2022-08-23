from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

import pandas as pd

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.assets.base_games import BaseGamesAsset
from transfermarkt_datasets.assets.base_appearances import BaseAppearancesAsset
from transfermarkt_datasets.assets.base_players import BasePlayersAsset

class CurAppearancesAsset(Asset):

  name = "cur_appearances"
  description = """
  The `appearances` asset contains one records per player appearance. That is, one record per player per game played.
  All `appearances` are bound to one particular `player`. 
  """
  file_name = "appearances.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name="appearance_id", type="integer"))
    self.schema.add_field(Field(name="game_id", type="integer"))
    self.schema.add_field(Field(name="date", type="date"))
    self.schema.add_field(Field(name="player_pretty_name", type="string"))

    self.schema.primary_key = ["appearance_id"]
    
    self.checks = [
      checks.table_dimensions(min_rows=1000000)
    ]

  def build(
    self,
    base_appearances: BaseAppearancesAsset,
    base_games: BaseGamesAsset,
    base_players: BasePlayersAsset):

    appearances = base_appearances.prep_df

    game_attributes = base_games.prep_df[
      ["game_id", "date"]
    ]

    player_attributes = base_players.prep_df[
      ["player_id", "pretty_name"]
    ]

    with_game_attributes = appearances.merge(
      game_attributes,
      how="left",
      on="game_id"
    )

    with_player_attributes = with_game_attributes.merge(
      player_attributes.rename(columns={"pretty_name": "player_pretty_name"}),
      how="left",
      on="player_id"
    )

    self.prep_df = with_player_attributes
