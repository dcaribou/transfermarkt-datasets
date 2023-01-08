from frictionless import checks

import pandas as pd

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.assets.base_games import BaseGamesAsset
from transfermarkt_datasets.assets.base_game_events import BaseGameEventsAsset
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

    self.schema = Schema(
      fields=[
        Field(name="appearance_id", type="integer"),
        Field(name="game_id", type="integer"),
        Field(name="player_id", type="integer"),
        Field(
          name="player_club_id",
          type="integer",
          description="ID of the club that the player belonged to at the time of the game."
        ),
        Field(
          name="player_current_club_id",
          type="integer",
          description="ID of the club that the player currently belongs to."
        ),
        Field(name="date", type="date", tags=["explore"]),
        Field(name="player_name", type="string", tags=["explore"]),
        Field(name="competition_id", type="string"),
        Field(name="yellow_cards", type="integer"),
        Field(name="red_cards", type="integer"),
        Field(name="goals", type="integer"),
        Field(name="assists", type="integer"),
        Field(name="minutes_played", type="integer")
      ]
    )

    self.schema.primary_key = ["appearance_id"]

    self.schema.foreign_keys = [
      {"fields": "game_id", "reference": {"resource": "cur_games", "fields": "game_id"}}
    ]
    
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
      ["player_id", "name", "current_club_id"]
    ]

    with_game_attributes = appearances.merge(
      game_attributes,
      how="left",
      on="game_id"
    )

    with_player_attributes = with_game_attributes.merge(
      player_attributes.rename(
        columns={"name": "player_name", "current_club_id": "player_current_club_id"}
        ),
      how="left",
      on="player_id"
    )

    with_player_attributes["player_current_club_id"] = (
      with_player_attributes["player_current_club_id"].fillna(-1).astype("int32")
    )

    self.prep_df = with_player_attributes
