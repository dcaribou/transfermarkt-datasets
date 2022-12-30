from frictionless import checks

import pandas

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field

class BaseGameEventsAsset(RawAsset):

  name = "base_game_events"
  raw_file_name = "games.json"
  file_name = "base_game_events.csv"
  public = False

  # TODO
  description = """
  Game events are records of...
  """

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema(
      fields=[
        Field(name="game_id", type="integer"),
        Field(name="minute", type="integer"),
        Field(name="type", type="string"),
        Field(name="club_id", type="integer"),
        Field(name="player_id", type="integer"),
        Field(name="description", type="string"),
        Field(name="player_in_id", type="integer")
      ]
    )

    self.schema.primary_key = ["game_id", "minute", "player_id"]
    self.schema.foreign_keys = [
      {"fields": "game_id", "reference": {"resource": "games", "fields": "game_id"}},
      {"fields": "club_id", "reference": {"resource": "clubs", "fields": "club_id"}},
      {"fields": "player_id", "reference": {"resource": "players", "fields": "player_id"}},
    ]

    # TODO
    self.checks = [
      checks.forbidden_value(field_name="minute", values=[None]),
      # checks.table_dimensions(min_rows=250000)
    ]

  def build(self):

    self.load_raw()
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(
      self.raw_df.to_dict(orient='records'),
      record_path="events",
      meta=["href"],
      errors="raise"
    )

    href_parts = json_normalized["href"].str.split('/', 5, True)
    club_href_parts = json_normalized["club.href"].str.split('/', 5, True)
    player_href_parts = json_normalized["player.href"].str.split('/', 5, True)
    player_in_href_parts = json_normalized["action.player_in.href"].str.split('/', 5, True)

    prep_df["game_id"] = href_parts[4]
    prep_df["minute"] = (json_normalized["minute"] + json_normalized["extra"].fillna(0)).astype("int")
    prep_df["type"] = json_normalized["type"]
    prep_df["club_id"] = club_href_parts[4]
    prep_df["player_id"] = player_href_parts[4]
    prep_df["description"] = json_normalized["action.description"]
    prep_df["player_in_id"] = player_in_href_parts[4]

    self.prep_df = prep_df

    # self.drop_duplicates()

    return prep_df
