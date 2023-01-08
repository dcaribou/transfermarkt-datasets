import numpy
import pandas

from frictionless import checks

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.core.utils import (
  cast_metric, cast_minutes_played,
  read_config
)
from transfermarkt_datasets.core.checks import too_many_missings

class BaseAppearancesAsset(RawAsset):

  name = "base_appearances"
  description = "Appearances for `players`. One row per appearance."
  file_name = "base_appearances.csv"
  public = False

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    
    self.schema = Schema(
      fields=[
        Field(name="player_id", type="integer"),
        Field(name="game_id", type="integer"),
        Field(name="appearance_id", type="string"),
        Field(name="competition_id", type="string"),
        Field(name="player_club_id", type="integer"),
        Field(name="goals", type="integer"),
        Field(name="assists", type="integer"),
        Field(name="minutes_played", type="integer"),
        Field(name="yellow_cards", type="integer"),
        Field(name="red_cards", type="integer")
      ]
    )

    self.schema.primary_key = ["appearance_id"]
    
    self.schema.foreign_keys = [
      {"fields": "game_id", "reference": {"resource": "base_games", "fields": "game_id"}}
    ]

    self.checks = [
      checks.table_dimensions(min_rows=1000000),
      too_many_missings(field_name="game_id",tolerance=0.0001)
    ]

  def build(self):

    self.load_raw()

    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(self.raw_df.to_dict(orient="records"))

    applicable_competitions = read_config()["competition_codes"]

    json_normalized = json_normalized[json_normalized["competition_code"].isin(applicable_competitions)]
  
    prep_df["player_id"] = json_normalized["parent.href"].str.split('/', 5, True)[4]
    prep_df["game_id"] = json_normalized["result.href"].str.split('/', 5, True)[4]
    prep_df["appearance_id"] = numpy.select(
      [prep_df["game_id"].isna()],
      [prep_df.index],
      default=prep_df["game_id"] + "_" + prep_df["player_id"]
    )
    prep_df["competition_id"] = json_normalized["competition_code"]
    prep_df["player_club_id"] = json_normalized["for.href"].str.split('/', 5, True)[4]
    prep_df["goals"] = json_normalized["goals"].apply(cast_metric)
    prep_df["assists"] = json_normalized["assists"].apply(cast_metric)
    prep_df["minutes_played"] = json_normalized["minutes_played"].apply(cast_minutes_played)
    prep_df["yellow_cards"] = (
      (json_normalized["yellow_cards"].str.len() > 0).astype("int32") + 
      (json_normalized["second_yellow_cards"].str.len() > 0).astype("int32")
    )
    prep_df["red_cards"] = (json_normalized["red_cards"].str.len() > 0).astype("int32")
    prep_df["game_id"] = prep_df["game_id"].fillna(-1)

    self.prep_df = prep_df

    self.drop_duplicates()

    return prep_df
