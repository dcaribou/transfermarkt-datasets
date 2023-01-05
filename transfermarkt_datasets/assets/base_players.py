from frictionless import checks

from inflection import titleize

import pandas
import numpy

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.core.utils import parse_market_value
from transfermarkt_datasets.core.checks import too_many_missings

class BasePlayersAsset(RawAsset):

  name = "base_players"
  file_name = "base_players.csv"
  description = "Players in `clubs`. One row per player."
  public = False

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema = Schema(
      fields=[
        Field(name="player_id", type="integer"),
        Field(name="player_code", type="string"),
        Field(name="name", type="string"),
        Field(name="first_name", type="string"),
        Field(name="last_name", type="string"),
        Field(name="current_club_id", type="integer"),
        Field(name="last_season", type="integer"),
        Field(name="country_of_birth", type="string"),
        Field(name="city_of_birth", type="string"),
        Field(name="country_of_citizenship", type="string"),
        Field(name="date_of_birth", type="date"),
        Field(name="position", type="string"),
        Field(name="sub_position", type="string"),
        Field(name="foot", type="string"),
        Field(name="height_in_cm", type="integer"),
        Field(name="market_value_in_gbp", type="number"),
        Field(name="highest_market_value_in_gbp", type="number"),
        Field(name="contract_expiration_date", type="date"),
        Field(name="agent_name", type="string"),
        Field(
          name="image_url",
          type="string",
          form="uri"
        ),
        Field(
          name="url",
          type="string",
          form="uri"
        )
      ])

    self.schema.primary_key = ["player_id"]
    self.schema.foreign_keys = [
      {"fields": "current_club_id", "reference": {"resource": "base_clubs", "fields": "club_id"}}
    ]

    self.checks = [
      checks.row_constraint(formula="position in 'Attack,Defender,Midfield,Goalkeeper,Missing'"),
      too_many_missings(field_name="market_value_in_gbp", tolerance=0.30),
      too_many_missings(field_name="contract_expiration_date", tolerance=0.35),
      checks.table_dimensions(min_rows=25000)

    ]

  def build(self):

    self.load_raw()
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(self.raw_df.to_dict(orient="records"))

    href_parts = json_normalized["href"].str.split('/', 5, True)
    parent_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df["player_id"] = href_parts[4]
    prep_df["name"] = json_normalized["name_in_home_country"]
    prep_df["first_name"] = json_normalized["name"]
    prep_df["last_name"] = json_normalized["last_name"]
    prep_df["last_season"] = json_normalized["season"]
    prep_df["current_club_id"] = parent_href_parts[4]
    prep_df["player_code"] = self.url_unquote(href_parts[1])
    prep_df["country_of_birth"] = json_normalized['place_of_birth.country']
    prep_df["city_of_birth"] = json_normalized['place_of_birth.city']
    prep_df["country_of_citizenship"] = json_normalized["citizenship"]
    prep_df["date_of_birth"] = (
      pandas
        .to_datetime(
          arg=json_normalized["date_of_birth"],
          errors="coerce"
        )
    )

    sub_position = json_normalized["position"]
    prep_df["position"] = numpy.select(
      condlist=[
          sub_position.str.contains(
            "|".join(["Centre-Forward", "Left Winger", "Right Winger", "Second Striker", "Attack"]),
            case=False
          ), 
          sub_position.str.contains(
            "|".join(["Centre-Back", "Left-Back", "Right-Back", "Defender"]),
            case=False
          ),
          sub_position.str.contains(
            "|".join(["Attacking Midfield", "Central Midfield", "Defensive Midfield",
            "Left Midfield", "Right Midfield", "Midfield"]),
            case=False
          ),
          sub_position.str.contains("Goalkeeper", case=False)
      ], 
      choicelist=[
          "Attack", 
          "Defender",
          "Midfield",
          "Goalkeeper"
      ],
      default="Missing"
    )
    prep_df["sub_position"] = sub_position.str.split(" - ", 2, True)[1]

    prep_df["foot"] = (
      json_normalized["foot"]
        .replace('N/A', numpy.nan)
        .str.capitalize()
    )
    prep_df["height_in_cm"] = (
      (json_normalized["height"]
        .replace("N/A", numpy.nan)
        .str.split("[\s\.]", 2, True)[0]
        .str.replace(",",".")
        .astype(dtype=float) * 100
      ).fillna(0).astype(int)
    )

    prep_df["market_value_in_gbp"] = (
      json_normalized["current_market_value"].apply(parse_market_value)
    )
    prep_df["highest_market_value_in_gbp"] = (
      json_normalized["highest_market_value"].apply(parse_market_value)
    )

    prep_df["agent_name"] = json_normalized["player_agent.name"]
    prep_df["contract_expiration_date"] = (
      pandas
        .to_datetime(
          arg=json_normalized["contract_expires"],
          errors="coerce"
        )
    )
    prep_df["image_url"] = json_normalized.get("image_url", pandas.NaT)
    prep_df["url"] = self.url_prepend(json_normalized["href"])

    self.prep_df = prep_df

    self.drop_duplicates()

    return prep_df
