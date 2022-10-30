from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

import pandas as pd
import numpy as np

from transfermarkt_datasets.core.asset import RawAsset

class BaseCompetitionsAsset(RawAsset):

  name = "base_competitions"
  description = "Competitions in Europe confederation. One row per league."
  file_name = "competitions.csv"
  public = False

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='competition_id', type='string'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='type', type='string'))
    self.schema.add_field(Field(name='sub_type', type='string'))
    self.schema.add_field(Field(name='country_id', type='integer'))
    self.schema.add_field(Field(name='country_name', type='string'))
    self.schema.add_field(Field(name='domestic_league_code', type='string'))
    self.schema.add_field(Field(name='confederation', type='string'))
    self.schema.add_field(Field(
        name='url',
        type='string',
        format='uri'
      )
    )

    self.schema.primary_key = ['competition_id']

    self.checks = [
      checks.table_dimensions(min_rows=40)
    ]

  def build(self):

    self.load_raw()
    
    prep_df = pd.DataFrame()

    json_normalized = pd.json_normalize(self.raw_df.to_dict(orient='records'))

    league_href_parts = json_normalized['href'].str.split('/', 5, True)
    confederation_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['competition_id'] = league_href_parts[4]
    prep_df['name'] = league_href_parts[1]
    prep_df['sub_type'] = json_normalized['competition_type']

    competition_type = np.select(
      condlist=[
        prep_df["sub_type"] == "first_tier",
        prep_df["sub_type"] == "domestic_cup",
        prep_df["sub_type"].isin(
          [
            "uefa_champions_league", "europa_league", "uefa_europa_conference_league_qualifiers",
            "uefa_champions_league_qualifying", "europa_league_qualifying"
          ]
        )
      ],
      choicelist=[
        "domestic_league",
        "domestic_cup",
        "international_cup"
      ],
      default="other" # includes domestic_super_cup, uefa_super_cup, uefa_champions_league and fifa_club_world_cup
    )
    prep_df["type"] = competition_type
    prep_df['country_id'] = json_normalized['country_id'].fillna(-1).astype('int32')
    prep_df['country_name'] = json_normalized['country_name']
    prep_df['domestic_league_code'] = json_normalized['country_code']
    
    prep_df['confederation'] = confederation_href_parts[2]
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    self.prep_df = prep_df

    self.drop_duplicates()

    return prep_df
