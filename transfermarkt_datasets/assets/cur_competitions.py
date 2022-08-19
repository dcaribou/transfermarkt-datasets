from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

import pandas as pd

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.utils import geocode

from transfermarkt_datasets.assets.base_competitions import BaseCompetitionsAsset

class CurCompetitionsAsset(RawAsset):

  name = "cur_competitions"
  description = """
  The `competitions` asset contains one row per competition in the dataset, including national leagues, cups and international tournaments. 
  """
  file_name = "competitions.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='competition_id', type='string'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='type', type='string'))
    self.schema.add_field(Field(name='country_id', type='integer'))
    self.schema.add_field(Field(name='country_name', type='string'))
    self.schema.add_field(Field(name='country_latitude', type='number'))
    self.schema.add_field(Field(name='country_longitude', type='number'))
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

  def build(self, base_competitions: BaseCompetitionsAsset):

    competitions = base_competitions.prep_df

    geocodes = competitions["country_name"].apply(geocode)
    geocodes = geocodes.apply(pd.Series)
    geocodes.columns = ["latitude", "longitude"]

    competitions["country_latitude"] = geocodes["latitude"]
    competitions["country_longitude"] = geocodes["longitude"]

    self.prep_df = competitions
