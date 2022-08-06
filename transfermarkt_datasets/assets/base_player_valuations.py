from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks
from inflection import titleize

import pandas

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.utils import parse_market_value

class BasePlayerValuationsAsset(RawAsset):

  name = "base_player_valuations"
  raw_file_name = "players.json"
  file_name = "player_valuations.csv"

  description = "Historical player market valuations. One row per market valuation record."

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='market_value', type='number'))

    self.schema.primary_key = ['player_id', 'date']
    self.schema.foreign_keys = [
      {"fields": "player_id", "reference": {"resource": "players", "fields": "player_id"}}
    ]

    self.checks = [
      checks.forbidden_value(field_name="market_value", values=[None]),
      checks.table_dimensions(min_rows=320000)
    ]

  def build(self):

    self.load_raw_from_stage()
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(
      self.raw_df.to_dict(orient='records'),
      record_path="market_value_history",
      meta=["href"],
      errors="ignore"
    )

    json_normalized = json_normalized[json_normalized["mw"] != "-"]

    href_parts = json_normalized['href'].str.split('/', 5, True)
    
    prep_df['player_id'] = href_parts[4]
    prep_df['date'] = pandas.to_datetime(json_normalized["datum_mw"])
    prep_df['market_value'] = (
      json_normalized["mw"].apply(parse_market_value)
    )

    self.prep_df = prep_df

    self.drop_duplicates()

    return prep_df
