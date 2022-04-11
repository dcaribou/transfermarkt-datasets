from frictionless.field import Field
from frictionless.schema import Schema
from inflection import titleize

import pandas

from .base import BaseProcessor
from .utils import parse_market_value

class PlayerValuationsProcessor(BaseProcessor):

  name = 'player_valuations'
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

  def process_segment(self, segment, season):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(
      segment.to_dict(orient='records'),
      record_path="market_value_history",
      meta=["href"],
      errors="ignore"
    )

    href_parts = json_normalized['href'].str.split('/', 5, True)
    
    prep_df['player_id'] = href_parts[4]
    prep_df['date'] = pandas.to_datetime(json_normalized["datum_mw"])
    prep_df['market_value'] = (
      json_normalized["mw"].apply(parse_market_value)
    )

    return prep_df

  def process(self):
    
    super().process()

    self.prep_dfs = [
      self.process_segment(prep_df, season)
      for prep_df, season in zip(self.raw_dfs, self.seasons)
    ]
    self.prep_df = pandas.concat(self.prep_dfs, axis=0).drop_duplicates(
      subset=["player_id", "date"],
      keep="last"
    )
