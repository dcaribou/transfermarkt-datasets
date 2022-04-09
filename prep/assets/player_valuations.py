from frictionless.field import Field
from frictionless.schema import Schema
from inflection import titleize

import pandas
import numpy

import re

from .base import BaseProcessor

class PlayerValuationsProcessor(BaseProcessor):

  name = 'player_valuations'
  description = "Historical player market valuations. One row per market valuation record."

  def __init__(self, raw_files_path, seasons, name, prep_file_path) -> None:
    super().__init__(raw_files_path, seasons, name, prep_file_path)

    self.schema = Schema()

    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='market_value', type='number'))
    self.schema.add_field(Field(
      name='url',
      type='string',
      format='uri'
      )
    )

    self.schema.primary_key = ['player_id', 'date']
    self.schema.foreign_keys = [
      {"fields": "player_id", "reference": {"resource": "players", "fields": "player_id"}}
    ]

  def process_segment(self, segment, season):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    href_parts = json_normalized['href'].str.split('/', 5, True)
    parent_href_parts = json_normalized['parent.href'].str.split('/', 5, True)


    def parse_market_value(market_value):
      """Parse a "market value" string into an integer number representing a GBP (british pounds) amount,
      such as "£240Th." or "£34.3m".

      :market_value: "Market value" string
      :return: An integer number representing a GBP amount
      """

      if market_value is not None:
        match = re.search('£([0-9\.]+)(Th|m)', market_value)
        if match:
          factor = match.group(2)
          if factor == 'Th':
            numeric_factor = 1000
          elif factor == 'm':
            numeric_factor = 1000000
          else:
            return None
          
          value = match.group(1)
          return int(float(value)*numeric_factor)
        else:
          return None
      else:
        return None

    prep_df['market_value_in_gbp'] = (
      json_normalized['current_market_value'].apply(parse_market_value)
    )
    prep_df['highest_market_value_in_gbp'] = (
      json_normalized['highest_market_value'].apply(parse_market_value)
    )

    prep_df['url'] = self.url_prepend(json_normalized['href'])

    self.set_checkpoint('prep', prep_df)
    return prep_df

  def process(self):
    self.prep_dfs = [
      self.process_segment(prep_df, season)
      for prep_df, season in zip(self.raw_dfs, self.seasons)
    ]
    self.prep_df = pandas.concat(self.prep_dfs, axis=0).drop_duplicates(
      subset='player_id',
      keep='last'
    )

  def get_validations(self):
      return []
