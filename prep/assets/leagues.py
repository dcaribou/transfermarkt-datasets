from frictionless.field import Field
from frictionless.schema import Schema

import pandas

from .base import BaseProcessor

class LeaguesProcessor(BaseProcessor):

  name = "leagues"
  description = "Leagues in Europe confederation. One row per league."

  def process_segment(self, segment):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    league_href_parts = json_normalized['href'].str.split('/', 5, True)
    confederation_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['league_id'] = league_href_parts[4]
    prep_df['name'] = league_href_parts[1]
    prep_df['confederation'] = confederation_href_parts[2]

    return prep_df

  def get_validations(self):
    return [
      'assert_df_not_empty'
    ]

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='league_id', type='string'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='confederation', type='string'))

    self.schema.primary_key = ['league_id']

    return self.schema
