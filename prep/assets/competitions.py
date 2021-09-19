from frictionless.field import Field
from frictionless.schema import Schema

import pandas

from .base import BaseProcessor

class CompetitionsProcessor(BaseProcessor):

  name = "competitions"
  description = "Competitions in Europe confederation. One row per league."

  def process_segment(self, segment):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    league_href_parts = json_normalized['href'].str.split('/', 5, True)
    confederation_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['competition_id'] = league_href_parts[4]
    prep_df['name'] = league_href_parts[1]
    prep_df['type'] = json_normalized['competition_type']
    prep_df['country_id'] = json_normalized['country_id']
    prep_df['country_name'] = json_normalized['country_name']
    prep_df['domestic_league_code'] = json_normalized['country_code']
    
    prep_df['confederation'] = confederation_href_parts[2]
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    return prep_df

  def get_validations(self):
    return [
      'assert_df_not_empty'
    ]

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='competition_id', type='string'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='type', type='string'))
    self.schema.add_field(Field(name='country_id', type='number'))
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

    return self.schema
