from frictionless.field import Field
from frictionless.schema import Schema

import pandas

from .base import BaseProcessor

class PlayersProcessor(BaseProcessor):

  name = 'players'
  description = "Players in `clubs`. One row per player."

  def process(self):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(self.raw_df.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    href_parts = json_normalized['href'].str.split('/', 5, True)
    parent_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['player_id'] = href_parts[4]
    prep_df['name'] = self.url_unquote(href_parts[1])
    prep_df['club_id'] = parent_href_parts[4]
    prep_df['position'] = json_normalized['position']
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    self.set_checkpoint('prep', prep_df)
    self.prep_df = prep_df

  def get_validations(self):
      return []

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='position', type='string'))
    self.schema.add_field(Field(
      name='url',
      type='string',
      format='uri'
      )
    )

    self.schema.primary_key = ['player_id']
    self.schema.foreign_keys = [
      {"fields": "club_id", "reference": {"resource": "clubs", "fields": "club_id"}}
    ]

    return self.schema
