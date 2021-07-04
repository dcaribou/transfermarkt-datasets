from frictionless.field import Field
from frictionless.schema import Schema
from inflection import titleize

import pandas

from .base import BaseProcessor

class ClubsProcessor(BaseProcessor):

  name = 'clubs'
  description = "Clubs in `leagues`. One row per club."

  def process_segment(self, segment):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    club_href_parts = json_normalized['href'].str.split('/', 5, True)
    league_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['club_id'] = club_href_parts[4]
    prep_df['name'] = self.url_unquote(club_href_parts[1])
    prep_df['pretty_name'] = prep_df['name'].apply(lambda x: titleize(x))
    prep_df['domestic_competition'] = json_normalized['parent.href'].str.split('/', 5, True)[4]
    prep_df['league_id'] = league_href_parts[4]

    self.set_checkpoint('prep', prep_df)
    return prep_df

  def get_validations(self):
    return [
      'assert_df_not_empty'
    ]

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='pretty_name', type='string'))
    self.schema.add_field(Field(name='domestic_competition', type='string'))
    self.schema.add_field(Field(name='league_id', type='string'))

    self.schema.primary_key = ['club_id']
    self.schema.foreign_keys = [
      {"fields": "league_id", "reference": {"resource": "leagues", "fields": "league_id"}}
    ]

    return self.schema
