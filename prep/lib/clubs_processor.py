from frictionless.field import Field
from frictionless.resource import Resource
from frictionless.schema import Schema

import numpy
import pandas

from .base_processor import BaseProcessor

class ClubsProcessor(BaseProcessor):

  def process(self):
    
    clubs = pandas.DataFrame()

    json_normalized = pandas.json_normalize(self.raw_df.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    club_href_parts = json_normalized['href'].str.split('/', 5, True)

    clubs['club_id'] = club_href_parts[4]
    clubs['name'] = club_href_parts[1]
    clubs['domestic_competition'] = json_normalized['parent.href'].str.split('/', 5, True)[4]

    self.set_checkpoint('prep', clubs)
    self.prep_df = clubs

  def get_validations(self):
    return [
      'assert_df_not_empty'
    ]

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='domestic_competition', type='string'))

    self.schema.primary_key = ['club_id']

    return self.schema
