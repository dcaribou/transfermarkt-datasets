from frictionless.field import Field
from frictionless.schema import Schema
from inflection import titleize

import pandas
import numpy

import re

from .base import BaseProcessor
from .utils import parse_market_value

class PlayersProcessor(BaseProcessor):

  name = 'players'
  description = "Players in `clubs`. One row per player."

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='last_season', type='integer'))
    self.schema.add_field(Field(name='current_club_id', type='integer'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='pretty_name', type='string'))
    self.schema.add_field(Field(name='country_of_birth', type='string'))
    self.schema.add_field(Field(name='country_of_citizenship', type='string'))
    self.schema.add_field(Field(name='date_of_birth', type='date'))
    self.schema.add_field(Field(name='position', type='string'))
    self.schema.add_field(Field(name='sub_position', type='string'))
    self.schema.add_field(Field(name='foot', type='string'))
    self.schema.add_field(Field(name='height_in_cm', type='integer'))
    self.schema.add_field(Field(name='market_value_in_gbp', type='number'))
    self.schema.add_field(Field(name='highest_market_value_in_gbp', type='number'))
    self.schema.add_field(Field(
      name='url',
      type='string',
      format='uri'
      )
    )

    self.schema.primary_key = ['player_id']
    self.schema.foreign_keys = [
      {"fields": "current_club_id", "reference": {"resource": "clubs", "fields": "club_id"}}
    ]

  def process_segment(self, segment, season):
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    href_parts = json_normalized['href'].str.split('/', 5, True)
    parent_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['player_id'] = href_parts[4]
    prep_df['last_season'] = season
    prep_df['current_club_id'] = parent_href_parts[4]
    prep_df['name'] = self.url_unquote(href_parts[1])
    prep_df['pretty_name'] = prep_df['name'].apply(lambda x: titleize(x))
    prep_df['country_of_birth'] = json_normalized['place_of_birth.country'].str.replace('Heute: ', '', regex=False)
    prep_df['country_of_citizenship'] = json_normalized['citizenship']
    prep_df['date_of_birth'] = (
      pandas
        .to_datetime(
          arg=json_normalized['date_of_birth'],
          errors='coerce'
        )
    )

    sub_position = json_normalized['position']
    prep_df['position'] = numpy.select(
      [
          sub_position.isin(
            ['Centre-Forward', 'Left Winger', 'Right Winger', 'Second Striker']
          ), 
          sub_position.isin(
            ['Centre-Back', 'Left-Back', 'Right-Back']
          ),
          sub_position.isin(
            ['Attacking Midfield', 'Central Midfield', 'Defensive Midfield',
            'Left Midfield', 'Right Midfield']
          ),
          sub_position == 'Goalkeeper'
      ], 
      [
          'Attack', 
          'Defender',
          'Midfield',
          'Goalkeeper'
      ]
    )
    prep_df['sub_position'] = sub_position

    prep_df['foot'] = (
      json_normalized['foot']
        .replace('N/A', numpy.nan)
        .str.capitalize()
    )
    prep_df['height_in_cm'] = (
      (json_normalized['height']
        .replace('N/A', numpy.nan)
        .str.split('m', 2, True)[0]
        .str.replace(',','.')
        .astype(dtype=float) * 100
      ).fillna(0).astype(int)
    )

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
