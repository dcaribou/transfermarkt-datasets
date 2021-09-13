from frictionless.field import Field
from frictionless.schema import Schema
from inflection import titleize

import pandas
import numpy

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
    prep_df['total_market_value'] = pandas.to_numeric(json_normalized['total_market_value'])
    prep_df['squad_size'] = json_normalized['squad_size'].astype('int32')
    prep_df['average_age'] = pandas.to_numeric(json_normalized['average_age'])
    prep_df['foreigners_number'] = json_normalized['foreigners_number'].fillna(0).astype('int32')
    prep_df['foreigners_percentage'] = (
      pandas.to_numeric(
        json_normalized['foreigners_percentage']
          .replace('%', numpy.nan)
          .str
          .split(' ', 2, True)[0]
      )
    )
    prep_df['national_team_players'] = json_normalized['national_team_players'].astype('int32')
    prep_df['stadium_name'] = json_normalized['stadium_name']
    prep_df['stadium_seats'] = (
      json_normalized['stadium_seats']
        .str.replace('.', '')
        .str.split(' ', 2, True)[0]
        .astype('int32')
    )
    prep_df['net_transfer_record'] = json_normalized['net_transfer_record']
    prep_df['coach_name'] = json_normalized['coach_name']

    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    self.set_checkpoint('prep', prep_df)
    return prep_df

  def process(self):
    self.prep_dfs = [self.process_segment(prep_df) for prep_df in self.raw_dfs]
    self.prep_df = pandas.concat(self.prep_dfs, axis=0).drop_duplicates(subset='club_id', keep='last')

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
    self.schema.add_field(Field(
        name='total_market_value',
        type='number',
        description="Aggregated players' Transfermarkt market value in millions of pounds"
      )
    )
    self.schema.add_field(Field(name='squad_size', type='integer'))
    self.schema.add_field(Field(name='average_age', type='number'))
    self.schema.add_field(Field(name='foreigners_number', type='integer'))
    self.schema.add_field(Field(name='foreigners_percentage', type='number'))
    self.schema.add_field(Field(name='national_team_players', type='integer'))
    self.schema.add_field(Field(name='stadium_name', type='string'))
    self.schema.add_field(Field(name='stadium_seats', type='integer'))
    self.schema.add_field(Field(name='net_transfer_record', type='string'))
    self.schema.add_field(Field(name='coach_name', type='string'))
    self.schema.add_field(Field(
      name='url',
      type='string',
      format='uri'
      )
    )

    self.schema.primary_key = ['club_id']
    self.schema.foreign_keys = [
      {"fields": "league_id", "reference": {"resource": "leagues", "fields": "league_id"}}
    ]

    return self.schema
