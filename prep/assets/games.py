from frictionless.field import Field
from frictionless.schema import Schema

from datetime import datetime

import pandas

from .base import BaseProcessor

class GamesProcessor(BaseProcessor):

  name = 'games'
  description = "Games in `leagues`. One row per game."

  def process_segment(self, segment):

    def parse_aggregate(series: pandas.Series) -> pandas.DataFrame:
      parsed = series.str.split(":", expand=True)
      cols = ['home_club_goals', 'away_club_goals']
      parsed.columns = cols
      parsed[cols] = parsed[cols].astype('int32',errors='ignore')
      return parsed

    def infer_season(series: pandas.Series) -> pandas.Series:
      def infer_season(date: datetime):
        year = date.year
        month = date.month
        if month >= 8 and month <= 12:
          return year
        if month >= 1 and month <8:
          return year - 1

      return series.apply(infer_season)
    
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))

    self.set_checkpoint('json_normalized', json_normalized)

    href_parts = json_normalized['href'].str.split('/', 5, True)
    parent_href_parts = json_normalized['parent.href'].str.split('/', 5, True)
    home_club_href_parts = json_normalized['home_club.href'].str.split('/', 5, True)
    away_club_href_parts = json_normalized['away_club.href'].str.split('/', 5, True)

    prep_df['game_id'] = href_parts[4]
    prep_df['league_code'] = parent_href_parts[4]
    prep_df['season'] = infer_season(json_normalized['date'])
    prep_df['round'] = json_normalized['matchday']
    prep_df['date'] = json_normalized['date']
    prep_df['time'] = pandas.to_datetime(json_normalized['time']).dt.time 
    prep_df['home_club_id'] = home_club_href_parts[4]
    prep_df['away_club_id'] = away_club_href_parts[4]
    prep_df[['home_club_goals', 'away_club_goals']] = parse_aggregate(json_normalized['result'])
    prep_df['home_club_position'] = json_normalized['home_club_position'].str.split(' ', 2, True)[2]
    prep_df['away_club_position'] = json_normalized['away_club_position'].str.split(' ', 2, True)[2]
    prep_df['stadium'] = json_normalized['stadium']
    prep_df['attendance'] = json_normalized['attendance'].str.split(' ', 2, True)[1]
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']


    self.set_checkpoint('prep', prep_df)
    return prep_df

  def get_validations(self):
      return []

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='game_id', type='integer'))
    self.schema.add_field(Field(name='league_code', type='string'))
    self.schema.add_field(Field(name='season', type='integer'))
    self.schema.add_field(Field(name='round', type='string'))
    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='time', type='date'))
    self.schema.add_field(Field(name='home_club_id', type='integer'))
    self.schema.add_field(Field(name='away_club_id', type='integer'))
    self.schema.add_field(Field(name='home_club_goals', type='integer'))
    self.schema.add_field(Field(name='away_club_goals', type='integer'))
    self.schema.add_field(Field(name='home_club_position', type='integer'))
    self.schema.add_field(Field(name='away_club_position', type='integer'))
    self.schema.add_field(Field(name='stadium', type='string'))
    self.schema.add_field(Field(name='attendance', type='integer'))
    self.schema.add_field(Field(
        name='url',
        type='string',
        format='uri'
      )
    )

    self.schema.primary_key = ['game_id']
    self.schema.foreign_keys = [
      {"fields": "home_club_id", "reference": {"resource": "clubs", "fields": "club_id"}},
      {"fields": "away_club_id", "reference": {"resource": "clubs", "fields": "club_id"}}
    ]

    return self.schema
