from frictionless.field import Field
from frictionless.schema import Schema

from datetime import datetime

import pandas

from .base import BaseProcessor

class GamesProcessor(BaseProcessor):

  name = 'games'
  description = "Games in `competitions`. One row per game."

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='game_id', type='integer'))
    self.schema.add_field(Field(name='competition_code', type='string'))
    self.schema.add_field(Field(name='season', type='integer'))
    self.schema.add_field(Field(name='round', type='string'))
    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='home_club_id', type='integer'))
    self.schema.add_field(Field(name='away_club_id', type='integer'))
    self.schema.add_field(Field(name='home_club_goals', type='integer'))
    self.schema.add_field(Field(name='away_club_goals', type='integer'))
    self.schema.add_field(Field(name='home_club_position', type='integer'))
    self.schema.add_field(Field(name='away_club_position', type='integer'))
    self.schema.add_field(Field(name='stadium', type='string'))
    self.schema.add_field(Field(name='attendance', type='integer'))
    self.schema.add_field(Field(name='referee', type='string'))
    self.schema.add_field(Field(
        name='url',
        type='string',
        format='uri'
      )
    )

    self.schema.primary_key = ['game_id']
    
    # with the inclusion of games from cups, supercups and non national competitions
    # it is not realistic to expect all referential integrity on club IDs, since that
    # would require that clubs up to the 4th or 5th tier are included in the dataset

    # self.schema.foreign_keys = [
    #   {"fields": "home_club_id", "reference": {"resource": "clubs", "fields": "club_id"}},
    #   {"fields": "away_club_id", "reference": {"resource": "clubs", "fields": "club_id"}}
    # ]

  def process_segment(self, segment, season):

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

    # it happens https://www.transfermarkt.co.uk/spielbericht/index/spielbericht/3465097
    json_normalized = json_normalized[json_normalized['result'] != '-:-']

    href_parts = json_normalized['href'].str.split('/', 5, True)
    parent_href_parts = json_normalized['parent.href'].str.split('/', 5, True)
    home_club_href_parts = json_normalized['home_club.href'].str.split('/', 5, True)
    away_club_href_parts = json_normalized['away_club.href'].str.split('/', 5, True)

    prep_df['game_id'] = href_parts[4]
    prep_df['competition_code'] = parent_href_parts[4]
    prep_df['season'] = infer_season(json_normalized['date']).fillna(-1).astype('int32')
    prep_df['round'] = json_normalized['matchday']
    prep_df['date'] = json_normalized['date']
    prep_df['home_club_id'] = home_club_href_parts[4]
    prep_df['away_club_id'] = away_club_href_parts[4]
    prep_df[['home_club_goals', 'away_club_goals']] = parse_aggregate(json_normalized['result'])
    prep_df['home_club_position'] = json_normalized['home_club_position'].str.split(' ', 2, True)[2].str.strip()
    prep_df['away_club_position'] = json_normalized['away_club_position'].str.split(' ', 2, True)[2].str.strip()
    prep_df['stadium'] = json_normalized['stadium']
    prep_df['attendance'] = (
      json_normalized['attendance'].str.split(' ', 2, True)[1]
        .str.replace('.', '', regex=False).str.strip()
    )
    prep_df['referee'] = json_normalized['referee']
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    return prep_df
