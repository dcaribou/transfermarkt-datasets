from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

from datetime import datetime

import pandas as pd

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.checks import too_many_missings

class BaseGamesAsset(RawAsset):

  name = "base_games"
  description = "Games in `competitions`. One row per game."
  file_name = "base_games.csv"
  public = False

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
    self.schema.add_field(Field(name='home_club_manager_name', type='string'))
    self.schema.add_field(Field(name='away_club_manager_name', type='string'))
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
    
    self.checks = [
      checks.table_dimensions(min_rows=55000),
      # too_many_missings(field_name="home_manager_name", tolerance=0.30),
      # too_many_missings(field_name="away_manager_name", tolerance=0.30)
    ]

  def build(self):

    self.load_raw()

    def parse_aggregate(series: pd.Series) -> pd.DataFrame:
      parsed = series.str.split(":", expand=True)
      cols = ['home_club_goals', 'away_club_goals']
      parsed.columns = cols
      parsed[cols] = parsed[cols].astype('int32',errors='ignore')
      return parsed

    def infer_season(series: pd.Series) -> pd.Series:
      def infer_season(date: datetime):
        year = date.year
        month = date.month
        if month >= 8 and month <= 12:
          return year
        if month >= 1 and month <8:
          return year - 1

      return series.apply(infer_season)
    
    prep_df = pd.DataFrame()

    json_normalized = pd.json_normalize(self.raw_df.to_dict(orient='records'))

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
    prep_df['home_club_position'] = (
      json_normalized['home_club_position'].str.split(' ', 2, True)[2].str.strip()
        .fillna(-1).astype('int32')
    )
    prep_df['away_club_position'] = (
      json_normalized['away_club_position'].str.split(' ', 2, True)[2].str.strip()
        .fillna(-1).astype('int32')
    )
    prep_df['home_club_manager_name'] = json_normalized['home_manager.name']
    prep_df['away_club_manager_name'] = json_normalized['away_manager.name']
    prep_df['stadium'] = json_normalized['stadium']
    prep_df['attendance'] = (
      json_normalized['attendance'].str.split(' ', 2, True)[1]
        .str.replace('.', '', regex=False).str.strip()
        .fillna(0).astype('int32')
    )
    prep_df['referee'] = json_normalized['referee']
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    self.prep_df = prep_df

    self.drop_duplicates()

    return prep_df
