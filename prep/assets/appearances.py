import pandas
from frictionless.schema import Schema
from frictionless.field import Field
from typing import List

from .base import BaseProcessor
from .utils import cast_metric, cast_minutes_played

class AppearancesProcessor(BaseProcessor):

  name = 'appearances'
  description = "Appearances for `players`. One row per appearance."

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    
    self.schema = Schema()

    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='game_id', type='integer'))
    self.schema.add_field(Field(name='appearance_id', type='string'))
    self.schema.add_field(Field(name='competition_id', type='string'))
    self.schema.add_field(Field(name='player_club_id', type='integer'))
    self.schema.add_field(Field(name='goals', type='integer'))
    self.schema.add_field(Field(name='assists', type='integer'))
    self.schema.add_field(Field(name='minutes_played', type='integer'))
    self.schema.add_field(Field(name='yellow_cards', type='integer'))
    self.schema.add_field(Field(name='red_cards', type='integer'))
    
    self.schema.primary_key = ['appearance_id']
    
    self.schema.foreign_keys = [
      {"fields": "game_id", "reference": {"resource": "games", "fields": "game_id"}}
    ]

    self.errors_tolerance = 100

  def process_segment(self, segment, season):

    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(segment.to_dict(orient='records'))


    applicable_competitions = self.settings['competition_codes']

    json_normalized = json_normalized[json_normalized['competition_code'].isin(applicable_competitions)]
  
    prep_df['player_id'] = json_normalized['parent.href'].str.split('/', 5, True)[4]
    prep_df['game_id'] = json_normalized['result.href'].str.split('/', 5, True)[4]
    prep_df['appearance_id'] = prep_df['game_id'] + '_' + prep_df['player_id']
    prep_df['competition_id'] = json_normalized['competition_code']
    prep_df['player_club_id'] = json_normalized['for.href'].str.split('/', 5, True)[4]
    prep_df['goals'] = json_normalized['goals'].apply(cast_metric)
    prep_df['assists'] = json_normalized['assists'].apply(cast_metric)
    prep_df['minutes_played'] = json_normalized['minutes_played'].apply(cast_minutes_played)
    prep_df['yellow_cards'] = (
      (json_normalized['yellow_cards'].str.len() > 0).astype('int32') + 
      (json_normalized['second_yellow_cards'].str.len() > 0).astype('int32')
    )
    prep_df['red_cards'] = (json_normalized['red_cards'].str.len() > 0).astype('int32')

    return prep_df
