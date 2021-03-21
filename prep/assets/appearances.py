import numpy
import pandas
from frictionless.schema import Schema
from frictionless.field import Field

from .base import BaseProcessor

class AppearancesProcessor(BaseProcessor):

  def process(self):

    # 1 - ADD

    def get_href_part(href, part):
      href_parts = href.split('/')
      return href_parts[part]

    df = self.raw_df.copy()

    df['for_club_id'] = df['for'].apply(lambda x: get_href_part(x['href'], 4)).astype('int32')

    df['opponent_club_id'] = df['opponent'].apply(lambda x: get_href_part(x['href'], 4)).astype(int)

    df['home_club_id'] = numpy.where(
      df['venue'] == 'H',
      df['for_club_id'],
      df['opponent_club_id']
    )

    df['away_club_id'] = numpy.where(
      df['venue'] == 'H',
      df['opponent_club_id'],
      df['for_club_id']
    )

    df['player_id'] = df['parent'].apply(lambda x: get_href_part(x['href'], 4)).astype('int32')
    df['player_name'] = df['parent'].apply(lambda x: get_href_part(x['href'], 1))
    df['player_club_id'] = df['for_club_id']

    df_new = self.create_surrogate_key('game_id', ['home_club_id', 'date'], df)
    df_new = self.create_surrogate_key('appearance_id', ['player_id', 'date'], df_new)

    df_new[['home_club_goals', 'away_club_goals']] = self.parse_aggregate(df_new['result'])
    del df_new['result']

    df_new['season'] = self.infer_season(df_new['date'])

    self.set_checkpoint('add', df_new)

    # 2 - RENAME

    mappings = {
      'matchday': 'round',
      'pos': 'player_position',
      # 'confederation': 'club_confederation', # TODO
      'competition_code': 'competition'
    }

    df = self.renames(
      self.get_checkpoint('add'), 
      mappings
    )

    self.set_checkpoint('renames', df)

    # 3 - IMPROVE columns

    def cast_metric(metric):
      if len(metric) == 0:
        return 0
      else:
        return int(metric)

    def cast_minutes_played(minutes_played):
      if len(minutes_played) > 0:
        numeric = minutes_played[:-1]
        return int(numeric)
      else:
        return 0

    df = self.get_checkpoint('renames')

    # recasts
    df['goals'] = df['goals'].apply(cast_metric)
    df['assists'] = df['assists'].apply(cast_metric)
    # df['own_goals'] = df['own_goals'].apply(cast_metric)
    df['date'] = pandas.to_datetime(df['date'])
    df['minutes_played'] = df['minutes_played'].apply(cast_minutes_played)

    # reshape cards columns
    df['yellow_cards'] = (df['yellow_cards'].str.len() > 0).astype('int32') + (df['second_yellow_cards'].str.len() > 0).astype('int32')
    del df['second_yellow_cards']

    df['red_cards'] = (df['red_cards'].str.len() > 0).astype('int32')

    # player position long name
    df['player_position'] = numpy.select(
      [
          df['player_position'] == 'RW', 
          df['player_position'] == 'RB',
          df['player_position'] == 'SW',
          df['player_position'] == 'LM',
          df['player_position'] == 'GK',
          df['player_position'] == 'LB',
          df['player_position'] == 'DM',
          df['player_position'] == 'CB',
          df['player_position'] == 'CF',
          df['player_position'] == 'LW',
          df['player_position'] == 'SS',
          df['player_position'] == 'CM',
          df['player_position'] == 'AM',
          df['player_position'] == 'RM',
      ], 
      [
          'Right Winger', 
          'Right-Back',
          'Sweeper',
          'Left Midfield',
          'Goalkeeper',
          'Left-Back',
          'Defensive Midfield',
          'Center-Back',
          'Center Forward',
          'Left Winger',
          'Second Striker',
          'Center Midfiled',
          'Attacking Midfield',
          'Right Midfield',
      ], 
      default=numpy.NaN
    )

    self.set_checkpoint('improve', df)

    # 3 - FILTER appearances

    df = self.get_checkpoint('improve')

    # get rid of 2017 season data as we only have it partially
    df = df[df['season'] == 2020]

    domestic_competitions = [
      'ES1', 'GB1', 'L1', 'IT1', 'FR1', 'GR1', 'PO1', 'BE1', 'UKR1', 'BE1', 'RU1', 'DK1', 'SC1', 'TR1', 'NL1'
    ]
    df = df[df['competition'].isin(domestic_competitions)]

    del df['type']
    del df['href']
    del df['parent']
    del df['venue']
    del df['for']
    del df['for_club_id']
    del df['opponent']
    del df['opponent_club_id']

    df = df.drop_duplicates()

    self.set_checkpoint('prep', df)
    self.prep_df = df

  def get_validations(self):
    return [
      'assert_df_not_empty',
      'assert_minutes_played_gt_120',
      'assert_goals_in_range',
      'assert_assists_in_range',
      # 'assert_own_goals_in_range', # TODO
      'assert_yellow_cards_range',
      'assert_red_cards_range',
      'assert_unique_on_player_and_date',
      'assert_clubs_per_competition',
      # it should pass for historical seasons with 20 clubs
      'assert_games_per_season_per_club',
      'assert_appearances_per_match',
      # 'assert_appearances_per_club_per_game', # TODO
      # 'assert_appearances_freshness_is_less_than_one_week', # TODO
      'assert_goals_ne_assists',
      # 'assert_goals_ne_own_goals', # TODO
      'assert_yellow_cards_not_constant',
      'assert_red_cards_not_constant'
    ]

  def resource_schema(self):
    self.schema = Schema()

    self.schema.add_field(Field(name='competition', type='string'))
    self.schema.add_field(Field(name='round', type='string'))
    self.schema.add_field(Field(name='date', type='date'))
    self.schema.add_field(Field(name='player_position', type='string'))
    self.schema.add_field(Field(name='goals', type='integer'))
    self.schema.add_field(Field(name='assists', type='integer'))
    self.schema.add_field(Field(name='yellow_cards', type='integer'))
    self.schema.add_field(Field(name='red_cards', type='integer'))
    self.schema.add_field(Field(name='minutes_played', type='integer'))
    self.schema.add_field(Field(name='home_club_id', type='integer'))
    self.schema.add_field(Field(name='away_club_id', type='integer'))
    self.schema.add_field(Field(name='player_id', type='integer'))
    self.schema.add_field(Field(name='player_name', type='string'))
    self.schema.add_field(Field(name='player_club_id', type='integer'))
    self.schema.add_field(Field(name='game_id', type='integer'))
    self.schema.add_field(Field(name='appearance_id', type='integer'))  
    self.schema.add_field(Field(name='home_club_goals', type='integer'))
    self.schema.add_field(Field(name='away_club_goals', type='integer'))
    self.schema.add_field(Field(name='season', type='integer'))
    
    # self.schema.add_field(Field(name='own_goals', type='integer'))
    # self.schema.add_field(Field(name='domestic_competition', type='string'))
    self.schema.primary_key = ['appearance_id']
    
    self.schema.foreign_keys = [
      {"fields": "home_club_id", "reference": {"resource": "clubs", "fields": "club_id"}}
    ]

    return self.schema
