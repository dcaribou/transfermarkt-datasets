from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

from datetime import datetime

import pandas

from transfermarkt_datasets.core.asset import Asset

class CurGamesAsset(Asset):

  name = 'cur_games'
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
    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='pretty_name', type='string'))
    self.schema.add_field(Field(name='domestic_competition_id', type='string'))
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
    # self.schema.add_field(Field(name='club_id_club_home', type='integer'))
    # self.schema.add_field(Field(name='name_club_home', type='string'))
    # self.schema.add_field(Field(name='pretty_name_club_home', type='string'))
    # self.schema.add_field(Field(name='domestic_competition_id_club_home', type='string'))
    # self.schema.add_field(Field(
    #     name='total_market_value_club_home',
    #     type='number',
    #     description="Aggregated players' Transfermarkt market value in millions of pounds"
    #   )
    # )
    # self.schema.add_field(Field(name='squad_size_club_home', type='integer'))
    # self.schema.add_field(Field(name='average_age_club_home', type='number'))
    # self.schema.add_field(Field(name='foreigners_number_club_home', type='integer'))
    # self.schema.add_field(Field(name='foreigners_percentage_club_home', type='number'))
    # self.schema.add_field(Field(name='national_team_players_club_home', type='integer'))
    # self.schema.add_field(Field(name='stadium_name_club_home', type='string'))
    # self.schema.add_field(Field(name='stadium_seats_club_home', type='integer'))
    # self.schema.add_field(Field(name='net_transfer_record_club_home', type='string'))
    # self.schema.add_field(Field(name='coach_name_club_home', type='string'))
    # self.schema.add_field(Field(
    #   name='url_club_home',
    #   type='string',
    #   format='uri'
    #   )
    # )

    self.schema.primary_key = ['game_id']
    
    self.checks = [
      checks.table_dimensions(min_rows=55000)
    ]

  def build(self, context, base_games: Asset, base_clubs: Asset):

    self.prep_df = base_games.prep_df.merge(
        base_clubs.prep_df,
        how="left",
        left_on="home_club_id",
        right_on="club_id",
        suffixes=[None, "_club_home"]
    )
