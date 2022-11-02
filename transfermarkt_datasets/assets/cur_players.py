from frictionless import checks

from inflection import titleize

import pandas
import numpy

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.core.utils import parse_market_value
from transfermarkt_datasets.core.checks import too_many_missings

from transfermarkt_datasets.assets.base_players import BasePlayersAsset
from transfermarkt_datasets.assets.base_clubs import BaseClubsAsset
from transfermarkt_datasets.assets.cur_competitions import CurCompetitionsAsset

class CurPlayersAsset(RawAsset):

  name = "cur_players"
  file_name = "players.csv"
  
  description = """
  The `players` asset contains one row per player in the dataset.
  All `players` are either currently part of a club in `clubs` or they have been at some point in the past.
  """
  
  "Players in `clubs`. One row per player."

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
    self.schema.add_field(Field(name='agent_name', type='string'))
    self.schema.add_field(Field(name='domestic_competition_id', type='string'))
    self.schema.add_field(Field(name='club_name', type='string'))
    self.schema.add_field(Field(
      name='image_url',
      type='string',
      form='uri'
      )
    )
    self.schema.add_field(Field(
      name='url',
      type='string',
      form='uri'
      )
    )

    self.schema.primary_key = ['player_id']
    self.schema.foreign_keys = [
      {"fields": "current_club_id", "reference": {"resource": "cur_clubs", "fields": "club_id"}},
      {"fields": "domestic_competition_id", "reference": {"resource": "cur_competition", "fields": "competition_id"}},
    ]

    self.checks = [
      checks.row_constraint(formula="position in 'Attack,Defender,Midfield,Goalkeeper,Missing'"),
      too_many_missings(field_name="market_value_in_gbp", tolerance=0.30),
      checks.table_dimensions(min_rows=22000)

    ]

  def build(
    self,
    base_players: BasePlayersAsset,
    base_clubs: BaseClubsAsset):

    players = base_players.prep_df
    clubs = base_clubs.prep_df[
      ["club_id", "domestic_competition_id", "name", "pretty_name"]
    ]

    with_club_attributes = players.merge(
      clubs.rename(columns={
          "club_id": "club_id",
          "domestic_competition_id": "domestic_competition_id",
          "name": "club_name",
          "pretty_name": "club_pretty_name"
        }
      ),
      how="left",
      left_on="current_club_id",
      right_on="club_id"
    )

    self.prep_df = with_club_attributes




