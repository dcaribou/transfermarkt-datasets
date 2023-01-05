from frictionless import checks

import pandas as pd
import numpy as np

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field

from transfermarkt_datasets.assets.base_players import BasePlayersAsset
from transfermarkt_datasets.assets.base_clubs import BaseClubsAsset

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

    self.schema = Schema(
      fields=[
        Field(name='player_id', type='integer'),
        Field(name='pretty_name', type='string'),
        Field(name='current_club_id', type='integer'),
        Field(name='club_pretty_name', type='string', tags=["explore"]),
        Field(name='country_of_citizenship', type='string'),
        Field(name='country_of_birth', type='string'),
        Field(name='city_of_birth', type='string'),
        Field(name='date_of_birth', type='date'),
        Field(name='position', type='string'),
        Field(name='sub_position', type='string'),
        Field(name='name', type='string'),
        Field(name='foot', type='string'),
        Field(name='height_in_cm', type='integer'),
        Field(name='market_value_in_gbp', type='number'),
        Field(name='highest_market_value_in_gbp', type='number'),
        Field(name='agent_name', type='string'),
        Field(name='contract_expiration_date', type='date'),
        Field(name='domestic_competition_id', type='string'),
        Field(name='club_name', type='string'),
        Field(
          name='image_url',
          type='string',
          form='uri'
        ),
        Field(name='last_season', type='integer'),
        Field(
          name='url',
          type='string',
          form='uri'
        )
      ]
    )

    self.schema.primary_key = ['player_id']
    self.schema.foreign_keys = [
      {"fields": "current_club_id", "reference": {"resource": "cur_clubs", "fields": "club_id"}},
      {"fields": "domestic_competition_id", "reference": {"resource": "cur_competition", "fields": "competition_id"}},
    ]

    self.checks = [
      checks.table_dimensions(min_rows=25000)
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
          "club_id": "current_club_id",
          "domestic_competition_id": "current_club_domestic_competition_id",
          "name": "current_club_name",
          "pretty_name": "current_club_pretty_name"
        }
      ),
      how="left",
      on="current_club_id"
    )

    self.prep_df = with_club_attributes




