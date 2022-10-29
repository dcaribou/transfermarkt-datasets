from frictionless.field import Field
from frictionless.schema import Schema
from frictionless import checks

import pandas as pd
import numpy as np

from transfermarkt_datasets.core.asset import Asset

from transfermarkt_datasets.assets.cur_clubs import CurClubsAsset
from transfermarkt_datasets.assets.cur_competitions import CurCompetitionsAsset
from transfermarkt_datasets.assets.cur_games import CurGamesAsset
from transfermarkt_datasets.assets.cur_club_games import CurClubGamesAsset

class CurClubSeasonResultsAsset(Asset):

  name = "cur_club_season_results"
  description = """
  The `club_season_results` asset contains one row per club **per season**.
  Each row shows the club performance on that season across the different competitions:
    national league (`l_`), national cup (`c_`) and international competition (`i_`).
  """
  file_name = "club_season_results.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='club_id', type='integer'))
    self.schema.add_field(Field(name='pretty_name', type='string'))
    self.schema.add_field(Field(name='season', type='integer'))
    self.schema.add_field(Field(name='competition_id', type='string'))
    self.schema.add_field(Field(name='competition_type', type='string'))
    self.schema.add_field(Field(
        name='l_perf',
        type='integer',
        description="Clubs performance in the national league."
      )
    )

    self.schema.primary_key = ["club_id", "season"]

    self.checks = [
      checks.table_dimensions(min_rows=400)
    ]

  def build(
    self,
    cur_games: CurGamesAsset,
    cur_clubs: CurClubsAsset,
    cur_club_games: CurClubGamesAsset,
    cur_competitions: CurCompetitionsAsset,
  ):
    
    games = cur_games.prep_df
    clubs = cur_clubs.prep_df
    club_games = cur_club_games.prep_df
    competitions = cur_competitions.prep_df

    with_game_attributes = club_games.merge(
      games[["game_id", "season", "competition_id"]],
      how="left",
      on="game_id"
    )

    with_competition_attributes = with_game_attributes.merge(
      competitions[["competition_id", "type"]].rename(columns={"type": "competition_type"}),
      how="left",
      on="competition_id"
    )

    points = np.select(
      condlist=[
        with_competition_attributes["own_goals"] > with_competition_attributes["opponent_goals"],
        with_competition_attributes["own_goals"] < with_competition_attributes["opponent_goals"]
      ],
      choicelist=[
        1,
        0
      ],
      default=0
    )

    with_competition_attributes["points"] = points

    club_season_results = (
      with_competition_attributes
        .groupby(by=["club_id", "season", "competition_type"])["points"]
        .agg(func=["count", "sum"])
        .reset_index()
    )

    club_season_results["pct_won"] = club_season_results["sum"] / club_season_results["count"]
    del club_season_results["sum"]
    del club_season_results["count"]

    pivoted = club_season_results.pivot(
      index=["club_id", "season"],
      columns="competition_type",
      values="pct_won"
    ).reset_index()

    with_club_attributes = pivoted.merge(
      clubs[["club_id", "pretty_name"]],
      how="left",
      on="club_id"
    )

    self.prep_df = with_club_attributes
