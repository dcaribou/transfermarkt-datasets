from frictionless import checks

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field
from transfermarkt_datasets.assets.base_games import BaseGamesAsset
from transfermarkt_datasets.assets.base_clubs import BaseClubsAsset
from transfermarkt_datasets.assets.base_competitions import BaseCompetitionsAsset

class CurGamesAsset(Asset):

  name = "cur_games"
  description = """
  The `games` asset contains one row per game in the dataset.
  All games are tied to one particular `competition`.
  """
  file_name = "games.csv"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema(
      fields=[
        Field(name='game_id', type='integer'),
        Field(name='competition_id', type='string', tags=["explore"]),
        Field(name='competition_type', type='string'),
        Field(name='season', type='integer', tags=["explore"]),
        Field(name='round', type='string', tags=["explore"]),
        Field(name='date', type='date', tags=["explore"]),
        Field(name='home_club_id', type='integer'),
        Field(name='away_club_id', type='integer'),
        Field(name='home_club_goals', type='integer'),
        Field(name='away_club_goals', type='integer'),
        Field(name='aggregate', type='string'),
        Field(name='home_club_position', type='integer'),
        Field(name='away_club_position', type='integer'),
        Field(name='club_home_name', type='string', tags=["explore"]),
        Field(name='club_away_name', type='string', tags=["explore"]),
        Field(name='home_club_manager_name', type='string'),
        Field(name='away_club_manager_name', type='string'),
        Field(name='stadium', type='string'),
        Field(name='attendance', type='integer'),
        Field(name='referee', type='string'),
        Field(
          name='url',
          type='string',
          form='uri'
        )
      ]
    )

    self.schema.primary_key = ['game_id']
    
    self.checks = [
      checks.table_dimensions(min_rows=55000)
    ]

  def build(
    self,
    base_games: BaseGamesAsset,
    base_clubs: BaseClubsAsset,
    base_competitions: BaseCompetitionsAsset
    ):

    games = base_games.prep_df
    competitions = base_competitions.prep_df

    clubs = base_clubs.prep_df[
      ["club_id", "name"]
    ]

    with_home_attributes = games.merge(
      clubs.rename(columns={"name": "club_home_name"}, errors="raise"),
      how="left",
      left_on="home_club_id",
      right_on="club_id"
    )
    del with_home_attributes["club_id"]

    with_away_attributes = with_home_attributes.merge(
      clubs.rename(columns={"name": "club_away_name"}, errors="raise"),
      how="left",
      left_on="away_club_id",
      right_on="club_id"
    )
    del with_away_attributes["club_id"]

    with_away_attributes["aggregate"] = (
      with_away_attributes["home_club_goals"].astype("string") + 
      ":" + 
      with_away_attributes["away_club_goals"].astype("string")
    )

    with_competitions_attributes = with_away_attributes.merge(
      competitions[["competition_id", "type"]].rename(
        columns={"type": "competition_type"}
      ),
      how="left",
      on="competition_id"
    )

    self.prep_df = with_competitions_attributes
