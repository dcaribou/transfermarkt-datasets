from frictionless import checks

from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field
class CurGamesAsset(Asset):

  name = "cur_games"
  description = """
  The `games` asset contains one row per game in the dataset.
  All games are tied to one particular `competition`.
  """
  file_name = "games.csv.gz"

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
        Field(name='home_club_name', type='string', tags=["explore"]),
        Field(name='away_club_name', type='string', tags=["explore"]),
        Field(name='home_club_manager_name', type='string'),
        Field(name='away_club_manager_name', type='string'),
        Field(name='home_club_formation', type='string'),
        Field(name='away_club_formation', type='string'),
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
