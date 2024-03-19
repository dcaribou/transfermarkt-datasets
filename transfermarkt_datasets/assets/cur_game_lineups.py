from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field

class CurGameLineupsAsset(Asset):

  name = "cur_game_lineups"
  description = """
  The `games_lineups` asset contains one row per game player in the dataset.
  Players are extracted from the game ["line-ups"](https://www.transfermarkt.co.uk/spielbericht/aufstellung/spielbericht/3098550) in transfermarkt and they are tied to one particular `game`, identified by the `game_id` column.
  """
  file_name = "game_lineups.csv.gz"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema(
      fields=[
        Field(
          name='game_lineups_id',
          type='string',
          description="Surrogate key"
        ),
        Field(name='game_id', type='integer'),
        Field(name='player_id', type='integer'),
        Field(name='club_id', type='integer'),
        Field(name='type', type='string'),
        Field(name='player_name', type='string'),
        Field(name='team_captain', type='string'),
        Field(name='number', type='string'),
        Field(name='position', type='string'),
        Field(name='date', type='date'),
      ]
    )

    self.schema.primary_key = [
      'game_id',
      'player_id',
      'club_id',
    ]
