from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.core.schema import Schema, Field

class CurGameEventsAsset(Asset):

  name = "cur_game_events"
  description = """
  The `games_events` asset contains one row per game event in the dataset.
  Events are extracted from the game ["match sheet"](https://www.transfermarkt.co.uk/spielbericht/index/spielbericht/3098550) in transfermarkt and they are tied to one particular `game`, identified by the `game_id` column.

  The asset currently contains `Goals`, `Subsitutions` and `Cards` event types.
  """
  file_name = "game_events.csv.gz"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema(
      fields=[
        Field(
          name='game_event_id',
          type='string',
          description="Surrogate key"
        ),
        Field(name='date', type='date'),
        Field(name='game_id', type='integer'),
        Field(name='player_id', type='integer'),
        Field(name='club_id', type='integer'),
        Field(name='type', type='string'),
        Field(name='minute', type='integer'),
        Field(name='description', type='string'),
        Field(
          name='player_in_id',
          type='string',
          description="For subsitution events, ID of the player who joins the game. Null otherwise"
        ),
        Field(
          name='player_assist_id',
          type='string',
          description="For goal events, ID of the player who did the assist. Null otherwise"
        ),
      ]
    )

    self.schema.primary_key = [
      'game_id',
      'player_id',
      'club_id',
      'type',
      'minute',
      'description',
      'player_in_id',
      'player_assist_id'
    ]
