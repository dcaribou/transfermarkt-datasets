from frictionless import checks

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field

class CurTransfersAsset(RawAsset):
    name = "cur_transfers"
    file_name = "transfers.csv.gz"

    description = """
    The `transfers` asset contains one row per transfer in the dataset.
    Each transfer is associated with a player, a 'from' club, and a 'to' club.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.schema = Schema(
            fields=[
                Field(name="player_id", type="integer"),
                Field(name="player_name", type="string"),
                Field(name="transfer_date", type="date"),
                Field(name="transfer_season", type="string"),
                Field(name="from_club_id", type="integer"),
                Field(name="to_club_id", type="integer"),
                Field(name="from_club_name", type="string", tags=["explore"]),
                Field(name="to_club_name", type="string", tags=["explore"]),
                Field(
                    name="transfer_fee",
                    type="number",
                    description="The transfer fee in EUR. Null if unknown, 0 if free transfer."
                ),
                Field(
                    name="market_value_in_eur",
                    type="number",
                    description="The player's market value at the time of transfer in EUR."
                )
            ]
        )

        self.schema.primary_key = ["player_id", "transfer_date"]
        self.schema.foreign_keys = [
            {"fields": "player_id", "reference": {"resource": "cur_players", "fields": "player_id"}}
        ]