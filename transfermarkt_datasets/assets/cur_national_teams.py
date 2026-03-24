from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field

class CurNationalTeamsAsset(RawAsset):
    name = "cur_national_teams"
    file_name = "national_teams.csv.gz"

    description = """
    The `national_teams` asset contains one row per national team in the dataset.
    Each national team includes squad details, market value, coaching staff, and FIFA ranking.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.schema = Schema(
            fields=[
                Field(name="national_team_id", type="integer"),
                Field(name="name", type="string", tags=["explore"]),
                Field(name="country_id", type="integer"),
                Field(name="country_name", type="string"),
                Field(name="confederation", type="string"),
                Field(name="squad_size", type="integer"),
                Field(
                    name="total_market_value",
                    type="number",
                    description="Aggregated players' Transfermarkt market value in EUR."
                ),
                Field(name="coach_name", type="string"),
                Field(name="fifa_ranking", type="integer"),
                Field(name="url", type="string", form="uri"),
            ]
        )

        self.schema.primary_key = ["national_team_id"]
        self.schema.foreign_keys = [
            {"fields": "country_id", "reference": {"resource": "cur_countries", "fields": "country_id"}}
        ]
