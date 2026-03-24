from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field

class CurCountriesAsset(RawAsset):
    name = "cur_countries"
    file_name = "countries.csv.gz"

    description = """
    The `countries` asset contains one row per country in the dataset.
    Each country includes aggregate statistics such as total clubs, total players, and confederation membership.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.schema = Schema(
            fields=[
                Field(name="country_id", type="integer"),
                Field(name="country_name", type="string", tags=["explore"]),
                Field(name="country_code", type="string"),
                Field(name="confederation", type="string"),
                Field(name="total_clubs", type="integer"),
                Field(name="total_players", type="integer"),
                Field(name="url", type="string", form="uri"),
            ]
        )

        self.schema.primary_key = ["country_id"]
