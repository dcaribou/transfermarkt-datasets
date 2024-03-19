from frictionless import checks

import pandas as pd

from transfermarkt_datasets.core.asset import RawAsset
from transfermarkt_datasets.core.schema import Schema, Field

class CurCompetitionsAsset(RawAsset):

  name = "cur_competitions"
  description = """
  The `competitions` asset contains one row per competition in the dataset, including national leagues, cups and international tournaments. 
  """
  file_name = "competitions.csv.gz"

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name="competition_id", type="string"))
    self.schema.add_field(Field(name="competition_code", type="string"))
    self.schema.add_field(Field(name="name", type="string"))
    self.schema.add_field(Field(name="type", type="string"))
    self.schema.add_field(Field(name="sub_type", type="string"))
    self.schema.add_field(Field(
      name="is_major_national_league",
      type="boolean",
      description="Competition is a major national league in the confederation."
      )
    )
    self.schema.add_field(Field(name="country_id", type="integer"))
    self.schema.add_field(Field(name="country_name", type="string"))
    self.schema.add_field(Field(name="domestic_league_code", type="string"))
    self.schema.add_field(Field(name="confederation", type="string", tags=["explore"]))
    self.schema.add_field(Field(
        name="url",
        type="string",
        form="uri"
      )
    )

    self.schema.primary_key = ["competition_id"]
