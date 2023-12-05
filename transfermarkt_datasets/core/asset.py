from frictionless import Detector
from frictionless.resource import Resource
import pandas as pd
import logging
import logging.config

from transfermarkt_datasets.core.schema import Schema

from transfermarkt_datasets.core.utils import (
  read_config,
  get_sample_values
)

class FailedAssetValidation(Exception):
  pass

class InvalidPreparedDF(Exception):
  pass

class Asset:

  description = None
  name = "generic"
  file_name = None
  public = True

  def __init__(
    self,
    settings: dict = None) -> None:

      self._prep_df = None
      self.settings = settings
      self.log = logging.getLogger("main")
      self.prep_location = "data/prep"
      self.datapackage_descriptor_path = f"{self.prep_location}/dataset-metadata.json"

      if not self.file_name:
        file_name = self.name.replace("base_", "")
        self.file_name = file_name + ".csv"

      self.schema = Schema()

  def __str__(self) -> str:
      return f'Asset(name={self.name})'

  @property
  def prep_df(self):
    return self._prep_df

  @prep_df.setter
  def prep_df(self, df):

    df_type = type(df)
    if df_type != pd.DataFrame:
      raise InvalidPreparedDF(f"Invalid df type: {df_type}")
    else:
      df_cols = list(df.columns.values)
      df_cols_set = set(df_cols)
      schema_cols_set = set(self.schema.field_names)
      set_difference = (df_cols_set - schema_cols_set).union(
        schema_cols_set - df_cols_set
      )
      if set_difference != set():
        raise InvalidPreparedDF(
          f"{self.name}: fields do not match provided schema: {set_difference}"
        )

    field_names = self.schema.field_names
    self._prep_df = df[field_names]

  @property
  def file_name(self) -> str:
    return self.name + ".csv.gz"
  
  @property
  def file_name_uncompressed(self) -> str:
    return self.file_name.replace(".gz", "")
  
  @property
  def prep_path(self) -> str:
    return f"{self.prep_location}/{self.file_name}"

  @property
  def frictionless_resource_name(self) -> str:
    return self.file_name_uncompressed.replace(".csv", "")

  def load_from_prep(self):
    """Load prepared dataset from the local to a pandas dataframe.
    """
    self.prep_df = pd.read_csv(
      filepath_or_buffer=self.prep_path
    )

  def load_from_stage(self):
    self.prep_df = pd.read_csv(
      filepath_or_buffer=self.stage_path
    )

  def save_to_stage(self):
    self.prep_df.to_csv(
      self.stage_path,
      index=False
    )

  def schema_as_dataframe(self) -> pd.DataFrame:
    """Render the asset schema as a pandas dataframe.

    Returns:
        pd.DataFrame: A pandas dataframe representing the asset schema.
    """

    fields = [field.name for field in  self.schema.fields]
    types = [field.type for field in  self.schema.fields]
    descriptions = [field.description for field in  self.schema.fields]
    sample_values = [
      get_sample_values(self.prep_df, field.name, 3)
      for field in self.schema.fields
    ]

    df = pd.DataFrame(
      data=dict(
        description=descriptions,
        type=types,
        sample_values=sample_values
      ),
      index=fields
    )
    
    return df

  def as_frictionless_resource(self) -> Resource:

    detector = Detector(schema_sync=True)
    resource = Resource(
      title=self.frictionless_resource_name,
      path=self.file_name_uncompressed,
      detector=detector,
      description=self.description,
      schema=self.schema.as_frictionless_schema()
    )

    return resource

class RawAsset(Asset):

  raw_file_name = None

  def __init__(self, settings: dict = None) -> None:
    super().__init__(settings)

    self.raw_df = None
    self.raw_files_path = "data/raw/transfermarkt-scraper"

    if not self.raw_file_name:
      file_name = self.name.replace("base_", "")
      self.raw_file_name = file_name + ".json.gz"

  def load_raw(self):

    raw_dfs = []

    if "competitions" in self.raw_file_name:
        df = pd.read_json(
          f"data/competitions.json",
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        raw_dfs.append(df)
    else:
      seasons = read_config()["defintions"]["seasons"]
      for season in seasons:

        season_file = f"{self.raw_files_path}/{season}/{self.raw_file_name}"

        self.log.debug("Reading raw data from %s", season_file)
        df = pd.read_json(
          season_file,
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        df["season"] = season
        df["season_file"] = season_file
        if len(df) > 0:
          raw_dfs.append(df)

    self.raw_df = pd.concat(raw_dfs, axis=0)
