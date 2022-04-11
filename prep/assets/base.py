from frictionless import Detector
from frictionless.resource import Resource
import pandas
import logging
import logging.config

class BaseProcessor:
  name = None
  description = None

  def __init__(self, raw_files_path, seasons, name, prep_file_path, raw_files_name: str = None, settings: dict = None) -> None:

      self.raw_files_path = raw_files_path
      self.seasons = seasons
      self.prep_file_path = prep_file_path

      self.raw_dfs = []
      self.prep_df = None
      self.validations = None
      self.validation_report = None
      self.errors_tolerance = 0

      if not raw_files_name:
        self.raw_files_name = name + ".json"
      else:
        self.raw_files_name = raw_files_name
      
      self.settings = settings

      logging.config.dictConfig(settings["logging"])
      self.log = logging.getLogger("main")

  def load_partitions(self):
    if self.name == 'competitions':
        df = pandas.read_json(
          f"data/competitions.json",
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        self.raw_dfs.append(df)
    else:
      for season in self.seasons:

        season_file = f"{self.raw_files_path}/{season}/{self.raw_files_name}"

        self.log.debug("Reading raw data from %s", season_file)
        df = pandas.read_json(
          season_file,
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        if len(df) > 0:
          self.raw_dfs.append(df)
  
  def process_segment(self, segment, season):
    """Process one segment of the asset. A segment is equivalent to one file.
    Segment processors are defined per asset on the corresponding asset processor.

    :param segment: A pandas dataframe with the contents of the raw file

    :returns: A pandas dataframe with parsed, cleaned asset fields
    """
    pass

  def process(self):

    self.log.info("Stared processing asset %s", self.name)

    self.load_partitions()

    self.prep_dfs = [
      self.process_segment(prep_df, season)
      for prep_df, season in zip(self.raw_dfs, self.seasons)
    ]
    concatenated = pandas.concat(self.prep_dfs, axis=0)

    del self.prep_dfs
    del self.raw_dfs

    if self.schema.primary_key:
      self.prep_df = concatenated.drop_duplicates(subset=self.schema.primary_key, keep='last')
    else:
      self.prep_df = concatenated.drop_duplicates(keep='last')

    self.log.info("Finished processing asset %s\n%s", self.name, self.output_summary())
  
  def url_unquote(self, url_series):
    from urllib.parse import unquote
    return url_series.apply(unquote)

  def url_prepend(self, partial_url_series):
    return 'https://www.transfermarkt.co.uk' + partial_url_series

  def get_columns(self):
    pass

  def output_summary(self):
    from tabulate import tabulate # https://github.com/astanin/python-tabulate
    summary = self.prep_df.describe()
    summary.insert(0, 'metric', summary.index)
    table = summary.values.tolist()
    return tabulate(table, headers=summary.columns, floatfmt=".2f")

  def export(self):
  
    self.prep_df.to_csv(
      self.prep_file_path,
      index=False
    )

  def get_resource(self, basepath):
    detector = Detector(schema_sync=True)
    resource = Resource(
      title=self.name,
      path=self.prep_file_path.split('/')[-1],
      trusted=True,
      detector=detector,
      description=self.description,
      basepath=basepath
    )
    resource.schema = self.schema
    return resource

  def is_valid(self):
    self.log.debug("Checking validation results for %s", self.name)
    results = self.validation_report['stats']['errors']
    self.log.debug("Failed validations are %i, tolerance is %i", results, self.errors_tolerance)
    if results > self.errors_tolerance:
      self.log.error("Invalid asset\n%s", self.validation_report['tasks'][0]['errors'][0])
      return False
    else:
      return True


