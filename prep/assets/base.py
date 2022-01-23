from frictionless import Detector
from frictionless.resource import Resource
from numpy.lib.function_base import select
import pandas
import numpy
from datetime import datetime, timedelta

class BaseProcessor:
  name = None
  description = None

  def __init__(self, raw_files_path, seasons, name, prep_file_path) -> None:

      self.raw_files_path = raw_files_path
      self.seasons = seasons
      self.prep_file_path = prep_file_path

      self.raw_dfs = []
      self.prep_df = None
      self.validations = None
      self.validation_report = None
      
      self.checkpoints = {}

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
        df = pandas.read_json(
          f"{self.raw_files_path}/{season}/{self.name}.json",
          lines=True,
          convert_dates=True,
          orient={'index', 'date'}
        )
        if len(df) > 0:
          self.raw_dfs.append(df)
  
  def process_segment(self, segment):
    """Process one segment of the asset. A segment is equivalent to one file.
    Segment processors are defined per asset on the corresponding asset processor.

    :param segment: A pandas dataframe with the contents of the raw file

    :returns: A pandas dataframe with parsed, cleaned asset fields
    """
    pass

  def process(self):
    self.prep_dfs = [self.process_segment(prep_df) for prep_df in self.raw_dfs]
    concatenated = pandas.concat(self.prep_dfs, axis=0)
    if self.schema.primary_key:
      self.prep_df = concatenated.drop_duplicates(subset=self.schema.primary_key, keep='last')
    else:
      self.prep_df = concatenated.drop_duplicates(keep='last')
  
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

  def set_checkpoint(self, name, df):
    self.checkpoints[name] = df

  def get_checkpoint(self, name):
    return self.checkpoints[name]

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


  def get_validations(self):
    pass

  def validate(self):
    validations = self.get_validations()
    failed_validations = []

    df = self.prep_df

    def assert_df_not_empty(df: pandas.DataFrame):
      assert len(df) > 0

    # -----------------------------
    # schema
    # -----------------------------
    def assert_expected_columns(df: pandas.DataFrame):
      pass;

    # -----------------------------
    # consistency
    # -----------------------------
    def assert_unique_on_column(df: pandas.DataFrame, columns):
      counts = df.groupby(columns).size().reset_index(name='counts')
      assert len(counts[counts['counts'] > 1]) == 0, counts[counts['counts'] > 1]
    def assert_minutes_played_gt_120(df: pandas.DataFrame):
      appearances_w_mp_gt_90 = len(df[df['minutes_played'] > 120])
      assert appearances_w_mp_gt_90 == 0, appearances_w_mp_gt_90
    def assert_goals_in_range(df: pandas.DataFrame):
      assert len(df[df['goals'] > 90]) == 0
    def assert_assists_in_range(df: pandas.DataFrame):
      assert len(df[~df['assists'].between(0, 4)]) == 0
    def assert_own_goals_in_range(df: pandas.DataFrame):
      assert len(df[~df['own_goals'].between(0, 3)]) == 0
    def assert_yellow_cards_range(df: pandas.DataFrame):
      assert len(df[~df['yellow_cards'].between(0,2)]) == 0
    def assert_red_cards_range(df: pandas.DataFrame):
      assert len(df[~df['red_cards'].between(0,1)]) == 0
    def assert_unique_on_player_and_date(df: pandas.DataFrame):
      assert_unique_on_column(df, ['player_id', 'date'])
    def assert_goals_ne_assists(df: pandas.DataFrame):
      assert (df['goals'] != df['assists']).sum() > 0
    def assert_goals_ne_own_goals(df: pandas.DataFrame):
      assert (df['goals'] != df['own_goals']).sum() > 0
    def assert_yellow_cards_not_constant(df: pandas.DataFrame):
      assert df['yellow_cards'].nunique() != 1
    def assert_red_cards_not_constant(df: pandas.DataFrame):
      assert df['red_cards'].nunique() != 1

    # -----------------------------
    # completeness
    # -----------------------------
    def assert_clubs_per_competition(df: pandas.DataFrame):
      clubs_per_domestic_competition = (
        df.groupby(['season', 'competition'])['player_club_id'].nunique()
      )
      # the number of clubs of the scotish league is 12
      # the number of clubs of the turkish league is 21
      failed_validations = clubs_per_domestic_competition[numpy.bitwise_not(clubs_per_domestic_competition.between(12, 21))]
      assert failed_validations.sum() == 0, failed_validations

    def assert_appearances_per_match(df: pandas.DataFrame):
      appearances_per_match = (
        df.groupby(['home_club_id', 'away_club_id', 'date'])['appearance_id'].nunique()
      )
      failed_validations = appearances_per_match < 11
      assert failed_validations.sum() == 0, failed_validations

    def assert_appearances_per_club_per_game(df: pandas.DataFrame):
      # allow inconsitencies for recent game match
      df_new = df[df['date'] < (datetime.now() - timedelta(days=2))]
      # similarly, each club must have at least 11 appearances per game
      appearances_per_club_per_game = (
        df_new.groupby(['player_club_id', 'game_id'])['appearance_id'].nunique()
      )
      faulty_games = (appearances_per_club_per_game < 11).sum()
      total_games = df_new['game_id'].nunique()
      assert faulty_games == 0, f"{faulty_games}/{total_games}"

    def assert_appearances_freshness_is_less_than_one_week(df: pandas.DataFrame):
      max_appearance_per_club = (
        df.groupby(['player_club_id'])['date'].max()
      )
      total_clubs = len(max_appearance_per_club)
      stale_clubs = (max_appearance_per_club < (datetime.utcnow() - timedelta(days=7))).sum()
      # if more than 5 clubs are stale, fail validation
      assert stale_clubs/total_clubs < 0.4, f"{stale_clubs}/{total_clubs}"

    validations_base = {
      'assert_df_not_empty': assert_df_not_empty,
      'assert_minutes_played_gt_120': assert_minutes_played_gt_120,
      'assert_goals_in_range': assert_goals_in_range,
      'assert_assists_in_range': assert_assists_in_range,
      'assert_own_goals_in_range': assert_own_goals_in_range,
      'assert_yellow_cards_range': assert_yellow_cards_range,
      'assert_red_cards_range': assert_red_cards_range,
      'assert_unique_on_player_and_date': assert_unique_on_player_and_date,
      'assert_clubs_per_competition': assert_clubs_per_competition,
      'assert_appearances_per_match': assert_appearances_per_match,
      'assert_appearances_per_club_per_game': assert_appearances_per_club_per_game,
      'assert_appearances_freshness_is_less_than_one_week': assert_appearances_freshness_is_less_than_one_week,
      'assert_goals_ne_own_goals': assert_goals_ne_own_goals,
      'assert_goals_ne_assists': assert_goals_ne_assists,
      'assert_yellow_cards_not_constant': assert_yellow_cards_not_constant,
      'assert_red_cards_not_constant': assert_red_cards_not_constant,
    }

    for validation in validations:
      try:
        validations_base[validation](df)
      except AssertionError as error:
        error_asset = error.args[0]
        failed_validations.append({
          'validation': validation,
          'error_asset': error_asset
        })

    self.validations = failed_validations
    return failed_validations



