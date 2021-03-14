from numpy.lib.function_base import select
import pandas
import numpy
import json
from typing import List
from datetime import datetime, timedelta

class BaseProcessor:
  def __init__(self, raw_file_path, prep_file_path=None) -> None:

      self.raw_file_path = raw_file_path
      if prep_file_path:
        self.prep_file_path = prep_file_path
      else:
        self.prep_file_path = (
          raw_file_path
            .replace('raw', 'prep')
            .replace('.json', '.csv')
          )

      self.raw_df = pandas.read_json(
        raw_file_path,
        lines=True,
        convert_dates=True,
        orient={'index', 'date'}
      )

      self.prep_df = None
      
      self.checkpoints = {}

  def set_checkpoint(self, name, df):
    self.checkpoints[name] = df

  def get_checkpoint(self, name):
    return self.checkpoints[name]

  def export(self):
  
    self.prep_df.to_csv(
      self.prep_file_path,
      index=False
    )


  def flatten(df: pandas.DataFrame, list_fields: List[str]) -> pandas.DataFrame:
    """Flatten a dataframe on a 'list' type colum
    Parameters:
      df -> A pandas dataframe
      list_fields -> A list of fields of type 'list'
    """
    
    non_list_fileds = [column for column in df.columns if column not in list_fields]

    df_as_json_lists_string = df.to_json(orient='records')
    df_as_json_lists = json.loads(df_as_json_lists_string)

    return pandas.json_normalize(
      df_as_json_lists,
      list_fields,
      non_list_fileds
    )

  @classmethod
  def renames(cls, df: pandas.DataFrame, mappings: dict) -> pandas.DataFrame:
    return df.rename(
      columns=mappings,
      errors="raise"
    )

  @classmethod
  def create_surrogate_key(cls, name: str, columns: List[str], df: pandas.DataFrame) -> pandas.DataFrame:
    games_df = df.drop_duplicates(subset=columns).sort_values(
      by=columns
    )[columns]
    games_df[name] = games_df.index.values + 1
    return df.merge(
      games_df,
      on=columns
    )

  @classmethod
  def parse_aggregate(cls, series: pandas.Series) -> pandas.DataFrame:
    parsed = series.str.split(":", expand=True)
    cols = ['home_club_goals', 'away_club_goals']
    parsed.columns = cols
    parsed[cols] = parsed[cols].apply(pandas.to_numeric, errors='coerce')
    return parsed

  @classmethod
  def infer_season(cls, series: pandas.Series) -> pandas.Series:
    def infer_season(date: datetime):
      year = date.year
      month = date.month
      if month >= 8 and month <= 12:
        return year
      if month >= 1 and month <8:
        return year - 1

    return series.apply(infer_season)

  def get_validations(self):
    pass

  def validate(self):
    validations = self.get_validations()
    failed_validations = 0

    df = self.prep_df

    def assert_df_not_empty(df: pandas.DataFrame):
      assert len(df) > 0
      
    def assert_unique_on_column(df: pandas.DataFrame, columns):
      counts = df.groupby(columns).size().reset_index(name='counts')
      assert len(counts[counts['counts'] > 1]) == 0, counts[counts['counts'] > 1]
    # -----------------------------
    # consistency
    # -----------------------------
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
      failed_validations = numpy.bitwise_not(clubs_per_domestic_competition.between(12, 21))
      assert failed_validations.sum() == 0, failed_validations

    def assert_games_per_season_per_club(df: pandas.DataFrame):
      games_per_season_per_club = (
        df.groupby(['season', 'competition', 'player_club_id'])['date'].nunique()
      )
      assert (games_per_season_per_club != 38).sum() == 0

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
      'assert_games_per_season_per_club': assert_games_per_season_per_club,
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
        failed_validations+=1
        error_asset = error.args[0]
        count = len(error_asset)
        print(f"--> Validation {validation} did not pass: {count}")
        print(error_asset)

    return failed_validations



