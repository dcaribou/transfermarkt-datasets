import pandas
import numpy
import json
from typing import List
from datetime import date,datetime

def flatten(df: pandas.DataFrame, list_fields: List[str]) -> pandas.DataFrame:

  non_list_fileds = [column for column in df.columns if column not in list_fields]

  df_as_json_lists_string = df.to_json(orient='records')
  df_as_json_lists = json.loads(df_as_json_lists_string)

  return pandas.json_normalize(
    df_as_json_lists,
    list_fields,
    non_list_fileds
  )

def renames(df: pandas.DataFrame, mappings: dict) -> pandas.DataFrame:
  return df.rename(
    columns=mappings,
    errors="raise"
  )

def create_surrogate_key(name: str, columns: List[str], df: pandas.DataFrame) -> pandas.DataFrame:
  games_df = df.drop_duplicates(subset=columns).sort_values(
    by=columns
  )[columns]
  games_df[name] = games_df.index.values + 1
  return df.merge(
    games_df,
    on=columns
  )

def parse_aggregate(series: pandas.Series) -> pandas.DataFrame:
  parsed = series.str.split(":", expand=True)
  cols = ['home_club_goals', 'away_club_goals']
  parsed.columns = cols
  parsed[cols] = parsed[cols].apply(pandas.to_numeric, errors='coerce')
  return parsed

def infer_season(series: pandas.Series) -> pandas.Series:
  def infer_season(date: datetime):
    year = date.year
    month = date.month
    if month >= 8 and month <= 12:
      return year
    if month >= 1 and month <8:
      return year - 1

  return series.apply(infer_season)

def add_new_columns(df: pandas.DataFrame) -> pandas.DataFrame:

  df_new = create_surrogate_key('game_id', ['home_club_name', 'date'], df)
  df_new = create_surrogate_key('player_id', ['player_name', 'player_club_name'], df_new)
  df_new = create_surrogate_key('appearance_id', ['player_id', 'date'], df_new)
  df_new = create_surrogate_key('home_club_id', ['home_club_name'], df_new)
  df_new = create_surrogate_key('away_club_id', ['away_club_name'], df_new)

  df_new[['home_club_goals', 'away_club_goals']] = parse_aggregate(df_new['result'])
  del df_new['result']

  df_new['season'] = infer_season(df_new['date'])

  return df_new

# - improved columns:
#  - yellow_cards / red_cards (no need for second_yellows)
#  - club name formatting: fc-watford -> FC Watford
#  - player name formatting: adam-masina -> Adam Masina
#  - position: use longer names instead of the chryptic 'LB', etc (use 'filter by position' in https://www.transfermarkt.co.uk/diogo-jota/leistungsdatendetails/spieler/340950/saison/2020/verein/0/liga/0/wettbewerb/GB1/pos/0/trainer_id/0/plus/1)
def improve_columns(df: pandas.DataFrame) -> pandas.DataFrame:
  # recasts
  df['goals'] = df['goals'].astype('int32')
  df['assists'] = df['goals'].astype('int32')
  df['own_goals'] = df['goals'].astype('int32')
  df['date'] = pandas.to_datetime(df['date'])

  # reshape cards columns
  df['yellow_cards'] = (df['yellow_cards'] != '0').astype('int32') + (df['second_yellows'] != '0').astype('int32')
  del df['second_yellows']

  df['red_cards'] = (df['red_cards'] != '0').astype('int32')

  return df

def filter_appearances(df: pandas.DataFrame) -> pandas.DataFrame:
  # get rid of 2017 season data as we only have it partially
  df = df[df['season'] == 2018]

  domestic_competitions = [
    'ES1', 'GB1', 'DFB'
  ]
  # filer appearances on a different league
  condition = numpy.bitwise_not(((df['club_domestic_competition'].isin(domestic_competitions)) & (df['club_domestic_competition'] != df['competition'])))
  df = df[condition]
  return df

def assert_unique_on_column(df: pandas.DataFrame, columns):
  counts = df.groupby(columns).size().reset_index(name='counts')
  assert len(counts[counts['counts'] > 1]) == 0

def validate(df: pandas.DataFrame):
  assert len(df) > 0
  # -----------------------------
  # consistency
  # -----------------------------
  assert_unique_on_column(df, ['player_id', 'date'])
  assert len(df[df['minutes_played'] > 90]) == 0
  assert len(df[~df['goals'].between(0, 5)]) == 0
  assert len(df[~df['assists'].between(0, 4)]) == 0
  assert len(df[~df['own_goals'].between(0, 3)]) == 0
  assert len(df[~df['yellow_cards'].between(0,2)]) == 0
  assert len(df[~df['red_cards'].between(0,1)]) == 0

  # -----------------------------
  # completeness
  # -----------------------------

  # number of teams per domestic competition must be exactly 20
  clubs_per_domestic_competition = (
    df.groupby(['season', 'club_domestic_competition'])['player_club_name'].nunique()
  )
  try:
    assert (clubs_per_domestic_competition != 20).sum() == 0
  except AssertionError:
    print("Validation clubs_per_domestic_competition did not pass")

  # each club must play 38 games per season on the domestic competition
  games_per_season_per_club = (
    df[df['competition'] == df['club_domestic_competition']]
    .groupby(['season', 'competition', 'player_club_name'])['date']
    .nunique()
  )
  try:
    assert (games_per_season_per_club != 38).sum() == 0
  except AssertionError:
    print("Validation games_per_season_per_club did not pass")

  # on each match, both clubs should have at least 11 appearances
  appearances_per_match = (
    df.groupby(['home_club_name', 'away_club_name', 'date'])['appearance_id'].nunique()
  )

  try:
    assert (appearances_per_match < 11).sum() == 0
  except AssertionError:
    print("Validation appearances_per_match did not pass")

  # similarly, each club must have at least 11 appearances per game
  appearances_per_club_per_game = (
    df.groupby(['player_club_name', 'game_id'])['appearance_id'].nunique()
  )
  
  try:
    assert (appearances_per_club_per_game < 11).sum() == 0
  except AssertionError:
    print("Validation appearances_per_club_per_game did not pass")
