from os import stat
from prep.prep_utils import *
import pytest
import pandas as pd
import pandas.testing as pdt

@pytest.fixture
def base_input():
    return pd.DataFrame(
      [
        {'player': 'lionel-messi', 'stats': [
          {'date': '2020-01-01', 'goals': 5},
          {'date': '2020-01-08', 'goals': 1},
        ]},
        {'player': 'vinicius-jr', 'stats': [
          {'date': '2020-01-01', 'goals': 0},
          {'date': '2020-01-08', 'goals': 0},
        ]}
      ]
    )

@pytest.fixture
def base_output():
  return pd.DataFrame(
      [
        {'date': '2020-01-01', 'goals': 5, 'player': 'lionel-messi'},
        {'date': '2020-01-08', 'goals': 1, 'player': 'lionel-messi'},
        {'date': '2020-01-01', 'goals': 0, 'player': 'vinicius-jr'},
        {'date': '2020-01-08', 'goals': 0, 'player': 'vinicius-jr'},
      ]
    )

@pytest.fixture
def base_input_more_columns(base_input: pandas.DataFrame):
  new_df = base_input.copy()
  new_df['position'] = ['striker', 'striker']
  return new_df
  
@pytest.fixture
def base_output_more_columns(base_output: pandas.DataFrame):
  new_df = base_output.copy()
  new_df['position'] = ['striker', 'striker', 'striker', 'striker']
  return new_df

def test_flatten(base_input, base_output):
  pdt.assert_frame_equal(
    flatten(base_input, ['stats']),
    base_output
  )

def test_flatten_more_columns(base_input_more_columns, base_output_more_columns):
  pdt.assert_frame_equal(
    flatten(base_input_more_columns, ['stats']),
    base_output_more_columns
  )

def test_create_surrogate_key():
  pdt.assert_frame_equal(
    create_surrogate_key(
      name='col3',
      columns=['col1', 'col2'],
      df=pd.DataFrame(
        [
          {'col1': 1, 'col2': 'one'},
          {'col1': 1, 'col2': 'two'},
          {'col1': 2, 'col2': 'one'},
          {'col1': 2, 'col2': 'one'},
        ]
      )
    ),
    pd.DataFrame(
      [
        {'col1': 1, 'col2': 'one', 'col3': 1},
        {'col1': 1, 'col2': 'two', 'col3': 2},
        {'col1': 2, 'col2': 'one', 'col3': 3},
        {'col1': 2, 'col2': 'one', 'col3': 3},
      ]
    ),
  )

def test_parse_aggregate():
  pdt.assert_frame_equal(
    parse_aggregate(pd.Series(['0:4', '1:3', '01:44', ':3', '1:22'])),
    pd.DataFrame(
      {
        'home_club_goals': [0,1,1,None,1],
        'away_club_goals': [4,3,44,3,22]
      }
    )
  )

def test_infer_season():
  pdt.assert_series_equal(
    infer_season(pd.Series([
      datetime(2019,1,1),
      datetime(2020,1,4),
      datetime(2020,8,23)
    ])),
    pd.Series(
      [2018,2019,2020]
    )
  )
