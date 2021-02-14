import pandas as pd
from lib.prep_lib import *

# TODO: read from arguments
raw_file = '../data/raw/appearances.json'
prep_file = '../data/prep/appearances.csv'

raw = pd.read_json(
  raw_file,
  lines=True,
  convert_dates=True,
  orient={'index','date'}
)

with_new_columns = add_new_columns(raw)

mappings = {
    'matchday': 'round',
    'for_club_name': 'player_club_name',
    'pos': 'player_position',
    # 'confederation': 'club_confederation', # TODO
    # 'domestic_competition': 'club_domestic_competition', # TODO
    'competition_code': 'competition'
}
with_renamed_columns = renames(with_new_columns, mappings)

with_improved_columns = improve_columns(with_renamed_columns)

with_filtered_appearances = filter_appearances(with_improved_columns)

validations = [
    'assert_df_not_empty',
    'assert_minutes_played_gt_120',
    'assert_goals_in_range',
    'assert_assists_in_range',
    # 'assert_own_goals_in_range', # TODO
    'assert_yellow_cards_range',
    'assert_red_cards_range',
    'assert_unique_on_player_and_date',
    'assert_clubs_per_competition',
    # it should pass for historical seasons with 20 clubs
    # 'assert_games_per_season_per_club',
    'assert_appearances_per_match',
    # 'assert_appearances_per_club_per_game', # TODO
    # 'assert_appearances_freshness_is_less_than_one_week', # TODO
    'assert_goals_ne_assists',
    # 'assert_goals_ne_own_goals', # TODO
    'assert_yellow_cards_not_constant',
    'assert_red_cards_not_constant'
]

failed_validations = validate(with_filtered_appearances, validations)
if failed_validations > 0:
    raise Exception(f"{failed_validations} validations did not pass")

with_filtered_appearances.to_csv(
  prep_file,
  index=False
)
