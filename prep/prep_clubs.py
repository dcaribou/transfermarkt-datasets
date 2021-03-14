import pandas as pd
from lib.prep_lib import *

# TODO: read from arguments
raw_file = '../data/raw/clubs.json'
prep_file = '../data/prep/clubs.csv'

raw_clubs = pd.read_json(raw_file,lines=True)

raw_clubs = pd.json_normalize(raw_clubs.to_dict(orient='records'))

clubs = pd.DataFrame()

club_href_parts = raw_clubs['href'].str.split('/', 5, True)
clubs['club_id'] = club_href_parts[4]
clubs['name'] = club_href_parts[1]
clubs['domestic_competition'] = raw_clubs['parent.href'].str.split('/', 5, True)[4]

clubs.to_csv(
  prep_file,
  index=False
)
