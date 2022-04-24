from frictionless.field import Field
from frictionless.schema import Schema

import pandas

from transfermarkt_datasets.assets.asset import Asset

class CompetitionsProcessor(Asset):

  name = "competitions"
  description = "Competitions in Europe confederation. One row per league."

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)

    self.schema = Schema()

    self.schema.add_field(Field(name='competition_id', type='string'))
    self.schema.add_field(Field(name='name', type='string'))
    self.schema.add_field(Field(name='type', type='string'))
    self.schema.add_field(Field(name='country_id', type='integer'))
    self.schema.add_field(Field(name='country_name', type='string'))
    self.schema.add_field(Field(name='domestic_league_code', type='string'))
    self.schema.add_field(Field(name='confederation', type='string'))
    self.schema.add_field(Field(
        name='url',
        type='string',
        format='uri'
      )
    )

    self.schema.primary_key = ['competition_id']

  def build(self):
    
    raw_df = self.get_stacked_data()
    prep_df = pandas.DataFrame()

    json_normalized = pandas.json_normalize(raw_df.to_dict(orient='records'))

    league_href_parts = json_normalized['href'].str.split('/', 5, True)
    confederation_href_parts = json_normalized['parent.href'].str.split('/', 5, True)

    prep_df['competition_id'] = league_href_parts[4]
    prep_df['name'] = league_href_parts[1]
    prep_df['type'] = json_normalized['competition_type']
    prep_df['country_id'] = json_normalized['country_id'].fillna(-1).astype('int32')
    prep_df['country_name'] = json_normalized['country_name']
    prep_df['domestic_league_code'] = json_normalized['country_code']
    
    prep_df['confederation'] = confederation_href_parts[2]
    prep_df['url'] = 'https://www.transfermarkt.co.uk' + json_normalized['href']

    self.prep_df = prep_df

    self.drop_duplicates()

    return prep_df
