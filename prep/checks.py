from frictionless import errors

def no_html_encodes_in_names(row):
    value = row['name']
    if value is not None and '%' in value:
        note = f"{value} contains a '%' character"
        yield errors.CellError.from_row(row, note=note, field_name='name')

def league_code_in_range(row):
  LEAGUE_CODES = [
    'ES1', 'GB1', 'L1', 'IT1', 'FR1', 'GR1', 'PO1', 
    'BE1', 'UKR1', 'BE1', 'RU1', 'DK1', 'SC1', 'TR1',
    'NL1'
  ]
  value = row['league_code']
  if value is not None and value not in LEAGUE_CODES:
      note = f"Invalid league_code {value}. Valid values are {LEAGUE_CODES}"
      yield errors.CellError.from_row(row, note=note, field_name='league_code')

def country_name_in_range(row):
  import pycountry
  headers = row.keys()
  for header in headers:
    if header.startswith('country_'):
      value = row[header]

      n_matches = 0

      if value is not None:
        try:
          country_search = pycountry.countries.search_fuzzy(value)
          n_matches = len(country_search)
        except LookupError:
          historic_country_search = pycountry.historic_countries.search_fuzzy(value)
          n_matches = len(historic_country_search)
        except LookupError:
          n_matches = 0

      if value is not None and n_matches == 0:
        note = f"Invalid country {value}. Check allowed values at https://github.com/flyingcircusio/pycountry"
        yield errors.CellError.from_row(row, note=note, field_name=header)

checks = [
  no_html_encodes_in_names,
  league_code_in_range,
  # this check takes ages as it is right now, we need to find a better way to validate this
  # country_name_in_range
]
