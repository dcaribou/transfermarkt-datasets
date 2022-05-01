from frictionless import Check, errors

from transfermarkt_datasets.core.dataset import read_config

def no_html_encodes_in_names(row):
    value = row['name']
    if value is not None and '%' in value:
        note = f"{value} contains a '%' character"
        yield errors.CellError.from_row(row, note=note, field_name='name')

def competition_code_in_range(row):
  COMPETITION_CODES = read_config()["settings"]["competition_codes"]

  value = row['competition_code']
  if value is not None and value not in COMPETITION_CODES:
      note = f"Invalid competition_code {value}. Valid values are {COMPETITION_CODES}"
      yield errors.CellError.from_row(row, note=note, field_name='competition_code')

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

class too_many_missings(Check):
    code = "too-many-missings"

    def __init__(self, descriptor=None, *, field_name, tolerance):
      self.setinitial("field_name", field_name)
      self.setinitial("tolerance", tolerance)
      super().__init__(descriptor)
      self.__memory = {}
      self.__field_name = self["field_name"]
      self.__tolerance = self["tolerance"]
      self.__memory["n_missings"] = 0
      self.__memory["total"] = 0

    def validate_row(self, row):
      value = row[self.__field_name]

      if not value:
        self.__memory["n_missings"] += 1

      self.__memory["total"] += 1

      yield from []

    def validate_end(self):
      limit_pct = self.__memory["n_missings"] / self.__memory["total"]
      if limit_pct > self.__tolerance:
        note = "Exceeded missings pct (%f) after %i recors" % (limit_pct, self.__memory["total"])
        yield MissingValuesPctExceededError(note=note)

    # Metadata

    metadata_profile = {  # type: ignore
        "type": "object",
        "properties": {},
    }
    
class MissingValuesPctExceededError(errors.TableError):
  code = "too-many-missings"
  name = "Missing Values Pct Exceeded"
  tags = ["#table", "#row", "#missings"]
  template = "Missing threshold exceeded: {note}"
  description = "The are too many missings for a field."

checks = [
  no_html_encodes_in_names,
  competition_code_in_range,
  # this check takes ages as it is right now, we need to find a better way to validate this
  # country_name_in_range
]
