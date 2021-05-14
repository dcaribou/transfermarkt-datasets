from frictionless.check import Check
from frictionless import errors

def no_html_encodes_in_names(row):
    value = row['name']
    if value is not None and '%' in value:
        note = f"{value} contains a '%' character"
        yield errors.CellError.from_row(row, note=note, field_name='name')

checks = [
  no_html_encodes_in_names
]
