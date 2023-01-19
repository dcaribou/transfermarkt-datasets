from frictionless import Check, errors
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
        note = "(%s): exceeded missings pct (%f) after %i recors" % (
          self.__field_name, limit_pct, self.__memory["total"]
        )
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
