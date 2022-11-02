
from typing import List

import frictionless

class Field:
    def __init__(
        self,
        name: str,
        type: str,
        description: str = None,
        form: str = None) -> None:
        
        self.name = name
        self.type = type
        self.description = description
        self.form = form

    def as_frictionless_field(self) -> frictionless.Field:
        fl_field = frictionless.Field(
            name=self.name,
            type=self.type,
            description=self.description
            # format=self.form
        )
        return fl_field

class Schema:
    def __init__(self, fields: List[Field] = []) -> None:
        self.fields: List[Field] = fields
        self.primary_key: List[str] = []
        self.foreign_keys: List[str] = []
    
    def add_field(self, field: Field) -> None:
        self.fields.append(
            field
        )

    def as_frictionless_schema(self) -> frictionless.Schema:

        fl_schema = frictionless.Schema()
        for field in self.fields:
            fl_schema.add_field(
                field.as_frictionless_field()
            )
        
        return fl_schema



