
from typing import List

import frictionless

class Field:
    def __init__(
        self,
        name: str,
        type: str,
        description: str = None,
        tags: List[str] = None,
        form: str = None) -> None:
        
        self.name = name
        self.type = type
        self.description = description
        self.form = form
        self.tags = tags or []

    def __eq__(self, __o: object) -> bool:
        return self.name == __o.name

    def as_frictionless_field(self) -> frictionless.Field:
        fl_field = frictionless.Field(
            name=self.name,
            type=self.type,
            description=self.description,
            format=self.form
        )

        return fl_field

    def has_tag(self, tag: str) -> bool:
        if tag in self.tags:
            return True
        else:
            return False

class Schema:
    def __init__(
        self,
        fields: List[Field] = None,
        primary_key: List[str] = None,
        foreign_keys: List[str] = None) -> None:

        self.fields = fields or []
        self.primary_key = primary_key or []
        self.foreign_keys = foreign_keys or []

    @property
    def field_names(self):
        return [field.name for field in self.fields]
    
    def add_field(self, field: Field) -> None:
        self.fields.append(
            field
        )

    def get_fields_by_tag(self, tag: str) -> List[Field]:

        matched_tag = [
            field for field in self.fields if field.has_tag(tag)
        ]

        return matched_tag

    def as_frictionless_schema(self) -> frictionless.schema.Schema:

        fl_fields = [field.as_frictionless_field()
            for field in self.fields
        ]
        fl_schema = frictionless.schema.Schema(
            fields=fl_fields
        )
        
        return fl_schema



