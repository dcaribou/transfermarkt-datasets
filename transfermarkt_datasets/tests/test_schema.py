import unittest

from transfermarkt_datasets.core.schema import Schema, Field

from frictionless.schema import Schema as FLSchema
from frictionless.field import Field as FLField

class TestSchema(unittest.TestCase):

    def test_add_field(self):

        schema = Schema()
        self.assertEquals(
            len(schema.fields),
            0
        )

        field = Field(name="name", type="type")
        schema.add_field(
            field
        )

        self.assertEqual(
            len(schema.fields),
            1
        )

    def test_as_frictionless_schema(self):

        schema = Schema()
        schema.add_field(
            Field(
                name="some_name",
                type="some_type"
            )
        )

        self.assertIsInstance(
            schema.as_frictionless_schema(),
            FLSchema
        )

        fl_schema = FLSchema()
        fl_schema.add_field(FLField(
            name="some_name",
            type="some_type"
        ))
        
        as_fl_schema = schema.as_frictionless_schema()
        self.assertEqual(
            as_fl_schema,
            fl_schema
        )

    def test_get_fiedls_by_tag(self):

        schema = Schema(
            fields=[
                Field(name="f1", type="t1", tags=["t1"]),
                Field(name="f2", type="t1", tags=["t2"]),
            ]
        )

        self.assertEqual(
            schema.get_fields_by_tag("t2"),
            [Field(name="f2", type="t1", tags=["t1"])]
        )
