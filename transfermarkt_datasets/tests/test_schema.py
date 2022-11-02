import unittest

from transfermarkt_datasets.core.schema import Schema, Field

import frictionless

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
            frictionless.Schema
        )

        fl_schema = frictionless.Schema()
        fl_schema.add_field(frictionless.Field(
            name="some_name",
            type="some_type"
        ))
        
        self.assertEqual(
            schema.as_frictionless_schema(),
            fl_schema
        )
