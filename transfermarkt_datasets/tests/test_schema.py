import unittest

from transfermarkt_datasets.core.schema import Schema, Field

from frictionless.schema import Schema as FLSchema

class TestSchema(unittest.TestCase):

    def test_add_field(self):

        schema = Schema()
        self.assertEqual(
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
