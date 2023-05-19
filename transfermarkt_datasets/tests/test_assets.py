
import unittest

import pandas as pd

from transfermarkt_datasets.core.asset import (
    Asset,
    RawAsset
)
from transfermarkt_datasets.core.schema import Schema, Field

class TestAsset(unittest.TestCase):

    def test_load(self):

        class BaseGamesAsset(RawAsset):
            name = "games"

        at = BaseGamesAsset()
        at.load_raw()

        self.assertGreater(
            len(at.raw_df),
            1000
        )

    def test_string_representation(self):

        class SomeAsset(Asset):
            name = "some_name"
        
        at = SomeAsset()

        self.assertEqual(
            str(at),
            "Asset(name=some_name)"
        )

    def test_df_assignment(self):

        class TestAsset(Asset):
            def __init__(self, settings: dict = None) -> None:
                super().__init__(settings)

                self.schema = Schema(
                    fields=[
                        Field(name="col1", type="type1"),
                        Field(name="col2", type="type1"),
                        Field(name="col3", type="type1")
                    ]
                )

            def build(self) -> None:
                self.prep_df = pd.DataFrame(
                    data={
                        "col2": [1, 2],
                        "col1": ["a", "b"],
                        "col3": [0.2, 0.4],
                    }
                )

        at = TestAsset()
        at.build()

        self.assertEqual(
            list(at.prep_df.columns.values),
            ["col1", "col2", "col3"]
        )

    def test_asset_file_name(self):
        class TestAssetAAsset(Asset):
            name = "asset_a"

        class TestAssetBAsset(Asset):
            name = "asset_b"
            file_name = "asset_b_filename.csv"

        a = TestAssetAAsset()
        b = TestAssetBAsset()

        self.assertEqual(
            a.file_name,
            "asset_a.csv.gz"
        )

        self.assertEqual(
            b.file_name,
            "asset_b_filename.csv"
        )

    def test_schema_as_dataframe(self):

        class TestAsset(Asset):
            def __init__(self, settings: dict = None) -> None:
                super().__init__(settings)

                self.schema = Schema()
                self.schema.add_field(
                    Field(name="some_field", type="string")
                )
                self.schema.add_field(
                    Field(name="some_other_field", type="integer")
                )
        
        at = TestAsset()
        at.prep_df = pd.DataFrame(
            data={
                "some_field": [1, 2],
                "some_other_field": ["b", "c"],
            }
        )
        df = at.schema_as_dataframe()

        df_expected = pd.DataFrame(
            data={
                "description": [None, None],
                "type": ["string", "integer"],
                "sample_values": [
                    [1,2],
                    ["b", "c"]
                ]
            },
            index=["some_field", "some_other_field"]
        )

        self.assertTrue(df.equals(df_expected))

