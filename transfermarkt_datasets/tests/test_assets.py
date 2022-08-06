
import unittest

from dagster import DependencyDefinition
from transfermarkt_datasets.core.asset import Asset, RawAsset

import inspect

class TestAsset(unittest.TestCase):

    def test_load(self):

        class BaseGamesAsset(RawAsset):
            name = "games"

        at = BaseGamesAsset()
        at.load_raw_from_stage()

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

    def test_asset_deps(self):

        class TestAssetAAsset(Asset):
            name = "asset_a"
        class TestAssetBAsset(Asset):
            name = "asset_b"
        class SomeTestAsset(Asset):
            name = "some_asset"
            def build(self, test_asset_a: TestAssetAAsset, test_asset_b: TestAssetBAsset):
                pass

        at = SomeTestAsset()
        
        s = inspect.signature(at.build)

        self.assertEquals(
            at.as_dagster_deps(),
            {
                "test_asset_a": DependencyDefinition("build_asset_a"),
                "test_asset_b": DependencyDefinition("build_asset_b")
            }
        )

    def test_asset_file_name(self):
        class TestAssetAAsset(Asset):
            name = "asset_a"

        class TestAssetBAsset(Asset):
            name = "asset_b"
            file_name = "asset_b_filename.csv"

        a = TestAssetAAsset()
        b = TestAssetBAsset()

        self.assertEquals(
            a.file_name,
            "asset_a.csv"
        )

        self.assertEquals(
            b.file_name,
            "asset_b_filename.csv"
        )
