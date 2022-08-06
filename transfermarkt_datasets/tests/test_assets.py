
import unittest

from dagster import DependencyDefinition
from transfermarkt_datasets.core.asset import Asset

import inspect

class TestAsset(unittest.TestCase):
    def test_initialization(self):

        # test parent asset
        at = Asset(
            name="games",
            seasons=[2013],
            source_path="data/raw",
            target_path="stage"
        )
        
        self.assertEqual(
            at.raw_files_name,
            "games.json"
        )

    def test_load(self):

        at = Asset(
            name="games",
            seasons=[2013],
            source_path="data/raw",
            target_path="stage"
        )

        self.assertGreater(
            len(at.get_stacked_data()),
            1000
        )

    def test_string_representation(self):

        at = Asset(
            name="games",
            seasons=[2013, 2014],
            source_path="data/raw",
            target_path="stage"
        )

        self.assertEqual(
            str(at),
            "Asset(name=games,season=2013..2014)"
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
