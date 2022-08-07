
import unittest

from dagster import DependencyDefinition
from transfermarkt_datasets.core.asset import Asset, RawAsset

from frictionless.resource import Resource
from frictionless.schema import Schema
from frictionless.field import Field

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

        self.assertEqual(
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

        self.assertEqual(
            a.file_name,
            "asset_a.csv"
        )

        self.assertEqual(
            b.file_name,
            "asset_b_filename.csv"
        )

    def test_frictionless(self):

        asset_schema = Schema(
            fields=[
                Field(name="col1", type="string")
            ]
        )

        asset_name = "some_asset"
        asset_file_name = "file.csv"

        class TestAsset(Asset):
            name = asset_name
            file_name = asset_file_name
            def __init__(self, settings: dict = None) -> None:
                super().__init__(settings)
                self.schema = asset_schema

        at = TestAsset()
        rs = at.as_frictionless_resource()

        self.assertEqual(rs.name, at.frictionless_resource_name)
        self.assertEqual(rs.path, at.file_name)
