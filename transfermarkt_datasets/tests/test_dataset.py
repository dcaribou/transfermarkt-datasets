
import pathlib
import unittest
from dagster import DependencyDefinition, GraphDefinition
import pytest
from transfermarkt_datasets.core.dataset import Dataset, AssetNotFound
from transfermarkt_datasets.core.asset import Asset
from frictionless.package import Package

import shutil, tempfile

import sys
from os import path

@pytest.fixture(scope="session")
def assets_folder(tmp_path_factory):
    fn = tmp_path_factory.mktemp("assets") / "base_something.py"
    fn.write_text(
"""
from transfermarkt_datasets.core.asset import Asset

class BaseSomethingAsset(Asset):
    name = "base_something"
    pass
"""
        )
    return fn


class TestDataset(unittest.TestCase):

    def test_build_all(self):
        td = Dataset(config_file="config.yml")

        td.discover_assets()
        td.build_assets()

    def test_datapackage(self):
        td = Dataset()
        td.discover_assets()

        td.generate_datapackage()

        self.assertIsInstance(
            td.datapackage,
            Package
        )
        self.assertEqual(
            set(td.datapackage.resource_names),
            set(["games", "players", "player_valuations",
            "competitions", "appearances", "clubs"
            ])
        )

    def test_discover(self):
        
        with tempfile.TemporaryDirectory() as tmprootdir:
            tmprootpath = pathlib.Path(tmprootdir)
            tmpassetpath = tmprootpath / "assets"
            tmpassetpath.mkdir()
            with open(path.join(str(tmpassetpath), "base_something.py"), "w") as f:
                f.write(
"""
from transfermarkt_datasets.core.asset import Asset

class BaseSomethingAsset(Asset):
    name = "base_something"
    pass
""")
            sys.path.insert(0,tmprootdir)

            td = Dataset(
                assets_root=str(tmprootdir),
                assets_relative_path="assets"
            )
            td.discover_assets()

            self.assertEquals(
                td.asset_names,
                ["base_something"]
            )

    def test_dependencies(self):
        class BaseSomethingAssetA(Asset):
            name = "base_something_a"

        class BaseSomethingAssetB(Asset):
            name = "base_something_b"
            def build(self, other: BaseSomethingAssetA):
                pass

        td = Dataset()
        td.assets = {
            "base_something_a": BaseSomethingAssetA(),
            "base_something_b": BaseSomethingAssetB()
        }

        self.assertEquals(
            td.get_dependencies(),
            {
                "base_something_a": {},
                "base_something_b": {
                    "other": DependencyDefinition("build_base_something_a")
                }
            }
        )
