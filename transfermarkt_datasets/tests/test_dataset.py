
import pathlib
import unittest
import pytest
from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset

from frictionless.package import Package

import tempfile

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

    def setUp(self) -> None:
        class BaseSomethingAssetA(Asset):
            name = "base_something_a"
            file_name = "file1.csv"

            def __init__(self, settings: dict = None) -> None:
                super().__init__(settings)
                self.schema.foreign_keys = [
                    {
                        "fields": "some_id",
                        "reference": {
                            "resource": "base_something_b",
                            "fields": "some_other_id"
                            }
                    }
                ]

        class BaseSomethingAssetB(Asset):
            name = "base_something_b"
            file_name = "file2.csv"
            public = False

            def build(self, other: BaseSomethingAssetA):
                pass

        td = Dataset()
        td.assets = {
            "base_something_a": BaseSomethingAssetA(),
            "base_something_b": BaseSomethingAssetB()
        }

        self.dataset = td

    def test_datapackage(self):
        td = Dataset()

        td.as_frictionless_package()

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

            self.assertEqual(
                td.asset_names,
                ["base_something"]
            )

    def test_datapackage(self):

        td = self.dataset
        dp : Package = td.as_frictionless_package()

        self.assertEqual(
            dp.resource_names,
            ["file1", "file2"]
        )

        self.assertEqual(
            [ resource.path for resource in dp.resources],
            ["file1.csv", "file2.csv"]
        )

        dp_excluded = td.as_frictionless_package(exclude_private=True)
        self.assertEqual(
            dp_excluded.resource_names,
            ["file1"]
        )
