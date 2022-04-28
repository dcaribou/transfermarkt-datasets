
import unittest
from transfermarkt_datasets.transfermarkt_datasets import AssetNotFound, TransfermarktDatasets
from frictionless.package import Package

class TestTransfermarktDatasets(unittest.TestCase):
    def setUp(self) -> None:
        self.td = TransfermarktDatasets()
        return super().setUp()
    def test_initialization_from_dict(self):

        config = {
            "assets": {
                "games": {"class": "GamesAsset"},
                "undefined_asset": {"class": "UndefinedAsset"}
            },
            "settings": {
                "source_path": "data/raw",
                "seasons": [2019],
                "competition_codes": "ES1"
            }
        }

        with self.assertRaises(AssetNotFound) as cm:
            td = TransfermarktDatasets(config=config)

        self.assertEqual(
            cm.exception.asset_name,
            "undefined_asset"
        )

        del config["assets"]["undefined_asset"]

        td = TransfermarktDatasets(config=config)
        self.assertSetEqual(
            set(["games"]),
            set(td.asset_names)
        )

    def test_initialization_from_file(self):
        td = TransfermarktDatasets(
            config_file="config.yml",
            seasons=[2013]
        )
        self.assertEqual(
            set(["games", "players", "player_valuations", "competitions", "appearances", "clubs"]),
            td.asset_names
        )

    def test_build_single(self):

        td = self.td

        td.build_assets(asset="games")

        self.assertGreater(
            len(td.assets["games"].prep_df),
            1000
        )

    def test_build_all(self):
        td = TransfermarktDatasets(
            config_file="config.yml",
            seasons=[2014]
        )

        td.build_assets()

    def test_datapackage(self):
        td = TransfermarktDatasets()

        td.generate_datapackage()

        self.assertIsInstance(
            td.datapackage,
            Package
        )
        self.assertEqual(
            set(td.datapackage.resource_names),
            set(["games", "players", "player_valuations", "competitions", "appearances", "clubs"])
        )
