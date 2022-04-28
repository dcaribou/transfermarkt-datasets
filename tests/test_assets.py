
import unittest
from transfermarkt_datasets.transfermarkt_datasets import Asset, AssetNotFound

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
