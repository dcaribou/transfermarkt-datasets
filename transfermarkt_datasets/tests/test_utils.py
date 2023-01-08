
import unittest

from transfermarkt_datasets.core.utils import parse_market_value
from transfermarkt_datasets.assets.base_players import build_name

import pandas as pd

class TestUtils(unittest.TestCase):

    def test_parse_market_value(self):

        values = [
            "€3M",
            "€50k",
            "£57k",
            "£70k",
        ]

        expected = [
            3e6,
            50e3,
            57e3,
            70e3
        ]

        parsed_values = [
            parse_market_value(value)
            for value in values
        ]

        self.assertEqual(
            parsed_values,
            expected
        )

    def test_build_name(self):
        
        real = build_name(
            pd.Series(["Lionel", None]),
            pd.Series(["Messi", "Messi"])
        )

        exp = pd.Series(["Lionel Messi", "Messi"])
        
        pd.testing.assert_series_equal(
            real, exp
        )
