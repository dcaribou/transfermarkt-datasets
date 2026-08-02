"""Tests for the transfermarkt-api acquirer.

These cover the safeguards that stop a blocked API run from destroying good
raw data. On 2026-07-11 the acquisition was blocked and wrote 22,324
`{"response": null}` rows over season 2025, wiping 242MB of transfer history
that then propagated to the published dataset.
"""

import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "acquiring" / "transfermarkt-api.py"


def load_acquirer():
    """Import the hyphenated acquirer script as a module."""

    spec = importlib.util.spec_from_file_location("transfermarkt_api_acquirer", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tm_api = load_acquirer()


def responses(player_ids, null_ids=()):
    """Build a response list, with `null_ids` coming back as failures."""

    return [
        {
            "response": None if pid in null_ids else {"transfers": [{"dateUnformatted": "2025-07-01"}]},
            "player_id": pid,
        }
        for pid in player_ids
    ]


class TestPersistGuard(unittest.TestCase):
    """persist_data must not overwrite good data with a failed run."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.path = str(pathlib.Path(self.tmpdir.name) / "transfers.json")
        self.good_content = json.dumps(responses([1, 2, 3])[0]) + "\n"
        with open(self.path, "w") as f:
            f.write(self.good_content)

    def test_refuses_to_overwrite_when_all_responses_are_null(self):
        """The exact 2026-07-11 failure: every response null."""

        data = responses(range(100), null_ids=range(100))

        with self.assertRaises(RuntimeError) as ctx:
            tm_api.persist_data(data, self.path, "transfers")

        self.assertIn("null", str(ctx.exception).lower())
        # the good data must survive untouched
        with open(self.path) as f:
            self.assertEqual(f.read(), self.good_content)

    def test_refuses_to_overwrite_when_null_rate_above_threshold(self):
        """A partially blocked run is still a failed run."""

        data = responses(range(100), null_ids=range(50))

        with self.assertRaises(RuntimeError):
            tm_api.persist_data(data, self.path, "transfers")

        with open(self.path) as f:
            self.assertEqual(f.read(), self.good_content)

    def test_refuses_to_write_empty_result(self):
        with self.assertRaises(RuntimeError):
            tm_api.persist_data([], self.path, "transfers")

        with open(self.path) as f:
            self.assertEqual(f.read(), self.good_content)

    def test_writes_a_healthy_result(self):
        """A clean run, and a few stragglers, must still be persisted."""

        data = responses(range(100), null_ids=[7])

        tm_api.persist_data(data, self.path, "transfers")

        with open(self.path) as f:
            written = [json.loads(line) for line in f]
        self.assertEqual(len(written), 100)


class TestTransfersBatchRetry(unittest.TestCase):
    """Transfers must get the batch-level retry market values already had."""

    def test_null_transfer_responses_are_retried(self):
        player_ids = [1, 2, 3, 4]
        failed_first_time = {2, 3}
        calls = []

        async def fake_get_transfers(ids):
            calls.append(list(ids))
            # first call fails for some players, retries then succeed
            if len(calls) == 1:
                return responses(ids, null_ids=failed_first_time)
            return responses(ids)

        with mock.patch.object(tm_api, "get_transfers", fake_get_transfers):
            result = tm_api.fetch_with_retries(tm_api.get_transfers, player_ids, "transfers")

        self.assertGreaterEqual(len(calls), 2, "null transfer responses were never retried")
        self.assertEqual(calls[1], [2, 3], "retry should only re-request the failed players")
        self.assertEqual([r["player_id"] for r in result], player_ids)
        self.assertEqual([r for r in result if r["response"] is None], [])


if __name__ == "__main__":
    unittest.main()
