import os
import unittest
from decimal import Decimal
from unittest.mock import patch

from aster_maker_taker import Config, floor_step, nonzero_positions


class AsterMakerTakerTests(unittest.TestCase):
    def test_floor_step(self):
        self.assertEqual(floor_step(Decimal("1.239"), Decimal("0.01")), Decimal("1.23"))

    def test_nonzero_positions(self):
        rows = [{"symbol": "XAUUSD1", "positionAmt": "0"}, {"symbol": "XAUUSD1", "positionAmt": "0.02"}]
        self.assertEqual(len(nonzero_positions(rows, "XAUUSD1")), 1)

    def test_live_defaults_locked(self):
        env = {
            "ASTER_ACCOUNT_ADDRESS": "0x" + "1" * 40,
            "ASTER_API_KEY": "0x" + "2" * 40,
            "ASTER_API_SECRET": "3" * 64,
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = Config.from_env()
        self.assertTrue(cfg.dry_run)
        self.assertEqual(cfg.live_confirm, "")
        self.assertEqual(cfg.symbol, "XAUUSD1")


if __name__ == "__main__":
    unittest.main()
