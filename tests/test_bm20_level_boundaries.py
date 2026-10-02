import math
import unittest

from scripts.bm20_level_utils import compute_daily_index, select_realtime_reference


OCT01 = 821.7364013426927
OCT02 = 825.2303812255543
OCT02_RET = 0.004251947312000003


class BM20BoundaryRegressionTests(unittest.TestCase):
    def test_oct01_to_oct02_daily_boundary(self):
        got = compute_daily_index(
            last_date="2026-10-01",
            last_index=OCT01,
            date="2026-10-02",
            ret=OCT02_RET,
        )
        self.assertTrue(math.isclose(got, OCT02, rel_tol=0, abs_tol=1e-9))

    def test_same_date_rerun_is_idempotent(self):
        first = compute_daily_index(
            last_date="2026-10-01",
            last_index=OCT01,
            date="2026-10-02",
            ret=OCT02_RET,
        )
        rerun = compute_daily_index(
            last_date="2026-10-02",
            last_index=first,
            date="2026-10-02",
            ret=OCT02_RET,
            previous_index=OCT01,
        )
        self.assertTrue(math.isclose(first, OCT02, rel_tol=0, abs_tol=1e-9))
        self.assertTrue(math.isclose(rerun, OCT02, rel_tol=0, abs_tol=1e-9))
        self.assertTrue(math.isclose(first, rerun, rel_tol=0, abs_tol=1e-12))

    def test_realtime_uses_previous_day_if_today_is_finalized(self):
        series = [
            {"date": "2026-10-01", "level": OCT01},
            {"date": "2026-10-02", "level": OCT02},
        ]
        base, official = select_realtime_reference(series, "2026-10-02")
        self.assertEqual(base, OCT01)
        self.assertEqual(official, OCT02)

    def test_realtime_uses_latest_if_today_not_finalized(self):
        series = [
            {"date": "2026-10-01", "level": OCT01},
            {"date": "2026-10-02", "level": OCT02},
        ]
        base, official = select_realtime_reference(series, "2026-10-03")
        self.assertEqual(base, OCT02)
        self.assertIsNone(official)


if __name__ == "__main__":
    unittest.main()
