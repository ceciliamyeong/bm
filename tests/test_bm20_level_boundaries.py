"""Regression fixtures from main's Oct 5 daily and letter commits; no API calls."""
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import update_bm20_latest as updater
from bm20_level_utils import newsletter_snapshot, select_realtime_reference

OCT04 = 827.4505657343389
OCT05 = 842.9033815107078
# Inferred from persisted level; the original raw API response was not archived.
RETURN = 856.75096 / OCT05 - 1
SERIES = [{'date': '2026-10-04', 'level': OCT04},
          {'date': '2026-10-05', 'level': OCT05}]


def quotes(ret=RETURN):
    return {cid: {'prev': 100, 'current': 100 * (1 + ret)} for cid in updater.BM20_IDS}


def stamp(day):
    return datetime.fromisoformat(f'2026-10-{day:02d}T08:39:48+09:00')


class BM20BoundaryRegressionTests(unittest.TestCase):
    def test_oct05_reproduces_old_double_application(self):
        self.assertEqual(round(OCT05 * (1 + RETURN), 6), 856.75096)
        proxy = updater.build_realtime_snapshot(SERIES, quotes(), stamp(5))
        self.assertEqual(proxy['bm20Level'], 841.044279)
        self.assertEqual(proxy['bm20PrevLevel'], round(OCT04, 6))
        self.assertEqual(proxy['bm20OfficialLevel'], OCT05)
        self.assertEqual(proxy['bm20ReferenceDate'], '2026-10-04')

    def test_same_date_rerun_uses_series_not_previous_proxy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'bm20_series.json').write_text(json.dumps(SERIES))
            official = {'asOf': '2026-10-05', 'bm20Level': OCT05}
            (root / 'bm20_latest.json').write_text(json.dumps(official))
            before = (root / 'bm20_latest.json').read_bytes()
            series_before = (root / 'bm20_series.json').read_bytes()
            with patch.object(updater, 'ROOT', root), patch.dict('os.environ', {'CMC_API_KEY': 'test'}), patch.object(updater, 'fetch_cmc_prices', return_value=quotes()), patch.object(updater, 'datetime') as clock:
                clock.now.return_value = stamp(5)
                updater.main()
                first = (root / 'bm20_realtime_latest.json').read_bytes()
                updater.main()
                self.assertEqual(first, (root / 'bm20_realtime_latest.json').read_bytes())
            self.assertEqual(before, (root / 'bm20_latest.json').read_bytes())
            self.assertEqual(series_before, (root / 'bm20_series.json').read_bytes())

    def test_date_transition_reanchors_to_oct05(self):
        proxy = updater.build_realtime_snapshot(SERIES, quotes(.01), stamp(6))
        self.assertEqual(proxy['asOf'], '2026-10-06')
        self.assertEqual(proxy['bm20ReferenceDate'], '2026-10-05')
        self.assertEqual(proxy['bm20Level'], round(OCT05 * 1.01, 6))
        self.assertNotIn('bm20OfficialLevel', proxy)
        self.assertEqual(set(proxy['returns']), {'1D'})

    def test_today_append_does_not_change_reference(self):
        prior = updater.build_realtime_snapshot(SERIES[:-1], quotes(), stamp(5))
        after = updater.build_realtime_snapshot(SERIES, quotes(), stamp(5))
        self.assertEqual(prior['bm20Level'], after['bm20Level'])

    def test_kst_date_at_utc_boundary(self):
        now = datetime(2026, 10, 5, 15, 1, tzinfo=timezone.utc)
        proxy = updater.build_realtime_snapshot(SERIES, quotes(), now)
        self.assertEqual(proxy['asOf'], '2026-10-06')
        self.assertEqual(proxy['bm20ReferenceDate'], '2026-10-05')

    def test_unsorted_and_future_rows_cannot_be_base(self):
        series = [SERIES[1], {'date': '2026-10-07', 'level': 999}, SERIES[0]]
        self.assertEqual(select_realtime_reference(series, '2026-10-05'), (OCT04, OCT05))

    def test_missing_previous_day_fails(self):
        for series, day in (([], '2026-10-06'), (SERIES[1:], '2026-10-05'), (SERIES[:1], '2026-10-06')):
            with self.subTest(series=series, day=day), self.assertRaises(ValueError):
                select_realtime_reference(series, day)

    def test_incomplete_or_invalid_quotes_fail(self):
        for value in (None, 0, float('nan')):
            data = quotes()
            if value is None:
                del data['bitcoin']
            else:
                data['bitcoin']['current'] = value
            with self.subTest(value=value), self.assertRaises((ValueError, KeyError)):
                updater.build_realtime_snapshot(SERIES, data, stamp(5))

    def test_reader_rejects_stale_proxy_and_keeps_official_data(self):
        official = {'bm20Level': OCT05, 'returns': {'1D': .018675213259, '7D': .0156}, 'kimchi': .006345}
        original = json.dumps(official)
        proxy = updater.build_realtime_snapshot(SERIES, quotes(), stamp(5))
        current = newsletter_snapshot(official, proxy, '2026-10-05')
        self.assertEqual(current['bm20Level'], 841.044279)
        self.assertEqual(current['returns']['1D'], proxy['returns']['1D'])
        self.assertEqual(current['kimchi'], official['kimchi'])
        self.assertEqual(newsletter_snapshot(official, proxy, '2026-10-06'), official)
        self.assertEqual(newsletter_snapshot(official, None, '2026-10-05'), official)
        self.assertEqual(json.dumps(official), original)

    def test_both_renderers_use_snapshot_reader(self):
        # Exercise the actual renderer entry points with offline inputs.
        for name in ('render_letter', 'render_letter_en'):
            spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / f'{name}.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            class ReadReached(Exception):
                pass
            with patch.object(module, 'load_json', return_value={}), patch.object(module, 'newsletter_snapshot', side_effect=ReadReached):
                with self.assertRaises(ReadReached):
                    (module.build_placeholders if name == 'render_letter' else module.load_bm20)()


if __name__ == '__main__':
    unittest.main()
