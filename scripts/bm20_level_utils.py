"""Date boundaries shared by the newsletter proxy and its readers."""
from datetime import date, timedelta
import math


def select_realtime_reference(series, today):
    """Require the previous KST calendar day's finalized level, never today."""
    yesterday = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
    levels = {}
    for row in series:
        day = date.fromisoformat(row['date']).isoformat()
        level = float(row['level'])
        if not math.isfinite(level) or level <= 0:
            raise ValueError('Invalid finalized level')
        if day in levels:
            raise ValueError(f'Duplicate finalized date: {day}')
        levels[day] = level
    if yesterday not in levels:
        raise ValueError(f'Missing previous finalized day: {yesterday}')
    return levels[yesterday], levels.get(today)


def newsletter_snapshot(official, realtime, today):
    """Overlay only a current proxy's level/1D; retain official ancillary data."""
    result = dict(official)
    yesterday = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
    if (realtime and realtime.get('asOf') == today
            and realtime.get('bm20ReferenceDate') == yesterday
            and realtime.get('bm20Mode') == 'realtime_24h_proxy'):
        for key in ('bm20Level', 'bm20PrevLevel', 'bm20PointChange', 'bm20ChangePct'):
            result[key] = realtime[key]
        result['returns'] = dict(official.get('returns', {}))
        result['returns']['1D'] = realtime['returns']['1D']
    return result
