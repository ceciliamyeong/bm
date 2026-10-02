"""Small pure helpers for BM20 daily/realtime level boundaries."""


def select_realtime_reference(series, today):
    """Return (base_level, official_today).

    If today's finalized level is already present in the daily series, realtime
    24h proxy calculations must start from the previous finalized day.
    Otherwise the latest finalized level is the base.
    """
    if not isinstance(series, list) or not series:
        return None, None

    last = series[-1]
    last_level = float(last["level"])
    last_date = str(last.get("date", ""))[:10]

    if last_date == today:
        if len(series) < 2:
            return None, last_level
        return float(series[-2]["level"]), last_level

    return last_level, None


def compute_daily_index(last_date, last_index, date, ret, previous_index=None):
    """Return the daily index without compounding the same date twice."""
    if date < last_date:
        raise ValueError(f"date {date} < last_date {last_date}")

    if date == last_date:
        if previous_index is None:
            raise ValueError("previous_index required for same-date rerun")
        return float(previous_index) * (1.0 + float(ret))

    return float(last_index) * (1.0 + float(ret))
