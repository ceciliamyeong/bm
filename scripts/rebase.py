# scripts/rebase.py
import pandas as pd

def _pick_base_value(s: pd.Series, base_date: str) -> float:
    ts = pd.to_datetime(base_date)
    s = s.dropna().copy()
    s.index = pd.to_datetime(s.index)

    exact = s.loc[s.index == ts]
    if not exact.empty:
        return float(exact.iloc[0])

    aft = s[s.index > ts]
    if not aft.empty:
        return float(aft.iloc[0])

    bef = s[s.index < ts]
    if not bef.empty:
        return float(bef.iloc[-1])

    raise ValueError("No data around base date")

def rebase_csv(in_csv: str, out_csv: str, date_col="date", value_col="index",
               base_date="2024-01-01", base_value=100.0):
    df = pd.read_csv(in_csv)
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).reset_index(drop=True)

    s = pd.Series(df[value_col].values, index=df[date_col].values)
    base = _pick_base_value(s, base_date)

    df[value_col] = (df[value_col] / base) * base_value
    df.to_csv(out_csv, index=False)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("in_csv")
    p.add_argument("out_csv")
    p.add_argument("--date-col", default="date")
    p.add_argument("--value-col", default="index")
    p.add_argument("--base-date", default="2024-01-01")
    p.add_argument("--base-value", type=float, default=100.0)
    a = p.parse_args()
    rebase_csv(a.in_csv, a.out_csv, a.date_col, a.value_col, a.base_date, a.base_value)
