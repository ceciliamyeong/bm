#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import plotly.express as px

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "bm20_daily_data_latest.csv"
OUT = ROOT / "assets/topcoins_treemap_latest.png"

def main():
    df = pd.read_csv(CSV)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["weight_ratio"] = pd.to_numeric(df["weight_ratio"], errors="coerce").fillna(0.0)
    df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce").fillna(0.0)

    fig = px.treemap(
        df.sort_values("weight_ratio", ascending=False).head(20),
        path=["symbol"],
        values="weight_ratio",
        color="price_change_pct",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
    )

    fig.update_traces(texttemplate="%{label}<br>%{color:+.2f}%")
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="white")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(OUT), width=900, height=520, scale=2)
    print("Treemap PNG written:", OUT)

if __name__ == "__main__":
    main()
