"""
Streamlit dashboard for web2sheets-tracker

Features (MVP):
- Reads Google Sheet (Snapshots by default)
- Filters by URL and time window
- Displays recent rows and a price-over-time chart
- Exports current sheet to CSV via existing export function

Requirements:
  pip install streamlit pandas
Run:
  streamlit run dashboard.py
"""
from __future__ import annotations

import os
import io
import time
import datetime as dt
from typing import List, Dict

import pandas as pd
import streamlit as st

# Reuse the existing Sheets helpers from the project
from sheets import _get_client, _get_all_rows, export_sheet_to_csv  # type: ignore

# ---- Helpers -----------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=60)
def load_sheet(sheet_name: str) -> pd.DataFrame:
    """Load an entire worksheet into a pandas DataFrame.
    Expects first row to be header. Returns empty DF on failure.
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        st.error("GOOGLE_SHEET_ID is not set in .env")
        return pd.DataFrame()

    try:
        client = _get_client()
        sh = client.open_by_key(sheet_id)
        ws = sh.worksheet(sheet_name)
        rows = _get_all_rows(ws)
        if not rows:
            return pd.DataFrame()
        header, *data = rows
        df = pd.DataFrame(data, columns=header)
    except Exception as e:
        st.exception(e)
        return pd.DataFrame()

    # Normalize common columns if present
    for col in ["timestamp", "created_at", "time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            break  # prefer the first matching timestamp column

    # Normalize price-like columns (best-effort)
    for pcol in [c for c in df.columns if "price" in c.lower()]:
        df[pcol] = (
            df[pcol]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
        )
        df[pcol] = pd.to_numeric(df[pcol], errors="coerce")

    # Try to ensure an URL column exists for filtering
    if "url" not in df.columns:
        # Look for something similar
        for cand in ("URL", "link", "product_url"):
            if cand in df.columns:
                df.rename(columns={cand: "url"}, inplace=True)
                break

    return df


def get_time_bounds(df: pd.DataFrame) -> tuple[dt.datetime | None, dt.datetime | None]:
    ts_col = None
    for c in ("timestamp", "created_at", "time"):
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None or df.empty:
        return None, None
    ts = df[ts_col].dropna()
    if ts.empty:
        return None, None
    return ts.min().to_pydatetime(), ts.max().to_pydatetime()


# ---- UI ----------------------------------------------------------------------

st.set_page_config(page_title="web2sheets-tracker", page_icon="📈", layout="wide")
st.title("📈 web2sheets-tracker — Dashboard")

with st.sidebar:
    st.header("Controls")
    default_sheet = os.getenv("EXPORT_DEFAULT_SHEET", "Snapshots")
    sheet_name = st.text_input("Sheet name", value=default_sheet, help="Worksheet/tab in your Google Sheet")

    since_hours = st.slider("Since (hours)", min_value=1, max_value=168, value=24, step=1)
    st.caption("Filter data within the last N hours (applied after loading)")

    st.divider()
    st.caption("Data refreshes from cache every ~60s. Use the button below to force refresh.")
    if st.button("🔄 Force reload", use_container_width=True):
        load_sheet.clear()

# Load data
_df = load_sheet(sheet_name)
if _df.empty:
    st.info("No data loaded. Check GOOGLE_SHEET_ID, credentials, and the sheet name.")
    st.stop()

min_t, max_t = get_time_bounds(_df)

# Time filter
ts_col = None
for c in ("timestamp", "created_at", "time"):
    if c in _df.columns:
        ts_col = c
        break

if ts_col:
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=since_hours)
    df = _df[_df[ts_col] >= cutoff]
else:
    df = _df.copy()

# URL filter
url_options: List[str] = []
if "url" in df.columns:
    url_options = sorted([u for u in df["url"].dropna().unique().tolist()])
selected_urls = st.multiselect("Filter by URL(s)", options=url_options, default=url_options[:3])
if selected_urls:
    df = df[df["url"].isin(selected_urls)]

# Metrics row
col1, col2, col3 = st.columns(3)
col1.metric("Rows (filtered)", f"{len(df):,}")
col2.metric("Distinct URLs", f"{df['url'].nunique() if 'url' in df.columns else 0}")
if max_t:
    col3.metric("Last update", max_t.strftime("%Y-%m-%d %H:%M:%S"))
else:
    col3.metric("Last update", "—")

st.divider()

# Chart: pick one URL for a price-over-time line
chart_url = None
if "url" in df.columns and len(url_options) > 0:
    chart_url = st.selectbox("Price chart URL", options=url_options, index=0)

price_cols = [c for c in df.columns if "price" in c.lower()]
if ts_col and chart_url and price_cols:
    chart_df = df[df["url"] == chart_url][[ts_col] + price_cols].sort_values(ts_col)
    chart_df = chart_df.set_index(ts_col)
    st.line_chart(chart_df, height=280)
else:
    st.caption("Add a price column (e.g., `price` or `price_usd`) and a timestamp column to enable charting.")

st.subheader("Recent rows")
st.dataframe(df.sort_values(ts_col or df.columns[0], ascending=False).head(500), use_container_width=True)

st.divider()

# Export current sheet to CSV via existing helper
col_a, col_b = st.columns([1, 2])
with col_a:
    st.write("**Export**")
    export_name = st.text_input("CSV filename", value=f"{sheet_name.lower()}_export.csv")
    if st.button("⬇️ Export current sheet to CSV", use_container_width=True):
        try:
            # Use the shared helper to export to disk, then offer download
            rc = export_sheet_to_csv(sheet_name=sheet_name, out_path=export_name, since_hours=None)
            if rc == 0 and os.path.exists(export_name):
                with open(export_name, "rb") as f:
                    st.download_button("Download CSV", data=f.read(), file_name=export_name, mime="text/csv", use_container_width=True)
                st.success(f"Exported to {export_name}")
            else:
                st.warning("Export finished but no file was produced.")
        except Exception as e:
            st.exception(e)

with col_b:
    st.info(
        "Tip: adjust the **Sheet name** on the left to point to `Logs` or any other tab."
    )