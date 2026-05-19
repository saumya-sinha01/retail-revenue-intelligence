import streamlit as st
import pandas as pd
from datetime import datetime

from data_loader import load_revenue_data, load_risk_data, load_supply_chain_data
from components.revenue import render_revenue
from components.risk import render_risk
from components.supply_chain import render_supply_chain

st.set_page_config(page_title="Retail Analytics Dashboard", layout="wide")
st.title("📊 Retail Revenue Intelligence Dashboard")

# ── Load all data ─────────────────────────────────────────────────────────
(rev_summary, rev_region, rev_channel,
 rev_month, rev_month_region, rev_month_channel,
 rev_sku) = load_revenue_data()

risk_summary, risk_region, risk_sku     = load_risk_data()
supply_summary, supply_wh, top_stockouts = load_supply_chain_data()

# ── Sidebar filters ───────────────────────────────────────────────────────
st.sidebar.header("🔍 Filters")

# Date range: derived from available months in data
available_months = sorted(rev_month["order_month"].dropna().unique().tolist()) if not rev_month.empty else []
if available_months:
    selected_months = st.sidebar.multiselect(
        "Month(s)",
        options=available_months,
        default=available_months,
    )
else:
    selected_months = []

# Region filter
available_regions = sorted(rev_region["region"].dropna().unique().tolist()) if not rev_region.empty else []
selected_regions = st.sidebar.multiselect(
    "Region(s)",
    options=available_regions,
    default=available_regions,
)

st.sidebar.divider()
st.sidebar.caption(f"Data refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ── Apply filters to monthly data ─────────────────────────────────────────
def _filter_months(df, month_col="order_month"):
    if df.empty or not selected_months:
        return df
    return df[df[month_col].isin(selected_months)]

def _filter_regions(df, region_col="region"):
    if df.empty or not selected_regions:
        return df
    return df[df[region_col].isin(selected_regions)]

rev_month_f          = _filter_months(rev_month)
rev_month_region_f   = _filter_months(_filter_regions(rev_month_region))
rev_month_channel_f  = _filter_months(rev_month_channel)

# Recalculate summary KPIs from filtered monthly data
def _summary_from_monthly(df):
    if df.empty:
        return pd.DataFrame()
    total_rev    = df["total_revenue"].sum()
    total_orders = df["total_orders"].sum()
    return pd.DataFrame([{
        "total_revenue":   total_rev,
        "total_orders":    total_orders,
        "avg_order_value": total_rev / total_orders if total_orders else 0,
    }])

# Aggregate filtered region/channel from month_region / month_channel tables
def _agg_from_monthly(df, group_col):
    if df.empty:
        return pd.DataFrame()
    return (
        df.groupby(group_col, as_index=False)
        .agg(total_revenue=("total_revenue", "sum"),
             total_orders=("total_orders", "sum"))
        .assign(avg_order_value=lambda d: d["total_revenue"] / d["total_orders"])
    )

rev_summary_f  = _summary_from_monthly(rev_month_region_f if not rev_month_region_f.empty else rev_month_f)
rev_region_f   = _agg_from_monthly(rev_month_region_f, "region")
rev_channel_f  = _agg_from_monthly(rev_month_channel_f, "channel")

# ── MoM delta helper ──────────────────────────────────────────────────────
def _mom_delta(by_month_df):
    """Return (current_month_revenue, delta_pct_str) for the metric widget."""
    if by_month_df.empty or len(by_month_df) < 2:
        return None, None
    sorted_df = by_month_df.sort_values("order_month")
    curr = sorted_df.iloc[-1]["total_revenue"]
    prev = sorted_df.iloc[-2]["total_revenue"]
    if prev == 0:
        return curr, None
    delta_pct = ((curr - prev) / prev) * 100
    sign = "▲" if delta_pct >= 0 else "▼"
    return curr, f"{sign} {abs(delta_pct):.1f}% vs prev month"

_, mom_delta_str = _mom_delta(rev_month_f)

# ── Render ────────────────────────────────────────────────────────────────
if rev_summary_f.empty:
    st.warning("⏳ Revenue data not yet available. Run the pipeline first.")
else:
    render_revenue(rev_summary_f, rev_region_f, rev_channel_f,
                   rev_month_f, rev_sku, mom_delta_str)

if risk_summary.empty:
    st.warning("⏳ Risk data not yet available. Run the pipeline first.")
else:
    render_risk(risk_summary, risk_region, risk_sku)

if supply_summary.empty:
    st.warning("⏳ Supply chain data not yet available. Run the pipeline first.")
else:
    render_supply_chain(supply_summary, supply_wh, top_stockouts)
