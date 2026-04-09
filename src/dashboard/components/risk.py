import streamlit as st
import pandas as pd


def render_risk(summary):

    st.header("⚠️ Revenue at Risk")

    col1, col2 = st.columns(2)

    # ✅ Safe extraction helper
    def safe_get(df, col_name):
        if col_name in df.columns and len(df) > 0:
            val = df[col_name].iloc[0]
            if pd.isna(val):
                return 0
            return int(val)
        return 0

    total_revenue_at_risk = safe_get(summary, "total_revenue_at_risk")
    stockout_orders = safe_get(summary, "stockout_orders")

    # ✅ Display metrics
    col1.metric(
        "Total Revenue at Risk",
        total_revenue_at_risk
    )

    col2.metric(
        "Stockout Orders",
        stockout_orders
    )

    # ✅ Optional: Show message if no risk
    if total_revenue_at_risk == 0:
        st.success("✅ No revenue at risk. Inventory levels are healthy.")
    else:
        st.warning("⚠️ Revenue loss detected due to stockouts.")