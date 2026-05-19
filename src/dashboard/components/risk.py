import streamlit as st
import pandas as pd


def render_risk(summary, by_region, by_sku):
    st.header("⚠️ Revenue at Risk")

    def _safe(df, col_name, fmt="int"):
        if col_name in df.columns and len(df) > 0:
            val = df[col_name].iloc[0]
            if pd.isna(val):
                return 0
            return int(val) if fmt == "int" else float(val)
        return 0

    total_at_risk   = _safe(summary, "total_revenue_at_risk")
    stockout_orders = _safe(summary, "stockout_orders")

    col1, col2 = st.columns(2)
    col1.metric("Total Revenue at Risk", f"${total_at_risk:,}")
    col2.metric("Stockout Orders",       f"{stockout_orders:,}")

    if total_at_risk == 0:
        st.success("✅ No revenue at risk. Inventory levels are healthy.")
    else:
        st.warning("⚠️ Revenue loss detected due to stockouts.")

    st.divider()

    col_left, col_right = st.columns(2)

    # Risk by region
    with col_left:
        if not by_region.empty and "region" in by_region.columns:
            st.subheader("🗺️ Revenue at Risk by Region")
            st.bar_chart(by_region.set_index("region")["total_revenue_at_risk"])

    # Top SKUs at risk
    with col_right:
        if not by_sku.empty:
            st.subheader("🔴 Top SKUs at Risk")
            top = by_sku.sort_values("total_revenue_at_risk", ascending=False).head(15)
            top = top[["sku", "total_revenue_at_risk", "stockout_orders"]].copy()
            top["total_revenue_at_risk"] = top["total_revenue_at_risk"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(top, use_container_width=True)
