import streamlit as st
import pandas as pd


def render_revenue(summary, by_region, by_channel, by_month, by_sku, mom_delta_str=None):
    st.header("💰 Revenue KPIs")

    total_rev    = int(summary["total_revenue"].iloc[0])
    total_orders = int(summary["total_orders"].iloc[0])
    avg_val      = round(float(summary["avg_order_value"].iloc[0]), 2)

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Total Revenue",
        f"${total_rev:,}",
        delta=mom_delta_str,
        delta_color="normal",
    )
    col2.metric("Total Orders",    f"{total_orders:,}")
    col3.metric("Avg Order Value", f"${avg_val:,.2f}")

    st.divider()

    # Monthly trend
    if not by_month.empty and "order_month" in by_month.columns:
        st.subheader("📈 Monthly Revenue Trend")
        trend = by_month.sort_values("order_month").set_index("order_month")["total_revenue"]
        st.line_chart(trend)

    col_left, col_right = st.columns(2)

    with col_left:
        if not by_region.empty:
            st.subheader("🗺️ Revenue by Region")
            region_chart = (
                by_region.set_index("region")["total_revenue"]
                .sort_values(ascending=False)
            )
            st.bar_chart(region_chart)

    with col_right:
        if not by_channel.empty:
            st.subheader("📡 Revenue by Channel")
            channel_chart = (
                by_channel.set_index("channel")["total_revenue"]
                .sort_values(ascending=False)
            )
            st.bar_chart(channel_chart)

    # Top 20 SKUs
    if not by_sku.empty:
        st.subheader("🏆 Top 20 SKUs by Revenue")
        top20 = by_sku.head(20)[["sku", "total_revenue", "total_orders", "avg_order_value"]].copy()
        top20["total_revenue"]   = top20["total_revenue"].apply(lambda x: f"${x:,.0f}")
        top20["avg_order_value"] = top20["avg_order_value"].apply(lambda x: f"${x:,.2f}")
        top20 = top20.reset_index(drop=True)
        st.dataframe(top20, use_container_width=True)
