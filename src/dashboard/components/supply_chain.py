import streamlit as st


def render_supply_chain(summary, by_warehouse, top_stockouts):
    st.header("🚚 Supply Chain Metrics")

    try:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Order Lines",     f"{int(summary['total_order_lines'].iloc[0]):,}")
        col2.metric("Stockout Rate",         f"{round(summary['stockout_rate'].iloc[0] * 100, 1)}%")
        col3.metric("Delivery Success Rate", f"{round(summary['delivery_success_rate'].iloc[0] * 100, 1)}%")
    except Exception as e:
        st.error(f"Supply Chain KPI Error: {e}")
        st.write(summary)
        return

    st.divider()

    col_left, col_right = st.columns(2)

    # Per-warehouse breakdown
    with col_left:
        if not by_warehouse.empty:
            st.subheader("🏭 Stockout Rate by Warehouse")
            wh = by_warehouse.set_index("warehouse_id")
            wh["stockout_rate_pct"] = (wh["stockout_rate"] * 100).round(1)
            st.bar_chart(wh["stockout_rate_pct"])

    # Top stockout SKUs
    with col_right:
        if not top_stockouts.empty:
            st.subheader("📦 Top Stockout SKUs")
            top = top_stockouts.head(15)[["sku", "stockout_orders", "revenue_at_risk"]].copy()
            top["revenue_at_risk"] = top["revenue_at_risk"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(top, use_container_width=True)

    # Warehouse detail table
    if not by_warehouse.empty:
        st.subheader("Warehouse Detail")
        display = by_warehouse.copy()
        display["stockout_rate"]         = (display["stockout_rate"] * 100).round(1).astype(str) + "%"
        display["delivery_success_rate"] = (display["delivery_success_rate"] * 100).round(1).astype(str) + "%"
        st.dataframe(display, use_container_width=True)
