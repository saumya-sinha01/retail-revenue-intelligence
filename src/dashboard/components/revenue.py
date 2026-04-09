import streamlit as st


def render_revenue(summary, by_region):

    st.header("💰 Revenue KPIs")

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Revenue", int(summary["total_revenue"].iloc[0]))
    col2.metric("Total Orders", int(summary["total_orders"].iloc[0]))
    col3.metric("Avg Order Value", round(summary["avg_order_value"].iloc[0], 2))

    st.subheader("📈 Revenue by Region")

    st.bar_chart(by_region.set_index("region")["total_revenue"])