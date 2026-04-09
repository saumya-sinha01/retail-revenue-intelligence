import streamlit as st


def render_supply_chain(summary):

    st.header("🚚 Supply Chain Metrics")

    try:
        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Total Orders",
            int(summary["total_order_lines"].iloc[0])
        )

        col2.metric(
            "Stockout Rate",
            round(summary["stockout_rate"].iloc[0], 2)
        )

        col3.metric(
            "Delivery Success Rate",
            round(summary["delivery_success_rate"].iloc[0], 2)
        )

    except Exception as e:
        st.error(f"Supply Chain KPI Error: {e}")
        st.write(summary)

    st.subheader("Raw Data")

    st.dataframe(summary)