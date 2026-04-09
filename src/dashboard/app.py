import streamlit as st

from data_loader import (
    load_revenue_data,
    load_risk_data,
    load_supply_chain_data
)

from components.revenue import render_revenue
from components.risk import render_risk
from components.supply_chain import render_supply_chain


st.set_page_config(
    page_title="Retail Analytics Dashboard",
    layout="wide"
)

st.title("📊 Retail Revenue Intelligence Dashboard")

# -------------------------------
# LOAD DATA
# -------------------------------
rev_summary, rev_region = load_revenue_data()
risk_summary = load_risk_data()
supply_summary = load_supply_chain_data()

st.write("Risk Columns:", risk_summary.columns)
st.write("Risk Data:", risk_summary)

# -------------------------------
# RENDER COMPONENTS
# -------------------------------
render_revenue(rev_summary, rev_region)
render_risk(risk_summary)
render_supply_chain(supply_summary)

