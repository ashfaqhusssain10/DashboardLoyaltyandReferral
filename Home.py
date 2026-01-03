"""
Admin Control Tower - Main Entry Point
"""
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Admin Control Tower",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #6b7280;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
    }
    .kpi-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .stButton > button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# Main page content
st.markdown('<p class="main-header">🏢 Admin Control Tower</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">CraftMyPlate Loyalty & Referral Management System</p>', unsafe_allow_html=True)

# Welcome section
st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 📊 Dashboard")
    st.write("View KPIs, referral trends, and system health at a glance.")
    if st.button("Go to Dashboard", key="btn_dashboard"):
        st.switch_page("pages/1_Dashboard.py")

with col2:
    st.markdown("### 💰 Coin Transactions")
    st.write("Search users, view coin history, leads, and referrals.")
    if st.button("Go to Transactions", key="btn_transactions"):
        st.switch_page("pages/2_Coin_Transactions.py")

with col3:
    st.markdown("### 🏧 Withdrawals")
    st.write("Review and approve/reject withdrawal requests.")
    if st.button("Go to Withdrawals", key="btn_withdrawals"):
        st.switch_page("pages/3_Withdrawals.py")

# Footer
st.markdown("---")
st.markdown(
    "<p style='text-align: center; color: #9ca3af;'>Admin Control Tower v1.0 | CraftMyPlate</p>",
    unsafe_allow_html=True
)
