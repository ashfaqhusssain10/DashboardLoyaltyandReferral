"""
Page 1: Dashboard
Shows KPIs with clickable popups, date-filtered referral chart, and quick navigation
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path
from datetime import date, timedelta

# Add app to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services import (
    get_active_users_count,
    get_total_coins_in_system,
    get_pending_count,
    get_today_referrals_count,
    get_today_leads_count,
    get_weekly_referral_stats,
    get_referral_stats_by_range,
    get_all_wallets,
    get_pending_withdrawals,
    get_today_referrals,
    get_today_leads,
    get_tier_name,
    get_user_by_id
)
from app.services.wallet_service import get_top_coin_holders, get_top_earners, get_daily_coin_activity, get_daily_coin_activity_by_range
from app.services.referral_service import get_top_referrers
from app.services.lead_service import get_top_lead_generators
from app.services.withdrawal_service import get_top_withdrawers

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")

# Custom CSS
st.markdown("""
<style>
    .kpi-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        cursor: pointer;
        transition: transform 0.2s;
    }
    .kpi-container:hover {
        transform: scale(1.02);
    }
    .kpi-container-green {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    .kpi-container-orange {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    .kpi-container-blue {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    .kpi-container-purple {
        background: linear-gradient(135deg, #a18cd1 0%, #fbc2eb 100%);
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
    .kpi-label {
        font-size: 0.95rem;
        opacity: 0.95;
    }
</style>
""", unsafe_allow_html=True)

# ======== POPUP DIALOGS ========

@st.dialog("👥 Users with Balance", width="large")
def show_users_with_balance():
    """Show all users with coin balance > 0."""
    wallets = get_all_wallets(limit=1000)
    wallets_with_balance = [w for w in wallets if float(w.get('remainingAmount', 0)) > 0]
    
    if wallets_with_balance:
        st.info(f"Total: {len(wallets_with_balance)} users with balance")
        st.caption("Click 'View Profile' to see user details")
        
        for w in wallets_with_balance[:50]:  # Limit to 50
            user_id = w.get('userId', 'N/A')
            user = get_user_by_id(user_id)
            user_name = user.get('userName', 'Unknown') if user else 'Unknown'
            phone = user.get('phoneNumber', 'N/A') if user else 'N/A'
            balance = float(w.get('remainingAmount', 0))
            tier = get_tier_name(user.get('tierId', '')) if user else 'Unknown'
            
            col1, col2, col3, col4, col5 = st.columns([2, 2, 1.5, 1, 1.5])
            with col1:
                st.write(f"**{user_name}**")
            with col2:
                st.write(f"📞 {phone}")
            with col3:
                st.write(f"💰 {balance:,.0f}")
            with col4:
                st.write(f"🏆 {tier}")
            with col5:
                if st.button("👤 View", key=f"kpi_user_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No users with balance found.")


@st.dialog("💰 Total Coins in System", width="large")
def show_total_coins():
    """Show all wallets with coins."""
    wallets = get_all_wallets(limit=1000)
    wallets_with_coins = sorted(
        [w for w in wallets if float(w.get('remainingAmount', 0)) > 0],
        key=lambda x: float(x.get('remainingAmount', 0)),
        reverse=True
    )
    
    if wallets_with_coins:
        total = sum(float(w.get('remainingAmount', 0)) for w in wallets_with_coins)
        st.success(f"**Total Coins in System: {total:,.0f}**")
        st.caption("Click 'View Profile' to see user details")
        
        for w in wallets_with_coins[:50]:
            user_id = w.get('userId', 'N/A')
            user = get_user_by_id(user_id)
            user_name = user.get('userName', 'Unknown') if user else 'Unknown'
            coins = float(w.get('remainingAmount', 0))
            
            col1, col2, col3 = st.columns([3, 1.5, 1.5])
            with col1:
                st.write(f"**{user_name}**")
            with col2:
                st.write(f"💰 {coins:,.0f}")
            with col3:
                if st.button("👤 View", key=f"kpi_coins_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No wallets with coins found.")


@st.dialog("⏳ Pending Withdrawals", width="large")
def show_pending_withdrawals():
    """Show all pending withdrawal requests."""
    pending = get_pending_withdrawals()
    
    if pending:
        total_amount = sum(float(w.get('requestedAmount', 0)) for w in pending)
        st.warning(f"**Total Pending Amount: {total_amount:,.0f} coins**")
        st.caption("Click 'View Profile' to see user details")
        
        for w in pending:
            user_id = w.get('userId', 'N/A')
            user = get_user_by_id(user_id)
            user_name = user.get('userName', 'Unknown') if user else 'Unknown'
            amount = float(w.get('requestedAmount', 0))
            
            col1, col2, col3, col4 = st.columns([2.5, 1.5, 1.5, 1.5])
            with col1:
                st.write(f"**{user_name}**")
            with col2:
                st.write(f"💰 {amount:,.0f}")
            with col3:
                st.write(f"⏳ Pending")
            with col4:
                if st.button("👤 View", key=f"kpi_pend_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.success("No pending withdrawals! 🎉")


@st.dialog("🔗 Today's Referrals", width="large")
def show_today_referrals():
    """Show today's referrals."""
    referrals = get_today_referrals()
    
    if referrals:
        st.info(f"Total today: {len(referrals)} referrals")
        st.caption("Click 'View Profile' to see referrer details")
        
        for r in referrals:
            user_id = r.get('userId', 'N/A')
            ref_name = r.get('referralName', 'Unknown')
            sent_to = r.get('sentTo', 'N/A')
            amount = float(r.get('sendedAmount', 0))
            
            col1, col2, col3, col4 = st.columns([2, 2, 1.5, 1.5])
            with col1:
                st.write(f"📞 {sent_to}")
            with col2:
                st.write(f"**{ref_name}**")
            with col3:
                st.write(f"💰 {amount:,.0f}")
            with col4:
                if st.button("👤 Referrer", key=f"kpi_ref_{r.get('tierReferralId', '')[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No referrals created today.")


@st.dialog("📈 Today's Leads", width="large")
def show_today_leads():
    """Show today's leads."""
    leads = get_today_leads()
    
    if leads:
        st.info(f"Total today: {len(leads)} leads")
        st.caption("Click 'View Profile' to see lead creator details")
        
        for l in leads:
            user_id = l.get('userId', 'N/A')
            lead_name = l.get('leadName', 'Unknown')
            phone = l.get('leadNumber', 'N/A')
            occasion = l.get('occasionName', 'N/A')
            
            col1, col2, col3, col4 = st.columns([2, 2, 2, 1.5])
            with col1:
                st.write(f"**{lead_name}**")
            with col2:
                st.write(f"📞 {phone}")
            with col3:
                st.write(f"🎉 {occasion}")
            with col4:
                if st.button("👤 Creator", key=f"kpi_lead_{l.get('leadId', '')[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No leads created today.")


# ======== MAIN PAGE ========

# Check if we need to navigate to a user profile
if st.session_state.get('navigate_to_user'):
    user_id = st.session_state['navigate_to_user']
    # Clear the navigation state
    del st.session_state['navigate_to_user']
    # Set the user ID for Coin Transactions page
    st.session_state['selected_user_id'] = user_id
    # Navigate to Coin Transactions
    st.switch_page("pages/2_Coin_Transactions.py")

st.title("📊 Dashboard")
st.markdown("Quick overview of your loyalty & referral system")

# KPI Cards
st.markdown("### Key Metrics")
st.caption("Click any card to see detailed data")

try:
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        active_users = get_active_users_count()
        st.markdown(f"""
        <div class="kpi-container">
            <div class="kpi-value">{active_users:,}</div>
            <div class="kpi-label">Users with Balance</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("📊 View Details", key="btn_users", use_container_width=True):
            show_users_with_balance()
    
    with col2:
        total_coins = get_total_coins_in_system()
        st.markdown(f"""
        <div class="kpi-container kpi-container-green">
            <div class="kpi-value">{total_coins:,.0f}</div>
            <div class="kpi-label">Total Coins in System</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("📊 View Details", key="btn_coins", use_container_width=True):
            show_total_coins()
    
    with col3:
        pending = get_pending_count()
        st.markdown(f"""
        <div class="kpi-container kpi-container-orange">
            <div class="kpi-value">{pending}</div>
            <div class="kpi-label">Pending Withdrawals</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("📊 View Details", key="btn_pending", use_container_width=True):
            show_pending_withdrawals()
    
    with col4:
        today_referrals = get_today_referrals_count()
        st.markdown(f"""
        <div class="kpi-container kpi-container-blue">
            <div class="kpi-value">{today_referrals}</div>
            <div class="kpi-label">Today's Referrals</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("📊 View Details", key="btn_referrals", use_container_width=True):
            show_today_referrals()
    
    with col5:
        today_leads = get_today_leads_count()
        st.markdown(f"""
        <div class="kpi-container kpi-container-purple">
            <div class="kpi-value">{today_leads}</div>
            <div class="kpi-label">Today's Leads</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("📊 View Details", key="btn_leads", use_container_width=True):
            show_today_leads()

except Exception as e:
    st.error(f"Error loading KPIs: {e}")
    st.info("Make sure your AWS credentials are configured correctly.")

# Debug expander DISABLED - was causing 6+ second delays due to full table scans
# Uncomment only when debugging pagination issues
# with st.expander("🔍 Pagination Debug Info"):
#     try:
#         from app.services.dynamodb_service import db_service
#         
#         # Get counts from each table
#         st.write("**Records fetched from each table:**")
#         
#         # WalletTable
#         wallets = db_service.scan_all("WalletTable", limit=None)
#         st.write(f"• **WalletTable**: {len(wallets)} records")
#         
#         # WalletTransactionTable
#         txns = db_service.scan_all("WalletTransactionTable", limit=None)
#         st.write(f"• **WalletTransactionTable**: {len(txns)} records")
#         
#         # TierReferralTable
#         refs = db_service.scan_all("TierReferralTable", limit=None)
#         st.write(f"• **TierReferralTable**: {len(refs)} records")
#         
#         # LeadTable
#         leads = db_service.scan_all("LeadTable", limit=None)
#         st.write(f"• **LeadTable**: {len(leads)} records")
#         
#         # WithdrawnTable
#         wdraws = db_service.scan_all("WithdrawnTable", limit=None)
#         st.write(f"• **WithdrawnTable**: {len(wdraws)} records")
#         
#         st.success("✅ Pagination is working - fetching all records!")
#         
#     except Exception as e:
#         st.warning(f"Debug error: {e}")

st.markdown("---")


# ======== LEADERBOARDS SECTION (G01-G04) ========
st.markdown("### 🏆 Leaderboards")

# Leaderboard popup dialogs
@st.dialog("💰 All Top Coin Holders", width="large")
def show_all_coin_holders():
    """Show all users sorted by coin balance."""
    data = get_top_coin_holders(50)
    if data:
        st.info(f"Showing top {len(data)} users by coin balance")
        st.caption("Click 'View' to see user profile")
        
        for i, user in enumerate(data, 1):
            user_id = user['userId']
            col1, col2, col3, col4 = st.columns([0.5, 2, 1.5, 1])
            with col1:
                st.write(f"**{i}**")
            with col2:
                st.write(f"**{user['userName']}**")
            with col3:
                st.write(f"💰 {user['coins']:,.0f}")
            with col4:
                if st.button("👤 View", key=f"lb_coin_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No data available")

@st.dialog("🔗 All Top Referrers", width="large")
def show_all_referrers():
    """Show all users sorted by referral count."""
    data = get_top_referrers(50)
    if data:
        st.info(f"Showing top {len(data)} users by referral count")
        st.caption("Click 'View' to see user profile")
        
        for i, user in enumerate(data, 1):
            user_id = user['userId']
            col1, col2, col3, col4 = st.columns([0.5, 2, 1.5, 1])
            with col1:
                st.write(f"**{i}**")
            with col2:
                st.write(f"**{user['userName']}**")
            with col3:
                st.write(f"🔗 {user['referralCount']}")
            with col4:
                if st.button("👤 View", key=f"lb_ref_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No data available")

@st.dialog("📈 All Top Lead Generators", width="large")
def show_all_lead_generators():
    """Show all users sorted by lead count."""
    data = get_top_lead_generators(50)
    if data:
        st.info(f"Showing top {len(data)} users by lead count")
        st.caption("Click 'View' to see user profile")
        
        for i, user in enumerate(data, 1):
            user_id = user['userId']
            col1, col2, col3, col4 = st.columns([0.5, 2, 1.5, 1])
            with col1:
                st.write(f"**{i}**")
            with col2:
                st.write(f"**{user['userName']}**")
            with col3:
                st.write(f"📈 {user['leadCount']}")
            with col4:
                if st.button("👤 View", key=f"lb_lead_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No data available")

@st.dialog("⭐ All Top Earners", width="large")
def show_all_earners():
    """Show all users sorted by total earnings."""
    data = get_top_earners(50)
    if data:
        st.info(f"Showing top {len(data)} users by total coins earned")
        st.caption("Click 'View' to see user profile")
        
        for i, user in enumerate(data, 1):
            user_id = user['userId']
            col1, col2, col3, col4 = st.columns([0.5, 2, 1.5, 1])
            with col1:
                st.write(f"**{i}**")
            with col2:
                st.write(f"**{user['userName']}**")
            with col3:
                st.write(f"⭐ {user['totalEarned']:,.0f}")
            with col4:
                if st.button("👤 View", key=f"lb_earn_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No data available")

@st.dialog("🏧 All Top Withdrawers", width="large")
def show_all_withdrawers():
    """Show all users sorted by withdrawal count."""
    data = get_top_withdrawers(50)
    if data:
        st.info(f"Showing top {len(data)} users by withdrawal requests")
        st.caption("Click 'View' to see user profile")
        
        for i, user in enumerate(data, 1):
            user_id = user['userId']
            col1, col2, col3, col4, col5 = st.columns([0.5, 2, 1, 1.5, 1])
            with col1:
                st.write(f"**{i}**")
            with col2:
                st.write(f"**{user['userName']}**")
            with col3:
                st.write(f"🏧 {user['withdrawalCount']}")
            with col4:
                st.write(f"₹{user['totalAmount']:,.0f}")
            with col5:
                if st.button("👤 View", key=f"lb_wdraw_{user_id[:8]}", use_container_width=True):
                    st.session_state['navigate_to_user'] = user_id
                    st.rerun()
    else:
        st.info("No data available")

try:
    lb_col1, lb_col2, lb_col3, lb_col4 = st.columns(4)
    
    with lb_col1:
        st.markdown("**💰 Top Coin Holders**")
        top_coins = get_top_coin_holders(5)
        if top_coins:
            df = pd.DataFrame([
                {'#': i+1, 'Name': u['userName'], 'Coins': f"{u['coins']:,.0f}"}
                for i, u in enumerate(top_coins)
            ])
            st.dataframe(df, use_container_width=True, hide_index=True, height=210)
            if st.button("📊 View All", key="lb_coins_detail", use_container_width=True):
                show_all_coin_holders()
        else:
            st.caption("No data")
    
    with lb_col2:
        st.markdown("**🔗 Top Referrers**")
        top_refs = get_top_referrers(5)
        if top_refs:
            df = pd.DataFrame([
                {'#': i+1, 'Name': u['userName'], 'Referrals': u['referralCount']}
                for i, u in enumerate(top_refs)
            ])
            st.dataframe(df, use_container_width=True, hide_index=True, height=210)
            if st.button("📊 View All", key="lb_refs_detail", use_container_width=True):
                show_all_referrers()
        else:
            st.caption("No data")
    
    with lb_col3:
        st.markdown("**📈 Top Lead Generators**")
        top_leads_lb = get_top_lead_generators(5)
        if top_leads_lb:
            df = pd.DataFrame([
                {'#': i+1, 'Name': u['userName'], 'Leads': u['leadCount']}
                for i, u in enumerate(top_leads_lb)
            ])
            st.dataframe(df, use_container_width=True, hide_index=True, height=210)
            if st.button("📊 View All", key="lb_leads_detail", use_container_width=True):
                show_all_lead_generators()
        else:
            st.caption("No data")
    
    with lb_col4:
        st.markdown("**⭐ Top Earners**")
        top_earners = get_top_earners(5)
        if top_earners:
            df = pd.DataFrame([
                {'#': i+1, 'Name': u['userName'], 'Earned': f"{u['totalEarned']:,.0f}"}
                for i, u in enumerate(top_earners)
            ])
            st.dataframe(df, use_container_width=True, hide_index=True, height=210)
            if st.button("📊 View All", key="lb_earners_detail", use_container_width=True):
                show_all_earners()
        else:
            st.caption("No data")

except Exception as e:
    st.warning(f"Could not load leaderboards: {e}")

# 5th Leaderboard - Top Withdrawers
try:
    st.markdown("**🏧 Top Withdrawers**")
    top_withdrawers = get_top_withdrawers(5)
    if top_withdrawers:
        lb_w_col1, lb_w_col2, lb_w_col3, lb_w_col4, lb_w_col5 = st.columns(5)
        
        for i, user in enumerate(top_withdrawers):
            with [lb_w_col1, lb_w_col2, lb_w_col3, lb_w_col4, lb_w_col5][i]:
                st.markdown(f"**{i+1}. {user['userName']}**")
                st.caption(f"🏧 {user['withdrawalCount']} requests")
                st.caption(f"₹{user['totalAmount']:,.0f}")
        
        if st.button("📊 View All Withdrawers", key="lb_withdraw_detail"):
            show_all_withdrawers()
    else:
        st.caption("No withdrawal data")
except Exception as e:
    st.warning(f"Could not load withdrawers: {e}")

st.markdown("---")

# ======== DAILY COIN ACTIVITY CHART WITH DATE FILTER (G05) ========
st.markdown("### 📊 Daily Coin Activity")

# Date filter for coin activity
coin_filter_col1, coin_filter_col2, coin_filter_col3 = st.columns([1, 1, 2])

with coin_filter_col1:
    coin_start_date = st.date_input(
        "Start Date",
        value=date.today() - timedelta(days=6),
        max_value=date.today(),
        key="coin_start"
    )

with coin_filter_col2:
    coin_end_date = st.date_input(
        "End Date",
        value=date.today(),
        max_value=date.today(),
        key="coin_end"
    )

if coin_start_date > coin_end_date:
    st.error("Start date must be before end date")
else:
    try:
        daily_activity = get_daily_coin_activity_by_range(coin_start_date, coin_end_date)
        
        if daily_activity:
            df_activity = pd.DataFrame(daily_activity)
            df_activity['date'] = pd.to_datetime(df_activity['date'])
            
            # Melt for grouped bar chart
            df_melted = df_activity.melt(
                id_vars=['date'], 
                value_vars=['credits', 'debits'],
                var_name='Type', 
                value_name='Amount'
            )
            
            fig_activity = px.bar(
                df_melted,
                x='date',
                y='Amount',
                color='Type',
                barmode='group',
                color_discrete_map={'credits': '#10b981', 'debits': '#ef4444'},
                labels={'date': 'Date', 'Amount': 'Coins'}
            )
            fig_activity.update_layout(
                height=350,
                showlegend=True,
                legend_title_text='',
                xaxis_title="Date",
                yaxis_title="Coins"
            )
            st.plotly_chart(fig_activity, use_container_width=True)
            
            # Summary
            total_credits = sum(d['credits'] for d in daily_activity)
            total_debits = sum(d['debits'] for d in daily_activity)
            net = total_credits - total_debits
            days = (coin_end_date - coin_start_date).days + 1
            st.caption(f"**Credits: {total_credits:,.0f}** | **Debits: {total_debits:,.0f}** | **Net: {net:+,.0f}** | Period: {days} days")
        else:
            st.info("No coin activity data available for selected range.")

    except Exception as e:
        st.warning(f"Could not load daily activity: {e}")

st.markdown("---")

# Referral Chart with Date Filter
st.markdown("### 📈 Referral Trend")

# Date filter
filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])

with filter_col1:
    start_date = st.date_input(
        "Start Date",
        value=date.today() - timedelta(days=6),
        max_value=date.today()
    )

with filter_col2:
    end_date = st.date_input(
        "End Date",
        value=date.today(),
        max_value=date.today()
    )

# Validate date range
if start_date > end_date:
    st.error("Start date must be before end date")
else:
    try:
        # Get stats for selected date range
        stats = get_referral_stats_by_range(start_date, end_date)
        
        if stats:
            df = pd.DataFrame(stats)
            df['date'] = pd.to_datetime(df['date'])
            
            fig = px.bar(
                df, 
                x='date', 
                y='count',
                labels={'date': 'Date', 'count': 'Referrals'},
                color_discrete_sequence=['#667eea']
            )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Referrals",
                showlegend=False,
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary stats
            total_referrals = sum(s['count'] for s in stats)
            days = (end_date - start_date).days + 1
            avg_per_day = total_referrals / days if days > 0 else 0
            st.caption(f"**Total: {total_referrals} referrals** | Average: {avg_per_day:.1f}/day | Period: {days} days")
        else:
            st.info("No referral data available for the selected date range.")
            
    except Exception as e:
        st.error(f"Error loading chart: {e}")

st.markdown("---")

# Quick Navigation
st.markdown("### 🚀 Quick Actions")

col1, col2 = st.columns(2)

with col1:
    if st.button("💰 Go to Coin Transactions", use_container_width=True):
        st.switch_page("pages/2_Coin_Transactions.py")

with col2:
    if st.button("🏧 Go to Withdrawal Requests", use_container_width=True):
        st.switch_page("pages/3_Withdrawals.py")
