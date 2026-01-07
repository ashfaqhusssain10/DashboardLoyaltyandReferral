"""
Admin Control Tower - Consolidated Dashboard
Combines Dashboard + Coin Transactions into a single page with tabs
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
    get_user_by_id,
    search_users,
    get_wallet_by_user,
    get_transactions_by_user,
    get_leads_by_user,
    get_referrals_by_user,
    get_referral_revenue_for_user,
    get_withdrawals_by_user,
    get_orders_by_user
)
from app.services.wallet_service import get_top_coin_holders, get_top_earners, get_top_added_to_wallet, get_daily_coin_activity, get_daily_coin_activity_by_range, get_coins_by_tier, coins_to_rupees
from app.services.referral_service import get_top_referrers
from app.services.lead_service import get_top_lead_generators
from app.services.withdrawal_service import get_top_withdrawers
from app.utils import format_date

# Redshift service for optimized analytics queries
import os
USE_REDSHIFT = os.environ.get('USE_REDSHIFT', 'true').lower() == 'true'

if USE_REDSHIFT:
    try:
        from app.services import redshift_service
        
        # Cached Redshift functions (results cached for 1 hour since data updates daily)
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_top_coin_holders(limit=10):
            return [
                {'userId': r['user_id'], 'userName': r['user_name'], 'coins': float(r['coins'])}
                for r in redshift_service.get_top_coin_holders(limit)
            ]
        
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_top_earners(limit=10):
            return [
                {'userId': r['user_id'], 'userName': r['user_name'], 'totalEarned': float(r['total_earned'])}
                for r in redshift_service.get_top_earners(limit)
            ]
        
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_top_referrers(limit=10):
            return [
                {'userId': r['user_id'], 'userName': r['user_name'], 'referralCount': int(r['referral_count'])}
                for r in redshift_service.get_top_referrers(limit)
            ]
        
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_top_lead_generators(limit=10):
            return [
                {'userId': r['user_id'], 'userName': r['user_name'], 'leadCount': int(r['lead_count'])}
                for r in redshift_service.get_top_lead_generators(limit)
            ]
        
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_daily_coin_activity_by_range(start, end):
            return [
                {'date': str(r['date']), 'credits': float(r['credits']), 'debits': float(r['debits'])}
                for r in redshift_service.get_daily_coin_activity_by_range(start, end)
            ]
        
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_top_added_to_wallet(limit=10):
            return [
                {'userId': r['user_id'], 'userName': r['user_name'], 'totalAdded': float(r['total_added'])}
                for r in redshift_service.get_top_added_to_wallet(limit)
            ]
        
        @st.cache_data(ttl=3600, show_spinner=False)
        def get_referral_stats_by_range(start, end):
            return [
                {'date': str(r['date']), 'count': int(r['count'])}
                for r in redshift_service.get_referral_stats_by_range(start, end)
            ]
        
        print("📊 Using Redshift for analytics queries (with 1-hour caching)")
    except Exception as e:
        print(f"⚠️ Redshift not available, falling back to DynamoDB: {e}")
        USE_REDSHIFT = False

# Page config
st.set_page_config(page_title="Admin Control Tower", page_icon="🏢", layout="wide", initial_sidebar_state="collapsed")

# ======== SESSION STATE INITIALIZATION ========
if 'selected_user_id' not in st.session_state:
    st.session_state.selected_user_id = None
if 'pending_navigation' not in st.session_state:
    st.session_state.pending_navigation = None
if 'search_query' not in st.session_state:
    st.session_state.search_query = ''
if 'search_results' not in st.session_state:
    st.session_state.search_results = None

# ======== HANDLE PENDING NAVIGATION (from dialogs) ========
if st.session_state.pending_navigation:
    st.session_state.selected_user_id = st.session_state.pending_navigation
    st.session_state.pending_navigation = None

# ======== CUSTOM CSS ========
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
    .tier-card {
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .tier-bronze { background: linear-gradient(135deg, #cd7f32 0%, #8b4513 100%); }
    .tier-silver { background: linear-gradient(135deg, #c0c0c0 0%, #808080 100%); }
    .tier-gold { background: linear-gradient(135deg, #ffd700 0%, #daa520 100%); color: #333; }
    .tier-unknown { background: linear-gradient(135deg, #6c757d 0%, #495057 100%); }
    .tier-value { font-size: 1.5rem; font-weight: 700; }
    .tier-label { font-size: 0.85rem; opacity: 0.9; }
    .rupee-value { font-size: 1.1rem; color: #90EE90; margin-top: 0.3rem; }
    .profile-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        margin-bottom: 1rem;
    }
    .profile-kpi {
        background: rgba(255,255,255,0.18);
        padding: 1.2rem 1.5rem;
        border-radius: 12px;
        text-align: center;
        min-width: 100px;
        flex: 1;
        backdrop-filter: blur(5px);
    }
    .profile-kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
    .profile-kpi-label {
        font-size: 0.9rem;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
</style>
""", unsafe_allow_html=True)

# ======== POPUP DIALOGS FOR DASHBOARD ========

@st.dialog("👥 Users with Balance", width="large")
def show_users_with_balance():
    """Show top users with coin balance > 0."""
    from app.services.aggregates_service import get_active_users_from_aggregates
    
    total_users = get_active_users_from_aggregates() or 0
    wallets = get_all_wallets(limit=1000)
    wallets_with_balance = sorted(
        [w for w in wallets if float(w.get('remainingAmount', 0)) > 0],
        key=lambda x: float(x.get('remainingAmount', 0)),
        reverse=True
    )[:50]
    
    if wallets_with_balance:
        st.info(f"Showing {len(wallets_with_balance)} of {total_users:,} users with balance")
        st.caption("Click 'View Profile' to see user details")
        
        for w in wallets_with_balance:
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info("No users with balance found.")


@st.dialog("💰 Total Coins in System", width="large")
def show_total_coins():
    """Show top wallets with coins."""
    from app.services.aggregates_service import get_total_coins_from_aggregates
    
    total_coins = get_total_coins_from_aggregates() or 0
    wallets = get_all_wallets(limit=1000)
    wallets_with_coins = sorted(
        [w for w in wallets if float(w.get('remainingAmount', 0)) > 0],
        key=lambda x: float(x.get('remainingAmount', 0)),
        reverse=True
    )[:50]
    
    if wallets_with_coins:
        st.success(f"**Total Coins in System: {total_coins:,.0f}**")
        st.info(f"Showing top {len(wallets_with_coins)} coin holders")
        
        for w in wallets_with_coins:
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
                    st.session_state.pending_navigation = user_id
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.success("No pending withdrawals! 🎉")


@st.dialog("🔗 Today's Referrals", width="large")
def show_today_referrals():
    """Show today's referrals."""
    referrals = get_today_referrals()
    
    if referrals:
        st.info(f"Total today: {len(referrals)} referrals")
        
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info("No referrals created today.")


@st.dialog("📈 Today's Leads", width="large")
def show_today_leads():
    """Show today's leads."""
    leads = get_today_leads()
    
    if leads:
        st.info(f"Total today: {len(leads)} leads")
        
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info("No leads created today.")


# ======== LEADERBOARD POPUP DIALOGS ========

@st.dialog("💰 All Top Coin Holders", width="large")
def show_all_coin_holders():
    data = get_top_coin_holders(50)
    if data:
        st.info(f"Showing top {len(data)} users by coin balance")
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info("No data available")

@st.dialog("🔗 All Top Referrers", width="large")
def show_all_referrers():
    period = st.session_state.get('lb_period', 'all')
    period_labels = {'all': 'All Time', 'week': 'This Week', 'today': 'Today'}
    
    # Get time-filtered data from Redshift
    raw_data = redshift_service.get_top_referrers_by_period(50, period)
    data = [
        {'userId': r['user_id'], 'userName': r.get('user_name', 'Unknown'), 'referralCount': int(r.get('referral_count', 0))}
        for r in raw_data
    ] if raw_data else []
    
    if data:
        st.info(f"Showing top {len(data)} referrers ({period_labels[period]})")
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info(f"No referral data for {period_labels[period]}")

@st.dialog("📈 All Top Lead Generators", width="large")
def show_all_lead_generators():
    period = st.session_state.get('lb_period', 'all')
    period_labels = {'all': 'All Time', 'week': 'This Week', 'today': 'Today'}
    
    raw_data = redshift_service.get_top_lead_generators_by_period(50, period)
    data = [
        {'userId': r['user_id'], 'userName': r.get('user_name', 'Unknown'), 'leadCount': int(r.get('lead_count', 0))}
        for r in raw_data
    ] if raw_data else []
    
    if data:
        st.info(f"Showing top {len(data)} lead generators ({period_labels[period]})")
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info(f"No lead data for {period_labels[period]}")

@st.dialog("⭐ All Top Earners", width="large")
def show_all_earners():
    period = st.session_state.get('lb_period', 'all')
    period_labels = {'all': 'All Time', 'week': 'This Week', 'today': 'Today'}
    
    raw_data = redshift_service.get_top_earners_by_period(50, period)
    data = [
        {'userId': r['user_id'], 'userName': r.get('user_name', 'Unknown'), 'totalEarned': float(r.get('total_earned', 0))}
        for r in raw_data
    ] if raw_data else []
    
    if data:
        st.info(f"Showing top {len(data)} earners ({period_labels[period]})")
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info(f"No earnings data for {period_labels[period]}")

@st.dialog("🏧 All Top Withdrawers", width="large")
def show_all_withdrawers():
    period = st.session_state.get('lb_period', 'all')
    period_labels = {'all': 'All Time', 'week': 'This Week', 'today': 'Today'}
    
    raw_data = redshift_service.get_top_withdrawers_by_period(50, period)
    data = [
        {'userId': r['user_id'], 'userName': r.get('user_name', 'Unknown'), 'withdrawalCount': int(r.get('withdrawal_count', 0)), 'totalAmount': float(r.get('total_requested', 0))}
        for r in raw_data
    ] if raw_data else []
    
    if data:
        st.info(f"Showing top {len(data)} withdrawers ({period_labels[period]})")
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
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info(f"No withdrawal data for {period_labels[period]}")

@st.dialog("💵 All Top Added to Wallet", width="large")
def show_all_coin_holders_added():
    period = st.session_state.get('lb_period', 'all')
    period_labels = {'all': 'All Time', 'week': 'This Week', 'today': 'Today'}
    
    raw_data = redshift_service.get_top_added_to_wallet_by_period(50, period)
    data = [
        {'userId': r['user_id'], 'userName': r.get('user_name', 'Unknown'), 'totalAdded': float(r.get('total_added', 0))}
        for r in raw_data
    ] if raw_data else []
    
    if data:
        st.info(f"Showing top {len(data)} users by 'Added to Wallet' credits ({period_labels[period]})")
        for i, user in enumerate(data, 1):
            user_id = user['userId']
            col1, col2, col3, col4 = st.columns([0.5, 2, 1.5, 1])
            with col1:
                st.write(f"**{i}**")
            with col2:
                st.write(f"**{user['userName']}**")
            with col3:
                st.write(f"💵 {user['totalAdded']:,.0f}")
            with col4:
                if st.button("👤 View", key=f"lb_added_{user_id[:8]}", use_container_width=True):
                    st.session_state.pending_navigation = user_id
                    st.rerun()
    else:
        st.info(f"No 'Added to Wallet' data for {period_labels[period]}")


# ======== USER PROFILE FUNCTIONS (NEW 2-TAB DESIGN) ========

def display_user_profile_header(user: dict, wallet: dict, referrals: list, leads: list, revenue_data: dict, withdrawals: list):
    """Display enhanced user profile header with KPIs and ROI."""
    tier_name = get_tier_name(user.get('tierId', ''))
    coins = float(wallet.get('remainingAmount', 0))
    tier_rate = {'Gold': 1.0, 'Silver': 0.7, 'Bronze': 0.4}.get(tier_name, 0.4)
    coin_value = coins * tier_rate
    
    # Calculate totals
    total_referrals = len(referrals)
    total_leads = len(leads)
    total_revenue = revenue_data.get('total_revenue', 0) if revenue_data else 0
    total_withdrawn = sum(float(w.get('requestedAmount', 0)) for w in withdrawals if w.get('status', '').lower() == 'approved')
    
    # ROI calculation
    roi_text = ""
    if coin_value > 0 and total_revenue > 0:
        roi = total_revenue / coin_value
        roi_text = f"💡 ROI: Earned {coins:,.0f} coins (₹{coin_value:,.0f}) → Generated ₹{total_revenue:,.0f} revenue = {roi:.0f}x return"
    
    st.markdown(f"""
    <div class="profile-header">
        <h2 style="margin: 0; color: white;">👤 {user.get('userName', 'Unknown')}</h2>
        <div style="display: flex; gap: 2rem; margin-top: 0.8rem; flex-wrap: wrap; font-size: 0.95rem;">
            <div><strong>ID:</strong> {user.get('userId', 'N/A')[:12]}...</div>
            <div><strong>Phone:</strong> {user.get('phoneNumber', 'N/A')}</div>
            <div><strong>Tier:</strong> 🏆 {tier_name}</div>
            <div><strong>Joined:</strong> {format_date(user.get('created_time'))}</div>
        </div>
        <div style="display: flex; gap: 1rem; margin-top: 1.2rem; flex-wrap: wrap; justify-content: space-between;">
            <div class="profile-kpi">
                <div class="profile-kpi-value">{coins:,.0f}</div>
                <div class="profile-kpi-label">Coins</div>
            </div>
            <div class="profile-kpi">
                <div class="profile-kpi-value">{total_referrals}</div>
                <div class="profile-kpi-label">Referrals</div>
            </div>
            <div class="profile-kpi">
                <div class="profile-kpi-value">{total_leads}</div>
                <div class="profile-kpi-label">Leads</div>
            </div>
            <div class="profile-kpi">
                <div class="profile-kpi-value">₹{total_revenue:,.0f}</div>
                <div class="profile-kpi-label">Revenue</div>
            </div>
            <div class="profile-kpi">
                <div class="profile-kpi-value">₹{total_withdrawn:,.0f}</div>
                <div class="profile-kpi-label">Withdrawn</div>
            </div>
        </div>
        {f'<div style="margin-top: 1rem; padding: 0.5rem; background: rgba(255,255,255,0.1); border-radius: 6px;">{roi_text}</div>' if roi_text else ''}
    </div>
    """, unsafe_allow_html=True)


def display_user_details(user_id: str):
    """Display full user details with 2-tab design."""
    user = get_user_by_id(user_id)
    wallet = get_wallet_by_user(user_id)
    
    if not user:
        st.warning(f"User {user_id} not found")
        return
    
    if not wallet:
        wallet = {'remainingAmount': 0, 'totalAmount': 0, 'usedAmount': 0}
    
    # Fetch all data upfront (cached in session for this user)
    transactions = get_transactions_by_user(user_id)
    leads = get_leads_by_user(user_id)
    referrals = get_referrals_by_user(user_id)
    
    try:
        revenue_data = get_referral_revenue_for_user(user_id)
    except:
        revenue_data = {'referrals': [], 'total_referrals': 0, 'converted_referrals': 0, 'total_revenue': 0}
    
    try:
        withdrawals = get_withdrawals_by_user(user_id)
    except:
        withdrawals = []
    
    try:
        orders = get_orders_by_user(user_id)
    except:
        orders = []
    
    # Display profile header with KPIs
    display_user_profile_header(user, wallet, referrals, leads, revenue_data, withdrawals)
    
    # 2-TAB DESIGN
    tab1, tab2 = st.tabs(["📥 Earning Activity", "📤 Spending & Revenue"])
    
    with tab1:
        # ====== COIN CREDITS ======
        st.markdown("#### 💰 Coin Credits")
        if transactions:
            credit_txns = [t for t in transactions if float(t.get('amount', 0)) > 0]
            if credit_txns:
                df = pd.DataFrame(credit_txns)
                cols = ['title', 'amount', 'reason', 'created_time']
                cols = [c for c in cols if c in df.columns]
                if cols:
                    df_display = df[cols].copy()
                    if 'created_time' in cols:
                        df_display['created_time'] = df_display['created_time'].apply(format_date)
                    df_display.columns = ['Type', 'Amount', 'Reason', 'Date'][:len(cols)]
                    st.dataframe(df_display, use_container_width=True, hide_index=True, height=200)
                    st.caption(f"Total credits: {sum(float(t.get('amount', 0)) for t in credit_txns):,.0f} coins")
            else:
                st.info("No coin credits found")
        else:
            st.info("No coin transactions found")
        
        st.markdown("---")
        
        # ====== LEADS ======
        st.markdown("#### 📈 Leads Submitted")
        if leads:
            df = pd.DataFrame(leads)
            cols = ['leadName', 'leadNumber', 'occasionName', 'leadStage', 'created_time']
            cols = [c for c in cols if c in df.columns]
            if cols:
                df_display = df[cols].copy()
                if 'created_time' in cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                st.dataframe(df_display, use_container_width=True, hide_index=True, height=200)
                st.caption(f"Total leads: {len(leads)}")
        else:
            st.info("No leads found")
        
        st.markdown("---")
        
        # ====== REFERRALS ======
        st.markdown("#### 🔗 Referrals Made")
        if referrals:
            df = pd.DataFrame(referrals)
            cols = ['referralName', 'sentTo', 'sendedAmount', 'status', 'created_time']
            cols = [c for c in cols if c in df.columns]
            if cols:
                df_display = df[cols].copy()
                if 'created_time' in cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                st.dataframe(df_display, use_container_width=True, hide_index=True, height=200)
                st.caption(f"Total referrals: {len(referrals)}")
        else:
            st.info("No referrals found")
    
    with tab2:
        # ====== ORDERS ======
        st.markdown("#### 📦 Orders Placed")
        if orders:
            successful = [o for o in orders if str(o.get('orderStatus', '')).upper() not in ['FAILED', 'CANCELLED', 'REJECTED']]
            total_value = sum(float(o.get('grandTotal', 0)) for o in successful)
            
            df = pd.DataFrame(orders)
            cols = ['orderId', 'grandTotal', 'orderStatus', 'paymentStatus', 'created_time']
            cols = [c for c in cols if c in df.columns]
            if cols:
                df_display = df[cols].copy()
                if 'created_time' in cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                if 'orderId' in cols:
                    df_display['orderId'] = df_display['orderId'].apply(lambda x: str(x)[:12] + '...')
                st.dataframe(df_display, use_container_width=True, hide_index=True, height=200)
                st.caption(f"Total orders: {len(orders)} | Successful: {len(successful)} | Value: ₹{total_value:,.0f}")
        else:
            st.info("No orders found")
        
        st.markdown("---")
        
        # ====== WITHDRAWALS ======
        st.markdown("#### 🏧 Withdrawals")
        if withdrawals:
            total_requested = sum(float(w.get('requestedAmount', 0)) for w in withdrawals)
            approved = [w for w in withdrawals if w.get('status', '').lower() == 'approved']
            
            df = pd.DataFrame(withdrawals)
            cols = ['requestedAmount', 'status', 'created_time']
            cols = [c for c in cols if c in df.columns]
            if cols:
                df_display = df[cols].copy()
                if 'created_time' in cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                st.dataframe(df_display, use_container_width=True, hide_index=True, height=150)
                st.caption(f"Total: {len(withdrawals)} requests | Approved: {len(approved)} | Amount: {total_requested:,.0f} coins")
        else:
            st.info("No withdrawals found")
        
        st.markdown("---")
        
        # ====== REFERRAL REVENUE ======
        st.markdown("#### 💵 Referral Revenue")
        if revenue_data and revenue_data.get('referrals'):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Referrals", revenue_data['total_referrals'])
            with col2:
                st.metric("Converted", revenue_data['converted_referrals'])
            with col3:
                st.metric("Revenue Generated", f"₹{revenue_data['total_revenue']:,.0f}")
            
            table_data = []
            for ref in revenue_data['referrals']:
                table_data.append({
                    'Name': ref['referralName'],
                    'Orders': ref['orders'],
                    'Revenue': f"₹{ref['revenue']:,.0f}" if ref['revenue'] > 0 else '-'
                })
            if table_data:
                st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True, height=150)
        else:
            st.info("No referral revenue data")


# ======== DASHBOARD CONTENT ========

def render_dashboard_tab():
    """Render the Dashboard tab content."""
    st.markdown("### 📅 Today's Metrics")
    st.caption("Live data • Click any card to see details")
    
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
            if total_coins >= 1_000_000:
                coins_display = f"{total_coins/1_000_000:.1f}M"
            elif total_coins >= 1_000:
                coins_display = f"{total_coins/1_000:.0f}K"
            else:
                coins_display = f"{total_coins:,.0f}"
            st.markdown(f"""
            <div class="kpi-container kpi-container-green">
                <div class="kpi-value">{coins_display}</div>
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
    
    st.markdown("---")
    
    # ======== LEADERBOARDS ========
    st.markdown("### 🏆 Leaderboards")
    
    # Time filter for leaderboards
    lb_filter_col1, lb_filter_col2, lb_filter_col3, lb_filter_col4 = st.columns([1, 1, 1, 3])
    
    with lb_filter_col1:
        if st.button("📅 All Time", key="lb_all", use_container_width=True):
            st.session_state.lb_period = 'all'
    with lb_filter_col2:
        if st.button("📅 This Week", key="lb_week", use_container_width=True):
            st.session_state.lb_period = 'week'
    with lb_filter_col3:
        if st.button("📅 Today", key="lb_today", use_container_width=True):
            st.session_state.lb_period = 'today'
    
    # Initialize leaderboard period if not set
    if 'lb_period' not in st.session_state:
        st.session_state.lb_period = 'week'
    
    period_labels = {'all': 'All Time', 'week': 'This Week', 'today': 'Today'}
    st.caption(f"Showing: **{period_labels[st.session_state.lb_period]}**")
    
    try:
        lb_row1_col1, lb_row1_col2, lb_row1_col3 = st.columns(3)
        
        with lb_row1_col1:
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
        
        with lb_row1_col2:
            st.markdown("**💵 Top Added to Wallet**")
            top_added = redshift_service.get_top_added_to_wallet_by_period(5, st.session_state.lb_period)
            if top_added:
                df = pd.DataFrame([
                    {'#': i+1, 'Name': u.get('user_name', 'Unknown'), 'Added': f"{float(u.get('total_added', 0)):,.0f}"}
                    for i, u in enumerate(top_added)
                ])
                st.dataframe(df, use_container_width=True, hide_index=True, height=210)
                if st.button("📊 View All", key="lb_added_detail", use_container_width=True):
                    show_all_coin_holders_added()
            else:
                st.caption("No data")
        
        with lb_row1_col3:
            st.markdown("**⭐ Top Earners**")
            top_earners = redshift_service.get_top_earners_by_period(5, st.session_state.lb_period)
            if top_earners:
                df = pd.DataFrame([
                    {'#': i+1, 'Name': u.get('user_name', 'Unknown'), 'Earned': f"{float(u.get('total_earned', 0)):,.0f}"}
                    for i, u in enumerate(top_earners)
                ])
                st.dataframe(df, use_container_width=True, hide_index=True, height=210)
                if st.button("📊 View All", key="lb_earners_detail", use_container_width=True):
                    show_all_earners()
            else:
                st.caption("No data")
        
        lb_row2_col1, lb_row2_col2, lb_row2_col3 = st.columns(3)
        
        with lb_row2_col1:
            st.markdown("**🔗 Top Referrers**")
            top_refs = redshift_service.get_top_referrers_by_period(5, st.session_state.lb_period)
            if top_refs:
                df = pd.DataFrame([
                    {'#': i+1, 'Name': u.get('user_name', 'Unknown'), 'Referrals': int(u.get('referral_count', 0))}
                    for i, u in enumerate(top_refs)
                ])
                st.dataframe(df, use_container_width=True, hide_index=True, height=210)
                if st.button("📊 View All", key="lb_refs_detail", use_container_width=True):
                    show_all_referrers()
            else:
                st.caption("No data")
        
        with lb_row2_col2:
            st.markdown("**📈 Top Lead Generators**")
            top_leads_lb = redshift_service.get_top_lead_generators_by_period(5, st.session_state.lb_period)
            if top_leads_lb:
                df = pd.DataFrame([
                    {'#': i+1, 'Name': u.get('user_name', 'Unknown'), 'Leads': int(u.get('lead_count', 0))}
                    for i, u in enumerate(top_leads_lb)
                ])
                st.dataframe(df, use_container_width=True, hide_index=True, height=210)
                if st.button("📊 View All", key="lb_leads_detail", use_container_width=True):
                    show_all_lead_generators()
            else:
                st.caption("No data")
        
        with lb_row2_col3:
            st.markdown("**🏧 Top Withdrawers**")
            top_withdrawers = redshift_service.get_top_withdrawers_by_period(5, st.session_state.lb_period)
            if top_withdrawers:
                df = pd.DataFrame([
                    {'#': i+1, 'Name': u.get('user_name', 'Unknown'), 'Requests': int(u.get('withdrawal_count', 0)), 'Amount': f"₹{float(u.get('total_requested', 0)):,.0f}"}
                    for i, u in enumerate(top_withdrawers)
                ])
                st.dataframe(df, use_container_width=True, hide_index=True, height=210)
                if st.button("📊 View All Withdrawers", key="lb_withdraw_detail", use_container_width=True):
                    show_all_withdrawers()
            else:
                st.caption("No data")
    
    except Exception as e:
        st.warning(f"Could not load leaderboards: {e}")
    
    st.markdown("---")
    
    # ======== CHARTS ========
    from datetime import datetime
    
    st.markdown("### 📊 Monthly Trends")
    
    # Quick date presets
    preset_col1, preset_col2, preset_col3, preset_col4, preset_col5 = st.columns([1, 1, 1, 1, 2])
    
    with preset_col1:
        if st.button("📅 7 Days", key="preset_7d", use_container_width=True):
            st.session_state.chart_days = 7
    with preset_col2:
        if st.button("📅 30 Days", key="preset_30d", use_container_width=True):
            st.session_state.chart_days = 30
    with preset_col3:
        if st.button("📅 90 Days", key="preset_90d", use_container_width=True):
            st.session_state.chart_days = 90
    with preset_col4:
        if st.button("📅 Custom", key="preset_custom", use_container_width=True):
            st.session_state.chart_days = 0  # Custom mode
    with preset_col5:
        st.caption(f"⏰ Last updated: {datetime.now().strftime('%H:%M:%S')}")
    
    # Initialize chart_days if not set
    if 'chart_days' not in st.session_state:
        st.session_state.chart_days = 30
    
    # Calculate dates based on preset
    if st.session_state.chart_days > 0:
        coin_start_date = date.today() - timedelta(days=st.session_state.chart_days - 1)
        coin_end_date = date.today()
        st.caption(f"Showing last {st.session_state.chart_days} days")
    else:
        # Custom date picker
        coin_filter_col1, coin_filter_col2, _ = st.columns([1, 1, 2])
        with coin_filter_col1:
            coin_start_date = st.date_input("Start Date", value=date.today() - timedelta(days=29), max_value=date.today(), key="coin_start")
        with coin_filter_col2:
            coin_end_date = st.date_input("End Date", value=date.today(), max_value=date.today(), key="coin_end")
    
    if coin_start_date > coin_end_date:
        st.error("Start date must be before end date")
    else:
        try:
            daily_activity = get_daily_coin_activity_by_range(coin_start_date, coin_end_date)
            
            if daily_activity:
                df_activity = pd.DataFrame(daily_activity)
                df_activity['date'] = pd.to_datetime(df_activity['date'])
                
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
    
    # Referral Chart
    st.markdown("### 📈 Referral Trend")
    
    filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])
    
    with filter_col1:
        start_date = st.date_input(
            "Start Date",
            value=date.today() - timedelta(days=6),
            max_value=date.today(),
            key="ref_start"
        )
    
    with filter_col2:
        end_date = st.date_input(
            "End Date",
            value=date.today(),
            max_value=date.today(),
            key="ref_end"
        )
    
    if start_date > end_date:
        st.error("Start date must be before end date")
    else:
        try:
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
    if st.button("🏧 Go to Withdrawal Requests", use_container_width=True):
        st.switch_page("pages/2_Withdrawals.py")


# ======== COIN TRANSACTIONS CONTENT ========

def render_coin_transactions_tab():
    """Render the Coin Transactions tab content."""
    
    # Tier-wise KPIs Section
    st.markdown("### 💎 Redeemable Market Value by Tier")
    st.caption("Bronze ×0.40 | Silver ×0.70 | Gold ×1.00")
    
    try:
        tier_stats = get_coins_by_tier()
        
        tier_order = ['Gold', 'Silver', 'Bronze', 'Unknown']
        tier_classes = {'Gold': 'tier-gold', 'Silver': 'tier-silver', 'Bronze': 'tier-bronze', 'Unknown': 'tier-unknown'}
        tier_rates = {'Gold': '×1.00', 'Silver': '×0.70', 'Bronze': '×0.40', 'Unknown': '×0.40'}
        
        cols = st.columns(len(tier_order))
        
        for i, tier_name in enumerate(tier_order):
            stats = tier_stats.get(tier_name, {'coins': 0, 'rupees': 0, 'users': 0, 'rate': 0.40})
            css_class = tier_classes.get(tier_name, 'tier-unknown')
            rate_label = tier_rates.get(tier_name, '×0.40')
            
            with cols[i]:
                st.markdown(f"""
                <div class="tier-card {css_class}">
                    <div class="tier-label">🏆 {tier_name} ({rate_label})</div>
                    <div class="tier-value">{stats['coins']:,.0f}</div>
                    <div class="tier-label">coins</div>
                    <div class="rupee-value">₹{stats['rupees']:,.0f}</div>
                    <div class="tier-label">{stats['users']} users</div>
                </div>
                """, unsafe_allow_html=True)
        
        total_coins = sum(s['coins'] for s in tier_stats.values())
        total_rupees = sum(s['rupees'] for s in tier_stats.values())
        st.markdown(f"**Total Market Value: {total_coins:,.0f} coins = ₹{total_rupees:,.0f}**")
    
    except Exception as e:
        st.warning(f"Could not load tier stats: {e}")
    
    st.markdown("---")
    
    # Search Section
    st.markdown("### 🔍 Search User")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "Enter User ID or Phone Number",
            placeholder="e.g., USR001 or 9876543210",
            label_visibility="collapsed",
            key="user_search"
        )
    
    with col2:
        search_clicked = st.button("🔍 Search", use_container_width=True, key="search_btn")
    
    st.caption("💡 Search by **phone number**, **email**, or **user ID** for instant results")
    
    st.markdown("---")
    
    # Main logic
    try:
        # If user selected - show their profile
        if st.session_state.selected_user_id:
            if st.button("← Back to Search", key="back_btn"):
                st.session_state.selected_user_id = None
                st.session_state.search_results = None
                st.rerun()
            
            display_user_details(st.session_state.selected_user_id)
        
        else:
            # When search button is clicked
            if search_clicked and search_query:
                users = search_users(search_query)
                st.session_state.search_results = users
                st.session_state.search_query = search_query
                
                if not users:
                    st.warning(f"⚠️ No user found with: **{search_query}**")
                    st.info("💡 Try searching with the exact phone number or user ID")
            
            # Display search results
            if st.session_state.search_results:
                users = st.session_state.search_results
                st.success(f"Found {len(users)} user(s)")
                
                for user in users:
                    user_id = user.get('userId')
                    wallet = get_wallet_by_user(user_id) or {'remainingAmount': 0}
                    
                    col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 1])
                    
                    with col1:
                        st.write(f"**{user.get('userName', 'Unknown')}**")
                    with col2:
                        st.write(f"📞 {user.get('phoneNumber', 'N/A')}")
                    with col3:
                        st.write(f"💰 {float(wallet.get('remainingAmount', 0)):,.0f} coins")
                    with col4:
                        tier = get_tier_name(user.get('tierId', ''))
                        st.write(f"🏆 {tier}")
                    with col5:
                        if st.button("View", key=f"view_{user_id}"):
                            st.session_state.selected_user_id = user_id
                            st.rerun()
                    
                    st.markdown("---")
            elif not search_query:
                st.info("Enter a phone number, email, or user ID to search")
    
    except Exception as e:
        st.error(f"Error: {e}")


# ======== MAIN PAGE ========

st.title("🏢 Admin Control Tower")
st.caption("CraftMyPlate Loyalty & Referral Management System")

# If a user is selected, show their profile OUTSIDE tabs (fixes navigation bug)
if st.session_state.selected_user_id:
    # Back button
    if st.button("← Back to Dashboard", key="back_to_dashboard"):
        st.session_state.selected_user_id = None
        st.session_state.search_results = None
        st.rerun()
    
    st.markdown("---")
    
    # Show user profile directly
    display_user_details(st.session_state.selected_user_id)

else:
    # Show main tabs when no user is selected
    main_tab1, main_tab2 = st.tabs(["📊 Dashboard", "💰 Coin Transactions"])
    
    with main_tab1:
        render_dashboard_tab()
    
    with main_tab2:
        render_coin_transactions_tab()

