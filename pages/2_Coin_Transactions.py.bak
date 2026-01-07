"""
Page 2: Coin Transactions
Search users and view their coin, lead, and referral history
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add app to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services import (
    search_users,
    get_user_by_id,
    get_wallet_by_user,
    get_transactions_by_user,
    get_leads_by_user,
    get_referrals_by_user,
    get_tier_name,
    get_all_users,
    get_referral_revenue_for_user,
    get_withdrawals_by_user,
    get_orders_by_user
)
from app.services.wallet_service import get_coins_by_tier, coins_to_rupees
from app.utils import format_date


st.set_page_config(page_title="Coin Transactions", page_icon="💰", layout="wide")

# Custom CSS for tier cards
st.markdown("""
<style>
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
</style>
""", unsafe_allow_html=True)

# Session state for selected user
if 'selected_user_id' not in st.session_state:
    st.session_state.selected_user_id = None

st.title("💰 Coin Transactions")
st.markdown("Search users and view their complete history")

# Tier-wise KPIs Section
st.markdown("### 💎 Redeemable Market Value by Tier")
st.caption("Bronze ×0.40 | Silver ×0.70 | Gold ×1.00")

try:
    tier_stats = get_coins_by_tier()
    
    # Define tier order and colors
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
    
    # Total summary
    total_coins = sum(s['coins'] for s in tier_stats.values())
    total_rupees = sum(s['rupees'] for s in tier_stats.values())
    st.markdown(f"**Total Market Value: {total_coins:,.0f} coins = ₹{total_rupees:,.0f}**")
    
    # Debug expander
    with st.expander("🔍 Debug Info"):
        st.write("**Tier Stats from DB:**")
        st.json(tier_stats)
        st.write(f"**Total users with balance:** {sum(s['users'] for s in tier_stats.values())}")
    
except Exception as e:
    st.warning(f"Could not load tier stats: {e}")

st.markdown("---")

# Search Section
st.markdown("### 🔍 Search User")

# Session state for search persistence
if 'search_query' not in st.session_state:
    st.session_state.search_query = ''
if 'search_results' not in st.session_state:
    st.session_state.search_results = None

col1, col2 = st.columns([3, 1])

with col1:
    search_query = st.text_input(
        "Enter User ID or Phone Number",
        placeholder="e.g., USR001 or 9876543210",
        label_visibility="collapsed"
    )

with col2:
    search_clicked = st.button("🔍 Search", use_container_width=True)

st.caption("💡 Search by **phone number**, **email**, or **user ID** for instant results")

st.markdown("---")

# Function to display user profile
def display_user_profile(user: dict, wallet: dict):
    """Display user profile header."""
    tier_name = get_tier_name(user.get('tierId', ''))
    
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                padding: 1.5rem; border-radius: 12px; color: white; margin-bottom: 1rem;">
        <h3 style="margin: 0; color: white;">👤 {user.get('userName', 'Unknown')}</h3>
        <div style="display: flex; gap: 2rem; margin-top: 1rem; flex-wrap: wrap;">
            <div><strong>User ID:</strong> {user.get('userId', 'N/A')}</div>
            <div><strong>Phone:</strong> {user.get('phoneNumber', 'N/A')}</div>
            <div><strong>Tier:</strong> {tier_name}</div>
            <div><strong>Coins:</strong> {float(wallet.get('remainingAmount', 0)):,.0f}</div>
            <div><strong>Signed Up:</strong> {format_date(user.get('created_time'))}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# Function to display user details with tabs
def display_user_details(user_id: str):
    """Display full user details with history tabs."""
    user = get_user_by_id(user_id)
    wallet = get_wallet_by_user(user_id)
    
    if not user:
        st.warning(f"User {user_id} not found")
        return
    
    if not wallet:
        wallet = {'remainingAmount': 0, 'totalAmount': 0, 'usedAmount': 0}
    
    # Display profile header
    display_user_profile(user, wallet)
    
    # Tabs for different histories
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["💰 Coin History", "📈 Lead History", "🔗 Referral History", "💵 Referral Revenue", "🏧 Withdrawal History", "📦 Order History"])
    
    with tab1:
        transactions = get_transactions_by_user(user_id)
        if transactions:
            df = pd.DataFrame(transactions)
            display_cols = ['transactionId', 'title', 'amount', 'reason', 'status', 'created_time']
            display_cols = [c for c in display_cols if c in df.columns]
            
            if display_cols:
                df_display = df[display_cols].copy()
                # Format date column
                if 'created_time' in display_cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                df_display.columns = ['Transaction ID', 'Type', 'Amount', 'Reason', 'Status', 'Date'][:len(display_cols)]
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No coin transactions found for this user.")
    
    with tab2:
        leads = get_leads_by_user(user_id)
        if leads:
            df = pd.DataFrame(leads)
            display_cols = ['leadId', 'leadName', 'leadNumber', 'occasionName', 'leadStage', 'orderValue', 'created_time']
            display_cols = [c for c in display_cols if c in df.columns]
            
            if display_cols:
                df_display = df[display_cols].copy()
                # Format date column
                if 'created_time' in display_cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                st.dataframe(df_display, use_container_width=True, hide_index=True)
                
                # Show clickable leads
                st.markdown("**Click on a lead to view their profile (if they're a user):**")
                for _, lead in df.iterrows():
                    lead_phone = lead.get('leadNumber', '')
                    if lead_phone and st.button(f"📞 {lead.get('leadName', 'Unknown')} - {lead_phone}", key=f"lead_{lead.get('leadId')}"):
                        # Search if this lead is also a user
                        found_users = search_users(lead_phone)
                        if found_users:
                            st.session_state.selected_user_id = found_users[0].get('userId')
                            st.rerun()
                        else:
                            st.warning(f"Lead {lead_phone} is not a registered user.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No leads found for this user.")
    
    with tab3:
        referrals = get_referrals_by_user(user_id)
        if referrals:
            df = pd.DataFrame(referrals)
            display_cols = ['tierReferralId', 'sentTo', 'referralName', 'sendedAmount', 'status', 'created_time']
            display_cols = [c for c in display_cols if c in df.columns]
            
            if display_cols:
                df_display = df[display_cols].copy()
                # Format date column
                if 'created_time' in display_cols:
                    df_display['created_time'] = df_display['created_time'].apply(format_date)
                st.dataframe(df_display, use_container_width=True, hide_index=True)
                
                # Show clickable referrals
                st.markdown("**Click on a referral to view their profile (if they're a user):**")
                for _, ref in df.iterrows():
                    ref_phone = ref.get('sentTo', '')
                    if ref_phone and st.button(f"👤 {ref.get('referralName', 'Unknown')} - {ref_phone}", key=f"ref_{ref.get('tierReferralId')}"):
                        # Search if this referral is also a user
                        found_users = search_users(ref_phone)
                        if found_users:
                            st.session_state.selected_user_id = found_users[0].get('userId')
                            st.rerun()
                        else:
                            st.warning(f"Referral {ref_phone} is not a registered user.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No referrals found for this user.")
    
    with tab4:
        st.markdown("**Revenue generated by this user's referrals:**")
        
        try:
            revenue_data = get_referral_revenue_for_user(user_id)
            
            if revenue_data['referrals']:
                # Summary metrics
                sum_col1, sum_col2, sum_col3 = st.columns(3)
                with sum_col1:
                    st.metric("Total Referrals", revenue_data['total_referrals'])
                with sum_col2:
                    st.metric("Converted (Placed Order)", revenue_data['converted_referrals'])
                with sum_col3:
                    st.metric("Total Revenue Generated", f"₹{revenue_data['total_revenue']:,.0f}")
                
                st.markdown("---")
                st.markdown("**Referral-wise Revenue Breakdown:**")
                
                # Build table
                table_data = []
                for ref in revenue_data['referrals']:
                    table_data.append({
                        'Name': ref['referralName'],
                        'Phone': ref['sentTo'],
                        'Status': ref['status'],
                        'Bonus (Coins)': ref['bonusEarned'],
                        'Orders': ref['orders'],
                        'Revenue (₹)': f"₹{ref['revenue']:,.0f}" if ref['revenue'] > 0 else '-'
                    })
                
                df_revenue = pd.DataFrame(table_data)
                st.dataframe(df_revenue, use_container_width=True, hide_index=True)
                
            else:
                st.info("No referrals found for this user.")
                
        except Exception as e:
            st.warning(f"Could not load referral revenue: {e}")
    
    with tab5:
        st.markdown("**Withdrawal requests made by this user:**")
        
        try:
            withdrawals = get_withdrawals_by_user(user_id)
            
            if withdrawals:
                # Summary
                total_requested = sum(float(w.get('requestedAmount', 0)) for w in withdrawals)
                pending = sum(1 for w in withdrawals if w.get('status', '').lower() == 'pending')
                approved = sum(1 for w in withdrawals if w.get('status', '').lower() == 'approved')
                
                sum_col1, sum_col2, sum_col3 = st.columns(3)
                with sum_col1:
                    st.metric("Total Requests", len(withdrawals))
                with sum_col2:
                    st.metric("Pending", pending)
                with sum_col3:
                    st.metric("Total Requested", f"{total_requested:,.0f} coins")
                
                st.markdown("---")
                
                # Build table
                table_data = []
                for w in withdrawals:
                    table_data.append({
                        'Request ID': str(w.get('requestedId', 'N/A'))[:12] + '...',
                        'Amount': float(w.get('requestedAmount', 0)),
                        'Status': w.get('status', 'Unknown'),
                        'Date': format_date(w.get('created_time'))
                    })
                
                df_withdrawals = pd.DataFrame(table_data)
                st.dataframe(df_withdrawals, use_container_width=True, hide_index=True)
                
            else:
                st.info("No withdrawal requests found for this user.")
                
        except Exception as e:
            st.warning(f"Could not load withdrawal history: {e}")
    
    with tab6:
        st.markdown("**Orders placed by this user:**")
        
        try:
            orders = get_orders_by_user(user_id)
            
            if orders:
                # Summary - exclude failed orders from total value
                total_orders = len(orders)
                successful_orders = [o for o in orders if str(o.get('orderStatus', '')).upper() not in ['FAILED', 'CANCELLED', 'REJECTED']]
                total_value = sum(float(o.get('grandTotal', 0)) for o in successful_orders)
                
                sum_col1, sum_col2, sum_col3 = st.columns(3)
                with sum_col1:
                    st.metric("Total Orders", total_orders)
                with sum_col2:
                    st.metric("Successful Orders", len(successful_orders))
                with sum_col3:
                    st.metric("Total Order Value", f"₹{total_value:,.0f}")
                
                st.caption("⚠️ Failed/Cancelled orders are excluded from Total Order Value")
                st.markdown("---")
                
                # Build table - show dash for failed orders
                table_data = []
                for o in orders:
                    status = str(o.get('orderStatus', 'Unknown')).upper()
                    is_failed = status in ['FAILED', 'CANCELLED', 'REJECTED']
                    
                    table_data.append({
                        'Order ID': str(o.get('orderId', 'N/A'))[:12] + '...',
                        'Amount (₹)': '-' if is_failed else f"₹{float(o.get('grandTotal', 0)):,.0f}",
                        'Status': o.get('orderStatus', 'Unknown'),
                        'Payment': o.get('paymentStatus', 'Unknown'),
                        'Date': format_date(o.get('created_time'))
                    })
                
                df_orders = pd.DataFrame(table_data)
                st.dataframe(df_orders, use_container_width=True, hide_index=True)
                
            else:
                st.info("No orders found for this user.")
                
        except Exception as e:
            st.warning(f"Could not load order history: {e}")


# Main logic
try:
    # If user selected from session state - show their profile
    if st.session_state.selected_user_id:
        if st.button("← Back to Search"):
            st.session_state.selected_user_id = None
            st.session_state.search_results = None
            st.rerun()
        
        display_user_details(st.session_state.selected_user_id)
    
    else:
        # When search button is clicked, save results
        if search_clicked and search_query:
            users = search_users(search_query)
            st.session_state.search_results = users
            st.session_state.search_query = search_query
            
            # Show "not found" message if no results
            if not users:
                st.warning(f"⚠️ No user found with phone number, email, or user ID: **{search_query}**")
                st.info("💡 Try searching with the exact phone number (e.g., +919876543210) or user ID")
        
        # Display search results (from session state)
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
    st.info("Make sure your AWS credentials are configured correctly.")
