"""
Page 3: Withdrawal Requests
View and manage withdrawal requests with Accept/Reject actions
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add app to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services import (
    get_all_withdrawals,
    get_pending_count,
    get_total_pending_amount,
    approve_withdrawal,
    reject_withdrawal,
    get_user_by_id,
    get_tier_name
)
from app.services.withdrawal_service import READ_ONLY_MODE

st.set_page_config(page_title="Withdrawals", page_icon="🏧", layout="wide")

# Session state for confirmation dialog
if 'show_confirm_dialog' not in st.session_state:
    st.session_state.show_confirm_dialog = None
if 'confirm_action' not in st.session_state:
    st.session_state.confirm_action = None
if 'confirm_request_id' not in st.session_state:
    st.session_state.confirm_request_id = None
if 'confirm_user_name' not in st.session_state:
    st.session_state.confirm_user_name = None
if 'confirm_amount' not in st.session_state:
    st.session_state.confirm_amount = None

st.title("🏧 Withdrawal Requests")
st.markdown("Review and approve/reject withdrawal requests")

# Show warning if in read-only mode
if READ_ONLY_MODE:
    st.warning("⚠️ **READ-ONLY MODE**: Approval/Rejection actions are disabled for production database migration. "
               "Data viewing is still available.")

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
    }
    .kpi-container-green {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    .kpi-container-orange {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    .kpi-container-red {
        background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
    .kpi-label {
        font-size: 0.9rem;
        opacity: 0.95;
    }
    .pending-badge {
        background-color: #fbbf24;
        color: #1f2937;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-weight: 600;
    }
    .approved-badge {
        background-color: #10b981;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-weight: 600;
    }
    .rejected-badge {
        background-color: #ef4444;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-weight: 600;
    }
    .confirm-dialog {
        background: #1e293b;
        border: 2px solid #3b82f6;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Confirmation Dialog
if st.session_state.show_confirm_dialog:
    action = st.session_state.confirm_action
    request_id = st.session_state.confirm_request_id
    user_name = st.session_state.confirm_user_name
    amount = st.session_state.confirm_amount
    
    action_color = "🟢" if action == "APPROVE" else "🔴"
    action_text = "APPROVE" if action == "APPROVE" else "REJECT"
    
    st.markdown("---")
    st.markdown(f"""
    ### ⚠️ Confirmation Required
    
    You are about to **{action_text}** the following withdrawal request:
    
    | Field | Value |
    |-------|-------|
    | **User** | {user_name} |
    | **Amount** | 💰 {amount:,.0f} coins |
    | **Request ID** | {request_id[:20]}... |
    | **Action** | {action_color} {action_text} |
    """)
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button(f"✅ Yes, {action_text}", type="primary", use_container_width=True):
            if action == "APPROVE":
                approve_withdrawal(request_id)
                st.success(f"✅ Withdrawal APPROVED for {user_name}!")
            else:
                reject_withdrawal(request_id)
                st.error(f"❌ Withdrawal REJECTED for {user_name}!")
            
            # Reset dialog state
            st.session_state.show_confirm_dialog = None
            st.session_state.confirm_action = None
            st.session_state.confirm_request_id = None
            st.session_state.confirm_user_name = None
            st.session_state.confirm_amount = None
            st.rerun()
    
    with col2:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_confirm_dialog = None
            st.session_state.confirm_action = None
            st.session_state.confirm_request_id = None
            st.session_state.confirm_user_name = None
            st.session_state.confirm_amount = None
            st.rerun()
    
    st.markdown("---")

try:
    # KPI Cards
    st.markdown("### Key Metrics")
    
    all_withdrawals = get_all_withdrawals(limit=500)
    pending_withdrawals = [w for w in all_withdrawals if w.get('status', '').lower() == 'pending']
    approved_today = [w for w in all_withdrawals if w.get('status', '').lower() == 'approved']
    rejected_today = [w for w in all_withdrawals if w.get('status', '').lower() == 'rejected']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="kpi-container">
            <div class="kpi-value">{len(pending_withdrawals)}</div>
            <div class="kpi-label">Pending Requests</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        total_pending = sum(float(w.get('requestedAmount', 0)) for w in pending_withdrawals)
        st.markdown(f"""
        <div class="kpi-container kpi-container-orange">
            <div class="kpi-value">{total_pending:,.0f}</div>
            <div class="kpi-label">Total Pending Amount</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="kpi-container kpi-container-green">
            <div class="kpi-value">{len(approved_today)}</div>
            <div class="kpi-label">Approved</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="kpi-container kpi-container-red">
            <div class="kpi-value">{len(rejected_today)}</div>
            <div class="kpi-label">Rejected</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Filters
    st.markdown("### Filters")
    col1, col2 = st.columns(2)
    
    with col1:
        status_filter = st.selectbox(
            "Status",
            ["All", "Pending", "Approved", "Rejected"],
            index=1  # Default to Pending
        )
    
    with col2:
        st.write("")  # Placeholder for future date filter
    
    # Filter withdrawals
    if status_filter != "All":
        filtered_withdrawals = [
            w for w in all_withdrawals 
            if w.get('status', '').lower() == status_filter.lower()
        ]
    else:
        filtered_withdrawals = all_withdrawals
    
    st.markdown("---")
    
    # Withdrawal Requests Table
    st.markdown(f"### Withdrawal Requests ({len(filtered_withdrawals)})")
    
    if filtered_withdrawals:
        for withdrawal in filtered_withdrawals:
            request_id = withdrawal.get('requestedId', 'N/A')
            user_id = withdrawal.get('userId', 'N/A')
            amount = float(withdrawal.get('requestedAmount', 0))
            status = withdrawal.get('status', 'Unknown')
            created_time = str(withdrawal.get('created_time', 'N/A'))
            
            # Get user details
            user = get_user_by_id(user_id)
            user_name = user.get('userName', 'Unknown') if user else 'Unknown'
            tier_id = user.get('tierId', '') if user else ''
            tier_name = get_tier_name(tier_id)
            
            # Create columns for each row
            col1, col2, col3, col4, col5, col6 = st.columns([1.5, 2, 1.5, 1.5, 1.5, 2])
            
            with col1:
                st.write(f"**{request_id[:8]}...**")
            
            with col2:
                st.write(f"👤 {user_name}")
                st.caption(f"ID: {user_id[:12] if len(user_id) > 12 else user_id}")
            
            with col3:
                st.write(f"🏆 {tier_name}")
            
            with col4:
                st.write(f"💰 **{amount:,.0f}**")
            
            with col5:
                if status.lower() == 'pending':
                    st.markdown('<span class="pending-badge">Pending</span>', unsafe_allow_html=True)
                elif status.lower() == 'approved':
                    st.markdown('<span class="approved-badge">Approved</span>', unsafe_allow_html=True)
                else:
                    st.markdown('<span class="rejected-badge">Rejected</span>', unsafe_allow_html=True)
            
            with col6:
                if status.lower() == 'pending':
                    if READ_ONLY_MODE:
                        # Buttons disabled in read-only mode
                        st.caption("🔒 Actions disabled (Read-only mode)")
                    else:
                        btn_col1, btn_col2 = st.columns(2)
                        with btn_col1:
                            if st.button("✅ Accept", key=f"accept_{request_id}", use_container_width=True):
                                st.session_state.show_confirm_dialog = True
                                st.session_state.confirm_action = "APPROVE"
                                st.session_state.confirm_request_id = request_id
                                st.session_state.confirm_user_name = user_name
                                st.session_state.confirm_amount = amount
                                st.rerun()
                        
                        with btn_col2:
                            if st.button("❌ Reject", key=f"reject_{request_id}", use_container_width=True):
                                st.session_state.show_confirm_dialog = True
                                st.session_state.confirm_action = "REJECT"
                                st.session_state.confirm_request_id = request_id
                                st.session_state.confirm_user_name = user_name
                                st.session_state.confirm_amount = amount
                                st.rerun()
                else:
                    st.caption(f"Processed: {created_time[:10] if created_time != 'N/A' else 'N/A'}")
            
            st.markdown("---")
    else:
        st.info("No withdrawal requests found matching your filter.")

except Exception as e:
    st.error(f"Error: {e}")
    st.info("Make sure your AWS credentials are configured correctly.")
