import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
from google import genai
from config import DB_CONFIG, CHART_CONFIG, GEMINI_CONFIG
from data_utils import (
    safe_numeric_conversion, 
    safe_decimal_conversion, 
    format_crypto_value, 
    format_percentage,
    safe_calculation,
    validate_dataframe_columns
)

# Set the environment variable for Google AI API key
os.environ["GEMINI_API_KEY"] = GEMINI_CONFIG["api_key"]

# Initialize Gemini client (API key is automatically picked up from environment)
client = genai.Client()

# Custom CSS for cards that actually works in Streamlit
st.markdown("""
<style>
/* Target containers directly for card styling */
div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stVerticalBlock"]) {
    background-color: #252d3d !important;
    border-radius: 10px !important;
    padding: 20px !important;
    margin: 15px 0 !important;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3) !important;
}

/* Ensure proper spacing between sections */
div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stVerticalBlock"]) + div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stVerticalBlock"]) {
    margin-top: 20px !important;
}
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_specific_bts_transaction_data(transaction_id):
    """
    Load specific BTS transaction data from PostgreSQL database by transaction ID.
    """
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        # Load specific transaction data with joins using parameterized query
        query = """
        SELECT 
            bt.id,
            bt.timestamp,
            bt.type,
            bt.amount,
            bt.price,
            bt."walletAddress",
            bt."tokenAddress",
            bt."BTSCoinInfoId",
            bt."amountInDollars",
            bt."botId",
            bci."coinPrice",
            bci."devPubkey",
            bci."devCapital",
            bci."devholderPercentage",
            bci."tokenSupply",
            bci."totalHoldersSupply",
            bci."isBundle",
            bci."liquidityToMcapRatio",
            bci."reservesInSOL",
            bci."dateCaptured",
            bb."confidence",
            bb."reasons",
            bb."suspiciousWallets",
            bb."timeClustering",
            bb."similarAmounts",
            bb."freshWallets",
            bb."coordinatedBehavior",
            bb."totalBuyers",
            bb."suspiciousBuyers",
            bb."dateCaptured" as "bundleDateCaptured",
            bb."isBundle" as "bundleIsBundle"
        FROM btstransaction bt
        LEFT JOIN btscoininfo bci ON bt."tokenAddress" = bci."tokenAddress"
        LEFT JOIN btsbundle bb ON bt."tokenAddress" = bb."tokenAddress"
        WHERE bt.id = %s
        """
        
        # Convert numpy.int64 to regular Python int for PostgreSQL compatibility
        transaction_id_int = int(transaction_id)
        
        data = pd.read_sql(query, engine, params=(transaction_id_int,))
        
        if data.empty:
            return None
        
        # Get the single row
        transaction_data = data.iloc[0]
        
        # Convert timestamp to datetime
        if 'timestamp' in transaction_data.index:
            transaction_data['timestamp'] = pd.to_datetime(transaction_data['timestamp'])
        
        # Calculate profit/loss based on type and current price using robust data handling
        if 'type' in transaction_data.index and 'price' in transaction_data.index and 'coinPrice' in transaction_data.index:
            # Convert price columns to numeric safely
            price = safe_numeric_conversion(transaction_data['price'])
            coin_price = safe_numeric_conversion(transaction_data['coinPrice'])
            amount = safe_numeric_conversion(transaction_data['amount'])
            
            if transaction_data['type'] == 'buy':
                # For buy transactions, profit = (current_price - buy_price) * amount
                profit = safe_calculation(lambda cp, p, a: (cp - p) * a, coin_price, price, amount)
                transaction_data['profit'] = profit
            elif transaction_data['type'] == 'sell':
                # For sell transactions, profit = (sell_price - current_price) * amount
                profit = safe_calculation(lambda p, cp, a: (p - cp) * a, price, coin_price, amount)
                transaction_data['profit'] = profit
            else:
                transaction_data['profit'] = 0.0
        
        return transaction_data
        
    except Exception as e:
        st.error(f"Error loading BTS transaction data: {str(e)}")
        return None

def show_bts_info():
    st.title("🎯 BTS Info")
    
    # Check if we have a selected transaction ID
    if 'selected_bts_transaction_id' not in st.session_state:
        st.info("Please select a transaction from the Bot Dashboard to view detailed analysis.")
        return
    
    # Load the specific transaction data from database
    selected_transaction_data = load_specific_bts_transaction_data(st.session_state.selected_bts_transaction_id)
    
    if selected_transaction_data is None:
        st.error("Transaction not found or error loading transaction data.")
        return
    
    # Display transaction details
    st.subheader("🔍 Detailed Analysis for Selected Transaction")
    
    # Card 1: Transaction Details and Data
    st.markdown("### 📅 Transaction Details & Data")
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📅 Transaction Details")
            colsub1, colsub2 = st.columns(2)

            with colsub1:
                # Handle timestamp formatting
                try:
                    timestamp_display = selected_transaction_data['timestamp'].strftime('%m-%d %H:%M:%S') if pd.notna(selected_transaction_data['timestamp']) else 'N/A'
                except (AttributeError, TypeError):
                    timestamp_display = 'N/A'
                st.write(f"**Date/Time:** {timestamp_display}")
                st.write(f"**Transaction ID:** {selected_transaction_data['id']}")
                st.write(f"**Type:** {selected_transaction_data['type'].upper()}")

                # Handle profit formatting with robust data handling
                profit_val = safe_numeric_conversion(selected_transaction_data['profit'])
                profit_display = format_crypto_value(profit_val)
                st.write(f"**Profit:** {profit_display}")

            with colsub2:
                # Handle amount formatting with robust data handling
                amount = safe_numeric_conversion(selected_transaction_data['amount'])
                amount_display = f"{amount:,.2f}"
                st.write(f"**Amount:** {amount_display}")
                
                # Handle price formatting with robust data handling
                price = safe_numeric_conversion(selected_transaction_data['price'])
                price_display = format_crypto_value(price, max_decimals=18)
                st.write(f"**Price:** {price_display}")

                # Handle amount in dollars formatting with robust data handling
                amount_dollars = safe_numeric_conversion(selected_transaction_data['amountInDollars'])
                amount_dollars_display = format_crypto_value(amount_dollars)
                st.write(f"**Amount in Dollars:** {amount_dollars_display}")
        
        with col2:
            # Transaction data
            st.markdown("#### 💰 Transaction Data")
            
            # Handle wallet address
            wallet_addr = selected_transaction_data.get('walletAddress', 'N/A')
            if pd.notna(wallet_addr):
                # Shorten wallet address for display
                wallet_display = f"{str(wallet_addr)[:8]}...{str(wallet_addr)[-8:]}" if len(str(wallet_addr)) > 16 else str(wallet_addr)
            else:
                wallet_display = "N/A"
            st.write(f"**Wallet Address:** {wallet_display}")
            
            # Handle token address
            token_addr = selected_transaction_data.get('tokenAddress', 'N/A')
            if pd.notna(token_addr):
                # Shorten token address for display
                token_display = f"{str(token_addr)[:8]}...{str(token_addr)[-8:]}" if len(str(token_addr)) > 16 else str(token_addr)
            else:
                token_display = "N/A"
            st.write(f"**Token Address:** {token_display}")
            
            # Handle bot ID
            bot_id = selected_transaction_data.get('botId', 'N/A')
            st.write(f"**Bot ID:** {bot_id}")
    
    # Card 2: Coin/Token Information
    st.markdown("### 🪙 Coin/Token Information")
    with st.container():
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 📊 Market Data")
            
            # Handle coin price formatting using robust data utilities
            coin_price = safe_numeric_conversion(selected_transaction_data['coinPrice'])
            coin_price_display = format_crypto_value(coin_price, max_decimals=18)
            st.write(f"**Current Coin Price:** {coin_price_display}")
            
            # Handle dev capital formatting using robust data utilities
            dev_capital = safe_numeric_conversion(selected_transaction_data['devCapital'])
            dev_capital_display = format_crypto_value(dev_capital)
            st.write(f"**Dev Capital:** {dev_capital_display}")
            
            # Handle token supply formatting using robust data utilities
            token_supply = safe_numeric_conversion(selected_transaction_data['tokenSupply'])
            token_supply_display = f"{token_supply:,.0f}"
            st.write(f"**Token Supply:** {token_supply_display}")
        
        with col2:
            st.markdown("#### 🏪 Developer Information")
            
            # Handle dev pubkey
            dev_pubkey = selected_transaction_data.get('devPubkey', 'N/A')
            if pd.notna(dev_pubkey):
                # Shorten dev pubkey for display
                dev_pubkey_display = f"{str(dev_pubkey)[:8]}...{str(dev_pubkey)[-8:]}" if len(str(dev_pubkey)) > 16 else str(dev_pubkey)
            else:
                dev_pubkey_display = "N/A"
            st.write(f"**Dev Pubkey:** {dev_pubkey_display}")
            
            # Handle dev holder percentage
            # Handle dev holder percentage formatting using robust data utilities
            dev_holder_pct = safe_numeric_conversion(selected_transaction_data['devholderPercentage'])
            dev_holder_pct_display = format_percentage(dev_holder_pct, decimals=6)
            st.write(f"**Dev Holder %:** {dev_holder_pct_display}")
            
            # Handle total holders supply using robust data utilities
            total_holders_supply = safe_numeric_conversion(selected_transaction_data['totalHoldersSupply'])
            total_holders_supply_display = f"{total_holders_supply:,.2f}"
            st.write(f"**Total Holders Supply:** {total_holders_supply_display}")
        
        with col3:
            st.markdown("#### 🔗 Additional Info")
            
            # Handle liquidity to mcap ratio
            # Handle liquidity to mcap ratio using robust data utilities
            liquidity_ratio = safe_numeric_conversion(selected_transaction_data['liquidityToMcapRatio'])
            liquidity_ratio_display = f"{liquidity_ratio:.6f}"
            st.write(f"**Liquidity/MCap Ratio:** {liquidity_ratio_display}")
            
            # Handle reserves in SOL
            # Handle reserves in SOL using robust data utilities
            reserves_sol = safe_numeric_conversion(selected_transaction_data['reservesInSOL'])
            reserves_sol_display = f"{reserves_sol:.2f} SOL"
            st.write(f"**Reserves in SOL:** {reserves_sol_display}")
            
            # Handle is bundle
            is_bundle = selected_transaction_data.get('isBundle', 'N/A')
            st.write(f"**Is Bundle:** {is_bundle}")
    
    # Card 3: Bundle Analysis (if available)
    if pd.notna(selected_transaction_data.get('confidence')):
        st.markdown("### 🎯 Bundle Analysis")
        with st.container():
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("#### 📊 Bundle Metrics")
                
                # Handle confidence
                # Handle confidence formatting using robust data utilities
                confidence = safe_numeric_conversion(selected_transaction_data['confidence'])
                confidence_display = format_percentage(confidence, decimals=0)
                st.write(f"**Confidence:** {confidence_display}")
                
                # Handle total buyers using robust data utilities
                total_buyers = safe_numeric_conversion(selected_transaction_data['totalBuyers'])
                total_buyers_display = f"{total_buyers:,.0f}"
                st.write(f"**Total Buyers:** {total_buyers_display}")
                
                # Handle suspicious buyers using robust data utilities
                suspicious_buyers = safe_numeric_conversion(selected_transaction_data['suspiciousBuyers'])
                suspicious_buyers_display = f"{suspicious_buyers:,.0f}"
                st.write(f"**Suspicious Buyers:** {suspicious_buyers_display}")
            
            with col2:
                st.markdown("#### 🔍 Bundle Indicators")
                
                # Handle time clustering
                time_clustering = selected_transaction_data.get('timeClustering', 'N/A')
                st.write(f"**Time Clustering:** {time_clustering}")
                
                # Handle similar amounts
                similar_amounts = selected_transaction_data.get('similarAmounts', 'N/A')
                st.write(f"**Similar Amounts:** {similar_amounts}")
                
                # Handle fresh wallets
                fresh_wallets = selected_transaction_data.get('freshWallets', 'N/A')
                st.write(f"**Fresh Wallets:** {fresh_wallets}")
            
            with col3:
                st.markdown("#### 🚨 Suspicious Activity")
                
                # Handle coordinated behavior
                coordinated_behavior = selected_transaction_data.get('coordinatedBehavior', 'N/A')
                st.write(f"**Coordinated Behavior:** {coordinated_behavior}")
                
                # Handle bundle status
                bundle_is_bundle = selected_transaction_data.get('bundleIsBundle', 'N/A')
                st.write(f"**Bundle Status:** {bundle_is_bundle}")
                
                # Handle reasons (if available)
                reasons = selected_transaction_data.get('reasons', 'N/A')
                if pd.notna(reasons) and reasons != 'N/A':
                    st.write(f"**Reasons:** {reasons}")
    
    # Create 70-30 split layout for transaction details and chat
    main_col, chat_col = st.columns([7, 3])
    
    with main_col:
        # Card 4: Profit Calculation Breakdown
        st.markdown("### 💰 Profit Calculation Breakdown")
        with st.container():
            if all(col in selected_transaction_data for col in ['amount', 'price', 'coinPrice']):
                # Handle transaction value calculation using robust data utilities
                amount = safe_numeric_conversion(selected_transaction_data['amount'])
                price = safe_numeric_conversion(selected_transaction_data['price'])
                coin_price = safe_numeric_conversion(selected_transaction_data['coinPrice'])
                
                transaction_value = safe_calculation(lambda a, p: a * p, amount, price)
                transaction_value_display = format_crypto_value(transaction_value, max_decimals=18)
                
                # Handle current value calculation using robust data utilities
                current_value = safe_calculation(lambda a, cp: a * cp, amount, coin_price)
                current_value_display = format_crypto_value(current_value, max_decimals=18)
                
                # Handle profit calculation using robust data utilities
                profit_val = safe_numeric_conversion(selected_transaction_data['profit'])
                profit_display = format_crypto_value(profit_val, max_decimals=18)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Transaction Value", transaction_value_display)
                with col2:
                    st.metric("Current Value", current_value_display)
                with col3:
                    st.metric("Profit/Loss", profit_display)
        
        # Card 5: Complete Transaction Data
        st.markdown("### 📋 Complete Transaction Data")
        formatted_transaction = selected_transaction_data.copy()
        
        # Format numeric columns based on requirements - handle mixed data types
        for col in formatted_transaction.index:
            # Get the scalar value from the Series - ensure it's a scalar
            col_value = formatted_transaction[col]
            
            # Convert pandas Series to scalar if needed - more robust approach
            if hasattr(col_value, 'iloc') and hasattr(col_value, '__len__'):
                try:
                    col_value = col_value.iloc[0] if len(col_value) > 0 else None
                except (IndexError, AttributeError):
                    col_value = None
            elif hasattr(col_value, 'item'):
                # Handle numpy scalars
                try:
                    col_value = col_value.item()
                except (ValueError, AttributeError):
                    pass
            
            # Safe null check function
            def is_valid_value(val):
                try:
                    if val is None:
                        return False
                    if hasattr(val, '__len__') and len(val) == 0:
                        return False
                    if pd.isna(val):
                        return False
                    return True
                except:
                    return False
            
            if col in ['profit', 'amountInDollars']:
                # Use robust data utilities for formatting
                numeric_val = safe_numeric_conversion(col_value)
                formatted_transaction[col] = format_crypto_value(numeric_val)
            elif col in ['price', 'coinPrice']:
                # Use robust data utilities for formatting with high precision
                numeric_val = safe_numeric_conversion(col_value)
                formatted_transaction[col] = format_crypto_value(numeric_val, max_decimals=18)
            elif col == 'timestamp':
                # Format date/time
                try:
                    if is_valid_value(col_value):
                        formatted_transaction[col] = col_value.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        formatted_transaction[col] = "N/A"
                except (AttributeError, TypeError):
                    formatted_transaction[col] = "N/A"
            elif isinstance(col_value, (int, float)) and is_valid_value(col_value):
                # Handle other numeric fields
                try:
                    formatted_transaction[col] = f"{col_value:,.0f}"
                except (ValueError, TypeError):
                    formatted_transaction[col] = "N/A"
            else:
                # Handle string/object fields
                if is_valid_value(col_value):
                    formatted_transaction[col] = str(col_value)
                else:
                    formatted_transaction[col] = "N/A"
        
        # Convert Series to DataFrame for display
        formatted_df = pd.DataFrame([formatted_transaction])
        st.dataframe(formatted_df, width='stretch')
        
        # Add back button
        if st.button("← Back to Bot Dashboard"):
            st.session_state.navigate_to_bot_dashboard = True
            st.rerun()
    
    # Chat column on the right
    with chat_col:
        # Card 6: AI Assistant
        st.markdown("### 🤖 AI Assistant")
        with st.container():
            # Initialize chat history with unique key for this transaction
            chat_key = f"messages_{selected_transaction_data.get('id', 'unknown')}"
            if chat_key not in st.session_state:
                st.session_state[chat_key] = []
            
            # Display chat messages from history on app rerun
            for message in st.session_state[chat_key]:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
            
            # Accept user input
            if prompt := st.chat_input("Ask about this transaction or token..."):
                # Add user message to chat history
                st.session_state[chat_key].append({"role": "user", "content": prompt})
                
                # Display user message in chat message container
                with st.chat_message("user"):
                    st.markdown(prompt)
                
                # Display assistant response in chat message container
                with st.chat_message("assistant"):
                    # Give AI all the raw data
                    context = f"""COMPLETE TRANSACTION DATA (RAW): {selected_transaction_data.to_dict()} TRANSACTION SUMMARY: ( Date/Time: {selected_transaction_data['timestamp']} Transaction ID: {selected_transaction_data['id']} Type: {selected_transaction_data['type']} Amount: {selected_transaction_data['amount']} Price: {selected_transaction_data['price']} Wallet Address: {selected_transaction_data['walletAddress']} Token Address: {selected_transaction_data['tokenAddress']} Profit: {selected_transaction_data['profit']} Current Coin Price: {selected_transaction_data['coinPrice']} Dev Capital: {selected_transaction_data['devCapital']} Bundle Confidence: {selected_transaction_data.get('confidence', 'N/A')})"""
                    
                    # Create prompt for Gemini
                    full_prompt = f"""You are a cryptocurrency trading analyst specializing in sniper bot transactions and bundle detection. Analyze this transaction data and answer the user's question. {context} USER QUESTION: {prompt} Provide a direct, helpful answer based on the data."""
                    
                    # Generate response using Gemini (following official docs)
                    try:
                        response = client.models.generate_content(
                            model=GEMINI_CONFIG["model"],
                            contents=full_prompt
                        )
                        response_text = response.text
                        st.markdown(response_text)
                        st.session_state[chat_key].append({"role": "assistant", "content": response_text})
                    except Exception as e:
                        error_message = f"Sorry, I encountered an error while generating the response: {str(e)}"
                        st.error(error_message)
                        st.session_state[chat_key].append({"role": "assistant", "content": error_message})
            
            # Add clear chat button
            if st.button("🗑️ Clear Chat", key=f"clear_chat_{selected_transaction_data.get('id', 'unknown')}"):
                st.session_state[chat_key] = []
                st.rerun()

# Run the BTS info page
show_bts_info()
