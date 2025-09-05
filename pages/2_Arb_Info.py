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
def load_specific_trade_data(trade_id):
    """
    Load specific trade data from PostgreSQL database by trade ID.
    """
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        # Load specific trade data using parameterized query to prevent SQL injection
        query = "SELECT * FROM arbtransaction WHERE id = %s"
        
        # Convert numpy.int64 to regular Python int for PostgreSQL compatibility
        trade_id_int = int(trade_id)
        
        data = pd.read_sql(query, engine, params=(trade_id_int,))
        
        if data.empty:
            return None
        
        # Get the single row
        trade_data = data.iloc[0]
        
        # Store original idealProfit from database
        original_idealProfit = trade_data.get('idealProfit', 0)
        
        # Calculate idealProfit using the formula: (sellVolume*sellVwap) - (buyVolume*buyVwap)
        if all(col in trade_data.index for col in ['sellVolume', 'sellVwap', 'buyVolume', 'buyVwap']):
            # Convert columns to numeric using robust data utilities
            sell_vol = safe_numeric_conversion(trade_data['sellVolume'])
            sell_vwap = safe_numeric_conversion(trade_data['sellVwap'])
            buy_vol = safe_numeric_conversion(trade_data['buyVolume'])
            buy_vwap = safe_numeric_conversion(trade_data['buyVwap'])
            
            # Calculate raw profit using safe calculation
            raw_profit = safe_calculation(
                lambda sv, svwap, bv, bvwap: (sv * svwap) - (bv * bvwap),
                sell_vol, sell_vwap, buy_vol, buy_vwap
            )
            
            # Calculate total investment (buyVolume * buyVwap)
            total_investment = safe_calculation(
                lambda bv, bvwap: bv * bvwap,
                buy_vol, buy_vwap
            )
            
            # Calculate profit as raw value (not percentage)
            trade_data['idealProfit'] = raw_profit if total_investment != 0 else 0
            trade_data['original_idealProfit'] = safe_numeric_conversion(original_idealProfit)
        else:
            trade_data['idealProfit'] = 0
            trade_data['original_idealProfit'] = original_idealProfit
        
        # Convert dateTraded to datetime
        if 'dateTraded' in trade_data.index:
            trade_data['dateTraded'] = pd.to_datetime(trade_data['dateTraded'])
        
        return trade_data
        
    except Exception as e:
        st.error(f"Error loading trade data: {str(e)}")
        return None

def show_arb_info():
    st.title("📊 Arb Info")
    
    # Check if we have a selected trade ID
    if 'selected_trade_id' not in st.session_state:
        st.info("Please select a trade from the Bot Dashboard to view detailed analysis.")
        return
    
    # Load the specific trade data from database
    selected_trade_data = load_specific_trade_data(st.session_state.selected_trade_id)
    
    if selected_trade_data is None:
        st.error("Trade not found or error loading trade data.")
        return
    
    # Display trade details
    st.subheader("🔍 Detailed Analysis for Selected Trade")
    
    # Card 1: Trade Details and Transaction Data
    st.markdown("### 📅 Trade Details & Transaction Data")
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📅 Trade Details")
            colsub1, colsub2 = st.columns(2)

            with colsub1:
                # Handle date formatting
                try:
                    date_display = selected_trade_data['dateTraded'].strftime('%m-%d %H:%M:%S') if pd.notna(selected_trade_data['dateTraded']) else 'N/A'
                except (AttributeError, TypeError):
                    date_display = 'N/A'
                st.write(f"**Date/Time:** {date_display}")
                st.write(f"**Trade ID:** {selected_trade_data['id']}")

                # Handle profit formatting using robust data utilities
                profit_val = safe_numeric_conversion(selected_trade_data['idealProfit'])
                profit_display = format_crypto_value(profit_val)
                st.write(f"**Profit:** {profit_display}")

            with colsub2:

                # Handle buy volume formatting using robust data utilities
                buy_vol = safe_numeric_conversion(selected_trade_data['buyVolume'])
                buy_vol_display = f"{buy_vol:,.2f}"
                st.write(f"**Buy Volume:** {buy_vol_display}")
                
                # Handle buy VWAP formatting using robust data utilities
                buy_vwap = safe_numeric_conversion(selected_trade_data['buyVwap'])
                buy_vwap_display = format_crypto_value(buy_vwap, max_decimals=18)
                st.write(f"**Buy VWAP:** {buy_vwap_display}")

                # Handle sell volume formatting using robust data utilities
                sell_vol = safe_numeric_conversion(selected_trade_data['sellVolume'])
                sell_vol_display = f"{sell_vol:,.2f}"
                st.write(f"**Sell Volume:** {sell_vol_display}")

                # Handle sell VWAP formatting using robust data utilities
                sell_vwap = safe_numeric_conversion(selected_trade_data['sellVwap'])
                sell_vwap_display = format_crypto_value(sell_vwap, max_decimals=18)
                st.write(f"**Sell VWAP:** {sell_vwap_display}")
        
        with col2:
            # Transaction data
            st.markdown("#### 💰 Transaction Data")
            # Handle buy total calculation using robust data utilities
            buy_vol = safe_numeric_conversion(selected_trade_data['buyVolume'])
            buy_vwap = safe_numeric_conversion(selected_trade_data['buyVwap'])
            buy_total = safe_calculation(lambda bv, bvwap: bv * bvwap, buy_vol, buy_vwap)
            buy_total_display = format_crypto_value(buy_total, max_decimals=18)
            st.metric("Buy Total", buy_total_display)
        
            # Handle sell total calculation using robust data utilities
            sell_vol = safe_numeric_conversion(selected_trade_data['sellVolume'])
            sell_vwap = safe_numeric_conversion(selected_trade_data['sellVwap'])
            sell_total = safe_calculation(lambda sv, svwap: sv * svwap, sell_vol, sell_vwap)
            sell_total_display = format_crypto_value(sell_total, max_decimals=18)
            st.metric("Sell Total", sell_total_display)
    
    # Card 2: Coin/Token Information
    st.markdown("### 🪙 Coin/Token Information")
    with st.container():
        # Look for token/coin related columns
        token_cols = [col for col in selected_trade_data.index if any(keyword in col.lower() for keyword in ['token', 'coin', 'symbol', 'pair', 'market'])]
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
             # Exchange information
            st.markdown("#### 🏪 Exchange Information")
        
            st.write(f"**Buy Exchange:** {selected_trade_data['buyExchange']}")

            st.write(f"**Sell Exchange:** {selected_trade_data['sellExchange']}")
        
            st.write(f"**Exchange Route:** {selected_trade_data['buyExchange']} → {selected_trade_data['sellExchange']}")
        
        with col2:
            st.markdown("#### 📊 Market Data")
            # Look for market-related columns
            market_cols = [col for col in selected_trade_data.index if any(keyword in col.lower() for keyword in ['price', 'market', 'cap', 'volume', 'liquidity'])]
            
            if market_cols:
                for col in market_cols[:3]:  # Show first 3 market columns
                    if pd.notna(selected_trade_data[col]):
                        try:
                            # Try to convert to numeric for formatting
                            numeric_val = safe_numeric_conversion(selected_trade_data[col])
                            if pd.notna(numeric_val):
                                st.write(f"**{col.replace('_', ' ').title()}:** {numeric_val}")
                            else:
                                st.write(f"**{col.replace('_', ' ').title()}:** {selected_trade_data[col]}")
                        except (ValueError, TypeError):
                            st.write(f"**{col.replace('_', ' ').title()}:** {selected_trade_data[col]}")
            else:
                st.write("**Market data not available**")
        
        with col3:
            st.markdown("#### 🔗 Additional Info")
            # Show other relevant columns that haven't been displayed yet
            displayed_cols = ['dateTraded', 'id', 'idealProfit', 'buyVolume', 'buyVwap', 'sellVolume', 'sellVwap'] + ['buyExchange', 'sellExchange'] + token_cols + market_cols
            other_cols = [col for col in selected_trade_data.index if col not in displayed_cols and pd.notna(selected_trade_data[col])]
            
            if other_cols:
                for col in other_cols[:3]:  # Show first 3 other columns
                    if pd.notna(selected_trade_data[col]):
                        try:
                            # Try to convert to numeric for formatting
                            numeric_val = safe_numeric_conversion(selected_trade_data[col])
                            if pd.notna(numeric_val):
                                st.write(f"**{col.replace('_', ' ').title()}:** {numeric_val}")
                            else:
                                st.write(f"**{col.replace('_', ' ').title()}:** {selected_trade_data[col]}")
                        except (ValueError, TypeError):
                            st.write(f"**{col.replace('_', ' ').title()}:** {selected_trade_data[col]}")
            else:
                st.write("**No additional data available**")
    
    # Extract coin information for LLM chat
    coin_info = {}
    if token_cols:
        for col in token_cols:
            if pd.notna(selected_trade_data[col]):
                coin_info[col] = selected_trade_data[col]
    
    # Create 70-30 split layout for trade details and chat
    main_col, chat_col = st.columns([7, 3])
    
    with main_col:
        # Card 3: Profit Calculation Breakdown
        st.markdown("### 💰 Profit Calculation Breakdown")
        with st.container():
            if all(col in selected_trade_data for col in ['buyVolume', 'buyVwap', 'sellVolume', 'sellVwap']):
                # Handle buy total calculation using robust data utilities
                buy_vol = safe_numeric_conversion(selected_trade_data['buyVolume'])
                buy_vwap = safe_numeric_conversion(selected_trade_data['buyVwap'])
                buy_total = safe_calculation(lambda bv, bvwap: bv * bvwap, buy_vol, buy_vwap)
                buy_total_display = format_crypto_value(buy_total, max_decimals=18)
                
                # Handle sell total calculation using robust data utilities
                sell_vol = safe_numeric_conversion(selected_trade_data['sellVolume'])
                sell_vwap = safe_numeric_conversion(selected_trade_data['sellVwap'])
                sell_total = safe_calculation(lambda sv, svwap: sv * svwap, sell_vol, sell_vwap)
                sell_total_display = format_crypto_value(sell_total, max_decimals=18)
                
                # Handle profit calculation using robust data utilities
                profit_val = safe_numeric_conversion(selected_trade_data['idealProfit'])
                profit_display = format_crypto_value(profit_val, max_decimals=18)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Buy Total", buy_total_display)
                with col2:
                    st.metric("Sell Total", sell_total_display)
                with col3:
                    st.metric("Profit", profit_display)
        
        # Card 4: Complete Trade Data
        st.markdown("### 📋 Complete Trade Data")
        formatted_trade = selected_trade_data.copy()
        
        # Format numeric columns based on requirements using robust data utilities
        for col in formatted_trade.index:
            # Get the scalar value from the Series - ensure it's a scalar
            col_value = formatted_trade[col]
            
            # Convert pandas Series to scalar if needed
            if hasattr(col_value, 'iloc') and hasattr(col_value, '__len__'):
                try:
                    col_value = col_value.iloc[0] if len(col_value) > 0 else None
                except (IndexError, AttributeError):
                    col_value = None
            
            if col in ['idealProfit', 'buyVolume', 'sellVolume', 'buyTotal', 'sellTotal']:
                # Use robust data utilities for formatting
                numeric_val = safe_numeric_conversion(col_value)
                formatted_trade[col] = format_crypto_value(numeric_val, max_decimals=18)
            elif col in ['buyVwap', 'sellVwap']:
                # Use robust data utilities for VWAP formatting
                numeric_val = safe_numeric_conversion(col_value)
                formatted_trade[col] = format_crypto_value(numeric_val, max_decimals=18)
            elif col == 'dateTraded':
                # Format date/time
                try:
                    if pd.notna(formatted_trade[col]):
                        formatted_trade[col] = formatted_trade[col].strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        formatted_trade[col] = "N/A"
                except (AttributeError, TypeError):
                    formatted_trade[col] = "N/A"
            elif isinstance(formatted_trade[col], (int, float)):
                # Handle other numeric fields
                try:
                    if pd.notna(formatted_trade[col]):
                        formatted_trade[col] = f"{formatted_trade[col]:,.0f}"
                    else:
                        formatted_trade[col] = "N/A"
                except (ValueError, TypeError):
                    formatted_trade[col] = "N/A"
            else:
                # Handle string/object fields
                if pd.notna(formatted_trade[col]):
                    formatted_trade[col] = str(formatted_trade[col])
                else:
                    formatted_trade[col] = "N/A"
        
        # Convert Series to DataFrame for display
        formatted_df = pd.DataFrame([formatted_trade])
        st.dataframe(formatted_df, width='stretch')
        
        # Add back button
        if st.button("← Back to Bot Dashboard"):
            st.session_state.navigate_to_bot_dashboard = True
            st.rerun()
    
    # Chat column on the right
    with chat_col:
        # Card 5: AI Assistant
        st.markdown("### 🤖 AI Assistant")
        with st.container():
            # Initialize chat history with unique key for this trade
            chat_key = f"messages_{selected_trade_data.get('id', 'unknown')}"
            if chat_key not in st.session_state:
                st.session_state[chat_key] = []
            
            # Display chat messages from history on app rerun
            for message in st.session_state[chat_key]:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
            
            # Accept user input
            if prompt := st.chat_input("Ask about this trade or coin..."):
                # Add user message to chat history
                st.session_state[chat_key].append({"role": "user", "content": prompt})
                
                # Display user message in chat message container
                with st.chat_message("user"):
                    st.markdown(prompt)
                
                # Display assistant response in chat message container
                with st.chat_message("assistant"):
                    # Give AI all the raw data
                    context = f"""COMPLETE TRADE DATA (RAW): {selected_trade_data.to_dict()} TRADE SUMMARY: ( Date/Time: {selected_trade_data['dateTraded']} Trade ID: {selected_trade_data['id']} Buy Exchange: {selected_trade_data['buyExchange']} Sell Exchange: {selected_trade_data['sellExchange']} Buy Volume: {selected_trade_data['buyVolume']} Buy VWAP: {selected_trade_data['buyVwap']} Sell Volume: {selected_trade_data['sellVolume']} Sell VWAP: {selected_trade_data['sellVwap']} Profit: {selected_trade_data['idealProfit']})"""
                    
                    # Create prompt for Gemini
                    full_prompt = f"""You are a cryptocurrency trading analyst. Analyze this trade data and answer the user's question. {context} USER QUESTION: {prompt} Provide a direct, helpful answer based on the data."""
                    
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
            if st.button("🗑️ Clear Chat", key=f"clear_chat_{selected_trade_data.get('id', 'unknown')}"):
                st.session_state[chat_key] = []
                st.rerun()

# Run the arb info page
show_arb_info()
