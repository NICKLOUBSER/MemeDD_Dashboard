import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
from google import genai
from config import DB_CONFIG, CHART_CONFIG, GEMINI_CONFIG

# Set the environment variable for Google AI API key
os.environ["GEMINI_API_KEY"] = GEMINI_CONFIG["api_key"]

# Initialize Gemini client (API key is automatically picked up from environment)
client = genai.Client()

def show_arb_info():
    st.title("📊 Arb Info")
    
    # Check if we have a selected trade
    if 'selected_trade_data' not in st.session_state:
        st.info("Please select a trade from the Bot Dashboard to view detailed analysis.")
        return
    
    selected_trade_data = st.session_state.selected_trade_data
    
    # Display trade details
    st.subheader("🔍 Detailed Analysis for Selected Trade")
    
    # Trade details section
    st.subheader("📅 Trade Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Handle date formatting
        try:
            date_display = selected_trade_data['dateTraded'].strftime('%%m-%d %H:%M:%S') if pd.notna(selected_trade_data['dateTraded']) else 'N/A'
        except (AttributeError, TypeError):
            date_display = 'N/A'
        st.write(f"**Date/Time:** {date_display}")
        st.write(f"**Trade ID:** {selected_trade_data['id']}")
        
        # Handle profit formatting
        try:
            profit_val = pd.to_numeric(selected_trade_data['idealProfit'], errors='coerce')
            if pd.notna(profit_val):
                profit_display = f"${profit_val:,.0f}"
            else:
                profit_display = "N/A"
        except (ValueError, TypeError):
            profit_display = "N/A"
        st.write(f"**Profit:** {profit_display}")
    
    with col2:
        # Handle buy volume formatting
        try:
            buy_vol = pd.to_numeric(selected_trade_data['buyVolume'], errors='coerce')
            if pd.notna(buy_vol):
                buy_vol_display = f"{buy_vol:,.0f}"
            else:
                buy_vol_display = "N/A"
        except (ValueError, TypeError):
            buy_vol_display = "N/A"
        st.write(f"**Buy Volume:** {buy_vol_display}")
        
        # Handle buy VWAP formatting
        try:
            buy_vwap = pd.to_numeric(selected_trade_data['buyVwap'], errors='coerce')
            if pd.notna(buy_vwap):
                buy_vwap_display = f"${buy_vwap:,.4f}"
            else:
                buy_vwap_display = "N/A"
        except (ValueError, TypeError):
            buy_vwap_display = "N/A"
        st.write(f"**Buy VWAP:** {buy_vwap_display}")
        
        # Handle sell volume formatting
        try:
            sell_vol = pd.to_numeric(selected_trade_data['sellVolume'], errors='coerce')
            if pd.notna(sell_vol):
                sell_vol_display = f"{sell_vol:,.0f}"
            else:
                sell_vol_display = "N/A"
        except (ValueError, TypeError):
            sell_vol_display = "N/A"
        st.write(f"**Sell Volume:** {sell_vol_display}")
        
        # Handle sell VWAP formatting
        try:
            sell_vwap = pd.to_numeric(selected_trade_data['sellVwap'], errors='coerce')
            if pd.notna(sell_vwap):
                sell_vwap_display = f"${sell_vwap:,.4f}"
            else:
                sell_vwap_display = "N/A"
        except (ValueError, TypeError):
            sell_vwap_display = "N/A"
        st.write(f"**Sell VWAP:** {sell_vwap_display}")
    
    # Transaction data
    st.subheader("💰 Transaction Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Handle buy total calculation
        try:
            buy_vol = pd.to_numeric(selected_trade_data['buyVolume'], errors='coerce')
            buy_vwap = pd.to_numeric(selected_trade_data['buyVwap'], errors='coerce')
            if pd.notna(buy_vol) and pd.notna(buy_vwap):
                buy_total = buy_vol * buy_vwap
                buy_total_display = f"${buy_total:,.0f}"
            else:
                buy_total_display = "N/A"
        except (ValueError, TypeError):
            buy_total_display = "N/A"
        st.metric("Buy Total", buy_total_display)
    
    with col2:
        # Handle sell total calculation
        try:
            sell_vol = pd.to_numeric(selected_trade_data['sellVolume'], errors='coerce')
            sell_vwap = pd.to_numeric(selected_trade_data['sellVwap'], errors='coerce')
            if pd.notna(sell_vol) and pd.notna(sell_vwap):
                sell_total = sell_vol * sell_vwap
                sell_total_display = f"${sell_total:,.0f}"
            else:
                sell_total_display = "N/A"
        except (ValueError, TypeError):
            sell_total_display = "N/A"
        st.metric("Sell Total", sell_total_display)
    
    # Exchange information
    st.subheader("🏪 Exchange Information")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write(f"**Buy Exchange:** {selected_trade_data['buyExchange']}")
    
    with col2:
        st.write(f"**Sell Exchange:** {selected_trade_data['sellExchange']}")
    
    with col3:
        st.write(f"**Exchange Route:** {selected_trade_data['buyExchange']} → {selected_trade_data['sellExchange']}")
    
    # Coin/Token information
    st.subheader("🪙 Coin/Token Information")
    
    # Look for token/coin related columns
    token_cols = [col for col in selected_trade_data.index if any(keyword in col.lower() for keyword in ['token', 'coin', 'symbol', 'pair', 'market'])]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 🪙 Token Details")
        
        # Try to extract coin symbol from trade data
        coin_symbol = None
        
        # First priority: Check for 'sell_base' column (contains coin symbol)
        if 'sell_base' in selected_trade_data.index and pd.notna(selected_trade_data['sell_base']):
            coin_symbol = str(selected_trade_data['sell_base']).upper()
        # Second priority: Check for 'sellBase' column (alternative naming)
        elif 'sellBase' in selected_trade_data.index and pd.notna(selected_trade_data['sellBase']):
            coin_symbol = str(selected_trade_data['sellBase']).upper()
        # Third priority: Check if we already found a symbol from token_cols
        elif token_cols:
            for col in token_cols:
                if 'symbol' in col.lower() and pd.notna(selected_trade_data[col]):
                    coin_symbol = selected_trade_data[col]
                    break
        
        # If no symbol found, look for common coin symbol patterns in the data
        if not coin_symbol:
            for col in selected_trade_data.index:
                if pd.notna(selected_trade_data[col]):
                    value = str(selected_trade_data[col]).upper()
                    # Check if it looks like a coin symbol (2-5 letters, common crypto symbols)
                    if len(value) >= 2 and len(value) <= 5 and value.isalpha():
                        # Skip invalid symbols
                        invalid_symbols = ['TBC', 'N/A', 'UNKNOWN', 'NULL', 'NONE', 'TBD', 'TO BE CONFIRMED', 'PENDING']
                        if value not in invalid_symbols:
                            # Check if it's a common cryptocurrency symbol
                            common_symbols = ['BTC', 'ETH', 'DOGE', 'ADA', 'DOT', 'LINK', 'LTC', 'BCH', 'XRP', 'SOL', 'MATIC', 'AVAX', 'UNI', 'ATOM', 'FTT', 'NEAR', 'ALGO', 'VET', 'ICP', 'FIL', 'TRX', 'ETC', 'XLM', 'THETA', 'XMR', 'EOS', 'AAVE', 'CAKE', 'MKR', 'COMP', 'SNX', 'YFI', 'CRV', 'SUSHI', '1INCH', 'BAL', 'REN', 'ZRX', 'BAND', 'OCEAN', 'ALPHA', 'ANKR', 'BAT', 'CHZ', 'DASH', 'ENJ', 'GRT', 'HOT', 'ICX', 'KSM', 'MANA', 'NANO', 'OMG', 'QTUM', 'RVN', 'SC', 'SKL', 'STORJ', 'SXP', 'TFUEL', 'VTHO', 'WAVES', 'XTZ', 'ZEC', 'ZIL', 'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'FRAX', 'GUSD', 'HUSD', 'PAX', 'SUSD', 'USDN', 'USDK', 'USDJ', 'USDQ', 'USDS', 'USDX', 'USDY', 'USDZ']
                            if value in common_symbols:
                                coin_symbol = value
                                break
        
        if coin_symbol:
            st.success(f"🎯 Detected: {coin_symbol}")
            st.info("Token symbol detected from trade data")
        else:
            st.info("No valid coin symbol detected in trade data. The symbol may be 'TBC' (To Be Confirmed) or another placeholder value.")
    
    with col2:
        st.markdown("### 📊 Market Data")
        # Look for market-related columns
        market_cols = [col for col in selected_trade_data.index if any(keyword in col.lower() for keyword in ['price', 'market', 'cap', 'volume', 'liquidity'])]
        
        if market_cols:
            for col in market_cols[:3]:  # Show first 3 market columns
                if pd.notna(selected_trade_data[col]):
                    try:
                        # Try to convert to numeric for formatting
                        numeric_val = pd.to_numeric(selected_trade_data[col], errors='coerce')
                        if pd.notna(numeric_val):
                            st.write(f"**{col.replace('_', ' ').title()}:** {numeric_val:,.0f}")
                        else:
                            st.write(f"**{col.replace('_', ' ').title()}:** {selected_trade_data[col]}")
                    except (ValueError, TypeError):
                        st.write(f"**{col.replace('_', ' ').title()}:** {selected_trade_data[col]}")
        else:
            st.write("**Market data not available**")
    
    with col3:
        st.markdown("### 🔗 Additional Info")
        # Show other relevant columns that haven't been displayed yet
        displayed_cols = ['dateTraded', 'id', 'idealProfit', 'buyVolume', 'buyVwap', 'sellVolume', 'sellVwap'] + ['buyExchange', 'sellExchange'] + token_cols + market_cols
        other_cols = [col for col in selected_trade_data.index if col not in displayed_cols and pd.notna(selected_trade_data[col])]
        
        if other_cols:
            for col in other_cols[:3]:  # Show first 3 other columns
                if pd.notna(selected_trade_data[col]):
                    try:
                        # Try to convert to numeric for formatting
                        numeric_val = pd.to_numeric(selected_trade_data[col], errors='coerce')
                        if pd.notna(numeric_val):
                            st.write(f"**{col.replace('_', ' ').title()}:** {numeric_val:,.0f}")
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
        # Show calculation breakdown
        st.subheader("💰 Profit Calculation Breakdown")
        
        if all(col in selected_trade_data for col in ['buyVolume', 'buyVwap', 'sellVolume', 'sellVwap']):
            # Handle buy total calculation
            try:
                buy_vol = pd.to_numeric(selected_trade_data['buyVolume'], errors='coerce')
                buy_vwap = pd.to_numeric(selected_trade_data['buyVwap'], errors='coerce')
                if pd.notna(buy_vol) and pd.notna(buy_vwap):
                    buy_total = buy_vol * buy_vwap
                    buy_total_display = f"${buy_total:,.0f}"
                else:
                    buy_total_display = "N/A"
            except (ValueError, TypeError):
                buy_total_display = "N/A"
            
            # Handle sell total calculation
            try:
                sell_vol = pd.to_numeric(selected_trade_data['sellVolume'], errors='coerce')
                sell_vwap = pd.to_numeric(selected_trade_data['sellVwap'], errors='coerce')
                if pd.notna(sell_vol) and pd.notna(sell_vwap):
                    sell_total = sell_vol * sell_vwap
                    sell_total_display = f"${sell_total:,.0f}"
                else:
                    sell_total_display = "N/A"
            except (ValueError, TypeError):
                sell_total_display = "N/A"
            
            # Handle profit calculation
            try:
                profit_val = pd.to_numeric(selected_trade_data['idealProfit'], errors='coerce')
                if pd.notna(profit_val):
                    profit_display = f"${profit_val:,.0f}"
                else:
                    profit_display = "N/A"
            except (ValueError, TypeError):
                profit_display = "N/A"
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Buy Total", buy_total_display)
            with col2:
                st.metric("Sell Total", sell_total_display)
            with col3:
                st.metric("Profit", profit_display)
        
        # Display the full trade data
        st.subheader("📋 Complete Trade Data")
        formatted_trade = selected_trade_data.copy()
        
        # Format numeric columns based on requirements - handle mixed data types
        for col in formatted_trade.index:
            if col in ['idealProfit', 'buyVolume', 'sellVolume', 'buyTotal', 'sellTotal']:
                # Convert to numeric first, then format
                try:
                    numeric_val = pd.to_numeric(formatted_trade[col], errors='coerce')
                    if pd.notna(numeric_val):
                        formatted_trade[col] = f"{numeric_val:,.0f}"
                    else:
                        formatted_trade[col] = "N/A"
                except (ValueError, TypeError):
                    formatted_trade[col] = "N/A"
            elif col in ['buyVwap', 'sellVwap']:
                # Convert to numeric first, then format with 4 decimals
                try:
                    numeric_val = pd.to_numeric(formatted_trade[col], errors='coerce')
                    if pd.notna(numeric_val):
                        formatted_trade[col] = f"{numeric_val:,.4f}"
                    else:
                        formatted_trade[col] = "N/A"
                except (ValueError, TypeError):
                    formatted_trade[col] = "N/A"
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
        
        st.dataframe(formatted_trade, width='stretch')
        
        # Add back button
        if st.button("← Back to Bot Dashboard"):
            st.session_state.navigate_to_bot_dashboard = True
            st.rerun()
    
    # Chat column on the right
    with chat_col:
        st.markdown("### 🤖 AI Assistant")
        
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
                context = f"""COMPLETE TRADE DATA (RAW): {selected_trade_data.to_string()} TRADE SUMMARY: ( Date/Time: {selected_trade_data['dateTraded']} Trade ID: {selected_trade_data['id']} Buy Exchange: {selected_trade_data['buyExchange']} Sell Exchange: {selected_trade_data['sellExchange']} Buy Volume: {selected_trade_data['buyVolume']} Buy VWAP: {selected_trade_data['buyVwap']} Sell Volume: {selected_trade_data['sellVolume']} Sell VWAP: {selected_trade_data['sellVwap']} Profit: {selected_trade_data['idealProfit']})"""
                
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
