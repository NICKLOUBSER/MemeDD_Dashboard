import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from config import DB_CONFIG, CHART_CONFIG
from data_utils import (
    safe_numeric_conversion, 
    safe_decimal_conversion, 
    format_crypto_value, 
    format_percentage,
    safe_calculation,
    validate_dataframe_columns,
    handle_outliers_iqr,
    create_safe_metrics
)

# Set page config
st.set_page_config(
    page_title="MemeDD Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_arb_transaction_data():
    """
    Load arbitrage transaction data from PostgreSQL database.
    """
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        # Load data
        query = "SELECT * FROM arbtransaction"
        
        data = pd.read_sql(query, engine)
        
        # Store original idealProfit from database
        original_idealProfit = data['idealProfit'].copy() if 'idealProfit' in data.columns else pd.Series([0] * len(data))
        
        # Calculate idealProfit using the formula: (sellVolume*sellVwap) - (buyVolume*buyVwap)
        if all(col in data.columns for col in ['sellVolume', 'sellVwap', 'buyVolume', 'buyVwap']):
            # Convert columns to numeric using robust data utilities
            data['sellVolume'] = data['sellVolume'].apply(lambda x: safe_numeric_conversion(x))
            data['sellVwap'] = data['sellVwap'].apply(lambda x: safe_numeric_conversion(x))
            data['buyVolume'] = data['buyVolume'].apply(lambda x: safe_numeric_conversion(x))
            data['buyVwap'] = data['buyVwap'].apply(lambda x: safe_numeric_conversion(x))
            
            # Calculate raw profit using safe calculations
            data['raw_profit'] = data.apply(lambda row: safe_calculation(
                lambda sv, svwap, bv, bvwap: (sv * svwap) - (bv * bvwap),
                row['sellVolume'], row['sellVwap'], row['buyVolume'], row['buyVwap']
            ), axis=1)
            
            # Calculate total investment (buyVolume * buyVwap)
            data['total_investment'] = data.apply(lambda row: safe_calculation(
                lambda bv, bvwap: bv * bvwap,
                row['buyVolume'], row['buyVwap']
            ), axis=1)
            
            # Convert to percentage (profit as percentage of investment)
            data['idealProfit'] = data.apply(lambda row: safe_calculation(
                lambda rp, ti: (rp / ti * 100) if ti != 0 else 0,
                row['raw_profit'], row['total_investment']
            ), axis=1)
            
            # Store original idealProfit using robust conversion
            data['original_idealProfit'] = original_idealProfit.apply(lambda x: safe_numeric_conversion(x))
        else:
            st.warning("Missing required columns for ideal profit calculation: sellVolume, sellVwap, buyVolume, buyVwap")
            data['idealProfit'] = 0
            data['original_idealProfit'] = original_idealProfit
        
        # Convert dateTraded to datetime
        if 'dateTraded' in data.columns:
            data['dateTraded'] = pd.to_datetime(data['dateTraded'])
        
        return data
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def load_bts_transaction_data():
    """
    Load BTS transaction data from PostgreSQL database with joins to related tables.
    """
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        # Load data with joins to get complete information
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
        ORDER BY bt.timestamp DESC
        """
        
        data = pd.read_sql(query, engine)
        
        # Convert timestamp to datetime
        if 'timestamp' in data.columns:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        # Calculate profit/loss based on type and current price using robust data handling
        if 'type' in data.columns and 'price' in data.columns and 'coinPrice' in data.columns:
            data['profit'] = 0.0
            for idx, row in data.iterrows():
                # Convert values safely
                price = safe_numeric_conversion(row['price'])
                coin_price = safe_numeric_conversion(row['coinPrice'])
                amount = safe_numeric_conversion(row['amount'])
                
                if row['type'] == 'buy':
                    # For buy transactions, profit = (current_price - buy_price) * amount
                    profit = safe_calculation(lambda cp, p, a: (cp - p) * a, coin_price, price, amount)
                    data.at[idx, 'profit'] = profit
                elif row['type'] == 'sell':
                    # For sell transactions, profit = (sell_price - current_price) * amount
                    profit = safe_calculation(lambda p, cp, a: (p - cp) * a, price, coin_price, amount)
                    data.at[idx, 'profit'] = profit
        
        return data
        
    except Exception as e:
        st.error(f"Error loading BTS transaction data: {str(e)}")
        return pd.DataFrame()

def generate_month_options():
    """
    Generate month options for filtering arbitrage data.
    """
    data = load_arb_transaction_data()
    if data.empty:
        return ['All Data'], ['All']
    
    # Get unique months from the data
    data['year_month'] = data['dateTraded'].dt.strftime('%Y-%m')
    unique_months = sorted(data['year_month'].unique(), reverse=True)
    
    # Create display options
    month_options = ['All Data'] + [f"{datetime.strptime(month, '%Y-%m').strftime('%B %Y')}" for month in unique_months]
    month_values = ['All'] + unique_months
    
    return month_options, month_values

def generate_bts_month_options():
    """
    Generate month options for filtering BTS transaction data.
    """
    data = load_bts_transaction_data()
    if data.empty:
        return ['All Data'], ['All']
    
    # Get unique months from the data
    data['year_month'] = data['timestamp'].dt.strftime('%Y-%m')
    unique_months = sorted(data['year_month'].unique(), reverse=True)
    
    # Create display options
    month_options = ['All Data'] + [f"{datetime.strptime(month, '%Y-%m').strftime('%B %Y')}" for month in unique_months]
    month_values = ['All'] + unique_months
    
    return month_options, month_values

def apply_month_filter(data, selected_month):
    """
    Apply month filter to arbitrage data for charts.
    """
    if selected_month == 'All':
        return data
    
    # Parse selected month (format: YYYY-MM)
    year, month = selected_month.split('-')
    start_date = pd.to_datetime(f"{year}-{month}-01")
    
    # Get the last day of the month
    if month == '12':
        next_month = pd.to_datetime(f"{int(year)+1}-01-01")
    else:
        next_month = pd.to_datetime(f"{year}-{int(month)+1:02d}-01")
    end_date = next_month - pd.Timedelta(days=1)
    
    # Filter by month
    return data[
        (data['dateTraded'] >= start_date) & 
        (data['dateTraded'] <= end_date)
    ].copy()

def apply_bts_month_filter(data, selected_month):
    """
    Apply month filter to BTS transaction data for charts.
    """
    if selected_month == 'All':
        return data
    
    # Parse selected month (format: YYYY-MM)
    year, month = selected_month.split('-')
    start_date = pd.to_datetime(f"{year}-{month}-01")
    
    # Get the last day of the month
    if month == '12':
        next_month = pd.to_datetime(f"{int(year)+1}-01-01")
    else:
        next_month = pd.to_datetime(f"{year}-{int(month)+1:02d}-01")
    end_date = next_month - pd.Timedelta(days=1)
    
    # Filter by month
    return data[
        (data['timestamp'] >= start_date) & 
        (data['timestamp'] <= end_date)
    ].copy()

# Main Bot Dashboard
st.title("🤖 Bot Dashboard")

# Bot type selection
bot_type = st.selectbox(
    "Select Bot Type",
    ["Arbitrage Bot", "Sniper Bot", "Failed Sniper Bot"],
    index=0
)

if bot_type == "Arbitrage Bot":
    # Load arbitrage data
    data = load_arb_transaction_data()
    
    if data.empty:
        st.warning("No arbitrage data available. Please check your database connection.")
    else:
        # Arbitrage Bot Dashboard Logic (existing code)
        st.subheader("Arbitrage Bot Dashboard")
        
        # Month filter
        month_options, month_values = generate_month_options()
        selected_month_display = st.selectbox("Filter by Month:", month_options, index=0)
        
        # Map display name to internal value
        if selected_month_display == 'All Data':
            selected_month = 'All'
        else:
            try:
                index = month_options.index(selected_month_display)
                selected_month = month_values[index]
            except (ValueError, IndexError):
                selected_month = 'All'
        
        # Apply month filter to all data
        if selected_month == 'All':
            filtered_data_for_display = data.copy()
        else:
            # Parse selected month (format: YYYY-MM)
            year, month = selected_month.split('-')
            start_date = pd.to_datetime(f"{year}-{month}-01")
            
            # Get the last day of the month
            if month == '12':
                next_month = pd.to_datetime(f"{int(year)+1}-01-01")
            else:
                next_month = pd.to_datetime(f"{year}-{int(month)+1:02d}-01")
            end_date = next_month - pd.Timedelta(days=1)
            
            # Filter by month
            filtered_data_for_display = data[
                (data['dateTraded'] >= start_date) & 
                (data['dateTraded'] <= end_date)
            ].copy()
        
        # Apply filter for charts (hourly aggregation)
        filtered_data_for_charts = apply_month_filter(data, selected_month)
        
        # Display chart
        st.subheader("📊 Trading Analytics")
        
        # Profit over time chart (Scatter)
        if not filtered_data_for_charts.empty:
            # Group by hour for better visualization
            hourly_data = filtered_data_for_charts.groupby(
                filtered_data_for_charts['dateTraded'].dt.floor('h')
            ).agg({
                'idealProfit': 'sum',
                'buyVolume': 'sum',
                'sellVolume': 'sum'
            }).reset_index()
            
            # Remove outliers to spread data better
            # Calculate Q1, Q3 and IQR for outlier detection
            Q1 = hourly_data['idealProfit'].quantile(0.25)
            Q3 = hourly_data['idealProfit'].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define outlier bounds (more conservative)
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Filter out extreme outliers for better visualization
            filtered_hourly_data = hourly_data[
                (hourly_data['idealProfit'] >= lower_bound) & 
                (hourly_data['idealProfit'] <= upper_bound)
            ].copy()
            
            # If we have data after filtering
            if not filtered_hourly_data.empty:
                fig_profit = px.scatter(
                    filtered_hourly_data, 
                    x='dateTraded', 
                    y='idealProfit',
                    title='Hourly Profit Over Time (Outliers Removed)',
                    labels={'idealProfit': 'Profit (%)', 'dateTraded': 'Time'},
                    size='idealProfit',  # Size points based on profit
                    color='idealProfit',  # Color points based on profit
                    color_continuous_scale='RdYlGn'  # Red to green color scale
                )
                fig_profit.update_layout(
                    xaxis_title="Time",
                    yaxis_title="Profit (%)",
                    height=400
                )
                st.plotly_chart(fig_profit, use_container_width=True)
                
                # Show outlier information
                outlier_count = len(hourly_data) - len(filtered_hourly_data)
                if outlier_count > 0:
                    st.info(f"📊 {outlier_count} outlier data points removed for better visualization. Showing {len(filtered_hourly_data)} of {len(hourly_data)} total points.")
            else:
                st.warning("No data points remain after outlier removal. Showing original data.")
                fig_profit = px.scatter(
                    hourly_data, 
                    x='dateTraded', 
                    y='idealProfit',
                    title='Hourly Profit Over Time',
                    labels={'idealProfit': 'Profit (%)', 'dateTraded': 'Time'},
                    size='idealProfit',
                    color='idealProfit',
                    color_continuous_scale='RdYlGn'
                )
                fig_profit.update_layout(
                    xaxis_title="Time",
                    yaxis_title="Profit (%)",
                    height=400
                )
                st.plotly_chart(fig_profit, use_container_width=True)
        
        # Summary statistics
        st.subheader("📈 Summary Statistics")
        
        if not filtered_data_for_display.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                total_trades = len(filtered_data_for_display)
                st.metric("Total Trades", f"{total_trades:,}")
            
            with col2:
                # Ensure idealProfit is numeric for calculations
                profit_series = filtered_data_for_display['idealProfit'].apply(lambda x: safe_numeric_conversion(x))
                profitable_trades = len(profit_series[profit_series > 0])
                win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0
                st.metric("Win Rate", f"{win_rate:.1f}%")
        
        # Trade selection dropdown
        st.subheader("🔍 Select Trade for Detailed Analysis")
        
        # Create formatted display data for searchable dropdown
        display_data = []
        for i, (idx, row) in enumerate(filtered_data_for_display.iterrows()):
            # Handle potential mixed data types in idealProfit
            profit_value = row['idealProfit']
            if pd.notna(profit_value) and profit_value is not None:
                try:
                    profit_display = f"{float(profit_value):.2f}%"
                except (ValueError, TypeError):
                    profit_display = "N/A"
            else:
                profit_display = "N/A"
            
            display_data.append({
                'position': i,  # Use position instead of original index
                'display': f"Trade #{row['id']} - {row['dateTraded'].strftime('%Y-%m-%d %H:%M')} - Profit: {profit_display} - {row.get('sellBase', 'Unknown')}"
            })
        
        # Create dropdown options
        dropdown_options = [item['display'] for item in display_data]
        
        if dropdown_options:
            # Use selectbox with search functionality
            selected_trade_display = st.selectbox(
                "Search and choose a trade to analyze:",
                dropdown_options,
                index=0,
                help="Type to search through trades by ID, date, profit, or token name"
            )
            
            # Find the selected trade data
            selected_index = dropdown_options.index(selected_trade_display)
            selected_trade_data = filtered_data_for_display.iloc[display_data[selected_index]['position']]
            
            # Store only the trade ID in session state for navigation
            st.session_state.selected_trade_id = selected_trade_data['id']
            
            # Show quick preview
            st.success(f"Selected: {selected_trade_display}")
            
            # Navigation button
            if st.button("📊 View Detailed Analysis"):
                # Navigate to Arb Info page
                st.switch_page("pages/2_Arb_Info.py")
        
        # Transaction table (no pagination)
        st.subheader("📋 Transaction Table")
        
        # Display data without original_idealProfit column
        display_columns = [col for col in filtered_data_for_display.columns if col != 'original_idealProfit']
        
        # Create a copy of the data for display formatting
        display_data = filtered_data_for_display[display_columns].copy()
        
        # Format idealProfit column to show percentages with 2 decimal places using robust utilities
        if 'idealProfit' in display_data.columns:
            display_data['idealProfit'] = display_data['idealProfit'].apply(
                lambda x: format_percentage(safe_numeric_conversion(x), decimals=2)
            )
        
        st.dataframe(display_data, width='stretch')

elif bot_type == "Sniper Bot":
    # Load BTS transaction data
    bts_data = load_bts_transaction_data()
    
    if bts_data.empty:
        st.warning("No sniper bot data available. Please check your database connection.")
    else:
        st.subheader("Sniper Bot Dashboard")
        
        # Month filter for BTS data
        month_options, month_values = generate_bts_month_options()
        selected_month_display = st.selectbox("Filter by Month:", month_options, index=0)
        
        # Map display name to internal value
        if selected_month_display == 'All Data':
            selected_month = 'All'
        else:
            try:
                index = month_options.index(selected_month_display)
                selected_month = month_values[index]
            except (ValueError, IndexError):
                selected_month = 'All'
        
        # Apply month filter to all data
        if selected_month == 'All':
            filtered_data_for_display = bts_data.copy()
        else:
            # Parse selected month (format: YYYY-MM)
            year, month = selected_month.split('-')
            start_date = pd.to_datetime(f"{year}-{month}-01")
            
            # Get the last day of the month
            if month == '12':
                next_month = pd.to_datetime(f"{int(year)+1}-01-01")
            else:
                next_month = pd.to_datetime(f"{year}-{int(month)+1:02d}-01")
            end_date = next_month - pd.Timedelta(days=1)
            
            # Filter by month
            filtered_data_for_display = bts_data[
                (bts_data['timestamp'] >= start_date) & 
                (bts_data['timestamp'] <= end_date)
            ].copy()
        
        # Apply filter for charts (hourly aggregation)
        filtered_data_for_charts = apply_bts_month_filter(bts_data, selected_month)
        
        # Display chart
        st.subheader("📊 Sniper Bot Analytics")
        
        # Profit over time chart (Scatter)
        if not filtered_data_for_charts.empty:
            # Group by hour for better visualization
            hourly_data = filtered_data_for_charts.groupby(
                filtered_data_for_charts['timestamp'].dt.floor('h')
            ).agg({
                'profit': 'sum',
                'amount': 'sum',
                'amountInDollars': 'sum'
            }).reset_index()
            
            # Remove outliers using robust data handling
            filtered_hourly_data = handle_outliers_iqr(hourly_data, 'profit', factor=1.5)
            
            # If we have data after filtering
            if not filtered_hourly_data.empty:
                # Create absolute profit for sizing (since Plotly doesn't accept negative sizes)
                filtered_hourly_data['abs_profit'] = filtered_hourly_data['profit'].abs()
                
                fig_profit = px.scatter(
                    filtered_hourly_data, 
                    x='timestamp', 
                    y='profit',
                    title='Hourly Profit Over Time (Outliers Removed)',
                    labels={'profit': 'Profit ($)', 'timestamp': 'Time'},
                    size='abs_profit',  # Size points based on absolute profit
                    color='profit',  # Color points based on profit
                    color_continuous_scale='RdYlGn'  # Red to green color scale
                )
                fig_profit.update_layout(
                    xaxis_title="Time",
                    yaxis_title="Profit ($)",
                    height=400
                )
                st.plotly_chart(fig_profit, use_container_width=True)
                
                # Show outlier information
                outlier_count = len(hourly_data) - len(filtered_hourly_data)
                if outlier_count > 0:
                    st.info(f"📊 {outlier_count} outlier data points removed for better visualization. Showing {len(filtered_hourly_data)} of {len(hourly_data)} total points.")
            else:
                st.warning("No data points remain after outlier removal. Showing original data.")
                # Create absolute profit for sizing (since Plotly doesn't accept negative sizes)
                hourly_data['abs_profit'] = hourly_data['profit'].abs()
                
                fig_profit = px.scatter(
                    hourly_data, 
                    x='timestamp', 
                    y='profit',
                    title='Hourly Profit Over Time',
                    labels={'profit': 'Profit ($)', 'timestamp': 'Time'},
                    size='abs_profit',
                    color='profit',
                    color_continuous_scale='RdYlGn'
                )
                fig_profit.update_layout(
                    xaxis_title="Time",
                    yaxis_title="Profit ($)",
                    height=400
                )
                st.plotly_chart(fig_profit, use_container_width=True)
        
        # Summary statistics
        st.subheader("📈 Summary Statistics")
        
        if not filtered_data_for_display.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_trades = len(filtered_data_for_display)
                st.metric("Total Trades", f"{total_trades:,}")
            
            with col2:
                # Calculate profitable trades
                profit_series = filtered_data_for_display['profit'].apply(lambda x: safe_numeric_conversion(x))
                profitable_trades = len(profit_series[profit_series > 0])
                win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0
                st.metric("Win Rate", f"{win_rate:.1f}%")
            
            with col3:
                # Calculate total profit
                total_profit = profit_series.sum()
                st.metric("Total Profit", f"${total_profit:,.2f}")
        
        # Transaction selection dropdown
        st.subheader("🔍 Select Transaction for Detailed Analysis")
        
        # Create formatted display data for searchable dropdown
        display_data = []
        for i, (idx, row) in enumerate(filtered_data_for_display.iterrows()):
            # Handle potential mixed data types in profit
            profit_value = row['profit']
            if pd.notna(profit_value) and profit_value is not None:
                try:
                    profit_display = f"${float(profit_value):.2f}"
                except (ValueError, TypeError):
                    profit_display = "N/A"
            else:
                profit_display = "N/A"
            
            # Get token address (shortened)
            token_addr = str(row['tokenAddress'])[:8] + "..." if pd.notna(row['tokenAddress']) else "Unknown"
            
            display_data.append({
                'position': i,  # Use position instead of original index
                'display': f"Transaction #{row['id']} - {row['timestamp'].strftime('%Y-%m-%d %H:%M')} - {row['type'].upper()} - Profit: {profit_display} - {token_addr}"
            })
        
        # Create dropdown options
        dropdown_options = [item['display'] for item in display_data]
        
        if dropdown_options:
            # Use selectbox with search functionality
            selected_trade_display = st.selectbox(
                "Search and choose a transaction to analyze:",
                dropdown_options,
                index=0,
                help="Type to search through transactions by ID, date, type, profit, or token address"
            )
            
            # Find the selected trade data
            selected_index = dropdown_options.index(selected_trade_display)
            selected_trade_data = filtered_data_for_display.iloc[display_data[selected_index]['position']]
            
            # Store only the transaction ID in session state for navigation
            st.session_state.selected_bts_transaction_id = selected_trade_data['id']
            
            # Show quick preview
            st.success(f"Selected: {selected_trade_display}")
            
            # Navigation button
            if st.button("📊 View Detailed Analysis"):
                # Navigate to BTS Info page
                st.switch_page("pages/3_BTS_Info.py")
        
        # Transaction table (no pagination)
        st.subheader("📋 Transaction Table")
        
        # Create a copy of the data for display formatting
        display_data = filtered_data_for_display.copy()
        
        # Format profit column to show dollar amounts using robust utilities
        if 'profit' in display_data.columns:
            display_data['profit'] = display_data['profit'].apply(
                lambda x: format_crypto_value(safe_numeric_conversion(x))
            )
        
        # Format amountInDollars column using robust utilities
        if 'amountInDollars' in display_data.columns:
            display_data['amountInDollars'] = display_data['amountInDollars'].apply(
                lambda x: format_crypto_value(safe_numeric_conversion(x))
            )
        
        st.dataframe(display_data, width='stretch')

elif bot_type == "Failed Sniper Bot":
    st.subheader("Failed Sniper Bot Dashboard")
    st.info("Failed Sniper Bot functionality coming soon!")
