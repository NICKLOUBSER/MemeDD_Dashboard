import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from config import DB_CONFIG, CHART_CONFIG

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
            # Convert columns to numeric before calculation
            data['sellVolume'] = pd.to_numeric(data['sellVolume'], errors='coerce')
            data['sellVwap'] = pd.to_numeric(data['sellVwap'], errors='coerce')
            data['buyVolume'] = pd.to_numeric(data['buyVolume'], errors='coerce')
            data['buyVwap'] = pd.to_numeric(data['buyVwap'], errors='coerce')
            
            # Calculate raw profit
            raw_profit = (data['sellVolume'] * data['sellVwap']) - (data['buyVolume'] * data['buyVwap'])
            
            # Calculate total investment (buyVolume * buyVwap)
            total_investment = data['buyVolume'] * data['buyVwap']
            
            # Convert to percentage (profit as percentage of investment)
            data['idealProfit'] = (raw_profit / total_investment * 100).fillna(0)
            data['original_idealProfit'] = pd.to_numeric(original_idealProfit, errors='coerce')
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

def generate_month_options():
    """
    Generate month options for filtering.
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

def apply_month_filter(data, selected_month):
    """
    Apply month filter to data for charts.
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

# Main Bot Dashboard
st.title("🤖 Bot Dashboard")

# Load data
data = load_arb_transaction_data()

if data.empty:
    st.warning("No data available. Please check your database connection.")
else:
    # Bot type selection
    bot_type = st.selectbox(
        "Select Bot Type",
        ["Arbitrage Bot", "Sniper Bot", "Failed Sniper Bot"],
        index=0
    )
    
    if bot_type == "Arbitrage Bot":
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
                profit_series = pd.to_numeric(filtered_data_for_display['idealProfit'], errors='coerce')
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
        
        # Format idealProfit column to show percentages with 2 decimal places
        if 'idealProfit' in display_data.columns:
            display_data['idealProfit'] = display_data['idealProfit'].apply(
                lambda x: f"{float(x):.2f}%" if pd.notna(x) and x is not None else "N/A"
            )
        
        st.dataframe(display_data, width='stretch')
