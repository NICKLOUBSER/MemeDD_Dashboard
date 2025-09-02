import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import os

# Import configuration first
from config import DB_CONFIG, APP_CONFIG, CHART_CONFIG

# Page configuration
st.set_page_config(
    page_title=APP_CONFIG["page_title"],
    page_icon=APP_CONFIG["page_icon"],
    layout=APP_CONFIG["layout"],
    initial_sidebar_state=APP_CONFIG["initial_sidebar_state"]
)

@st.cache_data(ttl=APP_CONFIG["cache_ttl"])
def load_arb_transaction_data():
    """
    Load arb_transaction data from the database.
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
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
            
            data['idealProfit'] = (data['sellVolume'] * data['sellVwap']) - (data['buyVolume'] * data['buyVwap'])
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

def apply_month_filter(data, selected_month):
    """
    Apply month filter to the data and aggregate by hour.
    """
    if data.empty or not selected_month:
        return pd.DataFrame()
    
    try:
        data_copy = data.copy()
        
        # Handle "All" case - show all data with hourly aggregation
        if selected_month == 'All':
            filtered = data_copy
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
            filtered = data_copy[
                (data_copy['dateTraded'] >= start_date) & 
                (data_copy['dateTraded'] <= end_date)
            ].copy()
        
        if filtered.empty:
            return pd.DataFrame()
        
        # Aggregate by hour - calculate average profit per hour
        filtered['date_hour'] = filtered['dateTraded'].dt.floor('h')
        
        hourly_data = filtered.groupby('date_hour').agg({
            'idealProfit': 'mean'
        }).reset_index()
        
        if not hourly_data.empty:
            hourly_data['idealProfit'] = pd.to_numeric(hourly_data['idealProfit'], errors='coerce')
        
        # Rename columns for chart
        hourly_data = hourly_data.rename(columns={'date_hour': 'dateTraded'})
        
        return hourly_data
    except Exception as e:
        st.error(f"Error applying filter: {str(e)}")
        return pd.DataFrame()

def generate_month_options():
    """
    Generate month options for the filter.
    """
    month_options = ['All Data']
    month_values = ['All']
    
    for i in range(12):
        month_date = datetime.now() - timedelta(days=30*i)
        month_str = month_date.strftime('%Y-%m')
        month_display = month_date.strftime('%B %Y')
        month_options.append(month_display)
        month_values.append(month_str)
    
    return month_options, month_values

def create_profit_chart(data):
    """
    Create a scatter plot for profit over time.
    """
    if data.empty:
        st.warning("No data available for the selected period.")
        return
    
    fig = px.scatter(
        data,
        x='dateTraded',
        y='idealProfit',
        title="Hourly Profit Aggregation",
        labels={'dateTraded': 'Date/Time (Hourly)', 'idealProfit': 'Total Profit per Hour (USD)'},
        color_discrete_sequence=[CHART_CONFIG["marker_color"]]
    )
    
    fig.update_traces(
        marker=dict(size=CHART_CONFIG["marker_size"], opacity=CHART_CONFIG["marker_opacity"]),
        line=dict(color=CHART_CONFIG["line_color"], width=CHART_CONFIG["line_width"])
    )
    
    fig.update_layout(
        xaxis=dict(tickangle=-45),
        yaxis=dict(tickformat=",.0f"),
        hovermode="x unified",
        showlegend=False
    )
    
    return fig

def main():
    # Sidebar navigation
    st.sidebar.title("🎯 MemeDD Dashboard")
    
    # Navigation
    page = st.sidebar.selectbox(
        "Select Page",
        ["Bot Dashboard", "Arb Info", "BTS Info", "Calculations"]
    )
    
    if page == "Bot Dashboard":
        show_bot_dashboard()
    elif page == "Calculations":
        show_calculations()
    elif page == "Arb Info":
        show_arb_info()
    elif page == "BTS Info":
        show_bts_info()

def show_bot_dashboard():
    st.title("🤖 Bot Dashboard")
    
    # Load data
    data = load_arb_transaction_data()
    
    if data.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
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
        
        # Apply filter
        filtered_data = apply_month_filter(data, selected_month)
        
        # Display chart
        st.subheader("Profit Over Time")
        if not filtered_data.empty:
            fig = create_profit_chart(filtered_data)
            st.plotly_chart(fig, width='stretch')
        else:
            st.warning("No data available for the selected period.")
        
        # Display transaction data
        st.subheader("Transaction Data")
        # Display summary statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Transactions", len(data))
        with col2:
            st.metric("Total Profit", f"${data['idealProfit'].sum():,.0f}")
        with col3:
            st.metric("Average Profit", f"${data['idealProfit'].mean():,.0f}")
        with col4:
            st.metric("Max Profit", f"${data['idealProfit'].max():,.0f}")
        
        # Format dataframe to show no decimal points
        formatted_data = data.copy()
        numeric_columns = formatted_data.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            formatted_data[col] = formatted_data[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else x)
        
        st.dataframe(formatted_data, width='stretch')
        
    
    elif bot_type == "Sniper Bot":
        st.subheader("Sniper Bot Dashboard")
        st.info("No data available for Sniper Bot yet")
    
    elif bot_type == "Failed Sniper Bot":
        st.subheader("Failed Sniper Bot Dashboard")
        st.info("No data available for Failed Sniper Bot yet")

def show_arb_info():
    st.title("📊 Arb Info")
    st.info("Arbitrage information page - content to be added")

def show_calculations():
    st.title("🧮 Profit Calculations Analysis")
    st.info("This page shows the comparison between calculated ideal profits and database values for verification purposes.")
    
    # Add detailed explanation about large profit values
    st.subheader("📊 Data Calculation Methodology")
    
    with st.expander("📋 How the profit data was calculated", expanded=True):
        st.markdown("""
        ### **Profit Calculation Method**
        
        **Formula**: `Ideal Profit = (Sell Volume × Sell VWAP) - (Buy Volume × Buy VWAP)`
        
        **Data Sources**:
        - **Sell Volume**: Total quantity of tokens sold per transaction
        - **Sell VWAP**: Volume Weighted Average Price of all sell transactions
        - **Buy Volume**: Total quantity of tokens bought per transaction  
        - **Buy VWAP**: Volume Weighted Average Price of all buy transactions
        
        **Calculation Process**:
        1. Raw transaction data loaded from `arbtransaction` table
        2. Volume and VWAP values converted to numeric format
        3. Ideal profit calculated using the formula above
        4. Results compared with stored `idealProfit` values from database
        
        **Aggregation Method**:
        - Individual transaction profits summed by hour for visualization
        - Total profits represent cumulative earnings across all trades
        - Profit margins calculated as (Total Profit / Total Buy Volume) × 100
        
        **Data Validation**:
        - Calculated values compared against database-stored values
        - Differences flagged for review if > $0.01
        - Both calculation methods should produce similar results
        """)
    
    # Load data
    data = load_arb_transaction_data()
    
    if data.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
    # Display comparison of calculated vs original values
    st.subheader("📊 Profit Comparison: Calculated vs Database Values")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Transactions", len(data))
    with col2:
        st.metric("Calculated Total Profit", f"${data['idealProfit'].sum():,.0f}")
    with col3:
        st.metric("Database Total Profit", f"${data['original_idealProfit'].sum():,.0f}")
    with col4:
        difference = data['idealProfit'].sum() - data['original_idealProfit'].sum()
        st.metric("Difference", f"${difference:,.0f}", delta=f"{difference:,.0f}")
    
    # Show detailed comparison
    st.subheader("📋 Detailed Comparison")
    
    # Add calculation methodology explanation
    with st.expander("🔬 Data Source Details", expanded=False):
        st.markdown("""
        ### **Database Schema**
        
        **Table**: `arbtransaction`
        **Key Columns**:
        - `sellVolume`, `sellVwap`: Sell transaction data
        - `buyVolume`, `buyVwap`: Buy transaction data
        - `idealProfit`: Stored profit calculation
        - `dateTraded`: Transaction timestamp
        
        ### **Calculation Verification**
        
        **Purpose**: Compare real-time calculations with stored database values
        **Tolerance**: $0.01 difference threshold
        **Output**: Flagged discrepancies for data integrity review
        
        ### **Data Processing**
        
        **Load**: PostgreSQL connection via SQLAlchemy
        **Transform**: Numeric conversion, datetime parsing
        **Aggregate**: Hourly grouping for visualization
        **Display**: Formatted with comma separators, no decimals
        """)
    
    comparison_data = data[['dateTraded', 'idealProfit', 'original_idealProfit']].copy()
    comparison_data['difference'] = comparison_data['idealProfit'] - comparison_data['original_idealProfit']
    comparison_data['match'] = comparison_data['difference'].abs() < 0.01  # Consider values within 0.01 as matching
    
    # Format the comparison data
    formatted_comparison = comparison_data.copy()
    numeric_cols = ['idealProfit', 'original_idealProfit', 'difference']
    for col in numeric_cols:
        formatted_comparison[col] = formatted_comparison[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else x)
    
    st.dataframe(formatted_comparison, width='stretch')
    
    # Show summary statistics
    st.subheader("📈 Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Calculated Avg Profit", f"${data['idealProfit'].mean():,.0f}")
    with col2:
        st.metric("Database Avg Profit", f"${data['original_idealProfit'].mean():,.0f}")
    with col3:
        st.metric("Calculated Max Profit", f"${data['idealProfit'].max():,.0f}")
    with col4:
        st.metric("Database Max Profit", f"${data['original_idealProfit'].max():,.0f}")
    
    # Add profit margin analysis
    st.subheader("💰 Profit Margin Analysis")
    
    # Calculate profit margins if volume data is available
    if all(col in data.columns for col in ['sellVolume', 'sellVwap', 'buyVolume', 'buyVwap']):
        # Calculate total volume and profit margins
        total_buy_volume = (data['buyVolume'] * data['buyVwap']).sum()
        total_sell_volume = (data['sellVolume'] * data['sellVwap']).sum()
        total_profit = data['idealProfit'].sum()
        
        # Calculate profit margins
        if total_buy_volume > 0:
            profit_margin_percentage = (total_profit / total_buy_volume) * 100
        else:
            profit_margin_percentage = 0
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Buy Volume", f"${total_buy_volume:,.0f}")
        with col2:
            st.metric("Total Sell Volume", f"${total_sell_volume:,.0f}")
        with col3:
            st.metric("Total Profit", f"${total_profit:,.0f}")
        with col4:
            st.metric("Profit Margin", f"{profit_margin_percentage:.2f}%")
        
        # Add explanation about profit margins
        st.info(f"""
        **Profit Margin**: {profit_margin_percentage:.2f}% represents the percentage return on total buy volume. 
        Calculated as (Total Profit / Total Buy Volume) × 100.
        """)
    else:
        st.warning("Volume data not available for profit margin calculation")
    
    # Show full data with both values
    st.subheader("📄 Full Transaction Data")
    # Format dataframe to show no decimal points
    formatted_data = data.copy()
    numeric_columns = formatted_data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        formatted_data[col] = formatted_data[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else x)
    
    st.dataframe(formatted_data, width='stretch')

def show_bts_info():
    st.title("🔧 BTS Info")
    st.info("BTS information page - content to be added")

if __name__ == "__main__":
    main()
