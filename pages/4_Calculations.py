import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from config import DB_CONFIG, CHART_CONFIG

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

def show_calculations():
    st.title("🧮 Calculations")
    
    # Load data
    data = load_arb_transaction_data()
    
    if data.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
    st.subheader("📊 Calculation Verification")
    
    # Explanation of the comparison
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
    
    # Display raw comparison data
    st.dataframe(comparison_data, width='stretch')
    
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
    # Display raw data without formatting
    st.dataframe(data, width='stretch')
    
    # New section: Group by sellBase within 20-minute windows
    st.subheader("🕐 SellBase Grouping (20-Minute Windows)")
    
    if 'sellBase' in data.columns:
        # Create a copy of data for grouping
        grouped_data = data.copy()
        
        # Round dateTraded to nearest 20-minute interval
        grouped_data['time_window'] = grouped_data['dateTraded'].dt.floor('20min')
        
        # Group by sellBase and 20-minute time window
        grouped_results = grouped_data.groupby(['sellBase', 'time_window']).agg({
            'idealProfit': ['sum', 'count', 'mean'],
            'buyVolume': 'sum',
            'sellVolume': 'sum',
            'buyVwap': 'mean',
            'sellVwap': 'mean',
            'id': 'count'  # Count of transactions
        }).reset_index()
        
        # Flatten column names
        grouped_results.columns = [
            'sellBase', 'time_window', 'total_profit', 'transaction_count', 
            'avg_profit', 'total_buy_volume', 'total_sell_volume', 
            'avg_buy_vwap', 'avg_sell_vwap', 'trade_count'
        ]
        
        # Sort by sellBase and time_window
        grouped_results = grouped_results.sort_values(['sellBase', 'time_window'])
        
        # Display the grouped data
        st.dataframe(grouped_results, width='stretch')
        
        # Summary statistics for grouped data
        st.subheader("📊 Grouped Data Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_groups = len(grouped_results)
            st.metric("Total Groups", f"{total_groups:,}")
        
        with col2:
            total_profit_groups = grouped_results['total_profit'].sum()
            st.metric("Total Profit (All Groups)", f"${total_profit_groups:,.0f}")
        
        with col3:
            avg_profit_per_group = grouped_results['total_profit'].mean()
            st.metric("Avg Profit per Group", f"${avg_profit_per_group:,.0f}")
        
        with col4:
            total_transactions = grouped_results['transaction_count'].sum()
            st.metric("Total Transactions", f"{total_transactions:,}")
        
        # Show top performing sellBase groups
        st.subheader("🏆 Top Performing SellBase Groups")
        
        # Get top 10 groups by total profit
        top_groups = grouped_results.nlargest(10, 'total_profit')
        
        fig_top_groups = px.bar(
            top_groups,
            x='sellBase',
            y='total_profit',
            title='Top 10 SellBase Groups by Total Profit',
            labels={'total_profit': 'Total Profit ($)', 'sellBase': 'Sell Base'},
            color='total_profit',
            color_continuous_scale='RdYlGn'
        )
        
        fig_top_groups.update_layout(
            xaxis_title="Sell Base",
            yaxis_title="Total Profit ($)",
            height=400,
            xaxis={'tickangle': -45}
        )
        
        st.plotly_chart(fig_top_groups, use_container_width=True)
        
    else:
        st.warning("sellBase column not found in the data")

# Run the calculations page
show_calculations()
