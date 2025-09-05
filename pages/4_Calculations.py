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
    validate_dataframe_columns
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
            
            # Calculate idealProfit using safe calculations
            data['idealProfit'] = data.apply(lambda row: safe_calculation(
                lambda sv, svwap, bv, bvwap: (sv * svwap) - (bv * bvwap),
                row['sellVolume'], row['sellVwap'], row['buyVolume'], row['buyVwap']
            ), axis=1)
            
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
    Load BTS transaction data from PostgreSQL database.
    """
    try:
        # Create database connection
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        # Load data with joins to get complete information
        query = """
        SELECT bt.id, bt.timestamp, bt.type, bt.amount, bt.price, bt."walletAddress", 
               bt."tokenAddress", bt."BTSCoinInfoId", bt."amountInDollars", bt."botId",
               bci."coinPrice", bci."devPubkey", bci."devCapital", bci."devholderPercentage", 
               bci."tokenSupply", bci."totalHoldersSupply", bci."isBundle", 
               bci."liquidityToMcapRatio", bci."reservesInSOL", bci."dateCaptured",
               bb.confidence, bb.reasons, bb."suspiciousWallets", bb."timeClustering", 
               bb."similarAmounts", bb."freshWallets", bb."coordinatedBehavior", 
               bb."totalBuyers", bb."suspiciousBuyers", bb."dateCaptured" as bundleDateCaptured, 
               bb."isBundle" as bundleIsBundle
        FROM btstransaction bt
        LEFT JOIN btscoininfo bci ON bt."tokenAddress" = bci."tokenAddress"
        LEFT JOIN btsbundle bb ON bt."tokenAddress" = bb."tokenAddress"
        ORDER BY bt.timestamp DESC
        """
        
        data = pd.read_sql(query, engine)
        
        # Convert columns to numeric using robust data utilities
        data['amount'] = data['amount'].apply(lambda x: safe_numeric_conversion(x))
        data['price'] = data['price'].apply(lambda x: safe_numeric_conversion(x))
        data['coinPrice'] = data['coinPrice'].apply(lambda x: safe_numeric_conversion(x))
        data['amountInDollars'] = data['amountInDollars'].apply(lambda x: safe_numeric_conversion(x))
        
        # Calculate profit: (amount * coinPrice) - (amount * price)
        data['profit'] = data.apply(lambda row: safe_calculation(
            lambda a, cp, p: (a * cp) - (a * p),
            row['amount'], row['coinPrice'], row['price']
        ), axis=1)
        
        
        # Convert timestamp to datetime
        if 'timestamp' in data.columns:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        return data
        
    except Exception as e:
        st.error(f"Error loading BTS transaction data: {str(e)}")
        return pd.DataFrame()

def show_calculations():
    st.title("🧮 Calculations")
    
    # Create tabs for different bot types
    tab1, tab2 = st.tabs(["🤖 Arbitrage Bot", "🎯 Sniper Bot"])
    
    with tab1:
        show_arbitrage_calculations()
    
    with tab2:
        show_sniper_calculations()

def show_arbitrage_calculations():
    """Show arbitrage bot calculations"""
    st.subheader("🤖 Arbitrage Bot Calculations")
    
    # Load data
    data = load_arb_transaction_data()
    
    if data.empty:
        st.warning("No arbitrage data available. Please check your database connection.")
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
        avg_calc = data['idealProfit'].mean()
        if abs(avg_calc) >= 1e9:
            st.metric("Calculated Avg Profit", f"${avg_calc/1e9:.2f}B")
        elif abs(avg_calc) >= 1e6:
            st.metric("Calculated Avg Profit", f"${avg_calc/1e6:.2f}M")
        elif abs(avg_calc) >= 1e3:
            st.metric("Calculated Avg Profit", f"${avg_calc/1e3:.2f}K")
        else:
            st.metric("Calculated Avg Profit", f"${avg_calc:,.2f}")
    with col2:
        avg_db = data['original_idealProfit'].mean()
        if abs(avg_db) >= 1e9:
            st.metric("Database Avg Profit", f"${avg_db/1e9:.2f}B")
        elif abs(avg_db) >= 1e6:
            st.metric("Database Avg Profit", f"${avg_db/1e6:.2f}M")
        elif abs(avg_db) >= 1e3:
            st.metric("Database Avg Profit", f"${avg_db/1e3:.2f}K")
        else:
            st.metric("Database Avg Profit", f"${avg_db:,.2f}")
    with col3:
        max_calc = data['idealProfit'].max()
        if abs(max_calc) >= 1e9:
            st.metric("Calculated Max Profit", f"${max_calc/1e9:.2f}B")
        elif abs(max_calc) >= 1e6:
            st.metric("Calculated Max Profit", f"${max_calc/1e6:.2f}M")
        elif abs(max_calc) >= 1e3:
            st.metric("Calculated Max Profit", f"${max_calc/1e3:.2f}K")
        else:
            st.metric("Calculated Max Profit", f"${max_calc:,.2f}")
    with col4:
        max_db = data['original_idealProfit'].max()
        if abs(max_db) >= 1e9:
            st.metric("Database Max Profit", f"${max_db/1e9:.2f}B")
        elif abs(max_db) >= 1e6:
            st.metric("Database Max Profit", f"${max_db/1e6:.2f}M")
        elif abs(max_db) >= 1e3:
            st.metric("Database Max Profit", f"${max_db/1e3:.2f}K")
        else:
            st.metric("Database Max Profit", f"${max_db:,.2f}")
    
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
            if abs(total_buy_volume) >= 1e9:
                st.metric("Total Buy Volume", f"${total_buy_volume/1e9:.2f}B")
            elif abs(total_buy_volume) >= 1e6:
                st.metric("Total Buy Volume", f"${total_buy_volume/1e6:.2f}M")
            elif abs(total_buy_volume) >= 1e3:
                st.metric("Total Buy Volume", f"${total_buy_volume/1e3:.2f}K")
            else:
                st.metric("Total Buy Volume", f"${total_buy_volume:,.2f}")
        with col2:
            if abs(total_sell_volume) >= 1e9:
                st.metric("Total Sell Volume", f"${total_sell_volume/1e9:.2f}B")
            elif abs(total_sell_volume) >= 1e6:
                st.metric("Total Sell Volume", f"${total_sell_volume/1e6:.2f}M")
            elif abs(total_sell_volume) >= 1e3:
                st.metric("Total Sell Volume", f"${total_sell_volume/1e3:.2f}K")
            else:
                st.metric("Total Sell Volume", f"${total_sell_volume:,.2f}")
        with col3:
            if abs(total_profit) >= 1e9:
                st.metric("Total Profit", f"${total_profit/1e9:.2f}B")
            elif abs(total_profit) >= 1e6:
                st.metric("Total Profit", f"${total_profit/1e6:.2f}M")
            elif abs(total_profit) >= 1e3:
                st.metric("Total Profit", f"${total_profit/1e3:.2f}K")
            else:
                st.metric("Total Profit", f"${total_profit:,.2f}")
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
            if abs(total_profit_groups) >= 1e9:
                st.metric("Total Profit (All Groups)", f"${total_profit_groups/1e9:.2f}B")
            elif abs(total_profit_groups) >= 1e6:
                st.metric("Total Profit (All Groups)", f"${total_profit_groups/1e6:.2f}M")
            elif abs(total_profit_groups) >= 1e3:
                st.metric("Total Profit (All Groups)", f"${total_profit_groups/1e3:.2f}K")
            else:
                st.metric("Total Profit (All Groups)", f"${total_profit_groups:,.2f}")
        
        with col3:
            avg_profit_per_group = grouped_results['total_profit'].mean()
            if abs(avg_profit_per_group) >= 1e9:
                st.metric("Avg Profit per Group", f"${avg_profit_per_group/1e9:.2f}B")
            elif abs(avg_profit_per_group) >= 1e6:
                st.metric("Avg Profit per Group", f"${avg_profit_per_group/1e6:.2f}M")
            elif abs(avg_profit_per_group) >= 1e3:
                st.metric("Avg Profit per Group", f"${avg_profit_per_group/1e3:.2f}K")
            else:
                st.metric("Avg Profit per Group", f"${avg_profit_per_group:,.2f}")
        
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

def show_sniper_calculations():
    """Show sniper bot calculations"""
    st.subheader("🎯 Sniper Bot Calculations")
    
    # Load data
    data = load_bts_transaction_data()
    
    if data.empty:
        st.warning("No sniper bot data available. Please check your database connection.")
        return
    
    st.subheader("📊 Sniper Bot Analysis")
    
    # Explanation of the sniper bot data
    with st.expander("🔬 Sniper Bot Data Source Details", expanded=False):
        st.markdown("""
        ### **Database Schema**
        
        **Tables**: `btstransaction`, `btscoininfo`, `btsbundle`
        **Key Columns**:
        - `amount`, `price`: Transaction data
        - `coinPrice`: Current coin price from btscoininfo
        - `profit`: Calculated as (amount × coinPrice) - (amount × price)
        - `timestamp`: Transaction timestamp
        - `type`: Transaction type (buy/sell)
        
        ### **Calculation Verification**
        
        **Purpose**: Analyze sniper bot performance and profit calculations
        **Formula**: Profit = (Amount × Current Coin Price) - (Amount × Transaction Price)
        **Output**: Comprehensive profit analysis and token performance metrics
        
        ### **Data Processing**
        
        **Load**: PostgreSQL connection with LEFT JOINs to btscoininfo and btsbundle
        **Transform**: Numeric conversion, datetime parsing, profit calculation
        **Aggregate**: Token-based grouping for performance analysis
        **Display**: Formatted with crypto value utilities
        """)
    
    # Show summary statistics
    st.subheader("📈 Sniper Bot Summary Statistics")
    
    # Add explanation about micro-dollar profits
    st.info("""
    💡 **Understanding Micro-Dollar Profits**: The profit values shown are in micro-dollars (millionths of a dollar), 
    which is normal for cryptocurrency trading. For example, $0.000000291084935044 = 0.29 micro-dollars. 
    These small values accumulate over thousands of transactions to create meaningful returns.
    """)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_transactions = len(data)
        st.metric("Total Transactions", f"{total_transactions:,}")
    
    with col2:
        total_profit = data['profit'].sum()
        profit_display = f"${total_profit:.18f}"
        st.metric("Total Profit", profit_display)
    
    with col3:
        avg_profit = data['profit'].mean()
        avg_profit_display = f"${avg_profit:.18f}"
        st.metric("Average Profit", avg_profit_display)
    
    with col4:
        profitable_trades = len(data[data['profit'] > 0])
        win_rate = (profitable_trades / total_transactions * 100) if total_transactions > 0 else 0
        st.metric("Win Rate", f"{win_rate:.1f}%")
    
    # Transaction type analysis
    st.subheader("📊 Transaction Type Analysis")
    
    if 'type' in data.columns:
        type_analysis = data.groupby('type').agg({
            'profit': ['sum', 'mean', 'count'],
            'amount': 'sum',
            'amountInDollars': 'sum'
        }).round(6)
        
        # Flatten column names
        type_analysis.columns = ['Total Profit', 'Avg Profit', 'Count', 'Total Amount', 'Total Amount ($)']
        
        # Format the dataframe to show small values properly
        formatted_type_analysis = type_analysis.copy()
        formatted_type_analysis['Total Profit'] = formatted_type_analysis['Total Profit'].apply(lambda x: f"${x:.18f}")
        formatted_type_analysis['Avg Profit'] = formatted_type_analysis['Avg Profit'].apply(lambda x: f"${x:.18f}")
        formatted_type_analysis['Total Amount'] = formatted_type_analysis['Total Amount'].apply(lambda x: f"{x:.18f}")
        formatted_type_analysis['Total Amount ($)'] = formatted_type_analysis['Total Amount ($)'].apply(lambda x: f"${x:.18f}")
        
        st.dataframe(formatted_type_analysis, width='stretch')
        
        # Visualize transaction types
        fig_type = px.pie(
            data.groupby('type').size().reset_index(name='count'),
            values='count',
            names='type',
            title='Transaction Type Distribution'
        )
        st.plotly_chart(fig_type, use_container_width=True)
    
    # Token performance analysis
    st.subheader("🪙 Token Performance Analysis")
    
    if 'tokenAddress' in data.columns:
        # Group by token address
        token_performance = data.groupby('tokenAddress').agg({
            'profit': ['sum', 'mean', 'count'],
            'amount': 'sum',
            'coinPrice': 'mean',
            'price': 'mean'
        }).round(6)
        
        # Flatten column names
        token_performance.columns = [
            'Total Profit', 'Avg Profit', 'Transaction Count', 
            'Total Amount', 'Avg Coin Price', 'Avg Transaction Price'
        ]
        
        # Sort by total profit
        token_performance = token_performance.sort_values('Total Profit', ascending=False)
        
        # Format the dataframe to show small values properly
        formatted_token_performance = token_performance.head(20).copy()
        formatted_token_performance['Total Profit'] = formatted_token_performance['Total Profit'].apply(lambda x: f"${x:.18f}")
        formatted_token_performance['Avg Profit'] = formatted_token_performance['Avg Profit'].apply(lambda x: f"${x:.18f}")
        formatted_token_performance['Total Amount'] = formatted_token_performance['Total Amount'].apply(lambda x: f"{x:.18f}")
        formatted_token_performance['Avg Coin Price'] = formatted_token_performance['Avg Coin Price'].apply(lambda x: f"${x:.18f}")
        formatted_token_performance['Avg Transaction Price'] = formatted_token_performance['Avg Transaction Price'].apply(lambda x: f"${x:.18f}")
        
        st.dataframe(formatted_token_performance, width='stretch')
        
    
    # Time-based analysis
    st.subheader("⏰ Time-Based Analysis")
    
    if 'timestamp' in data.columns:
        # Group by hour
        data['hour'] = data['timestamp'].dt.floor('H')
        hourly_analysis = data.groupby('hour').agg({
            'profit': ['sum', 'mean', 'count'],
            'amount': 'sum'
        }).round(6)
        
        # Flatten column names
        hourly_analysis.columns = ['Total Profit', 'Avg Profit', 'Transaction Count', 'Total Amount']
        
        # Sort by hour
        hourly_analysis = hourly_analysis.sort_index()
        
        # Format the dataframe to show small values properly
        formatted_hourly_analysis = hourly_analysis.copy()
        formatted_hourly_analysis['Total Profit'] = formatted_hourly_analysis['Total Profit'].apply(lambda x: f"${x:.18f}")
        formatted_hourly_analysis['Avg Profit'] = formatted_hourly_analysis['Avg Profit'].apply(lambda x: f"${x:.18f}")
        formatted_hourly_analysis['Total Amount'] = formatted_hourly_analysis['Total Amount'].apply(lambda x: f"{x:.18f}")
        
        st.dataframe(formatted_hourly_analysis, width='stretch')
        
        # Hourly profit chart
        fig_hourly = px.line(
            hourly_analysis.reset_index(),
            x='hour',
            y='Total Profit',
            title='Hourly Profit Trend',
            labels={'Total Profit': 'Total Profit ($)', 'hour': 'Hour'},
            markers=True
        )
        
        fig_hourly.update_layout(
            xaxis_title="Hour",
            yaxis_title="Total Profit ($)",
            height=400,
            yaxis={'tickformat': '.8f'}
        )
        
        # Add hover information
        fig_hourly.update_traces(
            hovertemplate='<b>%{x}</b><br>Total Profit: $%{y:.8f}<br>Avg Profit: $%{customdata[0]:.8f}<br>Transactions: %{customdata[1]}<extra></extra>',
            customdata=hourly_analysis[['Avg Profit', 'Transaction Count']].values
        )
        
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    # Bundle analysis (if bundle data is available)
    st.subheader("📦 Bundle Analysis")
    
    if 'confidence' in data.columns:
        # Filter data with bundle information
        bundle_data = data[data['confidence'].notna()]
        
        if not bundle_data.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_confidence = bundle_data['confidence'].mean()
                st.metric("Average Confidence", f"{avg_confidence:.1f}%")
            
            with col2:
                total_bundle_profit = bundle_data['profit'].sum()
                bundle_profit_display = f"${total_bundle_profit:.18f}"
                st.metric("Bundle Total Profit", bundle_profit_display)
            
            with col3:
                bundle_transactions = len(bundle_data)
                st.metric("Bundle Transactions", f"{bundle_transactions:,}")
            
            # Bundle confidence vs profit scatter plot
            # Create absolute profit for size (since size can't be negative)
            bundle_data_copy = bundle_data.copy()
            bundle_data_copy['abs_profit'] = bundle_data_copy['profit'].abs()
            
            fig_bundle = px.scatter(
                bundle_data_copy,
                x='confidence',
                y='profit',
                title='Bundle Confidence vs Profit',
                labels={'confidence': 'Confidence (%)', 'profit': 'Profit ($)'},
                color='profit',
                color_continuous_scale='RdYlGn',
                size='abs_profit',
                size_max=20
            )
            
            fig_bundle.update_layout(
                xaxis_title="Confidence (%)",
                yaxis_title="Profit ($)",
                height=400,
                yaxis={'tickformat': '.8f'}
            )
            
            # Add hover information
            fig_bundle.update_traces(
                hovertemplate='<b>Confidence: %{x}%</b><br>Profit: $%{y:.8f}<br>Amount: %{customdata[0]:.8f}<br>Price: $%{customdata[1]:.8f}<extra></extra>',
                customdata=bundle_data[['amount', 'price']].values
            )
            
            st.plotly_chart(fig_bundle, use_container_width=True)
        else:
            st.info("No bundle data available for analysis")
    
    # Show full transaction data
    st.subheader("📄 Full Sniper Bot Transaction Data")
    st.dataframe(data, width='stretch')

# Run the calculations page
show_calculations()
