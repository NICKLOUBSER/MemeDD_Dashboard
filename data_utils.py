"""
Data Science Utilities for MemeDD Dashboard
Handles cryptocurrency data with high precision decimals and mixed data types
"""

import pandas as pd
import numpy as np
from decimal import Decimal, getcontext
import warnings

# Set high precision for decimal calculations
getcontext().prec = 50

def safe_numeric_conversion(value, default=0.0, precision=18):
    """
    Safely convert values to numeric, handling strings, decimals, and mixed types.
    
    Args:
        value: Input value (can be string, float, int, or mixed)
        default: Default value if conversion fails
        precision: Number of decimal places for precision handling
    
    Returns:
        float: Converted numeric value
    """
    if pd.isna(value) or value is None:
        return default
    
    # Handle string values
    if isinstance(value, str):
        # Remove common formatting characters
        cleaned = value.replace(',', '').replace('$', '').replace('%', '').strip()
        if cleaned == '' or cleaned.lower() in ['nan', 'none', 'null']:
            return default
        
        try:
            # Try direct conversion first
            return float(cleaned)
        except ValueError:
            try:
                # Try with Decimal for high precision
                return float(Decimal(cleaned))
            except:
                return default
    
    # Handle pandas numeric types
    if pd.api.types.is_numeric_dtype(type(value)):
        return float(value)
    
    # Handle numpy types
    if isinstance(value, (np.integer, np.floating)):
        return float(value)
    
    # Handle Decimal types
    if isinstance(value, Decimal):
        return float(value)
    
    # Try pandas conversion as last resort
    try:
        converted = pd.to_numeric(value, errors='coerce')
        if pd.isna(converted):
            return default
        return float(converted)
    except:
        return default

def safe_decimal_conversion(value, precision=18):
    """
    Convert to Decimal for high precision calculations.
    
    Args:
        value: Input value
        precision: Decimal precision
    
    Returns:
        Decimal: High precision decimal value
    """
    numeric_val = safe_numeric_conversion(value, default=0.0, precision=precision)
    return Decimal(str(numeric_val))

def format_crypto_value(value, currency_symbol="$", max_decimals=18, min_decimals=2):
    """
    Format cryptocurrency values with appropriate decimal places.
    
    Args:
        value: Numeric value to format
        currency_symbol: Currency symbol to prepend
        max_decimals: Maximum decimal places to show
        min_decimals: Minimum decimal places to show
    
    Returns:
        str: Formatted currency string
    """
    numeric_val = safe_numeric_conversion(value)
    
    if numeric_val == 0:
        return f"{currency_symbol}0.00"
    
    # Determine appropriate decimal places based on value magnitude
    if abs(numeric_val) < 0.00000001:  # Very small values
        decimals = min(max_decimals, 18)
    elif abs(numeric_val) < 0.01:  # Small values
        decimals = min(max_decimals, 8)
    elif abs(numeric_val) < 1:  # Medium values
        decimals = min(max_decimals, 6)
    elif abs(numeric_val) < 1000:  # Large values
        decimals = min(max_decimals, 4)
    else:  # Very large values
        decimals = min(max_decimals, 2)
    
    # Ensure we don't go below minimum decimals
    decimals = max(decimals, min_decimals)
    
    return f"{currency_symbol}{numeric_val:,.{decimals}f}"

def format_percentage(value, decimals=2):
    """
    Format percentage values safely.
    
    Args:
        value: Numeric value to format as percentage
        decimals: Number of decimal places
    
    Returns:
        str: Formatted percentage string
    """
    numeric_val = safe_numeric_conversion(value)
    return f"{numeric_val:.{decimals}f}%"

def safe_calculation(operation, *values, default=0.0):
    """
    Perform safe calculations with error handling.
    
    Args:
        operation: Function to perform (e.g., lambda x, y: x + y)
        *values: Values to calculate with
        default: Default value if calculation fails
    
    Returns:
        float: Result of calculation or default value
    """
    try:
        # Convert all values to safe numeric format
        safe_values = [safe_numeric_conversion(v) for v in values]
        
        # Perform operation
        result = operation(*safe_values)
        
        # Validate result
        if pd.isna(result) or np.isinf(result):
            return default
        
        return float(result)
    except Exception as e:
        warnings.warn(f"Calculation failed: {e}, using default value {default}")
        return default

def validate_dataframe_columns(df, required_columns, numeric_columns=None):
    """
    Validate DataFrame columns and convert numeric columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        numeric_columns: List of columns that should be numeric
    
    Returns:
        pd.DataFrame: Validated and cleaned DataFrame
    """
    if df.empty:
        return df
    
    # Check required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        warnings.warn(f"Missing required columns: {missing_cols}")
    
    # Convert numeric columns
    if numeric_columns:
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: safe_numeric_conversion(x))
    
    return df

def safe_aggregation(df, group_col, agg_dict):
    """
    Perform safe aggregation operations.
    
    Args:
        df: DataFrame to aggregate
        group_col: Column to group by
        agg_dict: Dictionary of column: function mappings
    
    Returns:
        pd.DataFrame: Aggregated DataFrame
    """
    try:
        # Ensure group column exists and is clean
        if group_col not in df.columns:
            raise ValueError(f"Group column '{group_col}' not found")
        
        # Clean numeric columns before aggregation
        numeric_cols = list(agg_dict.keys())
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: safe_numeric_conversion(x))
        
        return df.groupby(group_col).agg(agg_dict).reset_index()
    except Exception as e:
        warnings.warn(f"Aggregation failed: {e}")
        return pd.DataFrame()

def handle_outliers_iqr(data, column, factor=1.5):
    """
    Handle outliers using IQR method with safe numeric conversion.
    
    Args:
        data: DataFrame
        column: Column name to check for outliers
        factor: IQR factor for outlier detection
    
    Returns:
        pd.DataFrame: DataFrame with outliers filtered
    """
    if column not in data.columns or data.empty:
        return data
    
    # Convert to numeric safely
    numeric_data = data[column].apply(lambda x: safe_numeric_conversion(x))
    
    # Calculate IQR
    Q1 = numeric_data.quantile(0.25)
    Q3 = numeric_data.quantile(0.75)
    IQR = Q3 - Q1
    
    # Define bounds
    lower_bound = Q1 - factor * IQR
    upper_bound = Q3 + factor * IQR
    
    # Filter outliers
    mask = (numeric_data >= lower_bound) & (numeric_data <= upper_bound)
    
    return data[mask].copy()

def create_safe_metrics(data, metric_configs):
    """
    Create safe metrics with error handling.
    
    Args:
        data: DataFrame
        metric_configs: List of dicts with 'name', 'column', 'func' keys
    
    Returns:
        dict: Dictionary of calculated metrics
    """
    metrics = {}
    
    for config in metric_configs:
        name = config['name']
        column = config['column']
        func = config['func']
        
        try:
            if column in data.columns:
                # Convert to numeric safely
                numeric_data = data[column].apply(lambda x: safe_numeric_conversion(x))
                
                # Apply function
                result = func(numeric_data)
                
                # Validate result
                if pd.isna(result) or np.isinf(result):
                    metrics[name] = 0
                else:
                    metrics[name] = float(result)
            else:
                metrics[name] = 0
        except Exception as e:
            warnings.warn(f"Metric calculation failed for {name}: {e}")
            metrics[name] = 0
    
    return metrics
