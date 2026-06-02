import pandas as pd
import re


def extract_users(file_path, last_processed_count=0):
    """
    Extract new rows from users CSV.
    
    Args:
        file_path: Path to users.csv
        last_processed_count: Number of rows already processed
    
    Returns:
        (clean_df, new_count)
    """
    # Read only new rows
    df = pd.read_csv(file_path, skiprows=range(1, last_processed_count + 1))
    
    raw_new_rows = len(df)
    
    if df.empty:
        return df, last_processed_count
    
    # Parse types
    df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce').dt.date
    
    # Validate required fields
    df = df.dropna(subset=['user_id', 'name', 'email', 'signup_date'])
    
    # Validate email format
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df = df[df['email'].apply(lambda x: bool(re.match(email_regex, str(x))))]
    
    # Drop duplicates
    df = df.drop_duplicates(subset=['user_id'])
    
    new_count = last_processed_count + raw_new_rows
    return df, str(new_count)


def extract_orders(file_path, last_processed_count=0):
    """
    Extract new rows from orders CSV.
    
    Args:
        file_path: Path to orders.csv
        last_processed_count: Number of rows already processed
    
    Returns:
        (clean_df, new_count)
    """
    # Read only new rows
    df = pd.read_csv(file_path, skiprows=range(1, last_processed_count + 1))
    
    raw_new_rows = len(df)
    
    if df.empty:
        return df, last_processed_count
    
    # Parse types
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    
    # Validate required fields
    df = df.dropna(subset=['order_id', 'user_id', 'timestamp', 'total', 'status'])
    
    # Validate constraints
    df = df[df['total'] >= 0]
    
    # Drop duplicates
    df = df.drop_duplicates(subset=['order_id'])
    
    new_count = last_processed_count + raw_new_rows
    return df, str(new_count)


def extract_products(file_path):
    """
    Extract all rows from products CSV (full reload).
    
    Args:
        file_path: Path to products.csv
    
    Returns:
        clean_df
    """
    df = pd.read_csv(file_path)
    
    # Parse types
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['popularity_score'] = pd.to_numeric(df['popularity_score'], errors='coerce')
    
    # Validate required fields
    df = df.dropna(subset=['product_id', 'name', 'price'])
    
    # Validate constraints
    df = df[df['price'] >= 0]
    df = df[(df['popularity_score'] >= 0) & (df['popularity_score'] <= 1)]
    
    # Drop duplicates
    df = df.drop_duplicates(subset=['product_id'])
    
    return df