#!/usr/bin/env python3
"""
Script to join PostgreSQL JSON and YDB TSV datasets.
"""

import pandas as pd
import json

def load_pg_data(filepath):
    """Load PostgreSQL JSON data into a DataFrame."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    # Convert single JSON object to DataFrame, treating all values as strings
    df = pd.DataFrame([{k: str(v) for k, v in data.items()}])
    return df

def load_ydb_data(filepath):
    """Load YDB TSV data into a DataFrame."""
    # Read as strings
    df = pd.read_csv(filepath, sep='\t', dtype=str)
    return df

def create_comparison_table(pg_df, ydb_df):
    """Create a pivoted comparison table with columns as rows."""
    # Get all columns from both datasets
    all_cols = sorted(set(pg_df.columns) | set(ydb_df.columns))
    
    # Create comparison data
    comparison_data = []
    for col in all_cols:
        pg_val = pg_df[col].iloc[0] if col in pg_df.columns else None
        ydb_val = ydb_df[col].iloc[0] if col in ydb_df.columns else None
        
        comparison_data.append({
            'Column': col,
            'PostgreSQL': pg_val,
            'YDB': ydb_val
        })
    
    return pd.DataFrame(comparison_data)

def main():
    pg_file = '/home/vitalyisaev/troubles/YQ-4497/cpa_order_promo_20251016.pg.json'
    ydb_file = '/home/vitalyisaev/troubles/YQ-4497/cpa_order_promo_20251016.ydb.tsv'
    
    # Load datasets
    pg_df = load_pg_data(pg_file)
    ydb_df = load_ydb_data(ydb_file)
    
    # Create comparison table
    comparison_df = create_comparison_table(pg_df, ydb_df)
    
    # Print the full comparison table
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    print("=" * 80)
    print("FULL COMPARISON TABLE")
    print("=" * 80)
    print(comparison_df.to_string(index=False))
    
    # Filter and print only differing rows
    diff_df = comparison_df[comparison_df['PostgreSQL'] != comparison_df['YDB']]
    
    if not diff_df.empty:
        print("\n" + "=" * 80)
        print("DIFFERING ROWS")
        print("=" * 80)
        print(diff_df.to_string(index=False))
    else:
        print("\n" + "=" * 80)
        print("No differences found - all values match!")
        print("=" * 80)

if __name__ == '__main__':
    main()