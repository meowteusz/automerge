#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "pandas",
# ]
# ///
"""
CSV Merge Debugger - Find duplicate issues in merge operations

Usage: uv run merge_debugger.py
"""

import pandas as pd
from pathlib import Path
from collections import Counter
import sys

def load_csv_robust(file_path, **kwargs):
    """
    Robustly load a CSV file with automatic encoding detection and error handling.
    
    Args:
        file_path: Path to the CSV file
        **kwargs: Additional arguments to pass to pd.read_csv
    
    Returns:
        pd.DataFrame or None if loading fails
    """
    encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16']
    
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file_path, encoding=encoding, **kwargs)
            if encoding != 'utf-8':
                print(f"⚠️  {file_path.name}: Loaded with {encoding} encoding")
            return df
        except UnicodeDecodeError as e:
            if encoding == encodings_to_try[-1]:
                # Calculate approximate row/column from byte position
                try:
                    with open(file_path, 'rb') as f:
                        content = f.read(e.start)
                        lines = content.count(b'\n')
                        last_line_start = content.rfind(b'\n')
                        if last_line_start == -1:
                            col_pos = e.start
                        else:
                            col_pos = e.start - last_line_start - 1
                    
                    print(f"❌ Error loading {file_path}: UTF-8 decode error")
                    print(f"   Problematic byte: 0x{e.object[e.start]:02x} at position {e.start}")
                    print(f"   Approximate location: row {lines + 1}, column {col_pos + 1}")
                    print(f"   Tried encodings: {', '.join(encodings_to_try)}")
                except Exception:
                    print(f"❌ Error loading {file_path}: UTF-8 decode error at position {e.start}")
                    print(f"   Tried encodings: {', '.join(encodings_to_try)}")
            continue
        except Exception as e:
            print(f"❌ Error loading {file_path}: {str(e)}")
            return None
    
    print(f"❌ Failed to load {file_path} with any encoding. Exiting.")
    sys.exit(1)

def debug_merge_duplicates(csv_directory: str):
    """Debug duplicate issues in CSV merges."""
    
    csv_path = Path(csv_directory)
    csv_files = list(csv_path.glob("*.csv"))
    
    print("🔍 MERGE DUPLICATE DEBUGGER")
    print("=" * 50)
    
    # Load all CSVs
    csvs = {}
    for csv_file in csv_files:
        df = load_csv_robust(csv_file)
        if df is not None:
            csvs[csv_file.stem] = df
            print(f"📁 {csv_file.stem}: {len(df)} rows")
        else:
            print(f"❌ Skipping {csv_file.stem} due to loading errors")
            sys.exit(1)
    
    print("\n" + "=" * 50)
    print("🔎 CHECKING FOR DUPLICATES IN JOIN COLUMNS")
    print("=" * 50)
    
    # Check each CSV for duplicates in potential join columns
    all_columns = set()
    for df in csvs.values():
        all_columns.update(df.columns)
    
    # Find columns that appear in multiple CSVs (potential join columns)
    column_counts = Counter()
    csv_columns = {}
    
    for csv_name, df in csvs.items():
        csv_columns[csv_name] = set(df.columns)
        for col in df.columns:
            column_counts[col] += 1
    
    potential_join_cols = [col for col, count in column_counts.items() if count > 1]
    
    print(f"🔗 Potential join columns: {potential_join_cols}")
    
    # Check for duplicates in each potential join column
    for col in potential_join_cols:
        print(f"\n📋 Checking column '{col}':")
        
        for csv_name, df in csvs.items():
            if col in df.columns:
                # Check for duplicates
                duplicates = df[col].duplicated()
                dup_count = duplicates.sum()
                
                if dup_count > 0:
                    print(f"  ❌ {csv_name}: {dup_count} duplicates found!")
                    
                    # Show the duplicate values
                    dup_values = df[df[col].duplicated(keep=False)][col].unique()
                    print(f"     Duplicate values: {list(dup_values)}")
                    
                    # Show full duplicate rows
                    dup_rows = df[df[col].duplicated(keep=False)]
                    print(f"     Duplicate rows:")
                    for idx, row in dup_rows.iterrows():
                        print(f"       Row {idx}: {dict(row)}")
                else:
                    print(f"  ✅ {csv_name}: No duplicates")
                
                # Check for whitespace/case issues
                cleaned_col = df[col].astype(str).str.strip().str.lower()
                if cleaned_col.duplicated().sum() > df[col].duplicated().sum():
                    print(f"  ⚠️  {csv_name}: Found case/whitespace differences!")
    
    print("\n" + "=" * 50)
    print("🔬 DETAILED VALUE ANALYSIS")
    print("=" * 50)
    
    # For each potential join column, show all unique values across CSVs
    for col in potential_join_cols:
        print(f"\n📊 Column '{col}' across all CSVs:")
        
        all_values = {}
        for csv_name, df in csvs.items():
            if col in df.columns:
                values = df[col].tolist()
                all_values[csv_name] = values
                print(f"  {csv_name}: {len(values)} values, {len(set(values))} unique")
                
                # Show a few sample values with their types
                sample_values = df[col].head(3).tolist()
                for val in sample_values:
                    print(f"    • {repr(val)} (type: {type(val).__name__})")
        
        # Check for cross-CSV issues
        if len(all_values) >= 2:
            csv_names = list(all_values.keys())
            for i, csv1 in enumerate(csv_names):
                for csv2 in csv_names[i+1:]:
                    common_values = set(all_values[csv1]) & set(all_values[csv2])
                    print(f"  🔗 {csv1} ∩ {csv2}: {len(common_values)} common values")
                    
                    if len(common_values) < 10:  # Show if not too many
                        print(f"    Common: {list(common_values)}")

def simulate_merge_step_by_step(csv_directory: str):
    """Simulate the merge step by step to identify where duplicates occur."""
    
    print("\n" + "=" * 50)
    print("🔄 STEP-BY-STEP MERGE SIMULATION")
    print("=" * 50)
    
    csv_path = Path(csv_directory)
    csv_files = list(csv_path.glob("*.csv"))
    
    # This is a simplified version - you'd need to adapt based on your actual merge logic
    if len(csv_files) >= 2:
        df1 = load_csv_robust(csv_files[0])
        df2 = load_csv_robust(csv_files[1])
        
        if df1 is None or df2 is None:
            print("❌ Failed to load CSV files for merge simulation")
            sys.exit(1)
        
        print(f"📊 Before merge:")
        print(f"  {csv_files[0].stem}: {len(df1)} rows")
        print(f"  {csv_files[1].stem}: {len(df2)} rows")
        
        # Find common columns
        common_cols = list(set(df1.columns) & set(df2.columns))
        
        if common_cols:
            join_col = common_cols[0]  # Use first common column
            print(f"🔗 Merging on: {join_col}")
            
            # Check for duplicates before merge
            print(f"  Duplicates in {csv_files[0].stem}[{join_col}]: {df1[join_col].duplicated().sum()}")
            print(f"  Duplicates in {csv_files[1].stem}[{join_col}]: {df2[join_col].duplicated().sum()}")
            
            # Perform merge
            merged = pd.merge(df1, df2, on=join_col, how='outer', suffixes=('_1', '_2'))
            
            print(f"📊 After merge: {len(merged)} rows")
            
            if len(merged) > len(df1) and len(merged) > len(df2):
                print("⚠️  Merge resulted in MORE rows than either input!")
                print("   This suggests duplicate values in join column(s)")
                
                # Find which values caused the explosion
                value_counts1 = df1[join_col].value_counts()
                value_counts2 = df2[join_col].value_counts()
                
                problematic_values = []
                for val in value_counts1.index:
                    if val in value_counts2.index:
                        if value_counts1[val] > 1 or value_counts2[val] > 1:
                            problematic_values.append((val, value_counts1[val], value_counts2[val]))
                
                if problematic_values:
                    print("🚨 Problematic values (value, count_in_df1, count_in_df2):")
                    for val, count1, count2 in problematic_values:
                        print(f"   • {repr(val)}: {count1} × {count2} = {count1 * count2} result rows")

if __name__ == "__main__":
    csv_dir = input("📁 Enter path to CSV directory: ").strip()
    
    if csv_dir.startswith('"') and csv_dir.endswith('"'):
        csv_dir = csv_dir[1:-1]
    
    debug_merge_duplicates(csv_dir)
    simulate_merge_step_by_step(csv_dir)
    
    print("\n✨ Debug complete!")
    print("\n💡 Common fixes:")
    print("   • Remove duplicate rows before merging")
    print("   • Use .drop_duplicates() on join columns")
    print("   • Check for whitespace: df['col'] = df['col'].str.strip()")
    print("   • Standardize case: df['col'] = df['col'].str.lower()")