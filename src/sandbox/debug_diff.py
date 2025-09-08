#!/usr/bin/env python3
import boto3
import pandas as pd
import io
import os
import logging
import datetime
import pytz
import sys

# Define JST timezone
JST = pytz.timezone('Asia/Tokyo')

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug_diff.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def debug_dataframe(df, name):
    """Print detailed debug information about a dataframe"""
    logger.info(f"=== DataFrame Debug: {name} ===")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")
    logger.info(f"Column dtypes:\n{df.dtypes}")
    logger.info(f"Index type: {type(df.index)}")
    logger.info(f"Index dtype: {df.index.dtype}")
    logger.info(f"First 5 index values: {df.index[:5].tolist() if len(df) > 0 else []}")
    logger.info(f"Sample data (first 2 rows):\n{df.head(2)}")
    
    # Check for duplicated column names
    dup_cols = df.columns[df.columns.duplicated()].tolist()
    if dup_cols:
        logger.warning(f"WARNING: Duplicated column names found: {dup_cols}")
    
    # Check for null values in the first few rows
    null_counts = df.head(10).isnull().sum()
    logger.info(f"Null counts in first 10 rows:\n{null_counts}")

def debug_comparison(today_df, yesterday_df, name):
    """Debug the comparison between two dataframes"""
    logger.info(f"=== Comparison Debug: {name} ===")
    
    # Compare column names
    today_cols = set(today_df.columns)
    yesterday_cols = set(yesterday_df.columns)
    
    logger.info(f"Columns only in today: {today_cols - yesterday_cols}")
    logger.info(f"Columns only in yesterday: {yesterday_cols - today_cols}")
    logger.info(f"Common columns: {len(today_cols & yesterday_cols)}")
    
    # Check dtypes of common columns
    common_cols = today_cols & yesterday_cols
    logger.info("Type comparison for common columns:")
    for col in common_cols:
        today_type = today_df[col].dtype
        yesterday_type = yesterday_df[col].dtype
        if today_type != yesterday_type:
            logger.warning(f"Column '{col}' has different types: today={today_type}, yesterday={yesterday_type}")

def clean_dataframe(df):
    """Clean and standardize dataframe columns and values"""
    logger.info("Cleaning dataframe...")
    
    # First, log pre-cleaning state
    logger.info(f"Pre-cleaning shape: {df.shape}")
    logger.info(f"Pre-cleaning dtypes sample: {df.dtypes.head()}")
    
    # Make a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'object':  # String columns
            logger.debug(f"Cleaning string column: {col}")
            
            # Replace empty strings with NaN
            empty_count = (cleaned_df[col] == '').sum()
            if empty_count > 0:
                logger.debug(f"  - Found {empty_count} empty strings in column '{col}'")
            cleaned_df[col] = cleaned_df[col].replace('', pd.NA)
            
            # Strip whitespace
            if not pd.api.types.is_numeric_dtype(cleaned_df[col]):
                try:
                    cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
                    logger.debug(f"  - Stripped whitespace in column '{col}'")
                except Exception as e:
                    logger.error(f"  - Error stripping whitespace in column '{col}': {e}")
            
            # Replace 'nan' strings with NaN
            nan_count = (cleaned_df[col] == 'nan').sum()
            if nan_count > 0:
                logger.debug(f"  - Found {nan_count} 'nan' strings in column '{col}'")
            cleaned_df[col] = cleaned_df[col].replace('nan', pd.NA)
            
            # Replace whitespace-only strings with NaN
            try:
                whitespace_mask = cleaned_df[col].astype(str).str.match(r'^\s*$')
                whitespace_count = whitespace_mask.sum()
                if whitespace_count > 0:
                    logger.debug(f"  - Found {whitespace_count} whitespace-only strings in column '{col}'")
                cleaned_df.loc[whitespace_mask, col] = pd.NA
            except Exception as e:
                logger.error(f"  - Error replacing whitespace-only strings in column '{col}': {e}")
    
    # Log post-cleaning state
    logger.info(f"Post-cleaning shape: {cleaned_df.shape}")
    logger.info(f"Post-cleaning dtypes sample: {cleaned_df.dtypes.head()}")
    
    return cleaned_df

def debug_buildings_diff(today_str, yesterday_str):
    """Debug the buildings diff function"""
    logger.info(f"=== Debugging buildings_diff: {today_str} vs {yesterday_str} ===")
    
    # S3 bucket and prefix
    bucket_name = "adi-external-integration"
    prefix = "pallet-cloud/prod/"
    
    # File paths
    today_file = f"{today_str}_PC_buildings.csv"
    yesterday_file = f"{yesterday_str}_PC_buildings.csv"
    
    today_s3_path = f"{prefix}{today_file}"
    yesterday_s3_path = f"{prefix}{yesterday_file}"
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Download and read today's file
        logger.info(f"Downloading today's file: {today_s3_path}")
        today_response = s3_client.get_object(Bucket=bucket_name, Key=today_s3_path)
        today_content = today_response['Body'].read()
        
        logger.info(f"Reading today's file: size={len(today_content)} bytes")
        today_df = pd.read_csv(io.BytesIO(today_content), encoding='utf-8', 
                             keep_default_na=True, na_values=['', 'NULL', 'null', 'None'])
        
        # Download and read yesterday's file
        logger.info(f"Downloading yesterday's file: {yesterday_s3_path}")
        yesterday_response = s3_client.get_object(Bucket=bucket_name, Key=yesterday_s3_path)
        yesterday_content = yesterday_response['Body'].read()
        
        logger.info(f"Reading yesterday's file: size={len(yesterday_content)} bytes")
        yesterday_df = pd.read_csv(io.BytesIO(yesterday_content), encoding='utf-8',
                                  keep_default_na=True, na_values=['', 'NULL', 'null', 'None'])
        
        # Debug raw dataframes
        logger.info("Debugging raw dataframes")
        debug_dataframe(today_df, "Today Raw")
        debug_dataframe(yesterday_df, "Yesterday Raw")
        debug_comparison(today_df, yesterday_df, "Raw Comparison")
        
        # Clean dataframes
        logger.info("Cleaning dataframes")
        today_df_clean = clean_dataframe(today_df)
        yesterday_df_clean = clean_dataframe(yesterday_df)
        
        # Debug cleaned dataframes
        logger.info("Debugging cleaned dataframes")
        debug_dataframe(today_df_clean, "Today Cleaned")
        debug_dataframe(yesterday_df_clean, "Yesterday Cleaned")
        debug_comparison(today_df_clean, yesterday_df_clean, "Cleaned Comparison")
        
        # Save original column order
        original_columns = today_df_clean.columns.tolist()
        logger.info(f"Original column order: {original_columns}")
        
        # Primary key (2nd column for buildings)
        if len(today_df_clean.columns) > 1:
            primary_key = today_df_clean.columns[1]
            logger.info(f"Using primary key: {primary_key} (index 1)")
            
            try:
                # Check for duplicates in primary key
                today_dup = today_df_clean[primary_key].duplicated().sum()
                yesterday_dup = yesterday_df_clean[primary_key].duplicated().sum()
                
                logger.info(f"Duplicate primary keys - Today: {today_dup}, Yesterday: {yesterday_dup}")
                
                if today_dup > 0:
                    logger.warning(f"WARNING: Today's file has {today_dup} duplicate keys")
                    dup_values = today_df_clean[today_df_clean[primary_key].duplicated(keep=False)][primary_key].unique()
                    logger.warning(f"Duplicate values: {dup_values[:10]}")
                
                if yesterday_dup > 0:
                    logger.warning(f"WARNING: Yesterday's file has {yesterday_dup} duplicate keys")
                    dup_values = yesterday_df_clean[yesterday_df_clean[primary_key].duplicated(keep=False)][primary_key].unique()
                    logger.warning(f"Duplicate values: {dup_values[:10]}")
                
                # Set index using primary key
                logger.info("Setting index on both dataframes")
                try:
                    today_df_indexed = today_df_clean.set_index(primary_key)
                    logger.info("Successfully set index on today's dataframe")
                except Exception as e:
                    logger.error(f"Error setting index on today's dataframe: {e}")
                    # Try to deduplicate and then set index
                    logger.info("Attempting to deduplicate today's dataframe")
                    today_df_clean = today_df_clean.drop_duplicates(subset=[primary_key], keep='first')
                    today_df_indexed = today_df_clean.set_index(primary_key)
                
                try:
                    yesterday_df_indexed = yesterday_df_clean.set_index(primary_key)
                    logger.info("Successfully set index on yesterday's dataframe")
                except Exception as e:
                    logger.error(f"Error setting index on yesterday's dataframe: {e}")
                    # Try to deduplicate and then set index
                    logger.info("Attempting to deduplicate yesterday's dataframe")
                    yesterday_df_clean = yesterday_df_clean.drop_duplicates(subset=[primary_key], keep='first')
                    yesterday_df_indexed = yesterday_df_clean.set_index(primary_key)
                
                # Debug indexed dataframes
                logger.info("Debugging indexed dataframes")
                debug_dataframe(today_df_indexed, "Today Indexed")
                debug_dataframe(yesterday_df_indexed, "Yesterday Indexed")
                
                # Compare sets of keys
                today_keys = set(today_df_indexed.index)
                yesterday_keys = set(yesterday_df_indexed.index)
                
                only_today_keys = today_keys - yesterday_keys
                only_yesterday_keys = yesterday_keys - today_keys
                common_keys = today_keys & yesterday_keys
                
                logger.info(f"Keys only in today: {len(only_today_keys)}")
                logger.info(f"Keys only in yesterday: {len(only_yesterday_keys)}")
                logger.info(f"Common keys: {len(common_keys)}")
                
                # Debug sample of keys
                if only_today_keys:
                    logger.info(f"Sample keys only in today: {list(only_today_keys)[:5]}")
                if only_yesterday_keys:
                    logger.info(f"Sample keys only in yesterday: {list(only_yesterday_keys)[:5]}")
                if common_keys:
                    logger.info(f"Sample common keys: {list(common_keys)[:5]}")
                
                # Extract rows that are only in today
                if only_today_keys:
                    only_in_today = today_df_indexed.loc[list(only_today_keys)]
                    logger.info(f"Extracted {len(only_in_today)} rows that are only in today's file")
                else:
                    only_in_today = pd.DataFrame()
                    logger.info("No rows found that are only in today's file")
                
                # Process common keys
                if common_keys:
                    logger.info(f"Processing {len(common_keys)} common keys")
                    
                    # Extract common data
                    try:
                        today_common = today_df_indexed.loc[list(common_keys)]
                        logger.info(f"Extracted today's common data: {today_common.shape}")
                    except Exception as e:
                        logger.error(f"Error extracting today's common data: {e}")
                        # Try a different approach
                        today_common = today_df_indexed[today_df_indexed.index.isin(common_keys)]
                        logger.info(f"Extracted today's common data (alternative method): {today_common.shape}")
                    
                    try:
                        yesterday_common = yesterday_df_indexed.loc[list(common_keys)]
                        logger.info(f"Extracted yesterday's common data: {yesterday_common.shape}")
                    except Exception as e:
                        logger.error(f"Error extracting yesterday's common data: {e}")
                        # Try a different approach
                        yesterday_common = yesterday_df_indexed[yesterday_df_indexed.index.isin(common_keys)]
                        logger.info(f"Extracted yesterday's common data (alternative method): {yesterday_common.shape}")
                    
                    # Debug common data
                    logger.info("Debugging common data")
                    debug_dataframe(today_common, "Today Common")
                    debug_dataframe(yesterday_common, "Yesterday Common")
                    debug_comparison(today_common, yesterday_common, "Common Comparison")
                    
                    # Ensure column alignment before comparison
                    common_columns = list(set(today_common.columns) & set(yesterday_common.columns))
                    logger.info(f"Using {len(common_columns)} common columns for comparison")
                    
                    if common_columns:
                        # Exclude specified columns
                        exclude_indices = [60, 61]  # As per original code
                        include_columns = [col for i, col in enumerate(common_columns) if i not in exclude_indices]
                        logger.info(f"Excluding columns at indices {exclude_indices}")
                        logger.info(f"Using {len(include_columns)} columns after exclusion")
                        
                        # Try to compare column by column to find differences
                        changed_indices = set()
                        
                        for col in include_columns:
                            if col in today_common.columns and col in yesterday_common.columns:
                                logger.info(f"Comparing column: {col}")
                                
                                try:
                                    today_vals = today_common[col]
                                    yesterday_vals = yesterday_common[col]
                                    
                                    # Log data types
                                    logger.info(f"  Today dtype: {today_vals.dtype}, Yesterday dtype: {yesterday_vals.dtype}")
                                    
                                    # Check for mixed types
                                    if today_vals.dtype != yesterday_vals.dtype:
                                        logger.warning(f"  WARNING: Different dtypes for column {col}")
                                    
                                    # Validate index alignment
                                    if not today_vals.index.equals(yesterday_vals.index):
                                        logger.error(f"  ERROR: Index mismatch for column {col}")
                                        # Try to align indexes
                                        logger.info("  Attempting to align indexes...")
                                        common_idx = today_vals.index.intersection(yesterday_vals.index)
                                        today_vals = today_vals.loc[common_idx]
                                        yesterday_vals = yesterday_vals.loc[common_idx]
                                        logger.info(f"  After alignment: {len(today_vals)} rows")
                                    
                                    # Compare values based on type
                                    if pd.api.types.is_numeric_dtype(today_vals):
                                        logger.info("  Comparing as numeric column")
                                        # Numeric comparison
                                        both_not_nan = pd.notna(today_vals) & pd.notna(yesterday_vals)
                                        diff_mask = both_not_nan & (abs(today_vals - yesterday_vals) >= 1)
                                        nan_diff_mask = today_vals.isna() != yesterday_vals.isna()
                                        combined_mask = diff_mask | nan_diff_mask
                                    else:
                                        logger.info("  Comparing as string column")
                                        # String comparison
                                        both_nan = pd.isna(today_vals) & pd.isna(yesterday_vals)
                                        both_not_nan = pd.notna(today_vals) & pd.notna(yesterday_vals)
                                        one_nan = pd.isna(today_vals) != pd.isna(yesterday_vals)
                                        
                                        # Careful string comparison
                                        try:
                                            value_diff = both_not_nan & (today_vals.astype(str) != yesterday_vals.astype(str))
                                            combined_mask = value_diff | one_nan
                                        except Exception as e:
                                            logger.error(f"  ERROR in string comparison: {e}")
                                            # Sample problem values
                                            logger.error("  Sample today values: " + str(today_vals.head().tolist()))
                                            logger.error("  Sample yesterday values: " + str(yesterday_vals.head().tolist()))
                                            continue
                                    
                                    # Log differences found
                                    different_keys = combined_mask[combined_mask].index.tolist()
                                    if different_keys:
                                        logger.info(f"  Found {len(different_keys)} differences")
                                        changed_indices.update(different_keys)
                                        
                                        # Sample differences
                                        for key in different_keys[:3]:
                                            t_val = today_vals.loc[key] if key in today_vals.index else 'KEY_NOT_FOUND'
                                            y_val = yesterday_vals.loc[key] if key in yesterday_vals.index else 'KEY_NOT_FOUND'
                                            logger.info(f"  Sample diff - Key: {key}, Today: {t_val}, Yesterday: {y_val}")
                                    else:
                                        logger.info("  No differences found")
                                
                                except Exception as e:
                                    logger.error(f"Error comparing column {col}: {e}")
                        
                        logger.info(f"Total changed indices: {len(changed_indices)}")
                        
                        # Extract changed rows
                        if changed_indices:
                            try:
                                changed_rows = today_df_indexed.loc[list(changed_indices)]
                                logger.info(f"Extracted {len(changed_rows)} changed rows")
                            except Exception as e:
                                logger.error(f"Error extracting changed rows: {e}")
                                changed_rows = pd.DataFrame()
                        else:
                            changed_rows = pd.DataFrame()
                            logger.info("No changed rows found")
                        
                        # Combine results
                        diff_frames = []
                        
                        if not only_in_today.empty:
                            try:
                                only_in_today_reset = only_in_today.reset_index()
                                only_in_today_ordered = only_in_today_reset[original_columns]
                                diff_frames.append(only_in_today_ordered)
                                logger.info(f"Added {len(only_in_today)} new rows to diff")
                            except Exception as e:
                                logger.error(f"Error processing only_in_today rows: {e}")
                        
                        if not changed_rows.empty:
                            try:
                                changed_rows_reset = changed_rows.reset_index()
                                changed_rows_ordered = changed_rows_reset[original_columns]
                                diff_frames.append(changed_rows_ordered)
                                logger.info(f"Added {len(changed_rows)} changed rows to diff")
                            except Exception as e:
                                logger.error(f"Error processing changed rows: {e}")
                        
                        if diff_frames:
                            try:
                                diff_df = pd.concat(diff_frames, ignore_index=True)
                                logger.info(f"Final diff has {len(diff_df)} rows")
                            except Exception as e:
                                logger.error(f"Error concatenating diff frames: {e}")
                                diff_df = pd.DataFrame()
                        else:
                            diff_df = pd.DataFrame()
                            logger.info("No differences found")
                        
                        # Save diff to CSV for inspection
                        if not diff_df.empty:
                            output_dir = f"./debug_output"
                            os.makedirs(output_dir, exist_ok=True)
                            output_path = f"{output_dir}/buildings_diff_{today_str}.csv"
                            diff_df.to_csv(output_path, index=False)
                            logger.info(f"Saved diff to {output_path}")
                    else:
                        logger.error("No common columns found between dataframes")
                else:
                    logger.info("No common keys found between today and yesterday")
            except Exception as e:
                logger.error(f"Error during diff processing: {e}")
        else:
            logger.error("Not enough columns in the dataframe")
    
    except Exception as e:
        logger.error(f"Error: {e}")

def debug_rooms_diff(today_str, yesterday_str):
    """Debug the rooms diff function"""
    logger.info(f"=== Debugging rooms_diff: {today_str} vs {yesterday_str} ===")
    
    # S3 bucket and prefix
    bucket_name = "adi-external-integration"
    prefix = "pallet-cloud/prod/"
    
    # File paths
    today_file = f"{today_str}_PC_rooms.csv"
    yesterday_file = f"{yesterday_str}_PC_rooms.csv"
    
    today_s3_path = f"{prefix}{today_file}"
    yesterday_s3_path = f"{prefix}{yesterday_file}"
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Download and read today's file
        logger.info(f"Downloading today's file: {today_s3_path}")
        today_response = s3_client.get_object(Bucket=bucket_name, Key=today_s3_path)
        today_content = today_response['Body'].read()
        
        logger.info(f"Reading today's file: size={len(today_content)} bytes")
        today_df = pd.read_csv(io.BytesIO(today_content), encoding='utf-8', 
                             keep_default_na=True, na_values=['', 'NULL', 'null', 'None'])
        
        # Download and read yesterday's file
        logger.info(f"Downloading yesterday's file: {yesterday_s3_path}")
        yesterday_response = s3_client.get_object(Bucket=bucket_name, Key=yesterday_s3_path)
        yesterday_content = yesterday_response['Body'].read()
        
        logger.info(f"Reading yesterday's file: size={len(yesterday_content)} bytes")
        yesterday_df = pd.read_csv(io.BytesIO(yesterday_content), encoding='utf-8',
                                  keep_default_na=True, na_values=['', 'NULL', 'null', 'None'])
        
        # Debug raw dataframes
        logger.info("Debugging raw dataframes")
        debug_dataframe(today_df, "Today Raw")
        debug_dataframe(yesterday_df, "Yesterday Raw")
        debug_comparison(today_df, yesterday_df, "Raw Comparison")
        
        # Clean dataframes
        logger.info("Cleaning dataframes")
        today_df_clean = clean_dataframe(today_df)
        yesterday_df_clean = clean_dataframe(yesterday_df)
        
        # Debug cleaned dataframes
        logger.info("Debugging cleaned dataframes")
        debug_dataframe(today_df_clean, "Today Cleaned")
        debug_dataframe(yesterday_df_clean, "Yesterday Cleaned")
        debug_comparison(today_df_clean, yesterday_df_clean, "Cleaned Comparison")
        
        # Save original column order
        original_columns = today_df_clean.columns.tolist()
        logger.info(f"Original column order (first 5): {original_columns[:5]}")
        
        # Primary key (32nd column for rooms, index 31)
        if len(today_df_clean.columns) > 31:
            primary_key = today_df_clean.columns[31]
            logger.info(f"Using primary key: {primary_key} (index 31)")
            
            try:
                # Check for duplicates in primary key
                today_dup = today_df_clean[primary_key].duplicated().sum()
                yesterday_dup = yesterday_df_clean[primary_key].duplicated().sum()
                
                logger.info(f"Duplicate primary keys - Today: {today_dup}, Yesterday: {yesterday_dup}")
                
                if today_dup > 0:
                    logger.warning(f"WARNING: Today's file has {today_dup} duplicate keys")
                    dup_values = today_df_clean[today_df_clean[primary_key].duplicated(keep=False)][primary_key].unique()
                    logger.warning(f"Duplicate values: {dup_values[:10]}")
                
                if yesterday_dup > 0:
                    logger.warning(f"WARNING: Yesterday's file has {yesterday_dup} duplicate keys")
                    dup_values = yesterday_df_clean[yesterday_df_clean[primary_key].duplicated(keep=False)][primary_key].unique()
                    logger.warning(f"Duplicate values: {dup_values[:10]}")
                
                # Set index using primary key
                logger.info("Setting index on both dataframes")
                try:
                    today_df_indexed = today_df_clean.set_index(primary_key)
                    logger.info("Successfully set index on today's dataframe")
                except Exception as e:
                    logger.error(f"Error setting index on today's dataframe: {e}")
                    # Try to deduplicate and then set index
                    logger.info("Attempting to deduplicate today's dataframe")
                    today_df_clean = today_df_clean.drop_duplicates(subset=[primary_key], keep='first')
                    today_df_indexed = today_df_clean.set_index(primary_key)
                
                try:
                    yesterday_df_indexed = yesterday_df_clean.set_index(primary_key)
                    logger.info("Successfully set index on yesterday's dataframe")
                except Exception as e:
                    logger.error(f"Error setting index on yesterday's dataframe: {e}")
                    # Try to deduplicate and then set index
                    logger.info("Attempting to deduplicate yesterday's dataframe")
                    yesterday_df_clean = yesterday_df_clean.drop_duplicates(subset=[primary_key], keep='first')
                    yesterday_df_indexed = yesterday_df_clean.set_index(primary_key)
                
                # Debug indexed dataframes
                logger.info("Debugging indexed dataframes")
                debug_dataframe(today_df_indexed, "Today Indexed")
                debug_dataframe(yesterday_df_indexed, "Yesterday Indexed")
                
                # Compare sets of keys
                today_keys = set(today_df_indexed.index)
                yesterday_keys = set(yesterday_df_indexed.index)
                
                only_today_keys = today_keys - yesterday_keys
                only_yesterday_keys = yesterday_keys - today_keys
                common_keys = today_keys & yesterday_keys
                
                logger.info(f"Keys only in today: {len(only_today_keys)}")
                logger.info(f"Keys only in yesterday: {len(only_yesterday_keys)}")
                logger.info(f"Common keys: {len(common_keys)}")
                
                # Debug sample of keys
                if only_today_keys:
                    logger.info(f"Sample keys only in today: {list(only_today_keys)[:5]}")
                if only_yesterday_keys:
                    logger.info(f"Sample keys only in yesterday: {list(only_yesterday_keys)[:5]}")
                if common_keys:
                    logger.info(f"Sample common keys: {list(common_keys)[:5]}")
                
                # Extract rows that are only in today
                if only_today_keys:
                    try:
                        only_in_today = today_df_indexed.loc[list(only_today_keys)]
                        logger.info(f"Extracted {len(only_in_today)} rows that are only in today's file")
                    except Exception as e:
                        logger.error(f"Error extracting only_in_today rows: {e}")
                        only_in_today = pd.DataFrame()
                else:
                    only_in_today = pd.DataFrame()
                    logger.info("No rows found that are only in today's file")
                
                # Process common keys
                if common_keys:
                    logger.info(f"Processing {len(common_keys)} common keys")
                    
                    # Extract common data
                    try:
                        today_common = today_df_indexed.loc[list(common_keys)]
                        logger.info(f"Extracted today's common data: {today_common.shape}")
                    except Exception as e:
                        logger.error(f"Error extracting today's common data: {e}")
                        # Try a different approach
                        today_common = today_df_indexed[today_df_indexed.index.isin(common_keys)]
                        logger.info(f"Extracted today's common data (alternative method): {today_common.shape}")
                    
                    try:
                        yesterday_common = yesterday_df_indexed.loc[list(common_keys)]
                        logger.info(f"Extracted yesterday's common data: {yesterday_common.shape}")
                    except Exception as e:
                        logger.error(f"Error extracting yesterday's common data: {e}")
                        # Try a different approach
                        yesterday_common = yesterday_df_indexed[yesterday_df_indexed.index.isin(common_keys)]
                        logger.info(f"Extracted yesterday's common data (alternative method): {yesterday_common.shape}")
                    
                    # Debug common data
                    logger.info("Debugging common data")
                    debug_dataframe(today_common, "Today Common")
                    debug_dataframe(yesterday_common, "Yesterday Common")
                    debug_comparison(today_common, yesterday_common, "Common Comparison")
                    
                    # Ensure column alignment before comparison
                    common_columns = list(set(today_common.columns) & set(yesterday_common.columns))
                    logger.info(f"Using {len(common_columns)} common columns for comparison")
                    
                    if common_columns:
                        # Exclude specified columns
                        exclude_indices = [19]  # As per original code
                        include_columns = [col for i, col in enumerate(common_columns) if i not in exclude_indices]
                        logger.info(f"Excluding columns at indices {exclude_indices}")
                        logger.info(f"Using {len(include_columns)} columns after exclusion")
                        
                        # Try to compare column by column to find differences
                        changed_indices = set()
                        
                        for col in include_columns:
                            if col in today_common.columns and col in yesterday_common.columns:
                                logger.info(f"Comparing column: {col}")
                                
                                try:
                                    today_vals = today_common[col]
                                    yesterday_vals = yesterday_common[col]
                                    
                                    # Log data types
                                    logger.info(f"  Today dtype: {today_vals.dtype}, Yesterday dtype: {yesterday_vals.dtype}")
                                    
                                    # Check for mixed types
                                    if today_vals.dtype != yesterday_vals.dtype:
                                        logger.warning(f"  WARNING: Different dtypes for column {col}")
                                    
                                    # Validate index alignment
                                    if not today_vals.index.equals(yesterday_vals.index):
                                        logger.error(f"  ERROR: Index mismatch for column {col}")
                                        # Try to align indexes
                                        logger.info("  Attempting to align indexes...")
                                        common_idx = today_vals.index.intersection(yesterday_vals.index)
                                        today_vals = today_vals.loc[common_idx]
                                        yesterday_vals = yesterday_vals.loc[common_idx]
                                        logger.info(f"  After alignment: {len(today_vals)} rows")
                                    
                                    # Compare values based on type
                                    if pd.api.types.is_numeric_dtype(today_vals):
                                        logger.info("  Comparing as numeric column")
                                        # Numeric comparison
                                        both_not_nan = pd.notna(today_vals) & pd.notna(yesterday_vals)
                                        diff_mask = both_not_nan & (abs(today_vals - yesterday_vals) >= 1)
                                        nan_diff_mask = today_vals.isna() != yesterday_vals.isna()
                                        combined_mask = diff_mask | nan_diff_mask
                                    else:
                                        logger.info("  Comparing as string column")
                                        # String comparison
                                        both_nan = pd.isna(today_vals) & pd.isna(yesterday_vals)
                                        both_not_nan = pd.notna(today_vals) & pd.notna(yesterday_vals)
                                        one_nan = pd.isna(today_vals) != pd.isna(yesterday_vals)
                                        
                                        # Careful string comparison
                                        try:
                                            value_diff = both_not_nan & (today_vals.astype(str) != yesterday_vals.astype(str))
                                            combined_mask = value_diff | one_nan
                                        except Exception as e:
                                            logger.error(f"  ERROR in string comparison: {e}")
                                            # Sample problem values
                                            logger.error("  Sample today values: " + str(today_vals.head().tolist()))
                                            logger.error("  Sample yesterday values: " + str(yesterday_vals.head().tolist()))
                                            continue
                                    
                                    # Log differences found
                                    different_keys = combined_mask[combined_mask].index.tolist()
                                    if different_keys:
                                        logger.info(f"  Found {len(different_keys)} differences")
                                        changed_indices.update(different_keys)
                                        
                                        # Sample differences
                                        for key in different_keys[:3]:
                                            t_val = today_vals.loc[key] if key in today_vals.index else 'KEY_NOT_FOUND'
                                            y_val = yesterday_vals.loc[key] if key in yesterday_vals.index else 'KEY_NOT_FOUND'
                                            logger.info(f"  Sample diff - Key: {key}, Today: {t_val}, Yesterday: {y_val}")
                                    else:
                                        logger.info("  No differences found")
                                
                                except Exception as e:
                                    logger.error(f"Error comparing column {col}: {e}")
                        
                        logger.info(f"Total changed indices: {len(changed_indices)}")
                        
                        # Extract changed rows
                        if changed_indices:
                            try:
                                changed_rows = today_df_indexed.loc[list(changed_indices)]
                                logger.info(f"Extracted {len(changed_rows)} changed rows")
                            except Exception as e:
                                logger.error(f"Error extracting changed rows: {e}")
                                changed_rows = pd.DataFrame()
                        else:
                            changed_rows = pd.DataFrame()
                            logger.info("No changed rows found")
                        
                        # Combine results
                        diff_frames = []
                        
                        if not only_in_today.empty:
                            try:
                                only_in_today_reset = only_in_today.reset_index()
                                only_in_today_ordered = only_in_today_reset[original_columns]
                                diff_frames.append(only_in_today_ordered)
                                logger.info(f"Added {len(only_in_today)} new rows to diff")
                            except Exception as e:
                                logger.error(f"Error processing only_in_today rows: {e}")
                        
                        if not changed_rows.empty:
                            try:
                                changed_rows_reset = changed_rows.reset_index()
                                changed_rows_ordered = changed_rows_reset[original_columns]
                                diff_frames.append(changed_rows_ordered)
                                logger.info(f"Added {len(changed_rows)} changed rows to diff")
                            except Exception as e:
                                logger.error(f"Error processing changed rows: {e}")
                        
                        if diff_frames:
                            try:
                                diff_df = pd.concat(diff_frames, ignore_index=True)
                                logger.info(f"Final diff has {len(diff_df)} rows")
                            except Exception as e:
                                logger.error(f"Error concatenating diff frames: {e}")
                                diff_df = pd.DataFrame()
                        else:
                            diff_df = pd.DataFrame()
                            logger.info("No differences found")
                        
                        # Save diff to CSV for inspection
                        if not diff_df.empty:
                            output_dir = f"./debug_output"
                            os.makedirs(output_dir, exist_ok=True)
                            output_path = f"{output_dir}/rooms_diff_{today_str}.csv"
                            diff_df.to_csv(output_path, index=False)
                            logger.info(f"Saved diff to {output_path}")
                    else:
                        logger.error("No common columns found between dataframes")
                else:
                    logger.info("No common keys found between today and yesterday")
            except Exception as e:
                logger.error(f"Error during diff processing: {e}")
        else:
            logger.error("Not enough columns in the dataframe")
    
    except Exception as e:
        logger.error(f"Error: {e}")

def try_alternative_approach_rooms(today_str, yesterday_str):
    """Try an alternative approach for rooms diff that might work around the issue"""
    logger.info(f"=== Trying alternative approach for rooms_diff: {today_str} vs {yesterday_str} ===")
    
    # S3 bucket and prefix
    bucket_name = "adi-external-integration"
    prefix = "pallet-cloud/prod/"
    
    # File paths
    today_file = f"{today_str}_PC_rooms.csv"
    yesterday_file = f"{yesterday_str}_PC_rooms.csv"
    
    today_s3_path = f"{prefix}{today_file}"
    yesterday_s3_path = f"{prefix}{yesterday_file}"
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Download files
        logger.info(f"Downloading today's file: {today_s3_path}")
        today_response = s3_client.get_object(Bucket=bucket_name, Key=today_s3_path)
        today_content = today_response['Body'].read()
        
        logger.info(f"Downloading yesterday's file: {yesterday_s3_path}")
        yesterday_response = s3_client.get_object(Bucket=bucket_name, Key=yesterday_s3_path)
        yesterday_content = yesterday_response['Body'].read()
        
        # Read files
        logger.info("Reading files with strict option")
        today_df = pd.read_csv(io.BytesIO(today_content), encoding='utf-8', 
                             keep_default_na=True, na_values=['', 'NULL', 'null', 'None'])
        yesterday_df = pd.read_csv(io.BytesIO(yesterday_content), encoding='utf-8',
                                  keep_default_na=True, na_values=['', 'NULL', 'null', 'None'])
        
        # Ensure column names are strings
        today_df.columns = today_df.columns.astype(str)
        yesterday_df.columns = yesterday_df.columns.astype(str)
        
        # Find primary key column (32nd column)
        if len(today_df.columns) > 31 and len(yesterday_df.columns) > 31:
            primary_key = today_df.columns[31]
            logger.info(f"Primary key column: {primary_key}")
            
            # Clean and convert primary key to string
            today_df[primary_key] = today_df[primary_key].astype(str).str.strip()
            yesterday_df[primary_key] = yesterday_df[primary_key].astype(str).str.strip()
            
            # Make sure there are no duplicates
            today_df = today_df.drop_duplicates(subset=[primary_key], keep='first')
            yesterday_df = yesterday_df.drop_duplicates(subset=[primary_key], keep='first')
            
            # Find new rows (only in today)
            today_keys = set(today_df[primary_key])
            yesterday_keys = set(yesterday_df[primary_key])
            
            only_today_keys = today_keys - yesterday_keys
            common_keys = today_keys & yesterday_keys
            
            logger.info(f"New rows: {len(only_today_keys)}")
            logger.info(f"Common rows: {len(common_keys)}")
            
            # Extract new rows
            only_in_today = today_df[today_df[primary_key].isin(only_today_keys)]
            logger.info(f"Extracted {len(only_in_today)} new rows")
            
            # Create a unique ID to merge on (convert to string and strip)
            logger.info("Creating modified dataframes for comparison")
            
            # Create copies to avoid modifying originals
            today_mod = today_df.copy()
            yesterday_mod = yesterday_df.copy()
            
            # Filter to only common keys
            today_mod = today_mod[today_mod[primary_key].isin(common_keys)]
            yesterday_mod = yesterday_mod[yesterday_mod[primary_key].isin(common_keys)]
            
            # Create a hash column from all columns except the excluded one
            logger.info("Creating hash columns for comparison")
            
            # Get column indices to exclude (19)
            exclude_col = today_mod.columns[19]
            logger.info(f"Excluding column for comparison: {exclude_col}")
            
            # Create hash columns
            def create_hash(row):
                values = []
                for col in row.index:
                    if col != exclude_col and col != primary_key:
                        val = str(row[col]) if pd.notna(row[col]) else "NA"
                        values.append(val)
                return hash(tuple(values))
            
            try:
                today_mod['_hash'] = today_mod.apply(create_hash, axis=1)
                yesterday_mod['_hash'] = yesterday_mod.apply(create_hash, axis=1)
                
                logger.info("Successfully created hash columns")
                
                # Merge the dataframes on primary key
                logger.info("Merging dataframes to find differences")
                merged = pd.merge(today_mod[[primary_key, '_hash']], 
                                yesterday_mod[[primary_key, '_hash']], 
                                on=primary_key, 
                                suffixes=('_today', '_yesterday'))
                
                logger.info(f"Merged dataframe shape: {merged.shape}")
                
                # Find rows where the hash values differ
                diff_mask = merged['_hash_today'] != merged['_hash_yesterday']
                diff_keys = merged[diff_mask][primary_key].tolist()
                
                logger.info(f"Found {len(diff_keys)} rows with differences")
                
                # Extract changed rows from original dataframe
                changed_rows = today_df[today_df[primary_key].isin(diff_keys)]
                logger.info(f"Extracted {len(changed_rows)} changed rows")
                
                # Combine new and changed rows
                diff_df = pd.concat([only_in_today, changed_rows], ignore_index=True)
                logger.info(f"Final diff has {len(diff_df)} rows")
                
                # Save diff to CSV for inspection
                if not diff_df.empty:
                    output_dir = f"./debug_output"
                    os.makedirs(output_dir, exist_ok=True)
                    output_path = f"{output_dir}/rooms_diff_alt_{today_str}.csv"
                    diff_df.to_csv(output_path, index=False)
                    logger.info(f"Saved alternative diff to {output_path}")
                
            except Exception as e:
                logger.error(f"Error in alternative approach: {e}")
        else:
            logger.error("Not enough columns in the dataframe for alternative approach")
    
    except Exception as e:
        logger.error(f"Error in alternative approach: {e}")

def main():
    """Main function to execute the debug"""
    if len(sys.argv) != 3:
        print("Usage: python debug_diff.py [today_date] [yesterday_date]")
        print("Example: python debug_diff.py 20230823 20230822")
        sys.exit(1)
    
    today_str = sys.argv[1]
    yesterday_str = sys.argv[2]
    
    logger.info(f"Starting debug for dates: {today_str} vs {yesterday_str}")
    logger.info("=" * 80)
    
    # Debug buildings diff
    logger.info("Starting buildings diff debug")
    debug_buildings_diff(today_str, yesterday_str)
    
    logger.info("=" * 80)
    
    # Debug rooms diff
    logger.info("Starting rooms diff debug")
    debug_rooms_diff(today_str, yesterday_str)
    
    logger.info("=" * 80)
    
    # Try alternative approach for rooms
    logger.info("Trying alternative approach for rooms diff")
    try_alternative_approach_rooms(today_str, yesterday_str)
    
    logger.info("=" * 80)
    logger.info("Debug completed")

if __name__ == "__main__":
    main()