"""
ETL Job using Pandas (No PySpark) - Works on Windows
This demonstrates the same logic as the PySpark version but uses pandas.
"""

import pandas as pd
import requests
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("PandasETL")

def get_hash(claim_id):
    """Fetches MD4 hash for a claim_id from external API."""
    if not claim_id:
        return ""
        
    try:
        url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json().get("Digest", "")
        return ""
    except Exception as e:
        logger.warning(f"Error fetching hash for {claim_id}: {e}")
        return ""

def extract_data(claims_path, policyholders_path):
    """Reads the input CSV files."""
    logger.info(f"Reading claims data from {claims_path}")
    claims_df = pd.read_csv(claims_path)
    
    logger.info(f"Reading policyholders data from {policyholders_path}")
    policyholders_df = pd.read_csv(policyholders_path)
    
    return claims_df, policyholders_df

def transform_data(claims_df, policyholders_df):
    """Applies the business transformations."""
    logger.info("Starting data transformation...")
    
    # 1. Fetch hashes using apply (simple UDF equivalent)
    logger.info("Fetching hashes from API...")
    claims_df['hash_id'] = claims_df['claim_id'].apply(get_hash)
    
    # 2. Join with policyholders
    logger.info("Joining with policyholders...")
    merged_df = pd.merge(
        claims_df,
        policyholders_df,
        on='policyholder_id',
        how='left'
    )
    
    # 3. Apply business transformations
    logger.info("Applying business rules...")
    
    # Claim type
    merged_df['claim_type'] = merged_df['claim_id'].apply(
        lambda x: 'Coinsurance' if x.startswith('CL') 
        else ('Reinsurance' if x.startswith('RX') else 'Unknown')
    )
    
    # Claim priority
    merged_df['claim_priority'] = merged_df['claim_amount'].apply(
        lambda x: 'Urgent' if x > 4000 else 'Normal'
    )
    
    # Claim period (convert date to YYYY-MM format)
    merged_df['claim_date'] = pd.to_datetime(merged_df['claim_date'])
    merged_df['claim_period'] = merged_df['claim_date'].dt.strftime('%Y-%m')
    
    # Source system ID
    merged_df['source_system_id'] = merged_df['claim_id'].str.split('_').str[1]
    
    # Select final columns
    final_df = merged_df[[
        'claim_id',
        'policyholder_name',
        'region',
        'claim_type',
        'claim_priority',
        'claim_amount',
        'claim_period',
        'source_system_id',
        'hash_id'
    ]]
    
    logger.info("Transformation complete")
    return final_df

def load_data(df, output_path):
    """Writes the result to a CSV file."""
    logger.info(f"Writing output to {output_path}")
    df.to_csv(output_path, index=False)
    logger.info(f"Successfully wrote {len(df)} rows to {output_path}")

def main():
    logger.info("Starting Pandas ETL job...")
    
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define paths
    claims_path = os.path.join(base_dir, "data", "claims_data.csv")
    policyholders_path = os.path.join(base_dir, "data", "policyholder_data.csv")
    output_path = os.path.join(base_dir, "processed_claims_pandas.csv")
    
    try:
        # Extract
        claims_df, policyholders_df = extract_data(claims_path, policyholders_path)
        logger.info(f"Loaded {len(claims_df)} claims and {len(policyholders_df)} policyholders")
        
        # Transform
        final_df = transform_data(claims_df, policyholders_df)
        
        # Load
        load_data(final_df, output_path)
        
        logger.info("✅ ETL job completed successfully!")
        
        # Display results
        print("\n" + "="*60)
        print("FINAL RESULTS")
        print("="*60)
        print(final_df.to_string(index=False))
        
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise

if __name__ == "__main__":
    main()
