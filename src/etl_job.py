from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, substring, split, date_format, lit, md5
from pyspark.sql.types import StringType
import logging
import requests
import json
import tempfile
import csv
import os

logger = logging.getLogger("ClaimsETL")

def create_spark_session(config=None):
    """Creates and returns a SparkSession with optional config."""
    logger.info("Creating Spark Session...")
    builder = SparkSession.builder
    
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
            
    # Set default app name if not in config, though config usually has it
    if not config or "spark.app.name" not in config:
        builder = builder.appName("ClaimsProcessing")
        
    return builder.getOrCreate()

def extract_data(spark, claims_path, policyholders_path):
    """Reads the input CSV files."""
    logger.info(f"Reading claims data from {claims_path}")
    claims_df = spark.read.csv(claims_path, header=True, inferSchema=True)
    
    logger.info(f"Reading policyholders data from {policyholders_path}")
    policyholders_df = spark.read.csv(policyholders_path, header=True, inferSchema=True)
    
    return claims_df, policyholders_df

def fetch_hashes_for_claims(claim_ids):
    """
    Fetches MD4 hashes for a list of claim_ids from an external API.
    Returns a list of tuples: (claim_id, hash_id)
    """
    logger.info(f"Fetching hashes for {len(claim_ids)} unique claims...")
    results = []
    # In a real scenario, we might batch these requests if the list is long
    # or use an endpoint that accepts multiple IDs.
    # For now, we'll iterate but on the driver side.
    
    # Using a session for connection pooling
    session = requests.Session()
    
    for i, claim_id in enumerate(claim_ids):
        if i % 10 == 0:
            logger.info(f"Fetched {i}/{len(claim_ids)} hashes...")
            
        try:
            url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                hash_val = response.json().get("Digest", "")
                results.append((claim_id, hash_val))
            else:
                results.append((claim_id, ""))
        except Exception as e:
            logger.error(f"Error fetching hash for {claim_id}: {e}")
            results.append((claim_id, ""))
            
    return results

def transform_data(claims_df, policyholders_df):
    """Applies the business transformations using PySpark DataFrame API."""
    logger.info("Starting data transformation using DataFrame API...")
    
    # 1. Get unique claim IDs to fetch hashes for
    # Collect to driver - assuming dataset fits in memory as per "small dataset" note
    unique_claims = [row.claim_id for row in claims_df.select("claim_id").distinct().collect()]
    
    # 2. Fetch hashes on driver
    hashes_data = fetch_hashes_for_claims(unique_claims)
    
    # 3. Create a DataFrame from the hashes
    # Workaround: Write to temp CSV and read back to avoid createDataFrame worker crash
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as tmp:
        writer = csv.writer(tmp)
        writer.writerow(["claim_id", "hash_id"])
        writer.writerows(hashes_data)
        tmp_path = tmp.name
        
    try:
        spark = claims_df.sparkSession
        hashes_df = spark.read.csv(tmp_path, header=True)
    finally:
        pass
    
    # 4. Join Claims and Policyholders
    # Using left join as per requirement
    joined_df = claims_df.join(
        policyholders_df, 
        claims_df.policyholder_id == policyholders_df.policyholder_id, 
        "left"
    ).select(
        claims_df["*"],
        policyholders_df["policyholder_name"]
    )
    
    # 5. Join with Hashes
    joined_with_hashes_df = joined_df.join(
        hashes_df,
        "claim_id",
        "left"
    )
    # joined_with_hashes_df = joined_df # Bypass hash join
    
    # Apply Transformations
    final_df = joined_with_hashes_df.withColumn(
        "claim_type",
        when(col("claim_id").like("CL%"), "Coinsurance")
        .when(col("claim_id").like("RX%"), "Reinsurance")
        .otherwise("Unknown")
    ).withColumn(
        "claim_priority",
        when(col("claim_amount") > 4000, "Urgent")
        .otherwise("Normal")
    ).withColumn(
        "claim_period",
        date_format(col("claim_date"), "yyyy-MM")
    ).withColumn(
        "source_system_id",
        split(col("claim_id"), "_").getItem(1)
    )
    
    # Select final columns in specific order
    final_df = final_df.select(
        "claim_id",
        "policyholder_name",
        "region",
        "claim_type",
        "claim_priority",
        "claim_amount",
        "claim_period",
        "source_system_id",
        "hash_id"
    )
    
    logger.info("Data transformation complete.")
    return final_df

def load_data(df, output_path):
    """Writes the result to a CSV file."""
    logger.info(f"Writing output to {output_path}")
    # Coalesce to 1 to get a single CSV file as output (for this small dataset)
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
