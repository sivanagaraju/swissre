from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, substring, split, date_format
import logging
import requests

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

def get_hash_with_session(session, claim_id):
    """
    Fetches MD4 hash using provided session (for connection reuse).
    Used by driver-side batching.
    """
    if not claim_id:
        return ""
        
    try:
        url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("Digest", "")
        return ""
    except Exception as e:
        logger.warning(f"Error fetching hash for {claim_id}: {e}")
        return ""

def fetch_hashes_in_batches(claim_ids, batch_size=100):
    """
    Fetches hashes for claim_ids in batches with connection pooling.
    Runs on DRIVER to avoid worker serialization issues.
    """
    logger.info(f"Fetching hashes for {len(claim_ids)} unique claims in batches of {batch_size}...")
    session = requests.Session()
    all_hashes = {}
    
    total_batches = (len(claim_ids) + batch_size - 1) // batch_size
    
    for i in range(0, len(claim_ids), batch_size):
        batch = claim_ids[i:i+batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"Processing batch {batch_num}/{total_batches}...")
        
        # Make batched requests with connection reuse
        for claim_id in batch:
            hash_val = get_hash_with_session(session, claim_id)
            all_hashes[claim_id] = hash_val
    
    session.close()
    logger.info(f"Successfully fetched {len(all_hashes)} hashes.")
    return all_hashes

def transform_data(claims_df, policyholders_df):
    """Applies the business transformations using PySpark DataFrame API."""
    logger.info("Starting data transformation using DataFrame API...")
    
    # 1. Get UNIQUE claim_ids (deduplicate at driver)
    unique_claims = [row.claim_id for row in 
                    claims_df.select("claim_id").distinct().collect()]
    
    # 2. Fetch all hashes on DRIVER with connection pooling
    hashes_dict = fetch_hashes_in_batches(unique_claims)
    
    # 3. Convert dict to DataFrame (avoid UDF serialization issues)
    spark = claims_df.sparkSession
    hashes_data = [(k, v) for k, v in hashes_dict.items()]
    hashes_df = spark.createDataFrame(hashes_data, ["claim_id", "hash_id"])
    
    # 4. Join Claims and Policyholders
    joined_df = claims_df.join(
        policyholders_df, 
        claims_df.policyholder_id == policyholders_df.policyholder_id, 
        "left"
    ).select(
        claims_df["*"],
        policyholders_df["policyholder_name"]
    )
    
    # 5. Join with hashes (join instead of UDF to avoid serialization issues)
    joined_with_hashes_df = joined_df.join(
        hashes_df,
        "claim_id",
        "left"
    )
    
    # 6. Apply Transformations
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
    """Writes the result to a Parquet file."""
    logger.info(f"Writing output to {output_path}")
    # Coalesce to 1 to get a single Parquet file as output (for this small dataset)
    df.coalesce(1).write.parquet(output_path, mode="overwrite")
