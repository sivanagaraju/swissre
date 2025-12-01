from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, split, date_format
from pyspark.sql.types import StringType
import logging
import requests

logger = logging.getLogger("ClaimsETL")

def create_spark_session():
    """Creates and returns a SparkSession with predefined configurations."""
    logger.info("Creating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("ClaimsProcessing") \
        .master("local[*]") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///c:/tmp/spark-events") \
        .config("spark.history.fs.logDirectory", "file:///c:/tmp/spark-events") \
        .config("spark.local.dir", "C:\\tmp\\spark_tmp_files") \
        .getOrCreate()
    
    return spark

def extract_data(spark, claims_path, policyholders_path):
    """Reads the input CSV files."""
    logger.info(f"Reading claims data from {claims_path}")
    claims_df = spark.read.csv(claims_path, header=True, inferSchema=True)
    
    logger.info(f"Reading policyholders data from {policyholders_path}")
    policyholders_df = spark.read.csv(policyholders_path, header=True, inferSchema=True)
    
    return claims_df, policyholders_df

def get_hash_id(claim_id):
    """
    Fetches MD4 hash for a single claim_id from an external API.
    Returns the hash_id as a string, or empty string on error.
    """
    if not claim_id:
        return ""
    
    try:
        url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("Digest", "")
        else:
            return ""
    except Exception as e:
        logger.error(f"Error fetching hash for {claim_id}: {e}")
        return ""

def transform_data(claims_df, policyholders_df):
    """Applies the business transformations using PySpark DataFrame API."""
    logger.info("Starting data transformation using DataFrame API...")
    
    # Register the UDF for fetching hash_id
    hash_udf = udf(get_hash_id, StringType())
    
    # Join Claims and Policyholders using left join as per requirement
    joined_df = claims_df.join(
        policyholders_df, 
        claims_df.policyholder_id == policyholders_df.policyholder_id, 
        "left"
    ).select(
        claims_df["*"],
        policyholders_df["policyholder_name"]
    )
    
    # Apply Transformations including hash_id from UDF
    final_df = joined_df.withColumn(
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
    ).withColumn(
        "hash_id",
        hash_udf(col("claim_id"))
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
