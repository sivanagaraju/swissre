from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, substring, split, date_format, lit, md5
from pyspark.sql.types import StringType
# from src.utils import get_hash_id # Not used anymore
import logging

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

def transform_data(claims_df, policyholders_df):
    """Applies the business transformations using Spark SQL."""
    logger.info("Starting data transformation using Spark SQL...")
    
    spark = claims_df.sparkSession
    # UDF registration removed to avoid worker crash. Using native md5 function.

    # Create Temporary Views
    claims_df.createOrReplaceTempView("claims")
    policyholders_df.createOrReplaceTempView("policyholders")
    
    # Execute SQL Transformation
    query = """
    SELECT 
        c.claim_id,
        p.policyholder_name,
        c.region,
        CASE 
            WHEN c.claim_id LIKE 'CL%' THEN 'Coinsurance'
            WHEN c.claim_id LIKE 'RX%' THEN 'Reinsurance'
            ELSE 'Unknown'
        END AS claim_type,
        CASE 
            WHEN c.claim_amount > 4000 THEN 'Urgent'
            ELSE 'Normal'
        END AS claim_priority,
        c.claim_amount,
        date_format(c.claim_date, 'yyyy-MM') AS claim_period,
        split(c.claim_id, '_')[1] AS source_system_id,
        md5(c.claim_id) AS hash_id
    FROM claims c
    LEFT JOIN policyholders p ON c.policyholder_id = p.policyholder_id
    """
    
    final_df = spark.sql(query)
    
    logger.info("Data transformation complete.")
    return final_df

def load_data(df, output_path):
    """Writes the result to a CSV file."""
    logger.info(f"Writing output to {output_path}")
    # Coalesce to 1 to get a single CSV file as output (for this small dataset)
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
