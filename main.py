import os
import sys

# Ensure PySpark uses the same Python version for driver and worker
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from src.etl_job import create_spark_session, extract_data, transform_data, load_data
from src.utils import setup_logger, rename_spark_output, load_config

def main():
    # Setup logger
    logger = setup_logger()
    
    # Load configuration manually
    config = load_config("config/spark-defaults.conf")
    spark = create_spark_session(config)
    
    # Define paths (assuming running from project root)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    claims_path = os.path.join(base_dir, "data", "claims_data.csv")
    policyholders_path = os.path.join(base_dir, "data", "policyholder_data.csv")
    output_path = os.path.join(base_dir, "processed_claims_output") # Spark writes to a directory
    final_output_file = os.path.join(base_dir, "processed_claims.csv")

    logger.info("Starting ETL job...")
    
    try:
        claims_df, policyholders_df = extract_data(spark, claims_path, policyholders_path)
        logger.info("Data extracted successfully.")
        
        final_df = transform_data(claims_df, policyholders_df)
        logger.info("Data transformed successfully.")
        
        load_data(final_df, output_path)
        logger.info(f"Data saved to {output_path}")

        rename_spark_output(output_path, final_output_file)
        
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
