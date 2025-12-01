import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit
from datetime import date
import os
import shutil
import tempfile
from src import etl_job

# --- Test get_hash_id ---

def test_get_hash_id_success():
    """Test get_hash_id returns digest on 200 OK."""
    with patch('src.etl_job.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Digest": "abcdef123456"}
        mock_get.return_value = mock_response

        result = etl_job.get_hash_id("claim123")
        assert result == "abcdef123456"

def test_get_hash_id_failure():
    """Test get_hash_id returns empty string on non-200 response."""
    with patch('src.etl_job.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = etl_job.get_hash_id("claim123")
        assert result == ""

def test_get_hash_id_exception():
    """Test get_hash_id returns empty string on exception."""
    with patch('src.etl_job.requests.get') as mock_get:
        mock_get.side_effect = Exception("Network error")

        result = etl_job.get_hash_id("claim123")
        assert result == ""

def test_get_hash_id_empty_input():
    """Test get_hash_id returns empty string for empty input."""
    assert etl_job.get_hash_id(None) == ""
    assert etl_job.get_hash_id("") == ""


# --- Test extract_data ---

def test_extract_data(spark):
    """Test extract_data reads CSVs and returns DataFrames."""
    # Use absolute paths to the dummy data
    base_dir = os.path.dirname(os.path.abspath(__file__))
    claims_path = os.path.join(base_dir, "data", "dummy_claims.csv").replace("\\", "/")
    policy_path = os.path.join(base_dir, "data", "dummy_policy.csv").replace("\\", "/")

    claims_df, policy_df = etl_job.extract_data(spark, claims_path, policy_path)
    
    # Verify we got DataFrames with correct data
    assert claims_df.count() == 3
    assert policy_df.count() == 3
    assert "claim_id" in claims_df.columns
    assert "policyholder_id" in policy_df.columns



def test_transform_data(spark):
    """Test transform_data applies correct transformations."""
    # 1. Prepare Data
    claims_data = [
        ("CL_SYS1_001", "P1", 5000.0, date(2023, 1, 15), "North"),
        ("RX_SYS2_002", "P2", 1000.0, date(2023, 2, 20), "South"),
        ("OT_SYS3_003", "P3", 3000.0, date(2023, 3, 10), "East")
    ]
    # Spark can infer schema from data + column names
    claims_df = spark.createDataFrame(
        claims_data, 
        ["claim_id", "policyholder_id", "claim_amount", "claim_date", "region"]
    )

    policy_data = [
        ("P1", "John Doe"),
        ("P2", "Jane Smith")
    ]
    policyholders_df = spark.createDataFrame(
        policy_data, 
        ["policyholder_id", "policyholder_name"]
    )

    # 2. Execute with Mock
    # Patch 'udf' to avoid actual UDF execution and serialization issues
    with patch('src.etl_job.udf') as mock_udf:
        # Make the mocked udf return a function that returns a literal column "mock_hash"
        mock_udf.side_effect = lambda func, returnType: (lambda col: lit("mock_hash"))
        
        final_df = etl_job.transform_data(claims_df, policyholders_df)
    
    # 3. Verify
    # Collect and sort locally for deterministic assertion
    results = final_df.orderBy("claim_id").collect()
    
    expected_values = [
        # (claim_id, policyholder_name, claim_type, claim_priority, claim_period, source_system_id)
        ("CL_SYS1_001", "John Doe",   "Coinsurance", "Urgent", "2023-01", "SYS1"),
        ("OT_SYS3_003", None,         "Unknown",     "Normal", "2023-03", "SYS3"),
        ("RX_SYS2_002", "Jane Smith", "Reinsurance", "Normal", "2023-02", "SYS2")
    ]
    
    for row, expected in zip(results, expected_values):
        assert row.claim_id == expected[0]
        assert row.policyholder_name == expected[1]
        assert row.claim_type == expected[2]
        assert row.claim_priority == expected[3]
        assert row.claim_period == expected[4]
        assert row.source_system_id == expected[5]
        assert row.hash_id == "mock_hash"


# --- Test load_data ---

def test_load_data(spark):
    """Test load_data writes to CSV."""
    # Create a small DF
    data = [("val1",), ("val2",)]
    df = spark.createDataFrame(data, ["col1"])
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        output_path = os.path.join(tmp_dir, "output.csv")
        
        etl_job.load_data(df, output_path)
        
        # Check if directory exists (Spark writes to a directory for csv)
        assert os.path.exists(output_path)
        
        # Check if success file or part files exist
        files = os.listdir(output_path)
        csv_files = [f for f in files if f.endswith(".csv")]
        assert len(csv_files) > 0
