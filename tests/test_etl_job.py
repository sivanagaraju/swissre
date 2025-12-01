import pytest
import pytest
import src.etl_job
from src.etl_job import extract_data, transform_data, load_data

from src.utils import load_config, rename_spark_output
import os
import tempfile
import shutil
import glob
import unittest.mock



def test_load_config():
    """Test that configuration loading works correctly."""
    config = load_config("config/spark-defaults.conf")
    
    # Verify config is loaded
    assert isinstance(config, dict)
    assert len(config) > 0
    
    # Verify key configurations are present
    assert "spark.app.name" in config
    assert config["spark.app.name"] == "ClaimsProcessing"
    assert "spark.master" in config


def test_extract_data(spark):
    """Test that data extraction works with sample CSV files."""
    # Use the actual test data files
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    claims_path = os.path.join(base_dir, "data", "claims_data.csv")
    policyholders_path = os.path.join(base_dir, "data", "policyholder_data.csv")
    
    claims_df, policyholders_df = extract_data(spark, claims_path, policyholders_path)
    
    # Verify DataFrames are created
    assert claims_df is not None
    assert policyholders_df is not None
    
    # Verify row counts
    assert claims_df.count() == 6
    assert policyholders_df.count() == 4
    
    # Verify columns exist
    assert "claim_id" in claims_df.columns
    assert "policyholder_id" in policyholders_df.columns


def test_transformation_logic(spark):
    """Test transformation logic with actual data."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    claims_path = os.path.join(base_dir, "data", "claims_data.csv")
    policyholders_path = os.path.join(base_dir, "data", "policyholder_data.csv")
    
def test_fetch_hashes_for_claims():
    """Test the fetch_hashes_for_claims function logic."""
    with unittest.mock.patch('src.etl_job.requests.Session') as mock_session_cls:
        mock_session = mock_session_cls.return_value
        
        # Test success case
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Digest": "12345hash"}
        mock_session.get.return_value = mock_response
        
        results = src.etl_job.fetch_hashes_for_claims(["CL_123"])
        assert results == [("CL_123", "12345hash")]
        
        # Test failure case
        mock_session.get.side_effect = Exception("API Error")
        results = src.etl_job.fetch_hashes_for_claims(["CL_456"])
        assert results == [("CL_456", "")]


def test_transformation_logic(spark):
    """Test transformation logic with mocked UDF to avoid worker crashes."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    claims_path = os.path.join(base_dir, "data", "claims_data.csv")
    policyholders_path = os.path.join(base_dir, "data", "policyholder_data.csv")
    
    claims_df, policyholders_df = extract_data(spark, claims_path, policyholders_path)
    
    def fake_fetch_hashes(claim_ids):
        return [(cid, "mocked_md4_hash") for cid in claim_ids]
        
    with unittest.mock.patch('src.etl_job.fetch_hashes_for_claims', side_effect=fake_fetch_hashes):
        result_df = transform_data(claims_df, policyholders_df)
        
        # Verify output schema
        expected_columns = [
            "claim_id", "policyholder_name", "region", "claim_type",
            "claim_priority", "claim_amount", "claim_period",
            "source_system_id", "hash_id"
        ]
        assert set(result_df.columns) == set(expected_columns)
        
        # Verify row count matches input
        assert result_df.count() == 6
        
        # Verify that the hash_id column contains our mocked value
        rows = result_df.select("hash_id").limit(1).collect()
        assert rows[0]["hash_id"] == "mocked_md4_hash"


def test_rename_spark_output():
    """Test that Spark output file renaming works."""
    # Create a temporary directory structure
    temp_dir = tempfile.mkdtemp()
    output_dir = os.path.join(temp_dir, "spark_output")
    os.makedirs(output_dir)
    
    try:
        # Create a mock Spark part file
        part_file = os.path.join(output_dir, "part-00000-test.csv")
        with open(part_file, 'w') as f:
            f.write("header1,header2\nvalue1,value2\n")
        
        # Test renaming
        destination = os.path.join(temp_dir, "final_output.csv")
        result = rename_spark_output(output_dir, destination)
        
        # Verify rename was successful
        assert result is True
        assert os.path.exists(destination)
        assert not os.path.exists(part_file)
        
        # Verify content is preserved
        with open(destination, 'r') as f:
            content = f.read()
            assert "header1,header2" in content
            assert "value1,value2" in content
    
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


def test_rename_spark_output_no_file():
    """Test rename_spark_output when no part file exists."""
    temp_dir = tempfile.mkdtemp()
    
    try:
        destination = os.path.join(temp_dir, "final_output.csv")
        result = rename_spark_output(temp_dir, destination)
        
        # Should return False when no file found
        assert result is False
        assert not os.path.exists(destination)
    
    finally:
        shutil.rmtree(temp_dir)


def test_end_to_end_etl(spark):
    """Integration test for the complete ETL pipeline."""
    # Setup paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    claims_path = os.path.join(base_dir, "data", "claims_data.csv")
    policyholders_path = os.path.join(base_dir, "data", "policyholder_data.csv")
    
    # Create temporary output directory
    temp_output = tempfile.mkdtemp()
    output_path = os.path.join(temp_output, "test_output")
    
    # Mock fetch_hashes_for_claims here too
    def fake_fetch_hashes(claim_ids):
        return [(cid, "mocked_hash") for cid in claim_ids]
    
    try:
        with unittest.mock.patch('src.etl_job.fetch_hashes_for_claims', side_effect=fake_fetch_hashes):
            # Run ETL
            claims_df, policyholders_df = extract_data(spark, claims_path, policyholders_path)
            final_df = transform_data(claims_df, policyholders_df)
            load_data(final_df, output_path)
            
            # Verify output was created
            csv_files = glob.glob(os.path.join(output_path, "part-*.csv"))
            assert len(csv_files) > 0
            
            # Verify we can read the output
            output_df = spark.read.csv(output_path, header=True)
            assert output_df.count() == 6
        
    finally:
        shutil.rmtree(temp_output)
