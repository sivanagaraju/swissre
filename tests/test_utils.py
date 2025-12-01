import pytest
import os
import tempfile
import shutil
from src.utils import rename_spark_output


def test_rename_spark_output_success():
    """Test successful renaming of Spark output file."""
    # Create temporary directory structure
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
