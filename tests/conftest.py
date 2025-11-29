import pytest
from pyspark.sql import SparkSession
import sys
import os

# Ensure PySpark uses the same Python version for driver and worker
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """Creates a SparkSession for testing."""
    
    import tempfile
    import shutil
    
    warehouse_dir = tempfile.mkdtemp()
    
    spark = SparkSession.builder \
        .appName("TestClaimsProcessing") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", f"file:///{warehouse_dir}") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    yield spark
    spark.stop()
    shutil.rmtree(warehouse_dir)
