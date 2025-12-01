import pytest
from pyspark.sql import SparkSession
import sys
import os

# Ensure PySpark uses the same Python version for driver and worker
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Add project root to path so workers can find src
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set PYTHONPATH for Spark workers
current_pythonpath = os.environ.get('PYTHONPATH', '')
os.environ['PYTHONPATH'] = f"{project_root}{os.pathsep}{current_pythonpath}"

@pytest.fixture(scope="session")
def spark():
    """Creates a SparkSession for testing."""
    
    import tempfile
    import shutil
    
    warehouse_dir = tempfile.mkdtemp()
    events_dir = tempfile.mkdtemp()
    local_dir = tempfile.mkdtemp()
    
    spark = SparkSession.builder \
        .appName("TestClaimsProcessing") \
        .master("local[*]") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", f"file:///{events_dir.replace(os.sep, '/')}") \
        .config("spark.history.fs.logDirectory", f"file:///{events_dir.replace(os.sep, '/')}") \
        .config("spark.local.dir", local_dir) \
        .getOrCreate()
    yield spark
    spark.stop()
    shutil.rmtree(warehouse_dir)
    shutil.rmtree(events_dir)
    shutil.rmtree(local_dir)
