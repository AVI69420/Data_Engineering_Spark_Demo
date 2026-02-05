from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
from src import  Ingestion, transform
import pandas as pd
import numpy as np
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("DemoIngestion").master("local[*]").getOrCreate()

def run_pipeline():
    print("[INFO] RUNNING THE DATA PIPELINE...")

    # Step 1: Ingest Data
    input_path = Ingestion.create_mock_data()
    Ingestion.ingest_data(input_path)

    # Step 2: Transform Data
    transform.transform_data()

    print("[SUCCESS] DATA PIPELINE COMPLETED.")

if __name__ == "__main__":
    run_pipeline()
