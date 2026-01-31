from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import config 
import os
import sys

# Point this to the python.exe that actually works
os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("DemoIngestion").master("local[*]").getOrCreate()

def create_mock_data():
    print(f"[INFO] GENERATING RAW DATA...")
    
    row_count = 1000

    raw_data = {
        "transaction_id": range(1, row_count + 1),
        "date": pd.date_range(start="2026-01-01", periods=row_count, freq='h'),
        "prod_category": np.random.choice(['Electronics', 'Clothing', 'Home', 'Toys'], row_count),
        "amount": np.random.uniform(10.00, 1500.00, row_count),
        "region": np.random.choice(["AP", "BBSR", "TN", "KA"], row_count)
    }

    p_df = pd.DataFrame(raw_data)

    df = spark.createDataFrame(p_df)
    
    file_path = config.RAW_DATA_DIR / "raw_data.parquet"
    
    df.coalesce(1).write.parquet(str(file_path), mode="overwrite")
    
    print(f"[INFO] SAVING RAW DATA TO PARQUET at {file_path}")
    return file_path

def ingest_data(input_path):
    print(f"[INFO] STARTING SPARK INGESTION")
    output_path = config.PROCESSED_DATA_DIR / "processed_data.parquet"

    df = spark.read.parquet(str(input_path))

    df_final = df.withColumn("amount", F.col("amount").cast("float"))
    df_final.coalesce(1).write.parquet(str(output_path), mode="overwrite")

    print(f"[SUCCESS] Data ingested and saved to: {output_path}")
    print("\n--- Data Quality Check ---")
    df_final.printSchema()
    return output_path
    
if __name__ == "__main__":
    input_path = create_mock_data()
    ingest_data(input_path)