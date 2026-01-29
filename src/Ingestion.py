from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from src import config # Make sure you fixed the import line here too!

def init_spark():
    """
    Initializes the Spark Engine.
    'local[*]' means 'Use all CPU cores on this machine'.
    """
    return SparkSession.builder \
        .appName("DemoIngestion") \
        .master("local[*]") \
        .getOrCreate()

def ingest_data_spark():
    spark = init_spark()
    
    # 1. Define Schema (The "Contract")
    # Unlike Pandas, Spark schemas are Objects, not dictionaries.
    # This is strictly enforced at the binary level.
    schema = StructType([
        StructField("transaction_id", IntegerType(), False), # False = Not Nullable
        StructField("date", TimestampType(), True),
        StructField("product_category", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("region", StringType(), True)
    ])
    
    input_path = str(config.RAW_DATA_DIR / "sales_raw.csv")
    output_path = str(config.PROCESSED_DATA_DIR / "sales_clean_spark.parquet")
    
    print("[INFO] Reading CSV with Spark...")
    
    # 2. READ (Lazy Operation)
    # We enforce the schema immediately. Spark will verify the file matches.
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(input_path)
    
    print("[INFO] Writing Parquet with Spark...")
    
    # 3. WRITE (Action)
    # mode("overwrite"): If the folder exists, delete it and replace it.
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
        
    print(f"[SUCCESS] Data saved to {output_path}")
    
    # Optional: Verify
    print("--- Previewing Data ---")
    df.show(5)
    
    spark.stop()

if __name__ == "__main__":
    ingest_data_spark()