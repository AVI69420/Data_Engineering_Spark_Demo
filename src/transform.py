from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src import config
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("TransformData").master("local[*]").getOrCreate()
def load_silver_data():
    input_path = config.PROCESSED_DATA_DIR / "processed_data.parquet"
    print(f"[INFO] LOADING SILVER DATA FROM {input_path}...")
    return spark.read.parquet(str(input_path))

def region_summary(df):
    summar_data = df.groupBy("region").agg(
        F.count("transaction_id").alias("transaction_count"),
        F.avg("amount").alias("average_amount"),
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount")
    )

    return summar_data

def window_analysis(df):
    windowData = Window.partitionBy("region").orderBy(F.asc("date"))
    df = df.withColumn("prev_amount", F.lag("amount", 1).over(windowData))
    return df.withColumn("growth", F.col("amount") - F.col("prev_amount"))

def transform_data():
    print(f"[INFO] STARTING DATA TRANSFORMATION FOR INPUT PATH:")

    df = load_silver_data()

    region_summary_df = region_summary(df)
    growth_df = window_analysis(df)

    output_path_1 = config.FINAL_DATA_DIR / "region_summary.parquet"
    region_summary_df.repartition(1).coalesce(1).write.parquet(str(output_path_1), mode="overwrite")

    output_path_2 = config.FINAL_DATA_DIR / "growth.parquet"
    growth_df.repartition(1).coalesce(1).write.parquet(str(output_path_2), mode="overwrite")

    print(f"[SUCCESS] TRANSFORMED DATA SAVED TO: [{output_path_1} \n {output_path_2}]")
    print("\n--- TRANSFORMED DATA SAMPLE ---")
    region_summary_df.show(5)
    print("")
    growth_df.show(5)

if __name__ == "__main__":
    transform_data()




