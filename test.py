import torch
import pyspark
from torch import cuda


print(pyspark.__version__)
print(torch.__version__)
print(cuda.is_available())
print(cuda.device_count())

spark = (
    pyspark.sql.SparkSession.builder.appName("test").master("local[*]").getOrCreate()
)

spark.read.parquet(f"taxi_data/yellow_tripdata/yellow_tripdata_taxi_data_2026_5.parquet").show(5)
