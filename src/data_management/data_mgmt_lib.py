import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from typing import Iterable, Literal, Optional
import os
import datetime
from pathlib import Path
import glob

PARENT_FOLDER = Path(os.path.abspath("")).parent.parent
DATA_FOLDER = PARENT_FOLDER / "taxi_data" / "yellow_tripdata"
GENERIC_FILENAME = "yellow_tripdata_taxi_data_"
PARQUET_FILES_NUM = len(glob.glob(str(DATA_FOLDER / "*.parquet")))

def create_unified_df(
    mode: Literal["spark", "pandas"],
    spark: Optional[SparkSession] = None,
    pq_num: int = PARQUET_FILES_NUM,
) -> DataFrame | pd.DataFrame:

    if pq_num > PARQUET_FILES_NUM:
        raise ValueError(f"pq_num greater than number of parquet files | current parquet files: {PARQUET_FILES_NUM}")

    if mode == "spark" and spark is None:
        raise ValueError("spark session is required in spark mode")
    elif mode == "pandas" and spark is not None:
        raise ValueError("spark session is not required in pandas mode")
    data_filenames = os.listdir(DATA_FOLDER)
    filenames_dict = {"_".join(filename.split(".")[0].split("_")[4:]): filename for filename in data_filenames}

    date_dict = {datetime.datetime(int(x.split("_")[0]), int(x.split("_")[1]), 1): x for x in
                 list(filenames_dict.keys())}
    sorted_date_dict = dict(sorted(date_dict.items(), key=lambda x: x[0]))

    #df_main = pd.DataFrame() if mode == "pandas" else spark.createDataFrame(data=[], schema=StructType([]), verifySchema=False)
    df_main = None

    for date in list(sorted_date_dict.values())[PARQUET_FILES_NUM - pq_num:]:
        filename = "".join([GENERIC_FILENAME, date, ".parquet"])
        filepath = Path.joinpath(DATA_FOLDER, filename)

        print(f"reading {filename}")
        if mode == "spark":
            if df_main is None:
                df_main = spark.read.parquet(str(filepath))
            else:
                df = spark.read.parquet(str(filepath))
                df_main = df_main.union(df)
        elif mode == "pandas":
            if df_main is None:
                df_main = pd.read_parquet(filepath, dtype_backend="pyarrow")
            else:
                df = pd.read_parquet(filepath, dtype_backend="pyarrow")
                df_main = pd.concat([df_main, df])

    return df_main