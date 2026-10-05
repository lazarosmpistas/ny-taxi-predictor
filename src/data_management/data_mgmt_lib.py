import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from typing import Iterable, Literal, Optional
import os
import datetime
from pathlib import Path
from pyspark.sql.types import StructType

PARENT_FOLDER = Path(os.path.abspath("")).parent.parent
DATA_FOLDER = PARENT_FOLDER / "taxi_data" / "yellow_tripdata"
GENERIC_FILENAME = "yellow_tripdata_taxi_data_"
PARQUET_FILES_NUM = os.listdir(DATA_FOLDER).count(".parquet")

def create_unified_df(
    mode: Literal["spark", "pandas"],
    spark: Optional[SparkSession],
    pq_num: int = PARQUET_FILES_NUM,
):
    data_filenames = os.listdir(DATA_FOLDER)
    filenames_dict = {"_".join(filename.split(".")[0].split("_")[4:]): filename for filename in data_filenames}

    date_dict = {datetime.datetime(int(x.split("_")[0]), int(x.split("_")[1]), 1): x for x in
                 list(filenames_dict.keys())}
    sorted_date_dict = dict(sorted(date_dict.items(), key=lambda x: x[0]))

    ### create empty dataframe???
    df_main = pd.DataFrame() if mode == "pandas" else SparkSession.createDataFrame(data=[], schema=StructType([]), verify_schema=False)
    ###
    for date in sorted_date_dict.values()[PARQUET_FILES_NUM - pq_num:]:
        filename = "".join([GENERIC_FILENAME, date, ".parquet"])
        filepath = Path.joinpath(DATA_FOLDER, filename)
        df = pd.read_parquet(filepath, dtype_backend="pyarrow")

        df_main = pd.concat([df_main, df])
