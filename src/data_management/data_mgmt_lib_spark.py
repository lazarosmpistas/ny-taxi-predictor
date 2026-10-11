import numpy as np
from pyspark.sql.classic.dataframe import DataFrame
import pyspark.sql.functions as F



def _print_df_count(df_dirty_count: int, df_clean_count: int) -> None:
    print(f"total rows before: {str(df_dirty_count)[:9]} | bad rows: {str(df_dirty_count - df_clean_count)[:9]} | total rows after: {str(df_clean_count)[:9]}")


def remove_bad_distance(df: DataFrame) -> DataFrame:
    zero_distance = (df["trip_distance"] == 0) & (df["trip_distance"].isNotNull())
    negative_distance = (df["trip_distance"] < 0) & (df["trip_distance"].isNotNull())
    null_distance = df["trip_distance"].isNull() | F.isnan(df["trip_distance"])

    df_cleaned = df[~zero_distance & ~negative_distance & ~null_distance]
    _print_df_count(df.count(), df_cleaned.count())

    return df_cleaned

def remove_bad_passengers(df: DataFrame) -> DataFrame:
    zero_passengers = (df["passenger_count"] == 0) & (df["passenger_count"].isNotNull())
    negative_passengers = (df["passenger_count"] < 0) & (df["passenger_count"].isNotNull())
    null_passengers = df["passenger_count"].isNull() | F.isnan(df["passenger_count"])

    df_clean = df[~zero_passengers & ~negative_passengers & ~null_passengers]
    _print_df_count(df.count(), df_clean.count())

    return df_clean

def remove_bad_vendor(df: DataFrame) -> DataFrame:
    invalid_vendor = (~df["VendorID"].isin([1, 2, 6, 7]) & (df["VendorID"].isNotNull()))
    null_vendor = df["VendorID"].isNull() | F.isnan(df["VendorID"])

    df_clean = df[~invalid_vendor & ~null_vendor]
    _print_df_count(df.count(), df_clean.count())

    return df_clean

def remove_bad_ratecode(df: DataFrame) -> DataFrame:
    invalid_ratecode = (~df["RatecodeID"].isin([1, 2, 3, 4, 5, 6, 99]) & (df["RatecodeID"].isNotNull()))
    null_ratecode = df["RatecodeID"].isNull() | F.isnan(df["RatecodeID"])

    df_clean = df[~invalid_ratecode & ~null_ratecode]
    _print_df_count(df.count(), df_clean.count())

    return df_clean

def remove_bad_dt(df: DataFrame) -> DataFrame:
    zero_dt = (df["tpep_pickup_datetime"] == df["tpep_dropoff_datetime"]) & (
        df["tpep_pickup_datetime"].isNotNull()) & (df["tpep_dropoff_datetime"].isNotNull())
    negative_dt = (df["tpep_pickup_datetime"] > df["tpep_dropoff_datetime"]) & (
        df["tpep_pickup_datetime"].isNotNull()) & (df["tpep_dropoff_datetime"].isNotNull())
    null_dt = (df["tpep_pickup_datetime"].isNull() | df["tpep_dropoff_datetime"].isNull())

    df_clean = df[~zero_dt & ~negative_dt & ~null_dt]
    _print_df_count(df.count(), df_clean.count())

    return df_clean

def remove_bad_rate(df: DataFrame) -> DataFrame:
    fare_cols = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                 "congestion_surcharge", "Airport_fee", "cbd_congestion_fee"]

    # df["total_fare_calculated"] = F.sum(F.col(fare_col) for fare_col in fare_cols)
    df = df.withColumn("total_fare_calculated", sum(F.coalesce(F.col(c), F.lit(0)) for c in fare_cols))

    zero_fare_amount = (df["total_amount"] == 0) & (df["total_amount"].isNotNull())
    negative_fare_amount = (df["total_amount"] < 0) & (df["total_amount"].isNotNull())
    null_fare_amount = df["total_amount"].isNull() | F.isnan(df["total_amount"])

    inconsistent_fare_amount = ((F.abs(df["total_fare_calculated"] - df["total_amount"]) >= 1e-2)
                                & (df["total_amount"].isNotNull()))

    df = df.drop(df.total_fare_calculated)

    df_clean = df[~zero_fare_amount & ~negative_fare_amount & ~null_fare_amount & ~inconsistent_fare_amount]
    _print_df_count(df.count(), df_clean.count())
    return df_clean

def remove_bad_entries(df: DataFrame) -> DataFrame:
    return remove_bad_distance(remove_bad_passengers(remove_bad_vendor(remove_bad_ratecode(remove_bad_dt(remove_bad_rate(df))))))