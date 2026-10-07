import numpy as np
from pyspark.sql.classic.dataframe import DataFrame
import pyspark.sql.functions as F


def remove_bad_distance(df: DataFrame) -> DataFrame:
    zero_distance = (df["trip_distance"] == 0) & (df["trip_distance"].isNotNull())
    negative_distance = (df["trip_distance"] < 0) & (df["trip_distance"].isNotNull())
    null_distance = df["trip_distance"].isNull() | F.isnan(df["trip_distance"])

    return df[~zero_distance & ~negative_distance & ~null_distance]

def remove_bad_passengers(df: DataFrame) -> DataFrame:
    zero_passengers = (df["passenger_count"] == 0) & (df["passenger_count"].isNotNull())
    negative_passengers = (df["passenger_count"] < 0) & (df["passenger_count"].isNotNull())
    null_passengers = df["passenger_count"].isNull() | F.isnan(df["passenger_count"])

    return df[~zero_passengers & ~negative_passengers & ~null_passengers]

def remove_bad_vendor(df: DataFrame) -> DataFrame:
    invalid_vendor = (~df["VendorID"].isin([1, 2, 6, 7]) & (df["VendorID"].isNotNull()))
    null_vendor = df["VendorID"].isNull() | F.isnan(df["VendorID"])

    return df[~invalid_vendor & ~null_vendor]

def remove_bad_ratecode(df: DataFrame) -> DataFrame:
    invalid_ratecode = (~df["RatecodeID"].isin([1, 2, 3, 4, 5, 6, 99]) & (df["RatecodeID"].isNotNull()))
    null_ratecode = df["RatecodeID"].isNull() | F.isnan(df["RatecodeID"])

    return df[~invalid_ratecode & ~null_ratecode]

def remove_bad_dt(df: DataFrame) -> DataFrame:
    zero_dt = (df["tpep_pickup_datetime"] == df["tpep_dropoff_datetime"]) & (
        df["tpep_pickup_datetime"].isNotNull()) & (df["tpep_dropoff_datetime"].isNotNull())
    negative_dt = (df["tpep_pickup_datetime"] > df["tpep_dropoff_datetime"]) & (
        df["tpep_pickup_datetime"].isNotNull()) & (df["tpep_dropoff_datetime"].isNotNull())
    null_dt = (df["tpep_pickup_datetime"].isNull() | df["tpep_dropoff_datetime"].isNull())

    return df[~zero_dt & ~negative_dt & ~null_dt]

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
    return df[~zero_fare_amount & ~negative_fare_amount & ~null_fare_amount & ~inconsistent_fare_amount]