# Data Loader
from pyspark.sql import SparkSession
import os
from google.cloud import storage


def load_data_from_bucket(bucket_name):
    spark = SparkSession.builder.appName("test").getOrCreate()

    # Initialize a Google Cloud Storage client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # List all blobs in the "input" folder
    blobs = bucket.list_blobs(prefix="input/", delimiter="/")

    # Find the latest file in the "input" folder
    latest_blob = max(
        [blob for blob in blobs if blob.name.endswith(".csv")],
        key=lambda blob: blob.time_created,
    )

    # Download the latest file to the current working directory
    cwd = os.getcwd()
    file_name = os.path.basename(latest_blob.name)
    file_path = os.path.join(cwd, file_name)
    latest_blob.download_to_filename(file_path)

    # Read the local CSV file into a Spark DataFrame
    df = (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .csv(f"file:///{file_path}")
    )

    return df


# Data Transformations
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofmonth,
    month,
    year,
    dayofweek,
    when,
    monotonically_increasing_id,
)
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Data_Transformation").getOrCreate()


def transform(df, *args, **kwargs):
    # Convert columns to timestamp type
    df = df.withColumn(
        "tpep_pickup_datetime", df["tpep_pickup_datetime"].cast("timestamp")
    )
    df = df.withColumn(
        "tpep_dropoff_datetime", df["tpep_dropoff_datetime"].cast("timestamp")
    )

    # Create datetime dimension
    datetime_dim = (
        df.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            hour("tpep_pickup_datetime").alias("pick_hour"),
            dayofmonth("tpep_pickup_datetime").alias("pick_day"),
            month("tpep_pickup_datetime").alias("pick_month"),
            year("tpep_pickup_datetime").alias("pick_year"),
            dayofweek("tpep_pickup_datetime").alias("pick_weekday"),
            hour("tpep_dropoff_datetime").alias("drop_hour"),
            dayofmonth("tpep_dropoff_datetime").alias("drop_day"),
            month("tpep_dropoff_datetime").alias("drop_month"),
            year("tpep_dropoff_datetime").alias("drop_year"),
            dayofweek("tpep_dropoff_datetime").alias("drop_weekday"),
        )
        .distinct()
        .withColumn("datetime_id", monotonically_increasing_id())
    )

    # Create other dimension tables
    passenger_count = {0: 1, 1: 2, 2: 3, 3: 5, 4: 6, 5: 4, 6: 0}

    passenger_count_dim = (
        df.select("passenger_count")
        .distinct()
        .withColumn("passenger_count_id", monotonically_increasing_id())
    )
    passenger_count_dim = passenger_count_dim.withColumn(
        "passenger_count",
        when(col("passenger_count_id") == 0, passenger_count[0])
        .when(col("passenger_count_id") == 1, passenger_count[1])
        .when(col("passenger_count_id") == 2, passenger_count[2])
        .when(col("passenger_count_id") == 3, passenger_count[3])
        .when(col("passenger_count_id") == 4, passenger_count[4])
        .when(col("passenger_count_id") == 5, passenger_count[5])
        .when(col("passenger_count_id") == 6, passenger_count[6]),
    )
    passenger_count_dim = passenger_count_dim.select(
        "passenger_count_id", "passenger_count"
    )

    trip_distance_dim = (
        df.select("trip_distance")
        .distinct()
        .withColumn("trip_distance_id", monotonically_increasing_id())
        .select("trip_distance_id", "trip_distance")
    )

    # Define rate code type dictionary
    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride",
    }

    # Create rate code dimension
    rate_code_dim = (
        df.select("RatecodeID")
        .distinct()
        .withColumn("rate_code_id", monotonically_increasing_id())
    )
    rate_code_dim = rate_code_dim.withColumn(
        "rate_code_name",
        when(col("RatecodeID") == 1, rate_code_type[1])
        .when(col("RatecodeID") == 2, rate_code_type[2])
        .when(col("RatecodeID") == 3, rate_code_type[3])
        .when(col("RatecodeID") == 4, rate_code_type[4])
        .when(col("RatecodeID") == 5, rate_code_type[5])
        .when(col("RatecodeID") == 6, rate_code_type[6])
        .otherwise("Group ride"),
    )
    rate_code_dim = rate_code_dim.select("rate_code_id", "RatecodeID", "rate_code_name")

    pickup_location_dim = (
        df.select("pickup_longitude", "pickup_latitude")
        .distinct()
        .withColumnRenamed("pickup_longitude", "pickup_longitude_id")
        .withColumnRenamed("pickup_latitude", "pickup_latitude_id")
    )
    pickup_location_dim = pickup_location_dim.withColumn(
        "pickup_location_id", monotonically_increasing_id()
    )

    dropoff_location_dim = (
        df.select("dropoff_longitude", "dropoff_latitude")
        .distinct()
        .withColumnRenamed("dropoff_longitude", "dropoff_longitude_id")
        .withColumnRenamed("dropoff_latitude", "dropoff_latitude_id")
    )
    dropoff_location_dim = dropoff_location_dim.withColumn(
        "dropoff_location_id", monotonically_increasing_id()
    )

    # Create payment type dimension
    # Define payment type name dictionary
    # Define payment type name dictionary
    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    # Create payment type dimension
    payment_type_dim = (
        df.select("payment_type")
        .distinct()
        .withColumn("payment_type_id", monotonically_increasing_id())
    )
    payment_type_dim = payment_type_dim.withColumn(
        "payment_type_name",
        when(col("payment_type") == 1, payment_type_name[1])
        .when(col("payment_type") == 2, payment_type_name[2])
        .when(col("payment_type") == 3, payment_type_name[3])
        .when(col("payment_type") == 4, payment_type_name[4])
        .when(col("payment_type") == 5, payment_type_name[5])
        .when(col("payment_type") == 6, payment_type_name[6])
        .otherwise("Dispute"),
    )
    payment_type_dim = payment_type_dim.select(
        "payment_type_id", "payment_type", "payment_type_name"
    )
    # Create fact table
    fact_table = (
        df.join(
            passenger_count_dim,
            df["passenger_count"] == passenger_count_dim["passenger_count"],
            how="left",
        )
        .join(
            trip_distance_dim,
            df["trip_distance"] == trip_distance_dim["trip_distance"],
            how="left",
        )
        .join(
            rate_code_dim, df["RatecodeID"] == rate_code_dim["RatecodeID"], how="left"
        )
        .join(
            pickup_location_dim,
            [
                df["pickup_longitude"] == pickup_location_dim["pickup_longitude_id"],
                df["pickup_latitude"] == pickup_location_dim["pickup_latitude_id"],
            ],
            how="left",
        )
        .join(
            dropoff_location_dim,
            [
                df["dropoff_longitude"] == dropoff_location_dim["dropoff_longitude_id"],
                df["dropoff_latitude"] == dropoff_location_dim["dropoff_latitude_id"],
            ],
            how="left",
        )
        .join(
            datetime_dim,
            [
                df["tpep_pickup_datetime"] == datetime_dim["tpep_pickup_datetime"],
                df["tpep_dropoff_datetime"] == datetime_dim["tpep_dropoff_datetime"],
            ],
            how="left",
        )
        .join(
            payment_type_dim,
            df["payment_type"] == payment_type_dim["payment_type"],
            how="left",
        )
        .select(
            "VendorID",
            "datetime_id",
            "passenger_count_id",
            "trip_distance_id",
            "rate_code_id",
            "store_and_fwd_flag",
            "pickup_location_id",
            "dropoff_location_id",
            "payment_type_id",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
        )
    )

    return {
        "datetime_dim": datetime_dim.toPandas().to_dict(orient="records"),
        "passenger_count_dim": passenger_count_dim.toPandas().to_dict(orient="records"),
        "trip_distance_dim": trip_distance_dim.toPandas().to_dict(orient="records"),
        "rate_code_dim": rate_code_dim.toPandas().to_dict(orient="records"),
        "pickup_location_dim": pickup_location_dim.toPandas().to_dict(orient="records"),
        "dropoff_location_dim": dropoff_location_dim.toPandas().to_dict(
            orient="records"
        ),
        "payment_type_dim": payment_type_dim.toPandas().to_dict(orient="records"),
        "fact_table": fact_table.toPandas().to_dict(orient="records"),
    }


# Data Exporter
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.oauth2 import service_account
from os import environ
from pandas import DataFrame

# Authentication setup"


def export_data_to_big_query(
    data,
    dataset_id="uber_data_temp",
    project_id="pde-3-414007",
    table_prefix="",
    if_exists="replace",
) -> None:
    """
    Export data to a BigQuery warehouse using Google Cloud's BigQuery client.
    """
    client = bigquery.Client(project=project_id)

    for key, value in data.items():
        table_id = (
            f"{project_id}.{dataset_id}.{table_prefix}{key}"  # Construct table ID
        )
        job_config = LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Corrected write disposition
        )
        client.load_table_from_dataframe(
            DataFrame(value), table_id, job_config=job_config
        ).result()

    return None


if __name__ == "__main__":
    loaded_data = load_data_from_bucket("uber-analytics-03")
    transformed_data = transform(df=loaded_data)
    print(export_data_to_big_query(data=transformed_data))
