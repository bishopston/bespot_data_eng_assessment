import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import count, desc, rank, avg, round, current_timestamp
from pyspark.sql.window import Window

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {"owner": "bespot", "retries": 5, "retry_delay": timedelta(minutes=5)}

spark = (
    SparkSession.builder.appName("bespot")
    .config(
        "spark.executor.extraClassPath",
        "/usr/local/airflow/jars/postgresql-42.6.0.jar",
    )
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.6.0.jar")
    .getOrCreate()
)

url = "jdbc:postgresql://postgres:5432/bespot"

properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

mode_ingest = "append"
mode_process = "overwrite"


def ingest():

    dataset1_schema = StructType(
        fields=[
            StructField("user", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("timestamp", IntegerType(), False),
            StructField("page", StringType(), False),
        ]
    )

    dataset2_schema = StructType(
        fields=[
            StructField("user", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("timestamp", IntegerType(), False),
            StructField("transaction", StringType(), False),
        ]
    )

    df_dataset1 = (
        spark.read.option("header", True)
        .schema(dataset1_schema)
        .csv("/usr/local/airflow/data/dataset1.csv")
    )
    df_dataset1 = df_dataset1.withColumnRenamed("user", "user_id").withColumn(
        "created_on", current_timestamp()
    )

    df_dataset2 = (
        spark.read.option("header", True)
        .schema(dataset2_schema)
        .csv("/usr/local/airflow/data/dataset2.csv")
    )
    df_dataset2 = df_dataset2.withColumnRenamed("user", "user_id").withColumn(
        "created_on", current_timestamp()
    )

    df_dataset1.show()
    df_dataset2.show()

    df_dataset1.write.jdbc(
        url=url, table="user_pages_process", mode=mode_ingest, properties=properties
    )

    df_dataset2.write.jdbc(
        url=url, table="user_trans_process", mode=mode_ingest, properties=properties
    )


def process():

    df_dataset1 = spark.read.jdbc(
        url=url,
        table="(SELECT * FROM user_pages_process) AS pages_table",
        properties=properties,
    )

    df_dataset1 = df_dataset1.withColumnRenamed("timestamp", "timestamp_page")

    df_dataset2 = spark.read.jdbc(
        url=url,
        table="(SELECT * FROM user_trans_process) AS trans_table",
        properties=properties,
    )

    df_dataset2 = df_dataset2.withColumnRenamed("timestamp", "timestamp_trans")

    # Page popularity:

    page_count_df = df_dataset1.groupBy("page").agg(count("page").alias("page_count"))

    pageRankSpec = Window.orderBy(desc("page_count"))
    page_popularity_df = page_count_df.withColumn("rank", rank().over(pageRankSpec))

    page_popularity_df = page_popularity_df.withColumn(
        "created_on", current_timestamp()
    )

    page_popularity_df.show()

    # Users per page:

    page_user_session_count_df = df_dataset1.groupBy(
        "user_id", "session_id", "page"
    ).agg(count("user_id").alias("page_count"))

    user_per_page_count_df_ = page_user_session_count_df.groupBy("page").agg(
        count("page_count").alias("user_per_page_count")
    )

    user_per_page_count_df_ = user_per_page_count_df_.withColumn(
        "created_on", current_timestamp()
    )

    user_per_page_count_df_.show()

    # Transactions per page:

    df_page_trans = df_dataset1.join(
        df_dataset2,
        (df_dataset1.user_id == df_dataset2.user_id)
        & (df_dataset1.session_id == df_dataset2.session_id),
    ).select(
        df_dataset1.user_id,
        df_dataset1.session_id,
        df_dataset1.timestamp_page,
        df_dataset2.timestamp_trans,
        df_dataset1.page,
    )

    df_page_trans_edit = df_page_trans.withColumn(
        "timestamp_delta",
        (df_page_trans.timestamp_trans - df_page_trans.timestamp_page),
    )

    timestampRankSpec = Window.partitionBy("user_id", "session_id").orderBy(
        "timestamp_delta"
    )
    df_page_trans_delta_ranked = df_page_trans_edit.withColumn(
        "rank", rank().over(timestampRankSpec)
    )

    trans_per_page_df = df_page_trans_delta_ranked.filter("rank = 1")

    trans_per_page_count_df = trans_per_page_df.groupBy("page").agg(
        count("rank").alias("trans_per_page")
    )

    trans_per_page_count_df = trans_per_page_count_df.withColumn(
        "created_on", current_timestamp()
    )

    trans_per_page_count_df.show()

    # Purchase time per user:

    timestampRankSpecDesc = Window.partitionBy("user_id", "session_id").orderBy(
        desc("timestamp_delta")
    )
    df_page_trans_delta_ranked_desc = df_page_trans_edit.withColumn(
        "rank", rank().over(timestampRankSpecDesc)
    )

    purchace_time_df = df_page_trans_delta_ranked_desc.filter("rank = 1")

    purchace_time_per_user_df = purchace_time_df.groupBy("user_id").agg(
        round(avg("timestamp_delta"), 2).alias("purchace_time_per_user")
    )

    purchace_time_per_user_df = purchace_time_per_user_df.withColumn(
        "created_on", current_timestamp()
    )

    purchace_time_per_user_df.show()

    # Write data to target tables

    page_popularity_df.write.jdbc(
        url=url, table="page_hits", mode=mode_process, properties=properties
    )
    user_per_page_count_df_.write.jdbc(
        url=url, table="page_users", mode=mode_process, properties=properties
    )
    trans_per_page_count_df.write.jdbc(
        url=url, table="page_trans", mode=mode_process, properties=properties
    )
    purchace_time_per_user_df.write.jdbc(
        url=url, table="user_purchasing_time", mode=mode_process, properties=properties
    )


with DAG(
    default_args=default_args,
    dag_id="dag_ingest_process_v27",
    description="Ingest and process data using solely python operators",
    start_date=datetime(2023, 5, 27),
    schedule_interval="@daily",
) as dag:

    task1 = PythonOperator(task_id="ingest", python_callable=ingest)
    task2 = PythonOperator(task_id="process", python_callable=process)

    task1 >> task2
