import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp


def main():

    spark = (
        SparkSession.builder.appName("bespot")
        .config("spark.executor.extraClassPath", "/mounted-data/postgresql-42.6.0.jar")
        .config("spark.jars", "/mounted-data/postgresql-42.6.0.jar")
        .getOrCreate()
    )

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
        .csv("/mounted-data/dataset1.csv")
    )
    df_dataset1 = df_dataset1.withColumnRenamed("user", "user_id").withColumn(
        "created_on", current_timestamp()
    )

    df_dataset2 = (
        spark.read.option("header", True)
        .schema(dataset2_schema)
        .csv("/mounted-data/dataset2.csv")
    )
    df_dataset2 = df_dataset2.withColumnRenamed("user", "user_id").withColumn(
        "created_on", current_timestamp()
    )

    df_dataset1.show()
    df_dataset2.show()

    url = "jdbc:postgresql://postgres/bespot"

    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
    }

    mode = "append"

    df_dataset1.write.jdbc(
        url=url, table="user_pages_process", mode=mode, properties=properties
    )

    df_dataset2.write.jdbc(
        url=url, table="user_trans_process", mode=mode, properties=properties
    )


if __name__ == "__main__":
    main()
