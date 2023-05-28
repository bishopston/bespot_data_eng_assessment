import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, rank, avg, round, current_timestamp
from pyspark.sql.window import Window


def main():

    spark = (
        SparkSession.builder.appName("bespot")
        .config("spark.executor.extraClassPath", "/mounted-data/postgresql-42.6.0.jar")
        .config("spark.jars", "/mounted-data/postgresql-42.6.0.jar")
        .getOrCreate()
    )

    df_dataset1 = spark.read.jdbc(
        url="jdbc:postgresql://postgres/bespot",
        table="(SELECT * FROM user_pages_process) AS pages_table",
        properties={
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
        },
    )

    df_dataset1 = df_dataset1.withColumnRenamed("timestamp", "timestamp_page")

    df_dataset2 = spark.read.jdbc(
        url="jdbc:postgresql://postgres/bespot",
        table="(SELECT * FROM user_trans_process) AS trans_table",
        properties={
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
        },
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

    url = "jdbc:postgresql://postgres/bespot"

    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
    }

    mode = "overwrite"

    page_popularity_df.write.jdbc(
        url=url, table="page_hits", mode=mode, properties=properties
    )
    user_per_page_count_df_.write.jdbc(
        url=url, table="page_users", mode=mode, properties=properties
    )
    trans_per_page_count_df.write.jdbc(
        url=url, table="page_trans", mode=mode, properties=properties
    )
    purchace_time_per_user_df.write.jdbc(
        url=url, table="user_purchasing_time", mode=mode, properties=properties
    )


if __name__ == "__main__":
    main()
