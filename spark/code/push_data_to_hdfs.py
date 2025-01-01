from pyspark.sql.session import SparkSession

from datetime import datetime

run_time = "{:%d%m%Y}".format(datetime.now())

if __name__ == "__main__":
    spark = SparkSession.builder.appName("extract_load").getOrCreate()
    df = spark.read.parquet("/opt/airflow/code/google_play_phone/" + run_time + ".parquet")
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/google_play_phone/" + run_time)
    df = spark.read.parquet("/opt/airflow/code/google_play_tablet/" + run_time + ".parquet")
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/google_play_tablet/" + run_time)


	# extract_load("https://apps.apple.com/vn/genre/ios-tr%C3%B2-ch%C6%A1i/id6014?l=vi", "/vn/app/", "", "app_store")