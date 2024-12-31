import boto3
import requests
import logging
import pandas as pd
import os
os.environ['PYSPARK_PYTHON'] = r"C:\Users\Boppana.haritha\cognito_rv2\Scripts\python.exe"
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

class ELTProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session
        logging.basicConfig(level=logging.INFO)

    def fetch_data(self, url: str):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data: {response.status_code}")
            return []

    def transform_data(self, data: list):
        df = pd.DataFrame(data)
        spark_df = self.spark.createDataFrame(df)
        spark_df_transformed = self.filter_data(spark_df)
        spark_df_transformed = self.add_additional_columns(spark_df_transformed)
        spark_df_transformed = self.perform_window_transformations(spark_df_transformed)
        spark_df_transformed = self.fill_missing_values(spark_df_transformed)
        return spark_df_transformed

    def filter_data(self, df):
        return df.filter(F.col("userId") == 1).select("id", "title", "body")

    def add_additional_columns(self, df):
        """
        splits the body column into an array of words using one or more whitespace 
        """
        df = df.withColumn("body_word_count", F.size(F.split(F.col("body"), r'\s+'))) \
               .withColumn("post_length", F.length(F.col("body"))) \
               .withColumn("contains_lorem", F.col("body").rlike("lorem")) \
               .withColumnRenamed("id", "post_id") \
               .withColumnRenamed("title", "post_title") \
               .withColumnRenamed("body", "post_body")
        
        # Trim whitespaces from title
        df = df.withColumn("post_title", F.trim(F.col("post_title")))

        return df

    def perform_window_transformations(self, df):
        """
        For example, rank posts by length and assign ranks.
        """
        window_spec = Window.orderBy(F.desc("post_length"))
        df = df.withColumn("post_rank", F.rank().over(window_spec))
        df = df.withColumn("cumulative_word_count", F.sum("body_word_count").over(window_spec))

        return df

    def fill_missing_values(self, df):
        return df.fillna({"post_body": "No content available", "post_title": "Untitled"})

    def upload_transformed_data_to_s3(self, df, s3_bucket: str, s3_key: str):
        try:
            df.write.option("header", "true").csv(f"s3a://{s3_bucket}/{s3_key}", mode="overwrite")
            logging.info(f"Successfully uploaded transformed data to s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            logging.error(f"Error uploading transformed data to S3: {e}")

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ELT Pipeline") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .getOrCreate()
    processor = ELTProcessor(spark)
    url = "https://jsonplaceholder.typicode.com/posts"
    raw_data = processor.fetch_data(url)
    transformed_df = processor.transform_data(raw_data)
    transformed_data_s3_bucket = "pysparrk-test"
    transformed_data_s3_key = "transformed_data/posts_transformed"
    processor.upload_transformed_data_to_s3(transformed_df, transformed_data_s3_bucket, transformed_data_s3_key)

    spark.stop()
