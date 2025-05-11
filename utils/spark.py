from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

def get_spark_session():
    load_dotenv()
    spark = SparkSession.builder.appName("Insurance_spark_app").getOrCreate()
    spark.conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark


def create_dataframe():
    spark=get_spark_session()
    ins_claim_df = (spark.read.format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("../raw-data/Insurance_Claims.csv"))

    ins_customer_df = (spark.read.format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("../raw-data/Insurance_Customers.csv"))

    ins_policies_df = (spark.read.format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("../raw-data/Insurance_Policies.csv"))


    return [ins_claim_df,ins_customer_df,ins_policies_df]

# [df1,df2,df3]=create_dataframe()
# df1.show(10)