from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.builder.appName("Insurance_spark_app").getOrCreate()
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