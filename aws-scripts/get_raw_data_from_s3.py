from utils.spark import get_spark_session



def get_raw_data():
    spark=get_spark_session()
    try:
        raw_claim_df=(spark.read.format("parquet")
                .option("inferSchema","true")
                .option("header","true")
                .load("s3a://insurance-raw-data26/raw-data/claim_data.parquet"))

        raw_customer_df = (spark.read.format("parquet")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load("s3a://insurance-raw-data26/raw-data/customer_data.parquet"))

        raw_policies_df = (spark.read.format("parquet")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load("s3a://insurance-raw-data26/raw-data/policies_data.parquet"))

    except Exception as e:
        print("Error occures:".format(e))

    else:
        return [raw_claim_df,raw_customer_df,raw_policies_df]


# dfs=get_raw_data()
# if len(dfs)>0:
#     [claim_df,customer_df,policies_df]=dfs
#     claim_df.show(5)
# else:
#     print("No files present in the bucket.")