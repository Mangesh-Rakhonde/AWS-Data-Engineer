from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_date, datediff, to_date, months_between
from botocore.exceptions import ClientError

# Import your module (must be zipped and added as Python library in Glue)
#from get_raw_data_from_s3 import get_raw_data

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def get_raw_data():
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


def process_data():
    dfs = get_raw_data()  # Make sure this function uses the Glue Spark session
    if len(dfs) > 0:
        [claim_df, customer_df, policies_df] = dfs

        # Customer transformation
        customer_df = customer_df.withColumn("Age", (datediff(current_date(), customer_df.DOB) / 365).cast("int"))

        # Policies transformation
        policies_df = policies_df.withColumn(
            "DurationMonths",
            months_between(to_date("EndDate"), to_date("StartDate")).cast("int")
        )

        # Join data
        claims_with_details = claim_df.join(policies_df, "PolicyID", "inner") \
            .join(customer_df, "CustomerID", "inner")

        # Save transformed data to S3
        try:
            claim_df.write.mode("overwrite").parquet("s3://insurance-raw-data26/processed-data/claim_data/")
            customer_df.write.mode("overwrite").parquet("s3://insurance-raw-data26/processed-data/customer_data/")
            policies_df.write.mode("overwrite").parquet("s3://insurance-raw-data26/processed-data/policies_data/")
            claims_with_details.write.mode("overwrite").parquet(
                "s3://insurance-raw-data26/processed-data/claims_with_details/")

        except ClientError as e:
            print(f"ClientError occurred: {e}")
        except Exception as e:
            print(f"Exception occurred: {e}")
        else:
            print("Processed DataFrames successfully added to S3.")
    else:
        print("Bucket is empty or no such files in bucket.")


process_data()
