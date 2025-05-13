from get_raw_data_from_s3 import get_raw_data
from pyspark.sql.functions import current_date, datediff
from pyspark.sql.functions import to_date, months_between
from botocore.exceptions import ClientError

def process_data():
    dfs=get_raw_data()
    spark=dfs[3]
    if len(dfs)>0:
        [claim_df,customer_df,policies_df,_] = dfs

        #Customer df transformation
        customer_df = customer_df.withColumn("Age", (datediff(current_date(), customer_df.DOB) / 365).cast("int"))
        #customer_df.show()

        #Policies Transformation
        policies_df=policies_df.withColumn(
            "DurationMonths",
            (
                months_between(to_date("EndDate"),to_date("StartDate")).cast("int")
            )
        )

        #policies_df.show()

        # Join claims with policies and customers to get full claim context
        claims_with_details = claim_df.join(policies_df, "PolicyID", "inner") \
            .join(customer_df, "CustomerID", "inner")
        #claims_with_details.show()



        #Load proceessed data back to s3 bucket
        try:
            (claim_df.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")  # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/processed-data/claim_data.parquet"))

            (customer_df.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")  # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/processed-data/customer_data.parquet"))

            (policies_df.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")  # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/processed-data/policies_data.parquet"))

            (claims_with_details.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")  # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/processed-data/claims_with_details.parquet"))
        except ClientError as e:
            print("Error occurs during keeping df in s3".format(e))

        except Exception as e:
            print("Error occures:".format(e))

        else:
            print("Processed DataFrames successfully added to s3.")

        spark.stop()

    else:
        print("Bucket is empty or no such files in bucket.")


process_data()