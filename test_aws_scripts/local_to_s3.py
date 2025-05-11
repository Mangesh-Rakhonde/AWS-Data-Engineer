import boto3
from botocore.exceptions import ClientError
from utils.spark import create_dataframe,get_spark_session


class DataValidation:

    @staticmethod
    def local_to_s3():

        [claim_df,customer_df,policies_df]=create_dataframe()

        #aading dfs in s3 bucket
        try:
            (claim_df.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")   # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/raw-data/claim_data.parquet"))

            (customer_df.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")  # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/raw-data/customer_data.parquet"))

            (policies_df.write
             .format("parquet")
             .option("header", "true")
             .mode("overwrite")  # Options: overwrite, append, ignore, error
             .save("s3a://insurance-raw-data26/raw-data/policies_data.parquet"))
        except ClientError as e:
            print("Error occurs during keeping df in s3".format(e))

        except Exception as e:
            print("Error occures:".format(e))

        else:
            print("DataFrames successfully added to s3.")

    @staticmethod
    def remove_from_s3():
        try:
            objects_to_delete = [
                {"Key": "raw-data/policies_data.parquet"},
                {"Key": "raw-data/customer_data.parquet"},
                {"Key": "raw-data/claim_data.parquet"},
                {"Key": "raw-data"}
            ]
            s3=boto3.client("s3")
            response = s3.delete_objects(
                Bucket="insurance-raw-data26",
                Delete={"Objects": objects_to_delete}
            )
            print(response)
        except ClientError as e:
            print("Error {}".format(e))

        except Exception as e:
            print("Error occures:".format(e))

        else:
            print("Deleted files:", [obj["Key"] for obj in response.get("Deleted", [])])

    @staticmethod
    def empty_bucket(bucket_name):
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket_name)

            # Delete all objects (files)
            bucket.objects.all().delete()

            # If versioning is enabled, delete all versions
            bucket.object_versions.delete()
        except ClientError as e:
            print(f"Error occures {e}")

        except Exception as e:
            print("Error occures:".format(e))

        else:
            print(f"Emptied bucket: {bucket_name}")



DataValidation.local_to_s3()

#DataValidation.remove_from_s3()
#DataValidation.empty_bucket("insurance-raw-data26")