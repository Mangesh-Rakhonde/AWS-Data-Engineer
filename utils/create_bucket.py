import boto3
from botocore.exceptions import ClientError

def create_s3_bucket(bucket_name, region="us-east-1"):
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=region)

        # Create bucket (different syntax for 'us-east-1' vs other regions)
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )

        print(f"✅ Bucket '{bucket_name}' created successfully in '{region}'.")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyExists':
            print(f"⚠️ Bucket '{bucket_name}' already exists!")
        else:
            print(f"❌ Error creating bucket: {e}")
        return False

def delete_s3_bucket(bucket_name):
    try:
        s3 = boto3.client('s3')
        s3.delete_bucket(Bucket=bucket_name)
        print(f"🗑️ Bucket '{bucket_name}' deleted successfully.")
    except ClientError as e:
        print(f"❌ Error deleting bucket: {e}")


create_s3_bucket("insurance-raw-data26")
#delete_s3_bucket("insurance-raw-data26")