from get_raw_data_from_s3 import get_raw_data

def process_data():
    dfs=get_raw_data()
    if len(dfs)>0:
        [claim_df, customer_df, policies_df] = dfs
    else:
        print("Bucket is empty or no such files in bucket.")


