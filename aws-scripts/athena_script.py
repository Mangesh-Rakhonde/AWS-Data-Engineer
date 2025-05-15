import boto3
import time

region = 'us-east-1'
database = 'insurance_db'
s3_output = 's3://insurance-raw-data26/athena-results/'  # Make sure this exists

athena = boto3.client('athena', region_name=region)

def run_athena_query(query, db=None):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': db} if db else {},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    return response['QueryExecutionId']

def wait_for_query(query_execution_id):
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status
        time.sleep(2)

def get_query_results(query_execution_id):
    results = athena.get_query_results(QueryExecutionId=query_execution_id)
    for row in results['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])

# 1. Create Database
print("🔧 Creating Athena database...")
run_athena_query(f"CREATE DATABASE IF NOT EXISTS {database};")
time.sleep(2)

# 2. Define table schemas and locations
tables = {
    "claim_data": {
        "location": "s3://insurance-raw-data26/processed-data/claim_data.parquet/",
        "schema": """
            ClaimID string,
            PolicyID string,
            CustomerID string,
            ClaimAmount double,
            ClaimDate string
        """
    },
    "customer_data": {
        "location": "s3://insurance-raw-data26/processed-data/customer_data.parquet/",
        "schema": """
            CustomerID string,
            FirstName string,
            LastName string,
            DOB string,
            Age int
        """
    },
    "policies_data": {
        "location": "s3://insurance-raw-data26/processed-data/policies_data.parquet/",
        "schema": """
            PolicyID string,
            CustomerID string,
            StartDate string,
            EndDate string,
            DurationMonths int
        """
    },
    "claims_with_details": {
        "location": "s3://insurance-raw-data26/processed-data/claims_with_details.parquet/",
        "schema": """
            ClaimID string,
            PolicyID string,
            CustomerID string,
            ClaimAmount double,
            ClaimDate string,
            FirstName string,
            LastName string,
            DOB string,
            Age int,
            StartDate string,
            EndDate string,
            DurationMonths int
        """
    }
}

# 3. Create tables
for table_name, info in tables.items():
    print(f"📁 Creating table: {table_name}")
    create_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
        {info['schema']}
    )
    STORED AS PARQUET
    LOCATION '{info['location']}';
    """
    qid = run_athena_query(create_query, database)
    if wait_for_query(qid) == 'SUCCEEDED':
        print(f"✅ Created: {table_name}")
    else:
        print(f"❌ Failed to create: {table_name}")

# 4. Sample query on each
for table_name in tables:
    print(f"🔍 Querying {table_name}...")
    qid = run_athena_query(f"SELECT * FROM {table_name} LIMIT 5;", database)
    if wait_for_query(qid) == 'SUCCEEDED':
        get_query_results(qid)
    else:
        print(f"❌ Query failed for: {table_name}")
