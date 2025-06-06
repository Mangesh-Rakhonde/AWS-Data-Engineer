import boto3

glue = boto3.client('glue', region_name='us-east-1')

job_name = 'insurance-data-pipeline-job'
#Starting glue job
response = glue.start_job_run(JobName=job_name)
run_id = response['JobRunId']
print(f"Started Glue Job run: {run_id}")


#To monitor job status
import time

while True:
    status = glue.get_job_run(JobName=job_name, RunId=run_id)
    state = status['JobRun']['JobRunState']
    print(f"Job status: {state}")
    if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
        break
    time.sleep(10)

# 3. Check result and fetch error if failed
job_run = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']

if state == 'SUCCEEDED':
    print("✅ Glue job completed successfully.")
else:
    print(f"❌ Glue job failed with state: {state}")
    print("🔍 Error message:")
    print(job_run.get('ErrorMessage', 'No detailed error message provided.'))