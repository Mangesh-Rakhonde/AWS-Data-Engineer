import boto3

glue = boto3.client('glue', region_name='us-east-1')  # Change region if needed

job_name = 'insurance-data-pipeline-job'
role_arn = 'arn:aws:iam::261000474012:user/Mangesh'  # Replace with your role
script_location = 's3://insurance-raw-data26/scripts/glue_script.py'
temp_dir = 's3://insurance-raw-data26/temp/'
#python_libs = 's3://insurance-raw-data26/libs/my_deps.zip'  # Optional

# 1. Create the Glue job
try:
    response = glue.create_job(
        Name=job_name,
        Role=role_arn,
        ExecutionProperty={'MaxConcurrentRuns': 1},
        Command={
            'Name': 'glueetl',
            'ScriptLocation': script_location,
            'PythonVersion': '3'
        },
        DefaultArguments={
            '--TempDir': temp_dir,
            '--job-language': 'python',
            '--additional-python-modules': 'boto3',  # Add if you use boto3 in script
            #'--extra-py-files': python_libs  # Optional if using external Python module
        },
        MaxRetries=0,
        GlueVersion='4.0',
        NumberOfWorkers=2,
        WorkerType='G.1X',
    )
except Exception as e:
    print("Error occured:".format(e))
else:
    print(f"Created Glue Job: {response['Name']}")





