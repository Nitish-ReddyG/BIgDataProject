ifrom boto3.session import *
import os
# Set up AWS credentials
access_kiey = 'XXXXXXXXXXXX'
secret_key = 'XXXXXXXXXXXX'

# Set up S3 bucket and file paths
bucket_name = 'crimes-raw'
s3_output_prefix = 'files'
local_input_path = '/home/ubuntu/newyork.csv'
s3_output_path = os.path.join(s3_output_prefix, os.path.basename(local_input_path))

# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Upload file to S3 bucket
with open(local_input_path, 'rb') as f:
    s3.upload_fileobj(f, bucket_name, s3_output_path)

print(f'File uploaded to s3://{bucket_name}/{s3_output_path}')
