'''
Benchmark downloading files from S3
'''

import boto3
import json
import time

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    start_time = time.time()
    src_bucket = event['bucket']
    src_keys = event['keys']
    
    total_bytes = 0.0

    # 모든 key를 다운로드하고 처리합니다.
    for key in src_keys:
        # get_object에서 실제 다운로드 발생
        response = s3_client.get_object(Bucket=src_bucket,Key=key)
        total_bytes += response['ContentLength']
        contents = response['Body'].read()

    time_in_secs = (time.time() - start_time)
    print "Time taken (s)",  time_in_secs
    print "Size (MB)", total_bytes / 1024/1024
    return time_in_secs