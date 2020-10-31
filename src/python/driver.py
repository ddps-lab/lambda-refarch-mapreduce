#-*- coding: utf-8 -*-
'''
 Driver to start BigLambda Job
 
 
 * Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License. 
'''

import boto3
import json
import math
import random
import re
from io import StringIO
import sys
import time

import lambdautils

import glob
import subprocess 
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

from botocore.client import Config

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

JOB_INFO = 'jobinfo.json'

### utils ####
# 라이브러리와 코드 zip 패키징 
def zipLambda(fname, zipname):
    # faster to zip with shell exec
    subprocess.call(['zip', zipname] + glob.glob(fname) + glob.glob(JOB_INFO) +
                        glob.glob("lambdautils.py"))

# S3 Bucket에 file name(key), json(data) 저장
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

# 실행 중인 job에 대한 정보를 json 으로 로컬에 저장
def write_job_config(job_id, job_bucket, n_mappers, r_func, r_handler):
    fname = "jobinfo.json"
    with open(fname, 'w') as f:
        data = json.dumps({
            "jobId": job_id,
            "jobBucket" : job_bucket,
            "mapCount": n_mappers,
            "reducerFunction": r_func,
            "reducerHandler": r_handler
            }, indent=4)
        f.write(data)


######### MAIN ############# 
## JOB ID 이름을 설정해주세요.
job_id =  "bl-release"

# Config 파일
config = json.loads(open('driverconfig.json', 'r').read())

# 1. Driver Job에 대한 설정 파일driverconfig) json 파일의 모든 key-value를 저장
bucket = config["bucket"]
job_bucket = config["jobBucket"]
region = config["region"]
lambda_memory = config["lambdaMemory"] # lambda 실제 메모리
concurrent_lambdas = config["concurrentLambdas"] # 동시 실행 가능 수
lambda_read_timeout = config["lambda_read_timeout"]
boto_max_connections = config["boto_max_connections"]

# Lambda의 결과를 읽기 위한 timeout을 길게, connections pool을 많이 지정합니다.
lambda_config = Config(read_timeout=lambda_read_timeout, max_pool_connections=boto_max_connections)
lambda_client = boto3.client('lambda', config=lambda_config)

# prefix와 일치하는 모든 S3 bucket의 key를 가져옵니다.
all_keys = []
for obj in s3.Bucket(bucket).objects.filter(Prefix=config["prefix"]).all():
    all_keys.append(obj)

bsize = lambdautils.compute_batch_size(all_keys, lambda_memory, concurrent_lambdas)
batches = lambdautils.batch_creator(all_keys, bsize)
n_mappers = len(batches) # 최종적으로 구한 batches의 개수가 mapper로 결정

# 2. Lambda Function 을 생성합니다.
L_PREFIX = "BL"

# Lambda Functions 이름을 지정합니다.
mapper_lambda_name = L_PREFIX + "-mapper-" +  job_id;
reducer_lambda_name = L_PREFIX + "-reducer-" +  job_id; 
rc_lambda_name = L_PREFIX + "-rc-" +  job_id;

# Job 환경 설정을 json으로 파일 씁니다.
write_job_config(job_id, job_bucket, n_mappers, reducer_lambda_name, config["reducer"]["handler"]);

# 각 mapper와 reducer와 coordinator의 lambda_handler 코드를 패키징하여 압축합니다.
zipLambda(config["mapper"]["name"], config["mapper"]["zip"])
zipLambda(config["reducer"]["name"], config["reducer"]["zip"])
zipLambda(config["reducerCoordinator"]["name"], config["reducerCoordinator"]["zip"])

# Mapper를 Lambda Function에 등록합니다.
l_mapper = lambdautils.LambdaManager(lambda_client, s3_client, region, config["mapper"]["zip"], job_id,
        mapper_lambda_name, config["mapper"]["handler"])
l_mapper.update_code_or_create_on_noexist()

# Reducer를 Lambda Function에 등록합니다.
l_reducer = lambdautils.LambdaManager(lambda_client, s3_client, region, config["reducer"]["zip"], job_id,
        reducer_lambda_name, config["reducer"]["handler"])
l_reducer.update_code_or_create_on_noexist()

# Coordinator를 Lambda Function에 등록합니다.
l_rc = lambdautils.LambdaManager(lambda_client, s3_client, region, config["reducerCoordinator"]["zip"], job_id,
        rc_lambda_name, config["reducerCoordinator"]["handler"])
l_rc.update_code_or_create_on_noexist()

# Coordinator에 작업을 할 Bucket에 대한 권한(permission)을 부여합니다.
l_rc.add_lambda_permission(random.randint(1,1000), job_bucket)

# Coordinator에 작업을 할 Bucket에 대한 알림(notification)을 부여합니다.
l_rc.create_s3_eventsource_notification(job_bucket)

# 실행 중인 job에 대한 정보를 json 으로 S3에 저장
j_key = job_id + "/jobdata"
data = json.dumps({
                "mapCount": n_mappers, 
                "totalS3Files": len(all_keys),
                "startTime": time.time()
                })
write_to_s3(job_bucket, j_key, data, {})

######## MR 실행 ########

mapper_outputs = []

# 3. Invoke Mappers
def invoke_lambda(batches, m_id):
    '''
    Lambda 함수를 호출(invoke) 합니다.
    '''

    batch = [k.key for k in batches[m_id-1]]

    resp = lambda_client.invoke( 
            FunctionName = mapper_lambda_name,
            InvocationType = 'RequestResponse',
            Payload =  json.dumps({
                "bucket": bucket,
                "keys": batch,
                "jobBucket": job_bucket,
                "jobId": job_id,
                "mapperId": m_id
            })
        )
    out = eval(resp['Payload'].read())
    mapper_outputs.append(out)
    print("mapper output", out)

# 병렬 실행 Parallel Execution
print("# of Mappers ", n_mappers)
pool = ThreadPool(n_mappers)
Ids = [i+1 for i in range(n_mappers)]
invoke_lambda_partial = partial(invoke_lambda, batches)

# Mapper의 개수 만큼 요청 Request Handling
mappers_executed = 0
while mappers_executed < n_mappers:
    nm = min(concurrent_lambdas, n_mappers)
    results = pool.map(invoke_lambda_partial, Ids[mappers_executed: mappers_executed + nm])
    mappers_executed += nm

pool.close()
pool.join()

print("all the mappers finished ...")

# Mapper Lambda function 삭제
# l_mapper.delete_function()

# 실제 Reduce 호출은 reducerCoordinator에서 실행

# 실행 시간을 이용해 대략적인 비용을 계산합니다.
total_lambda_secs = 0
total_s3_get_ops = 0
total_s3_put_ops = 0
s3_storage_hours = 0
total_lines = 0

for output in mapper_outputs:
    total_s3_get_ops += int(output[0])
    total_lines += int(output[1])
    total_lambda_secs += float(output[2])

mapper_lambda_time = total_lambda_secs

#Note: Wait for the job to complete so that we can compute total cost ; create a poll every 10 secs

# 모든 reducer의 keys를 가져옵니다.
reducer_keys = []

# Reducer의 전체 실행 시간을 가져옵니다.
reducer_lambda_time = 0

while True:
    job_keys = s3_client.list_objects(Bucket=job_bucket, Prefix=job_id)["Contents"]
    keys = [jk["Key"] for jk in job_keys]
    total_s3_size = sum([jk["Size"] for jk in job_keys])
    
    print("check to see if the job is done")

    # check job done
    if job_id + "/result" in keys:
        print("job done")
        reducer_lambda_time += float(s3.Object(job_bucket, job_id + "/result").metadata['processingtime'])
        for key in keys:
            if "task/reducer" in key:
                reducer_lambda_time += float(s3.Object(job_bucket, key).metadata['processingtime'])
                reducer_keys.append(key)
        break
    time.sleep(5)

# S3 Storage 비용 - mapper만 계산합니다.
# 비용은 3 cents/GB/month
s3_storage_hour_cost = 1 * 0.0000521574022522109 * (total_s3_size/1024.0/1024.0/1024.0) # cost per GB/hr 

s3_put_cost = len(job_keys) *  0.005/1000 # PUT, COPY, POST, LIST 요청 비용 Request 0.005 USD / request 1000

total_s3_get_ops += len(job_keys) 
s3_get_cost = total_s3_get_ops * 0.004/10000  # GET, SELECT, etc 요청 비용 Request 0.0004 USD / request 1000

# 전체 Lambda 비용 계산
# Lambda Memory 1024MB cost Request 100ms : 0.000001667 USD
total_lambda_secs += reducer_lambda_time
lambda_cost = total_lambda_secs * 0.00001667 * lambda_memory / 1024.0
s3_cost = (s3_get_cost + s3_put_cost + s3_storage_hour_cost)

# Cost 출력
#print "Reducer Lambda Cost", reducer_lambda_time * 0.00001667 * lambda_memory/ 1024.0
print("Mapper Execution Time", mapper_lambda_time)
print("Reducer Execution Time", reducer_lambda_time)
print("Tota Lambda Execution Time", total_lambda_secs)
print("Lambda Cost", lambda_cost)
print("S3 Storage Cost", s3_storage_hour_cost)
print("S3 Request Cost", s3_get_cost + s3_put_cost )
print("S3 Cost", s3_cost )
print("Total Cost: ", lambda_cost + s3_cost)
print("Total Latency: ", total_lambda_secs) 
print("Result Output Lines:", total_lines)

# Reducer Lambda function 삭제
# l_reducer.delete_function()
# l_rc.delete_function()
