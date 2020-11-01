'''
REDUCER Coordinator 

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
import lambdautils
import random
import re
import time
import urllib

DEFAULT_REGION = "us-east-1"

### STATES 상태 변수
MAPPERS_DONE = 0
REDUCER_STEP = 1

# S3 session 생성
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
# Lambda session 생성
lambda_client = boto3.client('lambda')


# 주어진 bucket 위치 경로에 파일 이름이 key인 object와 data를 저장합니다.
def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)


# Reducer의 상태 정보를 bucket에 저장합니다.
def write_reducer_state(n_reducers, n_s3, bucket, fname):
    ts = time.time()
    data = json.dumps({
        "reducerCount": '%s' % n_reducers,
        "totalS3Files": '%s' % n_s3,
        "start_time": '%s' % ts
    })
    write_to_s3(bucket, fname, data, {})


# mapper의 파일 개수를 카운트 합니다. 파일 개수가 reducer의 step 수를 결정
def get_mapper_files(files):
    ret = []
    for mf in files:
        if "task/mapper" in mf["Key"]:
            ret.append(mf)
    return ret


# reducer의 배치 ㅅ이지를 가져옵니다.
def get_reducer_batch_size(keys):
    # TODO: Paramertize memory size
    batch_size = lambdautils.compute_batch_size(keys, 1536, 1000)
    return max(batch_size, 2)  # 적어도 배치가 2개 라면 - 종료


# 작업이 끝났는 지 확인합니다.
def check_job_done(files):
    # TODO: USE re
    for f in files:
        if "result" in f["Key"]:
            return True
    return False


# Reducer의 state 정보를 Bucket에서 가져옵니다.
def get_reducer_state_info(files, job_id, job_bucket):
    reducers = [];
    max_index = 0;
    reducer_step = False;
    r_index = 0;

    # Step이 완료가 되었는지 확인합니다.
    # Reducer의 상태를 확인합니다.
    # 마지막 Reducer step인지 결정합니다.
    for f in files:
        if "reducerstate." in f['Key']:
            idx = int(f['Key'].split('.')[1])
            if idx > r_index:
                r_index = idx
            reducer_step = True

    # Reducer의 상태가 완료인지 확인합니다. 
    if reducer_step == False:
        return [MAPPERS_DONE, get_mapper_files(files)]
    else:
        # Reduce steop이 완료되었다면 Bucket에 작성합니다.
        key = "%s/reducerstate.%s" % (job_id, r_index)
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = json.loads(response['Body'].read())

        for f in files:
            fname = f['Key']
            parts = fname.split('/')
            if len(parts) < 3:
                continue
            rFname = 'reducer/' + str(r_index)
            if rFname in fname:
                reducers.append(f)

        if int(contents["reducerCount"]) == len(reducers):
            return (r_index, reducers)
        else:
            return (r_index, [])


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    start_time = time.time();

    # Job Bucket으로 이 Bucket으로부터 notification을 받습니다.
    bucket = event['Records'][0]['s3']['bucket']['name']
    config = json.loads(open('./jobinfo.json', "r").read())

    job_id = config["jobId"]
    map_count = config["mapCount"]
    r_function_name = config["reducerFunction"]
    r_handler = config["reducerHandler"]

    ### Mapper 완료된 수를 count 합니다. ###

    # Job 파일들을 가져옵니다.
    files = s3_client.list_objects(Bucket=bucket, Prefix=job_id)["Contents"]

    if check_job_done(files) == True:
        print("Job done!!! Check the result file")
        return
    else:
        mapper_keys = get_mapper_files(files)
        print("Mappers Done so far ", len(mapper_keys))

        if map_count == len(mapper_keys):

            # 모든 mapper가 완료되었다면, reducer를 시작합니다.
            stepInfo = get_reducer_state_info(files, job_id, bucket)

            print("stepInfo", stepInfo)

            step_number = stepInfo[0]
            reducer_keys = stepInfo[1]

            if len(reducer_keys) == 0:
                print("Still waiting to finish Reducer step ", step_number)
                return

            # 메타데이터(metadata)의 파일을 기반으로 Reduce의 배치 사이즈를 계산합니다.
            r_batch_size = get_reducer_batch_size(reducer_keys);

            print("Starting the the reducer step", step_number)
            print("Batch Size", r_batch_size)

            r_batch_params = lambdautils.batch_creator(reducer_keys, r_batch_size);

            n_reducers = len(r_batch_params)
            n_s3 = n_reducers * len(r_batch_params[0])
            step_id = step_number + 1

            for i in range(len(r_batch_params)):
                batch = [b['Key'] for b in r_batch_params[i]]

                # Reducer Lambda를 비동기식(asynchronously)으로 호출(invoke)합니다.
                resp = lambda_client.invoke(
                    FunctionName=r_function_name,
                    InvocationType='Event',
                    Payload=json.dumps({
                        "bucket": bucket,
                        "keys": batch,
                        "jobBucket": bucket,
                        "jobId": job_id,
                        "nReducers": n_reducers,
                        "stepId": step_id,
                        "reducerId": i
                    })
                )
                print(resp)

            # Reducer의 상태를 S3에 저장합니다.
            fname = "%s/reducerstate.%s" % (job_id, step_id)
            write_reducer_state(n_reducers, n_s3, bucket, fname)
        else:
            print("Still waiting for all the mappers to finish ..")
