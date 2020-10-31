# -*- coding: utf-8 -*-
'''
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
import botocore
import os


class LambdaManager(object):
    def __init__(self, l, s3, region, codepath, job_id, fname, handler, lmem=1024):
        self.awslambda = l
        self.region = "us-east-1" if region is None else region
        self.s3 = s3
        self.codefile = codepath
        self.job_id = job_id
        self.function_name = fname
        self.handler = handler
        self.role = os.environ.get('serverless_mapreduce_role')
        self.memory = lmem
        self.timeout = 900
        self.function_arn = None  # Lambda Function이 생성된 후에 설정됩니다.

    def create_lambda_function(self):
        '''
        AWS Lambda Function을 새로 생성하고 코드를 패키징한 zip 파일을 이용해 업데이트 합니다.
        '''
        runtime = 'python3.6'
        response = self.awslambda.create_function(
            FunctionName=self.function_name,
            Code={
                "ZipFile": open(self.codefile, 'rb').read()
            },
            Handler=self.handler,
            Role=self.role,
            Runtime=runtime,
            Description=self.function_name,
            MemorySize=self.memory,
            Timeout=self.timeout
        )
        self.function_arn = response['FunctionArn']
        print(response)

    def update_function(self):
        '''
        AWS Lambda Function의 코드를 패키징한 zip 파일을 이용해 업데이트 합니다.
        '''
        response = self.awslambda.update_function_code(
            FunctionName=self.function_name,
            ZipFile=open(self.codefile, 'rb').read(),
            Publish=True
        )
        updated_arn = response['FunctionArn']
        # parse arn and remove the release number (:n) 
        arn = ":".join(updated_arn.split(':')[:-1])
        self.function_arn = arn
        print(response)

    def update_code_or_create_on_noexist(self):
        '''
        AWS Lambda Functions가 존재한다면 업데이트를 하고, 없다면 생성합니다.
        '''
        try:
            self.create_lambda_function()
        except botocore.exceptions.ClientError as e:
            # parse (Function already exist) 
            self.update_function()

    def add_lambda_permission(self, sId, bucket):
        '''
        AWS Lambda의 권한(permission)을 설정합니다.
        S3 Bucket에서 이벤트시에 AWS Lambda를 Trigger 합니다.
        '''
        resp = self.awslambda.add_permission(
            Action='lambda:InvokeFunction',
            FunctionName=self.function_name,
            Principal='s3.amazonaws.com',
            StatementId='%s' % sId,
            SourceArn='arn:aws:s3:::' + bucket
        )
        print(resp)

    def create_s3_eventsource_notification(self, bucket, prefix=None):
        '''
        S3에서 발생하는 이벤트(Object 생성)를 Lambda function으로 알림(notifincation) 설정합니다.
        '''
        if not prefix:
            prefix = self.job_id + "/task";

        self.s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'Events': ['s3:ObjectCreated:*'],
                        'LambdaFunctionArn': self.function_arn,
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'prefix',
                                        'Value': prefix
                                    },
                                ]
                            }
                        }
                    }
                ],
                # 'TopicConfigurations' : [],
                # 'QueueConfigurations' : []
            }
        )

    def delete_function(self):
        '''
        등록된 AWS Lambda 함수(Function) 제거
        '''
        self.awslambda.delete_function(FunctionName=self.function_name)

    @classmethod
    def cleanup_logs(cls, func_name):
        '''
        CloudWatch에서 Lambda의 log group과 log streams을 제거합니다.
        '''
        log_client = boto3.client('logs')
        response = log_client.delete_log_group(logGroupName='/aws/lambda/' + func_name)
        return response


def compute_batch_size(keys, lambda_memory, concurrent_lambdas):
    '''
    Lambda의 메모리 크기와 동시 실행 수를 고려하여 batch size 계산합니다.
    '''
    max_mem_for_data = 0.6 * lambda_memory * 1000 * 1000;  # Lambda 메모리 전체에 적재 가능한 최대 데이터 크기
    size = 0.0  # 전체 데이터 셋 사이즈 여기서는 24.4 GB
    for key in keys:
        if isinstance(key, dict):
            size += key['Size']
        else:
            size += key.size
    avg_object_size = size / len(keys)  # object 당 평균 크기 / len(keys) : input object의 전체 개수
    print("Dataset size: %s, nKeys: %s, avg: %s" % (size, len(keys), avg_object_size))

    # 평균 object 크기가 하나의 Lambda에서 돌릴 수 있고, input object의 전체 개수가 동시 실행 수보다 적다면 
    if avg_object_size < max_mem_for_data and len(keys) < concurrent_lambdas:
        b_size = 1
    else:
        b_size = int(round(max_mem_for_data / avg_object_size))  # 전체 메모리에서 평균 object 크기를 나눠 batch size를 계산합니다.
        # 메모리가 충분히 크다면 하나의 Lambda에서 여러 개의 파일을 처리합니다.
    print("Batch size : %s" % (b_size))
    return b_size


def batch_creator(all_keys, batch_size):
    '''
    S3 Bucket에 있는 파일의 이름을 배치 사이즈 만큼 list로 저장해 반환합니다.
    '''

    batches = []
    batch = []
    for i in range(len(all_keys)):  # input object의 전체 개수 만큼 for loop
        batch.append(all_keys[i]);  # 단일 batch에 저장
        if (len(batch) >= batch_size):  # compute_batch_size 구한 batch_size와 비교
            batches.append(batch)
            batch = []

    if len(batch):
        batches.append(batch)
    return batches
