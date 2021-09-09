#!/usr/bin/env python
"""
This script invokes the AWS Lambda function 'python-process'
over hard-coded inputs:
-- The ranking of DOU articles in sections 1 and 2.

No input is required.
"""

import boto3
import json

# List of processing to do:
event1 = {"table_name": "python_process",
          "key": {"name":         {"S": "sort_dou_1"},
                  "capture_type": {"S": "off_daily_9am"}}}
event2 = {"table_name": "python_process",
          "key": {"name":         {"S": "sort_dou_2"},
                  "capture_type": {"S": "off_daily_9am"}}}

event_list = [event1, event2]
#event_list = [event1]

lamb = boto3.client('lambda')

# Loop for invoking processing:
for event in event_list:
    lamb.invoke(FunctionName='arn:aws:lambda:us-east-1:085250262607:function:python-process:PROD',
                InvocationType='Event', Payload=json.dumps(event))
