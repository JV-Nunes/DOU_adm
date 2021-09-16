#!/usr/bin/env python
"""
This script invokes the AWS Lambda function 'python-process'
over hard-coded inputs:
-- The ranking of DOU articles in sections 1 and 2.

No input is required.
"""

import sys
import boto3
import json

import auxiliar as aux

def run_python_process():
    """
    Call AWS Lambda function 'python-process' to run sorting
    models for DOU sections 1 and 2. If `os_credentials` is
    True, expect AWS credentials to be saved to environment 
    variables.
    """
    
    # List of processing to do:
    event1 = {"table_name": "python_process",
              "key": {"name":         {"S": "sort_dou_1"},
                      "capture_type": {"S": "off_daily_9am"}}}
    event2 = {"table_name": "python_process",
              "key": {"name":         {"S": "sort_dou_2"},
                      "capture_type": {"S": "off_daily_9am"}}}
    
    event_list = [event1, event2]
    #event_list = [event1]
    
    # Instantiate client:
    credentials = aux.load_aws_credentials()
    lamb = boto3.client('lambda', 
                        aws_access_key_id=credentials['aws_access_key_id'], 
                        aws_secret_access_key=credentials['aws_secret_access_key'],
                        region_name='us-east-1')
    
    # Loop for invoking processing:
    for event in event_list:
        lamb.invoke(FunctionName='arn:aws:lambda:us-east-1:085250262607:function:python-process:PROD',
                    InvocationType='Event', Payload=json.dumps(event))


def main(args=['script_filename']):
    """
    Function that runs this file as a script.
    `args` (list of str) can be passed to it 
    using sys.argv. Set `n_args` below to 
    the number of arguments the script accepts.
    """
    # Hard-coded:
    n_args = 0
    
    # Docstring output:
    if len(args) != 1 + n_args: 
        print(__doc__)
        sys.exit(1)

    # START OF SCRIPT:
    run_python_process()
        


# If running this code as a script:
if __name__ == '__main__':
    #main()
    main(sys.argv)


