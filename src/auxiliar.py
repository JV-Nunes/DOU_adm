#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 16:25:14 2021

@author: skems
"""

import streamlit as st
import boto3
import os
import json
import google.auth as ga
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError

def load_aws_credentials_from_os():
    """
    Load AWS credentials from environmental 
    variables and return a dict with them.
    """
    
    credentials = {'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'], 'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY']}

    return credentials


def load_aws_credentials_from_file(filename='.aws/credentials', from_home=True):
    """
    Load AWS credentials from a text file with structure
        [default] \n XXX_id=XXX \n XXX_key=XXX
     
    Parameters
    ----------
    filename : str
        Path to file containing AWS credentials.
    from_home : bool
        Whether the path `filename` is relative to 
        the users' home or not.
    
    Returns
    -------
    credentials : dict
        Dict with the structure 
        `{XXX_id: XXX, XXX_key: XXX}`.
    """
    if from_home:
        filename = os.path.join(os.path.expanduser('~'), filename)
    
    with open(filename, 'r') as f:
        content = f.read()
    
    variables   = content.split('\n')[1:]
    keys_values = [tuple(var.split('=')) for var in variables] 
    credentials = dict(keys_values)
    
    return credentials


def load_aws_credentials(filename='.aws/credentials', from_home=True):
    """
    Load AWS credentials from either environmental variables
    or a file.
    
    Parameters
    ----------
    filename : str
        Path to file containing AWS credentials, if existent.
    from_home : bool
        Whether the path `filename` is relative to 
        the users' home or not.
    
    Returns
    -------
    credentials : dict
        Dict with the structure 
        `{XXX_id: XXX, XXX_key: XXX}`.
    
    """
    try:
        credentials = load_aws_credentials_from_os()
    except KeyError:
        credentials = load_aws_credentials_from_file(filename, from_home)
    return credentials


def get_s3_file(bucket, key, decode=False):
    """
    Download file from AWS S3.
    
    Parameters
    ----------
    bucket : str
        The AWS S3 bucket where the file is stored.
    key : str
        The indentification of the file inside the bucket
        (i.e. the path).
    decode : bool
        Wheter to decode the file to UTF-8 text or not.
    
    Returns
    -------
    file : bytes or str
        The content of the file in binary (if `decode` is False)
        or as a string (if `decode` is True).
    """
    
    # Instantiate client with credentials:
    credentials = load_aws_credentials()
    s3 = boto3.client('s3', 
                      aws_access_key_id=credentials['aws_access_key_id'], 
                      aws_secret_access_key=credentials['aws_secret_access_key'])
    
    #a  = s3.get_object(Bucket='config-lambda', 
    #                   Key='layers/google-cloud-storage/gabinete-compartilhado.json')
    a  = s3.get_object(Bucket=bucket, Key=key)
    
    file = a['Body'].read()
    if decode == True:
        file = file.decode('utf-8')
    
    return file


def load_gcp_credentials_file_from_s3():
    """
    Downloads the GCP credentials from AWS S3 
    and return it as a str.
    """
    credentials_file = get_s3_file('config-lambda', 
                                   'layers/google-cloud-storage/gabinete-compartilhado.json',
                                   decode=True)
    
    return credentials_file


def string_to_credentials(json_str):
    """
    Convert a GCP credentials in JSON-formatted str `json_str` 
    into a google.oauth2.service_account.Credentials` object.
    """
    
    cred_dict   = json.loads(json_str)
    scopes      = ['https://www.googleapis.com/auth/drive', 
                   'https://www.googleapis.com/auth/bigquery',
                   'https://www.googleapis.com/auth/devstorage.read_only']
    credentials = Credentials.from_service_account_info(cred_dict, scopes=scopes)
    
    return credentials


def load_gcp_credentials_from_s3():
    """
    Download GCP gredentials from AWS S3 and return it 
    as a `Credentials` object.
    """
    credentials_text = load_gcp_credentials_file_from_s3()
    credentials = string_to_credentials(credentials_text)
    
    return credentials


def load_gcp_credentials(credentials_file='/home/skems/gabinete/projetos/keys-configs/gabinete-compartilhado.json'):
    """
    Load and return GCP credentials either stored in the local
    file `credentials_file` or in AWS S3.
    """
    
    try:
        # Load GCP credentials from local file:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_file
        credentials, project = ga.default(scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/devstorage.read_only'])
        
    except DefaultCredentialsError:
        # Load GCP credentials from AWS:
        credentials = load_gcp_credentials_from_s3()

    return credentials
