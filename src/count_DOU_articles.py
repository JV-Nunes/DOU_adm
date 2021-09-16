#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Count the number of DOU articles in each today's section 
in the following places:
-- The 'imprensa oficial website';
-- The dynamoDB tables used to keep track of downloaded files;
-- The files stored in S3;
-- The files stored in Google Storage;
-- The files loaded at 9am to BigQuery.

The count '-1' means that such information is not available in 
the current code implementation.

Written by: Henrique S. Xavier, hsxavier@gmail.com, on 18/may/2020.
"""

import sys
import requests
from lxml import html
import json
import datetime as dt
import boto3
#import os
#import google.auth
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numpy as np

import auxiliar as aux

debug = False

### Funções ###
        
def get_artigos_do(data, secao):
    """
    Para uma data (datetime) e uma seção (str) do DOU,
    retorna uma lista de jsons com todos os links e outros metadados dos 
    artigos daquele dia e seção. 
    """
    # Hard-coded:
    do_date_format = '%d-%m-%Y'
    # Transforma data:
    data_string = data.strftime(do_date_format)
    
    # Exemplo de URL: 'http://www.in.gov.br/leiturajornal?data=13-05-2019&secao=do1'
    url   = 'http://www.in.gov.br/leiturajornal?data=' + data_string + '&secao=do' + str(secao)

    # Specifies number of retries for GET:
    session = requests.Session()
    session.mount('http://www.in.gov.br', requests.adapters.HTTPAdapter(max_retries=3))
    
    # Captura a lista de artigos daquele dia e seção:
    try:
        res = session.get(url)
    except requests.exceptions.SSLError:
        res = session.get(url, verify=False)
    tree  = html.fromstring(res.content)
    xpath = '//*[@id="params"]/text()'
    return json.loads(tree.xpath(xpath)[0])['jsonArray']


def brasilia_day(yesterday=False):
    """
    No matter where the code is ran, return UTC-3 day
    (Brasilia local day, no daylight savings).

    If yesterday is True, return the previous day from today.
    """
    
    if yesterday:
        return (dt.datetime.utcnow() + dt.timedelta(hours=-3) - dt.timedelta(hours=24)).replace(hour=0, minute=0, second=0, microsecond=0)

    else:
        return (dt.datetime.utcnow() + dt.timedelta(hours=-3)).replace(hour=0, minute=0, second=0, microsecond=0)
    

def list_dynamo_items(table_name):
    """
    Return a list of all items in a AWS dynamoDB table
    `table_name`.
    """
    
    credentials = aux.load_aws_credentials()
    dynamodb = boto3.resource('dynamodb', 
                        aws_access_key_id=credentials['aws_access_key_id'], 
                        aws_secret_access_key=credentials['aws_secret_access_key'],
                        region_name='us-east-1')

    table = dynamodb.Table(table_name)

    # Get all items (following pagination if necessary):
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return data


def list_s3_files(bucket, prefix):
    """
    Returns a list of files in AWS in a given `bucket` and with a given `prefix`.
    """
        
    # Instantiate client:
    credentials = aux.load_aws_credentials()
    s3 = boto3.client('s3', 
                        aws_access_key_id=credentials['aws_access_key_id'], 
                        aws_secret_access_key=credentials['aws_secret_access_key'])


    if type(prefix) != list:
        prefix = [prefix]
    
    # Loop over prefixes:
    file_list = []
    for p in prefix:
        
        # Load one prefix:
        response  = s3.list_objects_v2(Bucket=bucket, Prefix=p)
        if response['KeyCount'] > 0:
            file_list = file_list + [d['Key'] for d in response['Contents']]
            while response['IsTruncated']:
                response  = s3.list_objects_v2(Bucket=bucket, Prefix=p, StartAfter=file_list[-1])
                file_list = file_list + [d['Key'] for d in response['Contents']]    
    
    return file_list


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """
    Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you just specify prefix = 'a', you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a' and delimiter='/', you'll get back:

        a/1.txt

    Additionally, the same request will return blobs.prefixes populated with:

        a/b/
    """

    credentials = aux.load_gcp_credentials()
    project = 'gabinete-compartilhado'
    storage_client = storage.Client(project=project, credentials=credentials)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )

    blob_list = [blob.name for blob in blobs]
    
    return blob_list
        

def count_dynamo(table_name, all_sections):
    n_items = {'source': table_name}
    for s in all_sections:
        n_items[s] = -1
    n_items['total'] = len(list_dynamo_items(table_name))
    
    return n_items
    
def count_website(current_date, all_sections):
    """
    Given a date (datetime) `current_date` and a list of DOU 
    sections `all_sections` (e.g. [1, 2, 3, 'e']), returns 
    a dict with a hard-coded 'source' name and the number of 
    articles in DOU website for each section.
    """
    website_n_articles = {}
    total = 0
    for s in all_sections:
        website_n_articles[s] = len(get_artigos_do(current_date, s))
        total = total + website_n_articles[s]
    website_n_articles['source'] = 'Site'
    website_n_articles['total']  = total
    return website_n_articles


def count_s3(current_date, all_sections):
    """
    Given a date (datetime) `current_date` and a DOU section list 
    `all_sections`, returns a dict with the source name 'S3' 
    and the number of articles saved in S3 belonging to the date 
    and sections.
    """
    bucket = 'brutos-publicos'
    prefix = 'executivo/federal/dou-partitioned/'
    
    s3_counts = {'source': 'S3'} 
    total = 0
    for s in all_sections:
        key = prefix + 'part_data_pub=' + current_date.strftime('%Y-%m-%d') + '/part_secao=' + str(s)
        s3_counts[s] = len(list_s3_files(bucket, key))
        total = total + s3_counts[s]
    
    s3_counts['total'] = total
    
    return s3_counts


def count_storage(current_date, all_sections):
    """
    Given a date (datetime) `current_date` and a DOU section list 
    `all_sections`, returns a dict with the source name 'GCP storage' 
    and the number of articles saved in Storage belonging to the date 
    and sections.
    """
    bucket = 'brutos-publicos'
    prefix = 'executivo/federal/dou-partitioned/'
    
    s3_counts = {'source': 'GCP storage'} 
    total = 0
    for s in all_sections:
        key = prefix + 'part_data_pub=' + current_date.strftime('%Y-%m-%d') + '/part_secao=' + str(s)
        s3_counts[s] = len(list_blobs_with_prefix(bucket, key))
        total = total + s3_counts[s]
    
    s3_counts['total'] = total
    
    return s3_counts


def query_bigquery(query):
    """
    Run a `query` in Google BigQuery and return the results as a list of dicts.
    """
    
    # Instantiate client w/ credentials:    
    credentials = aux.load_gcp_credentials()
    project = 'gabinete-compartilhado'
    bq = bigquery.Client(credentials=credentials, project=project)
    
    result = bq.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="US",
    )  # API request - starts the query
    
    result = [dict(r.items()) for r in result] 
    
    return result


def count_semana(current_date, all_sections):
    """
    Check the number of DOU articles in a BigQuery table
    (hard-coded to 'artigos_cleaned_da_semana') and return 
    them in a dict, along with the source name
    (hard-coded to 'BQ (semana)').
    """
    
    # Query bigQuery table:
    query_template = """
    SELECT secao, tipo_edicao, count(*) AS counts
    FROM `gabinete-compartilhado.executivo_federal_dou.artigos_cleaned_da_semana`
    WHERE data_pub = '%(date)s'
    GROUP by secao, tipo_edicao
    order by tipo_edicao DESC, secao
    """
    query   = query_template % {'date': current_date.strftime('%Y-%m-%d')}
    results = query_bigquery(query)

    # Parse results:
    counts_bq = {'source': 'BQ (semana)'}
    total = 0
    for s in all_sections:
        if s == 'e':
            counts_bq['e'] = sum([r['counts'] for r in results if r['tipo_edicao'] == 'Extra'])
        else:
            counts_bq[s] = sum([r['counts'] for r in results if r['tipo_edicao'] == 'Ordinária' and r['secao'] == int(s)])
        total = total + counts_bq[s]

    counts_bq['total'] = total
    
    return counts_bq


def count_rank_auto(current_date, all_sections):
    """
    Check the number of DOU articles in a BigQuery table
    (hard-coded to 'artigos_ranqueados_auto') and return 
    them in a dict, along with the source name
    (hard-coded to 'BQ (auto)').
    """
    
    # Query bigQuery table:
    query_template = """
    SELECT secao, tipo_edicao, count(*) AS counts
    FROM `gabinete-compartilhado.executivo_federal_dou.artigos_ranqueados_auto`
    WHERE data_pub = '%(date)s'
    GROUP by secao, tipo_edicao
    order by tipo_edicao DESC, secao
    """
    query   = query_template % {'date': current_date.strftime('%Y-%m-%d')}
    results = query_bigquery(query)

    # Parse results:
    counts_bq = {'source': 'BQ (auto)'}
    total = 0
    for s in all_sections:
        if s == 'e':
            counts_bq['e'] = sum([r['counts'] for r in results if r['tipo_edicao'] == 'Extra'])
        else:
            counts_bq[s] = sum([r['counts'] for r in results if r['tipo_edicao'] == 'Ordinária' and r['secao'] == int(s)])
        total = total + counts_bq[s]

    counts_bq['total'] = total
    
    return counts_bq


def get_total3(counts, all_sections):
    """
    Given a dict `counts` with DOU article counts per section
    and a list of sections `all_sections`, transform `counts` 
    in place by adding an extra entry with all counts except 
    for those in section 3.
    """
    
    if 3 in all_sections:
        if counts[3] >= 0:
            counts['tot-3'] = counts['total'] - counts[3]
        else:
            counts['tot-3'] = counts['total']
    elif '3' in all_sections:
        if counts['3'] >= 0:
            counts['tot-3'] = counts['total'] - counts['3']
        else:
            counts['tot-3'] = counts['total']        
    
        
def print_counts(source_counts, all_sections):
    """
    Given a dict with source name and article counts for each 
    DOU section `source_counts` and a list of DOU 
    sections `all_sections` (e.g. [1, 2, 3, 'e']), prints 
    the source and the counts to screen.
    """
    print('{:20s}'.format(source_counts['source']), end='')
    for s in all_sections:
        print('  {:4d}'.format(source_counts[s]), end='')
    print('  {:5d}'.format(source_counts['total']), end='')
    print('  {:5d}'.format(source_counts['tot-3']))


def failed_capture_actions(step_name, error_msgs, counts, exception):
    """
    Add entries to `error_msgs` (list or str) and `counts` (list of dict)
    representing failed actions (modify in place).
    
    Input
    -----
    step_name : str
        Name of the step in the capture pipeline, e.g. 'S3'.
    error_msgs : list of str
        List of error messages produced by the capture pipeline.
    counts : list of dict
        List of dicts containing the article counts for various DOU sections 
        and the pipeline step name.
    exception : Exception
        Exception generated by the capture pipeline.
    """
    
    # Hard-coded:
    failed_val   = -666
    failed_entry = {'1': failed_val, '2': failed_val, '3': failed_val, 
                    'e': failed_val, 'source': failed_val, 'total': failed_val, 
                    'tot-3': failed_val}
    
    # Append failed result to error messages and article counts:
    error_msgs.append('Failed {} capture: {}'.format(step_name, str(exception)))
    failed_entry.update({'source': step_name})
    counts.append(failed_entry)


def count_through_pipeline():
    """
    Build a DataFrame with article counts at each step of the 
    capturing pipeline.
    
    Return
    ------
    df : DataFrame
        The counts for each section and pipeline step.
    error_msgs : list of str
        The error messages that may have been generated at any step.
        If no errors, the list is empty.
    """
    
    # Hard-coded & settings:
    all_sections = ['1', '2', '3', 'e']
    current_date = brasilia_day()
    
    counts = []
    error_msgs = []

    # Count on website:
    if False:
        try:
            site_counts = count_website(current_date, all_sections)
            get_total3(site_counts, all_sections)
            counts.append(site_counts)
        except Exception as e:
            failed_capture_actions('Site', error_msgs, counts, e)
    
        # DynamoDB Slack-bot-warning counts:
        try:
            slack_counts = count_dynamo('dou_captured_urls', all_sections)
            get_total3(slack_counts, all_sections)
            slack_counts.update({'source': 'Gabi (bot no Slack)'})
            counts.append(slack_counts)
        except Exception as e:
            failed_capture_actions('Gabi (bot no Slack)', error_msgs, counts, e)
    
        # DynamoDB capture-to-Database counts:
        try:
            dyn_counts = count_dynamo('douDB_captured_urls', all_sections)
            get_total3(dyn_counts, all_sections)
            dyn_counts.update({'source': 'Sistema de captura'})
            counts.append(dyn_counts)
        except Exception as e:
            failed_capture_actions('Sistema de captura', error_msgs, counts, e)
    
    # S3 counts:
    try:
        saved_counts = count_s3(current_date, all_sections)
        get_total3(saved_counts, all_sections)
        saved_counts.update({'source': 'Cloud da Amazon'})
        counts.append(saved_counts)
    except Exception as e:
        failed_capture_actions('Cloud da Amazon', error_msgs, counts, e)

    if False:
        # Storage counts:
        try:
            storage_counts = count_storage(current_date, all_sections)
            get_total3(storage_counts, all_sections)
            storage_counts.update({'source': 'Cloud do Google'})
            counts.append(storage_counts)
        except Exception as e:
            failed_capture_actions('Cloud do Google', error_msgs, counts, e)
        
        # BigQuery (ranqueados auto table) counts:
        try:
            auto_counts = count_rank_auto(current_date, all_sections)
            get_total3(auto_counts, all_sections)
            auto_counts.update({'source': 'Ranqueados pela IA'})
            counts.append(auto_counts)
        except Exception as e:
            failed_capture_actions('Ranqueados pela IA', error_msgs, counts, e)    

    # Cria dataframe:
    df = pd.DataFrame(counts)
    new_cols = ['Estágio', '1', '2', '3', 'Extra', 'Total', 'Total s/ 3']
    old_cols = ['source', '1', '2', '3', 'e', 'total', 'tot-3']
    renamer  = dict(zip(old_cols, new_cols))
    df.rename(renamer, axis=1, inplace=True)
    df = df[new_cols]
    df.set_index('Estágio', inplace=True)
    
    return df, error_msgs


def gen_empty_counts_df(cols=['Estágio', '1', '2', '3', 'Extra', 'Total', 'Total s/ 3'],
                        rows=['Site', 'Gabi (bot no Slack)', 'Sistema de captura',
                              'Cloud da Amazon', 'Cloud do Google', 'Ranqueados pela IA'],
                        no_value=-1, dtype=int):
    """
    Returns an empty DataFrame with the same structure as 
    the one generated by `count_through_pipeline()`.
    
    Input
    -----
    cols : list
        Column names of the empty DataFrame.
    rows : list
        Index of the empty DataFrame.
    no_value : whatever
        Value to be set in the DataFrame.
    dtype : dtype
        Type of the `no_value` values.
    """
    
    n_cols = len(cols)
    n_rows = len(rows)
    data = [[np.NaN] * n_cols] * n_rows
    df = pd.DataFrame(data=data, columns=cols, index=rows).fillna(no_value).astype(dtype)
    
    return df


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

    current_date = brasilia_day()
    all_sections = ['1', '2', '3', 'e']
    
    # Header:
    template = '\033[1m{:20s}  ' + ('  '.join(['{:^4s}'] * len(all_sections)) + '  {:^5s}  {:^5s}\033[0m')
    print(template.format('Fonte', *all_sections, 'Total', 'Tot-3'))
    
    # Website counts:
    if True:
        site_counts = count_website(current_date, all_sections)
        get_total3(site_counts, all_sections)
        print_counts(site_counts, all_sections)
    
    # DynamoDB counts:
    slack_counts = count_dynamo('dou_captured_urls', all_sections)
    get_total3(slack_counts, all_sections)
    print_counts(slack_counts, all_sections)
    dyndb_counts = count_dynamo('douDB_captured_urls', all_sections)
    get_total3(dyndb_counts, all_sections)
    print_counts(dyndb_counts, all_sections)
    
    # S3 counts:
    saved_counts = count_s3(current_date, all_sections)
    get_total3(saved_counts, all_sections)
    print_counts(saved_counts, all_sections)
    
    # Storage counts:
    storage_counts = count_storage(current_date, all_sections)
    get_total3(storage_counts, all_sections)
    print_counts(storage_counts, all_sections)
    
    # BigQuery (semana table) counts:
    #semana_counts = count_semana(current_date, all_sections)
    #get_total3(semana_counts, all_sections)
    #print_counts(semana_counts, all_sections)
    
    # BigQuery (ranqueados auto table) counts:
    auto_counts = count_rank_auto(current_date, all_sections)
    get_total3(auto_counts, all_sections)
    print_counts(auto_counts, all_sections)


# If running this code as a script:
if __name__ == '__main__':
    #main()
    main(sys.argv)