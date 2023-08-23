'''

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: CC-BY-NC-4.0

'''

import json

import pandas as pd


def read_dataset(path):
    '''read a dataset for AS2. Accepted formats are .csv. These files can be compressed (.gz)'''
    if path.endswith('.csv') or path.endswith('.csv.gz'):
        ds = read_csv(path)
    elif path.endswith('.jsonl') or path.endswith('.jsonl.gz'):
        ds = read_json(path)
    elif path.endswith('.parquet'):
        ds = read_parquet(path)
    else:
        raise ValueError('File extension not recognized: %s' % ext)
    return ds



def read_csv(path):
    data = pd.read_csv(path,compression='gzip' if path.endswith('.gz') else None)
    data = data.fillna('')
    if 'doc_id' not in data.columns:
        data['doc_id'] = ''
    if 'title' not in data.columns:
        data['title'] = ''
    data = data.to_dict(orient='records')
    return data

def read_json(path):
    data = pd.read_json(path, lines=True, orient='records')
    data = data.fillna('')
    if 'doc_id' not in data.columns:
        data['doc_id'] = ''
    if 'title' not in data.columns:
        data['title'] = ''
    data = data.to_dict(orient='records')
    return data

def read_parquet(path):
    data = pd.read_parquet(path)
    data = data.fillna('')
    if 'doc_id' not in data.columns:
        data['doc_id'] = ''
    if 'title' not in data.columns:
        data['title'] = ''
    data = data.to_dict(orient='records')
    return data
