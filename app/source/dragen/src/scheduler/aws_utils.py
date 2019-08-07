#!/opt/workflow/bin/python2.7
#
# Copyright 2013-2016 Edico Genome Corporation. All rights reserved.
#
# This file contains confidential and proprietary information of the Edico Genome
# Corporation and is protected under the U.S. and international copyright and other
# intellectual property laws.
#
# $Id$
# $Author$
# $Change$
# $DateTime$
#
# AWS service utilities for schedulers to use: dragen_jobd, dragen_job_execute and node_update
#

from __future__ import absolute_import
from __future__ import division

from builtins import map
from builtins import str
from past.utils import old_div
import os
from glob import glob
from multiprocessing import Pool

import boto3
from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore import exceptions
from botocore.config import Config

from . import scheduler_utils as utils

# CONSTANTS ....
DOWNLOAD_THREAD_COUNT = 4


########################################################################################
# s3_download_file - Download a file from given "req_info" dict. Before actually downloading
#   the object see if it already exists locally
# req_info = {"bucket": <str>, "obj_key":<str>, "tgt_path":<str>, "region":<str>}
# Return: Downloaded file size
def s3_download_file(req_info, nosign=False):
    # If region is missing fill in default
    if not req_info['region']:
        req_info['region'] = 'us-east-1'

    # Configure the download
    if nosign:
        client = boto3.client('s3', req_info['region'], config=Config(signature_version=UNSIGNED))
    else:
        client = boto3.client('s3', req_info['region'])

    # Make sure the target directory exists
    tgt_dir = req_info['tgt_path'].rsplit('/', 1)[0]  # get the directory part
    utils.check_create_dir(tgt_dir)

    # Check if the object already exists locally and get the size on disk
    if os.path.exists(req_info['tgt_path']):
        loc_size = os.path.getsize(req_info['tgt_path'])
        # Check if the S3 object length matches the local file size
        obj_info = s3_get_object_info(req_info['bucket'], req_info['obj_key'])
        if obj_info['ContentLength'] == loc_size:
            return loc_size

    # Perform the download
    transfer = S3Transfer(client)
    transfer.download_file(req_info['bucket'], req_info['obj_key'], req_info['tgt_path'])

    # Once download is complete, get the file info to check the size
    return os.path.getsize(req_info['tgt_path'])


########################################################################################
# s3_download_dir - Download all the objects with the given "directory".
#   Inputs:
#       bucket - source bucket
#       src_dir - the prefix for the object key (i.e. 'references/hg19'
#       tgt_dir = directory to download to ending with '/'. The prefix dir is created if not existing)
#   Return: Total number of bytes downloaded
def s3_download_dir(bucket, src_dir, tgt_dir, region='us-east-1', nosign=False):
    # Get the list of objects specified within the "dir"
    if nosign:
        client = boto3.client('s3', region, config=Config(signature_version=UNSIGNED))
    else:
        client = boto3.client('s3', region)
    response = client.list_objects(Bucket=bucket, Prefix=src_dir)

    if not response['Contents']:
        return 0

    # Filter out any results that are "dirs" by checking for ending '/'
    object_list = [x for x in response['Contents'] if not x['Key'].endswith('/')]

    # To avoid a race condition for parallel downloads, make sure each has a directory created
    # - Create the full dir path of each object and make sure the dir exists
    list([utils.check_create_dir(str(tgt_dir.rstrip('/') + '/' + x['Key']).rsplit('/', 1)[0]) for x in object_list])

    # Convert the list of objects to a dict we can pass to the download function
    download_dict_list = [{
            'bucket': bucket,
            'obj_key': x['Key'],
            'tgt_path': tgt_dir.rstrip('/') + '/' + x['Key'],
            'region': region
        } for x in object_list]

    # Create a thread pools to handle the downloads faster
    pool = Pool(DOWNLOAD_THREAD_COUNT)

    # Use the multiple thread pools to divvy up the downloads
    results = pool.map(s3_download_file, download_dict_list)

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()

    # return the total number of bytes downloaded
    return sum(results)


########################################################################################
# s3_get_object_info - Get information about an S3 object without downloading it
#   Inputs:
#       bucket - object bucket
#       obj_path - The key for the object (aka the 'path')
#   Return: Total number of bytes downloaded, or raise a Client Error exception
def s3_get_object_info(bucket, obj_path):
    client = boto3.client('s3')
    info = client.head_object(
        Bucket=bucket,
        Key=obj_path
    )
    return info


########################################################################################
# s3_delete_object - Delete the specified object from S3 bucket
#   Inputs:
#       bucket - object bucket
#       obj_path - The key for the object (aka the 'path')
#   Return: Total number of bytes downloaded, or raise a Client Error exception
def s3_delete_object(bucket, obj_path):
    client = boto3.client('s3')
    resp = client.delete_objects(
        Bucket=bucket,
        Delete={
            'Objects': [
                {'Key': obj_path}
            ]
        }
    )
    return resp


########################################################################################
# s3_upload - Recursively upload source file(s) residing in the given input
# location (abs_src_path) to the bucket and S3 base path (key) provided as input
def s3_upload(abs_src_path, bucket, key):
    # Configure the upload
    s3_client, transfer_client = _s3_initialize_client(bucket)
    if os.path.isdir(abs_src_path):
        up_size = _s3_upload_files_recursively(abs_src_path, bucket, key, s3_client, transfer_client)
    elif os.path.isfile(abs_src_path):
        up_size = _s3_upload_file(abs_src_path, bucket, key, s3_client, transfer_client)
    else:
        raise ValueError(
            '{0} MUST be either a file or a directory'.format(abs_src_path))
    return up_size


########################################################################################
# ############################# LOCAL FUNCTIONS ########################################

def _s3_upload_files_recursively(dir_path, bucket, obj_key, s3_client, transfer_client):
    filenames = [fpath for dirpath in os.walk(dir_path) for fpath in
                 glob(os.path.join(dirpath[0], '*'))]
    # upload a finite number of files for safety
    filenames = filenames[:100]
    tot_bytes = 0

    # make sure there is a trailing '/' in obj_key to indicate it is 'root' and not actual keyname
    if not obj_key.endswith('/'):
        obj_key += '/'

    for filename in filenames:
        if os.path.isfile(filename):
            size = _s3_upload_file(filename, bucket, obj_key, s3_client, transfer_client)
            if size:
                tot_bytes += size
    return tot_bytes


def _s3_upload_file(file_path, bucket, obj_key, s3_client, transfer_client):
    # Check if the key is a 'root' instead of full key name
    if obj_key.endswith('/'):
        name_only = file_path.rsplit('/', 1)[1]  # strip out the leading directory path
        obj_key = obj_key + name_only
    transfer_client.upload_file(
        file_path,
        bucket,
        obj_key,
        extra_args={'ServerSideEncryption': 'AES256'}
    )

    # Once Upload is complete, get the object info to check the size
    response = s3_client.head_object(Bucket=bucket, Key=obj_key)
    return response['ContentLength'] if response else None


def _s3_initialize_client(s3_bucket):
    client = boto3.client('s3', region_name=_s3_get_bucket_location(s3_bucket))
    config = boto3.s3.transfer.TransferConfig(
        multipart_chunksize=256 * 1024 * 1024,
        max_concurrency=10,
        max_io_queue=1000,
        io_chunksize=2 * 1024 * 1024)
    transfer_client = boto3.s3.transfer.S3Transfer(client, config, boto3.s3.transfer.OSUtils())
    return client, transfer_client


def _s3_get_bucket_location(s3_bucket):
    client = boto3.client('s3')
    resp = client.head_bucket(Bucket=s3_bucket)
    location = resp['ResponseMetadata']['HTTPHeaders'].get('x-amz-bucket-region')
    return location
