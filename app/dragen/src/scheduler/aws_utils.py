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

BATCH_TO_WFMS_JOB_STATE_MAPPING = {
    "SUBMITTED": "PENDING",
    "PENDING": "PENDING",
    "RUNNABLE": "PENDING",
    "STARTING": "PENDING",
    "RUNNING": "RUNNING",
    "FAILED": "FAILED",
    "SUCCEEDED": "COMPLETED",
}

# DRAGEN_JOB_BATCH_LOOKUP_TABLE is indexed using [region][instance-type]
#   Where the supported options are
#       region = 'us-east-1', 'eu-west-1'
#       instance-type = 'f1.2xlarge', 'f1.16xlarge'

DRAGEN_JOB_BATCH_LOOKUP_TABLE = {
    'us-east-1': {
        'f1.2xlarge': 'dragen_exec_%s_us_east_1_%s',
        'f1.16xlarge': 'dragen_exec_16x_%s_us_east_1_%s'
    },
    'eu-west-1': {
        'f1.2xlarge': 'dragen_exec_%s_eu_west_1_%s',
        'f1.16xlarge': 'dragen_exec_16x_%s_eu_west_1_%s'
    }
}


########################################################################################
# batch_get_dragen_job_queue - Return the job queue based on input:
#      env, region, instance_type
#   Returns: queue name (str), or None if not found
#
def batch_get_dragen_job_queue(env, region, instance_type):
    queue_name = None
    try:
        queue_name = DRAGEN_JOB_BATCH_LOOKUP_TABLE[region][instance_type]
    except KeyError:
        return None
    return queue_name % ('queue', env)


########################################################################################
# batch_get_dragen_job_def - Return the job definition based on input:
#      env, region, instance_type
#   Returns: queue name (str), or None if not found
#
def batch_get_dragen_job_def(env, region, instance_type):
    def_name = None
    try:
        def_name = DRAGEN_JOB_BATCH_LOOKUP_TABLE[region][instance_type]
    except KeyError:
        return None
    return def_name % ('def', env)


########################################################################################
# batch_submit_job - Submit a batch job with the specified definition to a specific queue
#     The value returned is ID of the submitted job
#   Returns: job_id, error
#
def batch_submit_job(name, env, region, instance_type, args=None):
    definition = batch_get_dragen_job_def(env, region, instance_type)
    queue = batch_get_dragen_job_queue(env, region, instance_type)
    client = boto3.client('batch', region_name=region)
    if not args:
        args = []
    try:
        response = client.submit_job(
            jobName=name,
            jobQueue=queue,
            jobDefinition=definition,
            containerOverrides={
                'command': args
            },
            retryStrategy={
                'attempts': 2
            }
        )
    except exceptions.ClientError as err:
        return None, err

    return response['jobId'], None


########################################################################################
# batch_cancel_job - Stop a currently existing batch job (either pending or running).
#   Return error string
#
def batch_terminate_job(job_id, region, reason="Job Cancelled from WFMS"):
    client = boto3.client('batch', region_name=region)

    try:
        # Get the current status and use to determine action to Cancel
        response = client.describe_jobs(jobs=[job_id])
        job_resp = response['jobs'][0]

        if job_resp['status'] in ['SUBMITTED', 'PENDING', 'RUNNABLE']:
            client.cancel_job(jobId=job_id, reason=reason)

        if job_resp['status'] in ['STARTING', 'RUNNING']:
            client.terminate_job(jobId=job_id, reason=reason)
    except exceptions.ClientError as err:
        return err

    return None


########################################################################################
# batch_get_job_list_status - Get the info for a list of jobs. Input must be a
#     list of jobs (['string'].
#   Returns: job_info[], error
#   For each job a dict is returned with the following
#     fields:
#       'jobName': string,
#       'jobId': string,
#       'status': string
#       'createTime': UTC time
#       'startTime': UTC time
#       'stopTime': UTC time
#       'exitCode': integer
#       'reason': string
#
def batch_get_job_list_status(job_id_list, region):
    job_info_list = []
    if not type(job_id_list) is list:
        return job_info_list
    client = boto3.client('batch', region_name=region)
    try:
        response = client.describe_jobs(jobs=job_id_list)
    except exceptions.ClientError as err:
        return [], err

    response_list = response['jobs']
    for job_resp in response_list:
        wfms_status = BATCH_TO_WFMS_JOB_STATE_MAPPING[job_resp['status']]
        info = {
            'jobId': job_resp['jobId'],
            'jobName': job_resp['jobName'],
            'status': wfms_status,
            'createTime': int(old_div(job_resp['createdAt'], 1000)),
            'startTime': None,
            'stopTime': None,
            'reason': None,
            'exitCode': None
        }

        if wfms_status in ['RUNNING'] and 'startedAt' in job_resp:
            info['startTime'] = int(old_div(job_resp['startedAt'], 1000))

        if wfms_status in ['COMPLETED', 'FAILED']:
            if 'startedAt' in job_resp:
                info['startTime'] = int(old_div(job_resp['startedAt'], 1000))
            if 'stoppedAt' in job_resp:
                info['stopTime'] = int(old_div(job_resp['stoppedAt'], 1000))
            info['exitCode'] = job_resp['container'].get('exitCode')
            info['reason'] = job_resp['statusReason']

        job_info_list.append(info)

    return job_info_list, None


########################################################################################
# batch_get_job_status - Get the info for a single-job. Return the info, or None if info
# not found, and error status
def batch_get_job_status(job_id, region):
    result_array, err = batch_get_job_list_status([job_id], region)
    if not result_array:
        return [], err
    return result_array[0], err


########################################################################################
# batch_list_jobs - List the jobs that belong to a specific job queue
#
def batch_list_jobs(queue, status, region):
    try:
        client = boto3.client('batch', region_name=region)
        response = client.list_jobs(jobQueue=queue, jobStatus=status)
    except exceptions.ClientError as err:
        return None, err
    return response, None


########################################################################################
# asg_get_info - Get following info from AWS for input Auto-scaling group (ASG)
# Return an object (dict) with the following information :
#   instance cur_count (int), instance limits (int): min, max, and target,
#   and list of all instances (tuples): [ (id_1','state_1'), ('id_2','state_2') .... ]
#
def asg_get_info(asg_name):
    info = {}
    client = boto3.client('autoscaling')
    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name], )
    # Get the limits and target counts
    info['min'] = response['AutoScalingGroups'][0]['MinSize']  # Integer
    info['max'] = response['AutoScalingGroups'][0]['MaxSize']
    info['target'] = response['AutoScalingGroups'][0]['DesiredCapacity']

    # Evaluate the list of instances
    instances = response['AutoScalingGroups'][0]['Instances']
    info['instances'] = []
    info['cur_count'] = len(instances)
    for inst in instances:
        info['instances'].append((inst['InstanceId'], inst['LifecycleState']))

    return info


########################################################################################
# asg_add_instances - Add the given number of instances to the Auto Scaling Group
#   Inputs: asg_name, current_count, number_to_add
#   Return: HTTP response code (should be 200)
#
def asg_add_instances(asg_name, current_count, number_to_add):
    client = boto3.client('autoscaling')
    desired_count = current_count + number_to_add
    response = client.set_desired_capacity(
        AutoScalingGroupName=asg_name,
        DesiredCapacity=desired_count,
        HonorCooldown=False
    )
    return response['ResponseMetadata']['HTTPStatusCode']


########################################################################################
# asg_update_target - Change the ASG desired (target) number of instances
#   Return: HTTP response code (should be 200)
#
def asg_update_target(asg_name, target_count):
    client = boto3.client('autoscaling')
    response = client.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        DesiredCapacity=int(target_count),
    )
    return response['ResponseMetadata']['HTTPStatusCode']


########################################################################################
# ec2_delete - Terminate EC2 instances
#   Return: Count of the number of instances affected
#
def ec2_delete(instance_list):
    client = boto3.client('ec2')
    response = client.terminate_instances(
        InstanceIds=instance_list
    )
    return len(response['TerminatingInstances'])


########################################################################################
# ec2_get_state - Get the status of the specified instance
#   Return: The state string can be one of the following:
#      'pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped'
#
def ec2_get_state(instance_id):
    client = boto3.client('ec2')
    try:
        response = client.describe_instances(
            InstanceIds=[
                instance_id,
            ],
        )
        # First check if any instance info is available.
        if not len(response['Reservations']) or not len(response['Reservations'][0]['Instances'][0]):
            return None
        # Return the state only
        return response['Reservations'][0]['Instances'][0]['State']['Name']
    except exceptions.ClientError:
        return None


########################################################################################
# asg_remove_instances - Remove the specific list of instances from the ASG and reduce
#  the target so they don't get replaced
#  NOTE: The instances are not deleted automatically. This is done with a separate call
#  Return: Number of instances that are affected (removed)
def asg_remove_instances(asg_name, current_count, instance_list):
    # First bump down the desired target so that 'replacement' instances don't get spawned
    asg_update_target(asg_name, current_count - len(instance_list))

    # Detach the instances from the ASG
    client = boto3.client('autoscaling')
    response = client.detach_instances(
        AutoScalingGroupName=asg_name,
        InstanceIds=instance_list,
        ShouldDecrementDesiredCapacity=False,
    )

    return len(response['Activities'])


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
