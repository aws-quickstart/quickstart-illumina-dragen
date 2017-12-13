#!/usr/bin/env python
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
# Wrapper to start processes in the background and catch any signals used to
# terminate the process.  Multiple processes are executed sequentially.  Stops dragen
# processes gracefully by running dragen_reset when dragen_board is in use.
#

from __future__ import print_function

import datetime
import glob
import os
import resource
import shutil
import subprocess
import sys
import uuid
import six

from argparse import ArgumentParser


#########################################################################################
# exec_cmd - Execute command and return [stdout/stderr, exitstatus]
#
def exec_cmd(cmd, shell=True):
    print("Executing %s" % cmd.strip())

    if not shell:
        p = subprocess.Popen(cmd.split())
    else:
        p = subprocess.Popen(cmd, shell=True, executable='/bin/bash')

    err = p.wait()
    return err


#########################################################################################
# DragenJob - Dragen Job execution object
#
class DragenJob(object):
    DRAGEN_PATH = '/opt/edico/bin/dragen'
    D_HAUL_UTIL = 'python /root/quickstart/d_haul'
    DRAGEN_LOG_FILE_NAME = 'dragen_log.txt'
    DEFAULT_DATA_FOLDER = '/ephemeral/'
    CLOUD_SPILL_FOLDER = '/ephemeral/'

    FPGA_DOWNLOAD_STATUS_FILE = DEFAULT_DATA_FOLDER + 'fpga_dl_stat.txt'
    REDIRECT_OUTPUT_CMD_SUFFIX = '> %s 2>&1'

    ########################################################################################
    #
    def __init__(self, fastq1_s3_url, fastq2_s3_url, ref_s3_url, output_s3_url, enable_map, enable_vc):

        # Variables used generate Dragen command line
        self.fastq1_s3_url = fastq1_s3_url
        self.fastq2_s3_url = fastq2_s3_url
        self.ref_s3_url = ref_s3_url
        self.output_s3_url = output_s3_url
        self.enable_map = enable_map    # True/False
        self.enable_vc = enable_vc      # True/False
        self.output_file_prefix = None
        self.output_dir = None  # Output directory for current dragen process
        self.ref_dir = None

        # Run-time variables
        self.process_start_time = None  # Process start time
        self.process_end_time = None  # Process end time
        self.global_exit_code = 0  # Global exit code for this script - if one process fails,
        # then this script will exit with a non-zero status
        self.set_resource_limits()

    ########################################################################################
    # set_resource_limits - Set resource limits prior.
    #
    def set_resource_limits(self):
        # Set resource limits based on values in /etc/security.d/limits.d/99-edico.conf
        edico_limits = "/etc/security/limits.d/99-edico.conf"
        rlimit = {}
        if os.path.exists(edico_limits):
            limitsfd = open(edico_limits, 'r')
            for line in limitsfd:
                fields = line.split()
                if fields[0] == '*' and fields[2] in ['nproc', 'nofile', 'stack']:
                    res = eval("resource.RLIMIT_%s" % fields[2].upper())
                    if fields[3] == "unlimited":
                        rlimit[res] = resource.RLIM_INFINITY
                    else:
                        if fields[2] == 'stack':
                            # limits.conf file is in KB, setrlimit command takes bytes
                            rlimit[res] = int(fields[3]) * 1024
                        else:
                            rlimit[res] = int(fields[3])
        else:
            rlimit[resource.RLIMIT_NPROC] = 16384
            rlimit[resource.RLIMIT_NOFILE] = 65535
            rlimit[resource.RLIMIT_STACK] = 10240 * 1024

        for res, limit in six.iteritems(rlimit):
            print("Setting resource %s to %s" % (res, limit))
            try:
                resource.setrlimit(res, (limit, limit))
            except Exception as e:
                msg = "Could not set resource ID %s to hard/soft limit %s (error=%s)" \
                      % (res, limit, e)
                print(msg)

    ########################################################################################
    # copy_var_log_dragen_files - Copy over the most recent /var/log/dragen files to output dir
    # Look for the latest below type of files:
    # /var/log/dragen_run_<timestamp>_<pid>.log
    # /var/log/hang_diag_<timestamp>_<pid>.txt
    # /var/log/pstack_<timestamp>_<pid>.log
    # /var/log/dragen_info_<timestamp>_pid.log
    # /var/log/dragen_replay_<timestamp>_pid.json
    #
    def copy_var_log_dragen_files(self):
        files = filter(os.path.isfile, glob.glob("/var/log/dragen/dragen_run*"))
        if files:
            newest_run_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_run_file, self.output_dir)

        files = filter(os.path.isfile, glob.glob("/var/log/dragen/hang_diag*"))
        if files:
            newest_hang_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_hang_file, self.output_dir)

        files = filter(os.path.isfile, glob.glob("/var/log/dragen/pstack*"))
        if files:
            newest_pstack_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_pstack_file, self.output_dir)

        files = filter(os.path.isfile, glob.glob("/var/log/dragen/dragen_info*"))
        if files:
            newest_info_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_info_file, self.output_dir)

        files = filter(os.path.isfile, glob.glob("/var/log/dragen/dragen_replay*"))
        if files:
            newest_replay_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_replay_file, self.output_dir)

        return

    ########################################################################################
    # download_dragen_fpga - Perform the 'partial reconfig' to download binary image to FPGA
    #   NOTE: Should ONLY be called when self.FPGA_DOWNLOAD_STATUS_FILE does not exist
    #
    def download_dragen_fpga(self):
        exit_code = \
            exec_cmd("/opt/edico/bin/dragen --partial-reconfig DNA-MAPPER --ignore-version-check true -Z 0")

        if not exit_code:
            # PR complete success. Write '1' into status file
            f = open(self.FPGA_DOWNLOAD_STATUS_FILE, 'w')
            f.write('1')
            f.close()
            print('Completed Partial Reconfig for FPGA')
            return True

        # Error!
        print('Could not do initial Partial Reconfig program for FPGA')
        return False

    ########################################################################################
    # check_board_state - Check dragen_board state and run reset (if needed)
    #
    def check_board_state(self):
        exit_code = exec_cmd("/opt/edico/bin/dragen_reset -cv")
        if not exit_code:
            return

        print("Dragen board is in a bad state - running dragen_reset")
        exec_cmd("/opt/edico/bin/dragen_reset")

    ########################################################################################
    # Download directory of reference hash tables using the S3 "directory" prefix
    #    self.ref_s3_url should be in format s3://bucket/ref_objects_prefix
    # Returns:
    #    Full directory path where the reference hash table is downloaded
    def download_ref_tables(self):
        # Generate the command to download the HT
        ref_path = self.ref_s3_url.replace('//', '/')
        s3_bucket = ref_path.split('/')[1]
        s3_key = '/'.join(ref_path.split('/')[2:])

        if not s3_key or not s3_bucket:
            print('Error: could not get S3 bucket and key info from specified URL %s' % self.ref_s3_url)
            sys.exit(1)

        target_path = self.DEFAULT_DATA_FOLDER  # Specifies the root
        dl_cmd = '{bin} --mode download --bucket {bucket} --key {key} --path {target}'.format(
            bin=self.D_HAUL_UTIL,
            bucket=s3_bucket,
            key=s3_key,
            target=target_path)

        exit_code = exec_cmd(dl_cmd)

        if exit_code:
            print('Error: Failure downloading reference. Exiting with code %d' % exit_code)
            sys.exit(exit_code)

        self.ref_dir = self.DEFAULT_DATA_FOLDER + s3_key

    ########################################################################################
    # Upload the results of the job to the desired bucket location
    #    output_s3_url should be in format s3://bucket/output_objects_prefix
    # Returns:
    #    Nothing if success
    def upload_job_outputs(self):
        # Generate the command to download the HT
        output_path = self.output_s3_url.replace('//', '/')
        s3_bucket = output_path.split('/')[1]
        s3_key = '/'.join(output_path.split('/')[2:])

        if not s3_key or not s3_bucket:
            print('Error: could not get S3 bucket and key info from specified URL %s' % self.output_s3_url)
            sys.exit(1)

        ul_cmd = '{bin} --mode upload --bucket {bucket} --key {key} --path {file} -s'.format(
            bin=self.D_HAUL_UTIL,
            bucket=s3_bucket,
            key=s3_key,
            file=self.output_dir.rstrip('/'))

        exit_code = exec_cmd(ul_cmd)

        if exit_code:
            print('Error: Failure uploading outputs. Exiting with code %d' % exit_code)
            sys.exit(exit_code)

        return

    ########################################################################################
    # create_output_dir - Checks for existance of outdir and creates it if necessary,
    # and saves it to internal self.output_dir variable
    #
    def create_output_dir(self):
        if not self.output_dir or not os.path.exists(self.output_dir):
            self.output_dir = self.DEFAULT_DATA_FOLDER + str(uuid.uuid4())
            print("Output directory does not exist - creating %s" % self.output_dir)
            try:
                os.makedirs(self.output_dir)
            except:
                # dragen execution will fail
                print("Error: Could not create output_directory %s" % self.output_dir)
                sys.exit(1)
        else:
            print("Output directory %s already exists - Skip creating." % self.output_dir)

        return

    ########################################################################################
    # run_job - Create the command line for the given process, launch it, and monitor
    # it for completion.
    #
    def run_job(self):

        # Check if FPGA image download is needed
        if not os.path.isfile(self.FPGA_DOWNLOAD_STATUS_FILE):
            self.download_dragen_fpga()

        # If board is in bad state, run dragen_reset before next process starts
        self.check_board_state()

        # Setup unique output directory
        self.create_output_dir()

        # Generate an output file prefix
        self.output_file_prefix = self.fastq1_s3_url.split('/')[-1].split('.')[0]

        # Construct the 'common' Dragen command
        dragen_cmd = self.DRAGEN_PATH + " --ignore-version-check true --enable-duplicate-marking true " \
                     + "--output-directory %s " % self.output_dir \
                     + "--output-file-prefix %s " % self.output_file_prefix \
                     + "--output_status_file %s " % (self.output_dir + "/job-speedometer.log") \
                     + "--ref-dir %s " % self.ref_dir \
                     + "--intermediate-results-dir=%s" % self.CLOUD_SPILL_FOLDER

        # Add the input files
        dragen_cmd = "%s -1 %s" % (dragen_cmd, self.fastq1_s3_url)

        if self.fastq2_s3_url:
            dragen_cmd = "%s -2 %s" % (dragen_cmd, self.fastq2_s3_url)

        # Handle MAP-Align processing
        if self.enable_map:
            dragen_cmd = "%s --enable-map-align true --output-format BAM" % dragen_cmd

        # Handle Variant Calling
        if self.enable_vc:
            dragen_cmd = "%s --enable-variant-caller true --vc-sample-name DRAGEN_RGSM" % dragen_cmd

        # Save the Dragen output to a file instead of stdout
        output_log_path = self.output_dir + '/' + self.DRAGEN_LOG_FILE_NAME
        redirect_cmd = self.REDIRECT_OUTPUT_CMD_SUFFIX % output_log_path
        dragen_cmd = "%s %s" % (dragen_cmd, redirect_cmd)

        # Run the Dragen process
        self.process_start_time = datetime.datetime.utcnow()
        exit_code = exec_cmd(dragen_cmd)

        # Upload the results to S3 output bucket
        self.upload_job_outputs()

        # Delete the output results directory, i.e. /staging/<uuid4>
        # NOTE: Do not delete the reference directory enable re-use with another job
        rm_out_path = self.output_dir
        print("Removing Output dir %s" % rm_out_path)
        shutil.rmtree(rm_out_path, ignore_errors=True)

        # Handle error code
        if exit_code:
            self.copy_var_log_dragen_files()
            self.global_exit_code = exit_code
            if self.global_exit_code > 128 or self.global_exit_code < 0:
                if self.global_exit_code > 128:
                    signum = self.global_exit_code - 128
                else:
                    signum = -self.global_exit_code
                print("Job terminated due to signal %s" % signum)

        self.process_end_time = datetime.datetime.utcnow()
        return

    ########################################################################################
    # run - Run all processes in the job.
    #
    def run(self):
        try:
            self.run_job()
            print("Job is exiting with code %s" % self.global_exit_code)
            sys.exit(self.global_exit_code)

        except SystemExit as inst:
            if inst[0] != 0:  # System exit with exit code 0 is OK
                print("Caught SystemExit: Exiting with status %s" % inst[0])
                sys.exit(inst[0])
            else:
                print("Caught SystemExit: Exiting normally")

        except:
            # Log abnormal exists
            print("Unhandled exception in dragen_qs: %s" % sys.exc_info()[0])
            sys.exit(1)

        delta = self.process_end_time - self.process_start_time
        print("Job ran for %02d:%02d:%02d" % (delta.seconds//3600, delta.seconds//60 % 60, delta.seconds % 60))
        sys.exit(0)


#########################################################################################
# main
#
def main():
    # Configure command line arguments
    argparser = ArgumentParser()

    file_path_group = argparser.add_argument_group(title='File paths')
    file_path_group.add_argument('--fastq1_s3_url', type=str, help='Input FASTQ1 S3 URL', required=True)
    file_path_group.add_argument('--fastq2_s3_url', type=str, help='Input FASTQ2 S3 URL')
    file_path_group.add_argument('--ref_s3_url', type=str, help='Reference Hash Table S3 URL', required=True)
    file_path_group.add_argument('--output_s3_url', type=str, help='Output Prefix S3 URL', required=True)

    run_group = argparser.add_argument_group(title='DRAGEN run command args')
    run_group.add_argument('--enable_map', action='store_true', help='Enable MAP/Align and ouput BAM')
    run_group.add_argument('--enable_vc', action='store_true', help='Enable Variant Calling and ouput VCF')

    args = argparser.parse_args()

    if not args.enable_map and not args.enable_vc:
        print('Error: Must have one or more of enable_map and enable_vc flags set')
        sys.exit(1)

    dragen_job = DragenJob(args.fastq1_s3_url,
                           args.fastq2_s3_url,
                           args.ref_s3_url,
                           args.output_s3_url,
                           args.enable_map,
                           args.enable_vc)

    print('Downloading reference files')
    dragen_job.download_ref_tables()

    print('Run Analysis job')
    dragen_job.run()

if __name__ == "__main__":
    main()
