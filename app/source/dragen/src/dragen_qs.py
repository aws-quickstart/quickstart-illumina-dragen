#!/usr/bin/env python
# Copyright 2018 Illumina, Inc. All rights reserved.
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

from builtins import filter
from builtins import str
from builtins import object
import copy
import datetime
import glob
import os
import resource
import shutil
import subprocess
import sys
import time
import uuid
import six

from argparse import ArgumentParser


#########################################################################################
# find_arg_in_list - Interate through a list of arguments and find the first occurrence
# of any of the arguments listed as argv
#
def find_arg_in_list(arglist, *argv):
    index = -1
    for arg in argv:
        try:
            index = arglist.index(arg)
            break
        except ValueError:
            continue
    return index


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
    DRAGEN_LOG_FILE_NAME = 'dragen_log_%d.txt'
    DEFAULT_DATA_FOLDER = '/ephemeral/'
    CLOUD_SPILL_FOLDER = '/ephemeral/'

    FPGA_DOWNLOAD_STATUS_FILE = DEFAULT_DATA_FOLDER + 'fpga_dl_stat.txt'
    REDIRECT_OUTPUT_CMD_SUFFIX = '> %s 2>&1'

    ########################################################################################
    #
    def __init__(self, dragen_args):

        self.orig_args = dragen_args
        self.new_args = copy.copy(dragen_args)

        # Inputs needed for downloading
        self.ref_dir = None             # Create local directory to download reference
        self.input_dir = None           # Create local directory for Dragen input info

        self.ref_s3_url = None          # Determine from the -r or --ref-dir option
        self.ref_s3_index = -1

        self.fastq_list_url = None      # Determine from the --fastq-list option
        self.fastq_list_index = -1

        self.vc_tgt_bed_url = None      # Determine from the --vc-target-bed option
        self.vc_tgt_bed_index = -1

        self.vc_depth_url = None        # Determine from the ----vc-depth-intervals-bed
        self.vc_depth_index = -1

        # Output info
        self.output_s3_url = None       # Determine from the --output-directory field
        self.output_s3_index = -1
        self.output_dir = None          # Create local output directory for current dragen process

        # Run-time variables
        self.input_dir = None          # Create local output directory for current dragen process
        self.process_start_time = None  # Process start time
        self.process_end_time = None    # Process end time
        self.global_exit_code = 0       # Global exit code. If any process fails then we exit with a non-zero status

        self.set_resource_limits()
        self.parse_download_args()

    ########################################################################################
    # parse_download_args - Parse the command line looking for these specific options which
    # are used to download S3 files
    #
    def parse_download_args(self):
        # -r or --reference: S3 URL for reference HT
        opt_no = find_arg_in_list(self.orig_args, '-r', '--ref-dir')
        if opt_no >= 0:
            self.ref_s3_url = self.orig_args[opt_no + 1]
            self.ref_s3_index = opt_no + 1

        # --output-directory: S3 URL for output location
        opt_no = find_arg_in_list(self.orig_args, '--output-directory')
        if opt_no >= 0:
            self.output_s3_url = self.orig_args[opt_no + 1]
            self.output_s3_index = opt_no + 1

        # --fastq-list: URL (http or s3) for fastq list CSV file
        opt_no = find_arg_in_list(self.orig_args, '--fastq-list')
        if opt_no >= 0:
            self.fastq_list_url = self.orig_args[opt_no + 1]
            self.fastq_list_index = opt_no + 1

        # --vc-target-bed: URL for the VC target bed
        opt_no = find_arg_in_list(self.orig_args, '--vc-target-bed')
        if opt_no >= 0:
            self.fastq_list_url = self.orig_args[opt_no + 1]
            self.fastq_list_index = opt_no + 1

        # --vc-depth-intervals-bed: URL for the VC depth intervals
        opt_no = find_arg_in_list(self.orig_args, '--vc-depth-intervals-bed')
        if opt_no >= 0:
            self.fastq_list_url = self.orig_args[opt_no + 1]
            self.fastq_list_index = opt_no + 1

        return

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
        return

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
        files = list(filter(os.path.isfile, glob.glob("/var/log/dragen/dragen_run*")))
        if files:
            newest_run_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_run_file, self.output_dir)

        files = list(filter(os.path.isfile, glob.glob("/var/log/dragen/hang_diag*")))
        if files:
            newest_hang_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_hang_file, self.output_dir)

        files = list(filter(os.path.isfile, glob.glob("/var/log/dragen/pstack*")))
        if files:
            newest_pstack_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_pstack_file, self.output_dir)

        files = list(filter(os.path.isfile, glob.glob("/var/log/dragen/dragen_info*")))
        if files:
            newest_info_file = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)[0]
            shutil.copy2(newest_info_file, self.output_dir)

        files = list(filter(os.path.isfile, glob.glob("/var/log/dragen/dragen_replay*")))
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
        return

    ########################################################################################
    # exec_download - Download a file from the given URL to the target directory
    #
    def exec_url_download(self, url, target_dir):
        dl_cmd = '{bin} --mode download --url {url} --path {target}'.format(
            bin=self.D_HAUL_UTIL,
            url=url,
            target=target_dir)

        exit_code = exec_cmd(dl_cmd)

        if exit_code:
            print('Error: Failure downloading from S3. Exiting with code %d' % exit_code)
            sys.exit(exit_code)
        return

    ########################################################################################
    # download_inputs: Download specific Dragen inputs needed from provided URLs, and
    # replace them with a local path, i.e. fastq_list, bed files, etc.
    #
    def download_inputs(self):

        if not self.input_dir:
            self.input_dir = self.DEFAULT_DATA_FOLDER + 'inputs/'

        if self.fastq_list_url:
            self.exec_url_download(self.fastq_list_url, self.input_dir)
            filename = self.fastq_list_url.split('/')
            self.new_args[self.fastq_list_index] = self.input_dir + filename

        if self.vc_tgt_bed_url:
            self.exec_url_download(self.vc_tgt_bed_url, self.input_dir)
            filename = self.vc_tgt_bed_url.split('/')
            self.new_args[self.vc_tgt_bed_index] = self.input_dir + filename

        if self.vc_depth_url:
            self.exec_url_download(self.vc_depth_url, self.input_dir)
            filename = self.vc_depth_url.split('/')
            self.new_args[self.vc_depth_index] = self.input_dir + filename

        return

    ########################################################################################
    # download_ref_tables: Download directory of reference hash tables using the S3
    #  "directory" prefix self.ref_s3_url should be in format s3://bucket/ref_objects_prefix
    #
    def download_ref_tables(self):

        if not self.ref_s3_url:
            print('Warning: No reference HT directory URL specified!')
            return

        # Generate the params to download the HT based on URL s3://bucket/key
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
            print('Error: Failure downloading from S3. Exiting with code %d' % exit_code)
            sys.exit(exit_code)

        self.ref_dir = self.DEFAULT_DATA_FOLDER + s3_key
        self.new_args[self.ref_s3_index] = self.ref_dir
        return

    ########################################################################################
    # Upload the results of the job to the desired bucket location
    #    output_s3_url should be in format s3://bucket/output_objects_prefix
    # Returns:
    #    Nothing if success
    def upload_job_outputs(self):
        if not self.output_s3_url:
            print('Error: Output S3 location not specified!')
            return

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
            except os.error:
                # dragen execution will fail
                print("Error: Could not create output_directory %s" % self.output_dir)
                sys.exit(1)
        else:
            print("Output directory %s already exists - Skip creating." % self.output_dir)

        # Add or replace the output directory in the dragen parameters
        if self.output_s3_index >= 0:
            self.new_args[self.output_s3_index] = self.output_dir

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

        # Add some internally defined parameters
        self.new_args.extend(
            ['--output_status_file', self.output_dir + '/job-speedometer.log']
        )
        self.new_args.extend(
            ['--intermediate-results-dir', self.CLOUD_SPILL_FOLDER]
        )
        self.new_args.extend(
            ['--lic-no-print']
        )

        # expand the Dragen args to construct the full command
        dragen_opts = ' '.join(self.new_args)

        # Construct the 'main' Dragen command
        dragen_cmd = "%s %s " % (self.DRAGEN_PATH, dragen_opts)

        # Save the Dragen output to a file instead of stdout
        output_log_path = self.output_dir + '/' + self.DRAGEN_LOG_FILE_NAME % round(time.time())
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
    # import ipdb; ipdb.set_trace()
    # Configure command line arguments
    dragen_args = sys.argv[1:]

    # Debug print (remove later)
    print("[DEBUG] Dragen input commands: %s" % ' '.join(dragen_args))

    dragen_job = DragenJob(dragen_args)

    print('Downloading reference files')
    dragen_job.download_ref_tables()

    print('Downloading misc inputs (csv, bed)')
    dragen_job.download_inputs()

    print('Run Analysis job')
    dragen_job.run()


if __name__ == "__main__":
    main()
