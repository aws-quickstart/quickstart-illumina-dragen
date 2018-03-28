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
# Gerneral scheduler utilities for scheduler functions to use: dragen_jobd, dragen_job_execute and node_update
#

from __future__ import print_function

import os
from datetime import datetime

from dateutil import parser
from dateutil import tz


########################################################################################
# parse_iso_datetime_string - Parse and input string that is in ISO-8601 format, i.e.
#    YYYY-MM-DDThh:mm:ssTZD (eg 2017-07-16T19:20:30+01:00)
#    Note: could have 'Z' instead of the TZD to indicate UTC time
# Returns datetime object
#
def parse_iso_datetime_string(dts):
    return parser.parse(dts)


########################################################################################
# get_age_of_utc_string_in_secs - Given an input time string in UTC format, i.e.
#     YYYY-MM-DDThh:mm:ssZ (eg 2017-07-16T19:20:30Z)
# provide the number of seconds elapsed from that time
# Returns the age of utc string in seconds
#
def get_age_of_utc_string_in_secs(dts):
    # First get the current datetime with explicit TZD=0 (since parser result is TZ aware)
    utc_date = datetime.now(tz.tzoffset(None, 0))

    # Convert the input to a datetime
    input_date = parse_iso_datetime_string(dts)

    # Get the difference as timedelta() and convert
    delta = utc_date - input_date
    return delta.total_seconds()


########################################################################################
# check_create_dir - Check the specified local directory exists, and if not create it.
# Returns None
def check_create_dir(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)  # Attempt to create the directory
        except OSError as e:
            print("ERROR: Could not create directory %s!" % path)
            raise e

    if not os.path.isdir(path):
        err = "ERROR: Path %s exists and is not a directory!" % path
        print(err)
        raise OSError(err)

    return


########################################################################################
# localtime_to_utc - Convert local timestamp to UTC time.  FIXME: dateutil can not
# distinguish between the two 1:30 times that happen during a DST changeover. If you
# need to be able to distinguish between those two times, the workaround it to use pytz,
# which can handle it.  Alternatively, see if slurm mysql database will take care of
# this issue for us (maybe it stores timestamps with UTC time).
#
def localtime_to_utc(localtime):
    if len(localtime) == 0 or localtime == 'Unknown':
        return ""

    ltime = datetime.strptime(localtime, '%Y-%m-%dT%H:%M:%S')

    local_timezone = tz.tzlocal()
    ltime = ltime.replace(tzinfo=local_timezone)

    utc_timezone = tz.gettz('UTC')
    utc = ltime.astimezone(utc_timezone)

    return utc.strftime('%Y-%m-%dT%H:%M:%SZ')


########################################################################################
# seconds_to_hr_min_sec - Convert seconds to hours:min:sec format
#
def seconds_to_hr_min_sec(secs):
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)
