#!/opt/workflow/python/bin/python2.7
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
# Class for logging using syslog or logging timestamps directly to a file.
#

from __future__ import print_function

from builtins import object
import datetime
import sys
import syslog
import traceback


class Logger(object):
    ########################################################################################
    # Constructor - either cfg or logpath must be defined
    #
    def __init__(self, cfg=None, logpath=None, syslogger=False, procname=None, stdout=False):
        if cfg:
            self.log_level = cfg.verbose
        else:
            self.log_level = 1
        self.stdout = stdout
        self.syslogger = syslogger

        if procname:
            self.procname = procname
        else:
            self.procname = "dragen_jobd"

        if self.syslogger:
            self.logopt = syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY
            self.facility = syslog.LOG_USER
            self.logfd = None
        elif logpath:
            try:
                self.logfd = open(logpath, 'w', 0)
                self.logpath = logpath
            except Exception as e:
                print("ERROR: could not open %s for logging - log output redirected to stdout" % logpath)
                self.logfd = sys.stdout
        else:
            print("Log output is being redirected to stdout")
            self.logfd = sys.stdout
            self.stdout = True

    ########################################################################################
    # log
    #
    def log(self, msg, level=1):
        if self.log_level < level:
            return
        if self.syslogger:
            syslog.openlog(self.procname, self.logopt, self.facility)
            syslog.syslog(msg)
            syslog.closelog()

        elif self.stdout and self.logfd == sys.stdout:
            self.logfd.write("%s\n" % msg)
            self.logfd.flush()
        else:
            self.logfd.write("%s %s\n"
                             % (datetime.datetime.strftime(datetime.datetime.now(), '%b %d %Y %H:%M:%S'),
                                msg))
            self.logfd.flush()
        # Handle case where we want both stdout and logfile or syslog
        if self.stdout and self.logfd != sys.stdout:
            sys.stdout.write("%s\n" % msg)
            sys.stdout.flush()

    ########################################################################################
    # fatal
    #
    def fatal(self, msg):
        self.log("FATAL: %s" % msg)

    ########################################################################################
    # error
    #
    def error(self, msg):
        self.log("ERROR: %s" % msg)

    ########################################################################################
    # warning
    #
    def warning(self, msg):
        self.log("WARNING: %s" % msg)

    ########################################################################################
    # exception - Dump stack trace
    #
    def exception(self):
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            self.log(line)
