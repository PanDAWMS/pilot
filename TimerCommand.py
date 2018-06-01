#!/usr/bin/env python

# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wguan@cern.ch>, 2014-2018

import os
import signal
import time
import traceback

from Queue import Empty, Full
import subprocess, threading
import multiprocessing

class TimerCommand(object):
    def __init__(self, cmd=None):
        self.cmd = cmd
        self.process = None
        self.stdout = None
        self.stderr = None
        self.is_timeout = False

    def run(self, timeout=3600):
        def target():
            # print 'Thread started'
            self.process = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, preexec_fn=os.setsid)
            self.stdout, self.stderr = self.process.communicate()
            # print 'Thread finished'

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            self.is_timeout = True
            try:
                # print 'TimeOut. Terminating process'
                self.process.terminate()
                thread.join(2)
                if thread.is_alive():
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    thread.join(2)
            except:
                if thread.is_alive():
                    try:
                        os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    except:
                        pass
                    thread.join(2)

            if not self.stdout:
                self.stdout = ''
            self.stdout += "Command time-out: %s s" % timeout

        if not self.stderr:
            self.stderr = ''
        if not self.stdout:
            self.stdout = ''
        if self.stderr and self.stderr != '':
            self.stdout += " Error: " + self.stderr

        if self.process:
            returncode = self.process.returncode
        else:
            returncode = 1

        if returncode != 1 and 'Command time-out' in self.stdout:
            returncode = 1

        if returncode == None:
            returncode = 0

        return returncode, self.stdout

    def runFunction(self, func, args, timeout=3600):
        def target(func, args, retQ):
            error = ''
            try:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
            except:
                error = error + '%s\n' % traceback.format_exc()
            try:
                ret= func(*args)
                retQ.put(ret)
            except:
                retQ.put((-1, error + '%s\n' % traceback.format_exc()))

        retQ = multiprocessing.Queue()
        process = multiprocessing.Process(target=target, args=(func, args, retQ))
        try:
            process.start()
        except:
            timeout = 1
        ret = None
        try:
            ret = retQ.get(block=True, timeout=timeout)
        except Empty:
            ret = (-1, "function timeout, killed")
            try:
                if process.is_alive():
                    process.terminate()
                    process.join(2)
                if process.is_alive():
                    # os.kill(int(process.pid), signal.SIGKILL)
                    process.terminate()
                    process.join(2)
            except:
                if process.is_alive():
                    try:
                        # os.kill(int(process.pid), signal.SIGKILL)
                        process.terminate()
                    except:
                        pass
                    process.join(2)
        finally:
            while process.is_alive():
                process.terminate()
                process.join(2)
            multiprocessing.active_children()
            if ret is None:
                ret = (-1, "function failed with unknow error")
                
        return ret


def getstatusoutput(cmd, timeout=1800):
    timerCmd = TimerCommand(cmd)
    status, output = timerCmd.run(timeout=timeout)
    return status, output
