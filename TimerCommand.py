#!/usr/bin/env python

# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wguan@cern.ch>, 2014

import os
import signal
import time

from Queue import Empty, Full
import subprocess, threading
from multiprocessing import Process, Queue

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
            self.process = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
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
                    os.kill(int(self.process.pid), signal.SIGKILL)
                    thread.join(2)
            except:
                if thread.is_alive():
                    try:
                        os.kill(int(self.process.pid), signal.SIGKILL)
                    except:
                        pass
                    thread.join()

            if not self.stdout:
                self.stdout = ''
            self.stdout += "Command Timeout: %s s" % timeout

        return self.process.returncode, self.stdout

    def runFunction(self, func, args, timeout=3600):
        def target(func, args, retQ):
            ret= func(*args)
            retQ.put(ret)

        retQ = Queue()
        process = Process(target=target, args=(func, args, retQ))
        process.start()
        try:
            ret = retQ.get(block=True, timeout=timeout)
        except Empty:
            ret = (-1, "function timeout, killed")
            try:
                if process.is_alive():
                    process.terminate()
                    process.join(2)
                if process.is_alive():
                    os.kill(int(process.pid), signal.SIGKILL)
                    process.join(2)
            except:
                if process.is_alive():
                    try:
                        os.kill(int(process.pid), signal.SIGKILL)
                    except:
                        pass
                    process.join(2)
        return ret
