import commands
import datetime
import json
import logging
import os
import shutil
import socket
import sys
import time
import pickle
import signal
import threading
import traceback
from os.path import abspath as _abspath, join as _join

# logging.basicConfig(filename='Droid.log', level=logging.DEBUG)

from pandayoda.yodacore import Logger

import pUtil
from objectstoreSiteMover import objectstoreSiteMover
from Mover import getInitialTracingReport
from ThreadPool import ThreadPool

class DroidStager(threading.Thread):
    def __init__(self, globalWorkingDir, localWorkingDir, outputs=None, job=None, esJobManager=None, outputDir=None, rank=None, logger=None):
        threading.Thread.__init__(self)
        self.__globalWorkingDir = globalWorkingDir
        self.__localWorkingDir = localWorkingDir
        self.__currentDir = None
        self.__rank = rank
        if logger and False:
            self.__tmpLog = logger
        else:
            curdir = _abspath (self.__localWorkingDir)
            wkdirname = "rank_%s" % str(self.__rank)
            wkdir  = _abspath (_join(curdir,wkdirname))
            self.__tmpLog = Logger.Logger(filename=os.path.join(wkdir, 'Droid.log'))
        self.__job = job
        self.__esJobManager = esJobManager
        self.__stop = threading.Event()
        self.__isFinished = False
        self.__tmpLog.info("Rank %s: Global working dir: %s" % (self.__rank, self.__globalWorkingDir))
        os.environ['PilotHomeDir'] = os.path.dirname(self.__globalWorkingDir)

        self.__jobId = None
        self.__copyOutputToGlobal = False
        self.__outputDir = outputDir

        self.__hostname = socket.getfqdn()

        self.__outputs = outputs
        self.__threadpool = None
        self.setup(job)

    def setup(self, job):
        try:
            self.__jobId = job.get("JobId", None)
            self.__yodaToOS = job.get('yodaToOS', False)
            self.__yodaToZip = job.get('yodaToZip', False)
            self.__zipFileName = job.get('zipFileName', None)
            self.__zipEventRangesName = job.get('zipEventRangesName', None)
            self.__tmpLog.debug("Rank %s: zip file %s" % (self.__rank, self.__zipFileName))
            self.__tmpLog.debug("Rank %s: zip event range file %s" % (self.__rank, self.__zipEventRangesName))
            if self.__zipFileName is None or self.__zipEventRangesName is None:
                self.__tmpLog.debug("Rank %s: either zipFileName(%s) is None or zipEventRanagesName(%s) is None, will not use zip output" % (self.__rank, self.__zipFileName, self.__zipEventRangesName))
                self.__yodaToZip = False
            self.__copyOutputToGlobal =  job.get('copyOutputToGlobal', False)

            if self.__yodaToOS:
                setup = job.get('setup', None)
                self.__esPath = job.get('esPath', None)
                self.__os_bucket_id = job.get('os_bucket_id', None)
                self.__report =  getInitialTracingReport(userid='Yoda', sitename='Yoda', dsname=None, eventType="objectstore", analysisJob=False, jobId=None, jobDefId=None, dn='Yoda')
                self.__siteMover = objectstoreSiteMover(setup, useTimerCommand=False)
                self.__cores = int(job.get('ATHENA_PROC_NUMBER', 1))

                try:
                    self.__stageout_threads = int(job.get('stageout_threads', None))
                except:
                    self.__stageout_threads = self.__cores/8
                self.__tmpLog.debug("Rank %s: start threadpool with %s threads" % (self.__rank, self.__stageout_threads))
                self.__threadpool = ThreadPool(self.__stageout_threads)

        except:
            self.__tmpLog.error("Failed to setup Droid stager: %s" % str(traceback.format_exc()))

    def copyOutput(self, output, outputs):
        if self.__outputDir:
            for filename in outputs:
                #filename = output.split(",")[0]
                base_filename = os.path.basename(filename)
                new_file_name = os.path.join(self.__outputDir, base_filename)
                is_copied = False
                try:
                    os.rename(filename, new_file_name)
                    is_copied = True
                except:
                    self.__tmpLog.debug("Rank %s: failed to move output %s to %s, %s" % (self.__rank, filename, new_file_name, str(traceback.format_exc())))
                    is_copied = False
                if not is_copied:
                    shutil.copy(filename, new_file_name)
                    os.remove(filename)
                output = output.replace(filename, new_file_name)
            return 0, output
        elif self.__copyOutputToGlobal:
            for filename in outputs:
                #filename = output.split(",")[0]
                base_filename = os.path.basename(filename)
                new_file_name = os.path.join(self.__globalWorkingDir, base_filename)
                is_copied = False
                try:
                    os.rename(filename, new_file_name)
                    is_copied = True
                except:
                    self.__tmpLog.debug("Rank %s: failed to move output %s to %s, %s" % (self.__rank, filename, new_file_name, str(traceback.format_exc())))
                    is_copied = False
                if not is_copied:
                    shutil.copy(filename, new_file_name)
                    os.remove(filename)
                output = output.replace(filename, new_file_name)
            return 0, output
        else:
            if self.__localWorkingDir == self.__globalWorkingDir:
                return 0, output

            for filename in outputs:
            #filename = output.split(",")[0]
                new_file_name = filename.replace(self.__localWorkingDir, self.__globalWorkingDir)
                dirname = os.path.dirname(new_file_name)
                if not os.path.exists(dirname):
                     os.makedirs (dirname)
                shutil.copy(filename, new_file_name)
                os.remove(filename)
                output = output.replace(filename, new_file_name)
            return 0, output

    def stageOutToOS(self, outputs):
        ret_status = 0
        ret_outputs = []
        try:
            for filename in outputs:
                ret_status, pilotErrorDiag, surl, size, checksum, arch_type = self.__siteMover.put_data(filename, self.__esPath, lfn=os.path.basename(filename), report=self.__report, token=None, experiment='ATLAS')
                if ret_status == 0:
                    os.remove(filename)
                    ret_outputs.append(surl)
                else:
                    self.__tmpLog.debug("Failed to stageout %s: %s %s" % (filename, ret_status, pilotErrorDiag))
                    return ret_status, pilotErrorDiag
        except:
            self.__tmpLog.warning("Rank %s: Droid throws exception when staging out: %s" % (self.__rank, traceback.format_exc()))
            ret_status = -1
        return ret_status, ret_outputs

    def createAtomicLockFile(self, file_path):
        lockfile_name = os.path.join(os.path.dirname(file_path), "ATOMIC_LOCKFILE")
        try:
            # acquire the lock
            fd = os.open(lockfile_name, os.O_EXCL|os.O_CREAT)
        except OSError:
            # work dir is locked, so exit
            self.__tmpLog.warning("Found lock file: %s (wait)" % (lockfile_name))
            fd = None
        else:
            self.__tmpLog.debug("Created lock file: %s" % (lockfile_name))
        return fd, lockfile_name

    def releaseAtomicLockFile(self, fd, lockfile_name):
        try:
            os.close(fd)
            os.unlink(lockfile_name)
        except Exception, e:
            if "Bad file descriptor" in str(e):
                self.__tmpLog.warning("Lock file already released")
            else:
                self.__tmpLog.warning("WARNING: Could not release lock file: %s" % str(e))
        else:
            self.__tmpLog.warning("Released lock file: %s" % (lockfile_name))

    def zipOutputs(self, eventRangeID, eventStatus, outputs):
        try:
            for filename in outputs:
                command = "tar -rf " + self.__zipFileName + " --directory=%s %s" %(os.path.dirname(filename), os.path.basename(filename))
                self.__tmpLog.debug("Tar/zip: %s" % (command))
                status, ret = commands.getstatusoutput(command)
                if status:
                    self.__tmpLog.debug("Failed to zip %s: %s, %s" % (filename, status, ret))
                    return status, ret
                else:
                    os.remove(filename)
        except:
            self.__tmpLog.warning("Rank %s: Droid throws exception when zipping out: %s" % (self.__rank, traceback.format_exc()))
            return -1, "Failed to zip outputs"
        else:
            handler = open(self.__zipEventRangesName, "a")
            handler.write("%s %s %s\n" % (eventRangeID, eventStatus, outputs))
            handler.close()
        return 0, outputs

    def stageOut(self, eventRangeID, eventStatus, output, retries=0):
        if eventStatus.startswith("ERR"):
            request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': eventStatus, "output": output}
        else:
            outputs = output.split(",")[:-3]
            if self.__yodaToZip:
                self.__tmpLog.debug("Rank %s: start to zip outputs: %s" % (self.__rank, outputs))
                retStatus, retOutput = self.zipOutputs(eventRangeID, eventStatus, outputs)
                if retStatus != 0:
                    self.__tmpLog.error("Rank %s: failed to zip outputs %s: %s" % (self.__rank, outputs, retOutput))
                    request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': eventStatus, "output": output}
                else:
                    self.__tmpLog.info("Rank %s: finished to zip outputs %s: %s" % (self.__rank, outputs, retOutput))
                    request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': 'zipped', "output": retOutput}
            elif self.__yodaToOS:
                self.__tmpLog.debug("Rank %s: start to stage out outputs to objectstore: %s" % (self.__rank, outputs))
                retStatus, retOutput = self.stageOutToOS(outputs)
                if retStatus != 0:
                    self.__tmpLog.error("Rank %s: failed to stagout outputs %s to objectstore: %s" % (self.__rank, outputs, retOutput))
                    if retries < 1:
                        self.stageOut(eventRangeID, eventStatus, output, retries=retries+1)
                        request = None
                    else:
                        request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': eventStatus, "output": output}
                else:
                    self.__tmpLog.info("Rank %s: finished to stageout outputs %s to objectstore: %s" % (self.__rank, outputs, retOutput))
                    request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': 'stagedOut', "output": retOutput, 'objstoreID': self.__os_bucket_id}
            else:
                self.__tmpLog.debug("Rank %s: start to copy outputs: %s" % (self.__rank, outputs))
                retStatus, retOutput = self.copyOutput(output, outputs)
                if retStatus != 0:
                    self.__tmpLog.error("Rank %s: failed to copy outputs %s: %s" % (self.__rank, outputs, retOutput))
                    request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': eventStatus, "output": output}
                else:
                    self.__tmpLog.info("Rank %s: finished to copy outputs %s: %s" % (self.__rank, outputs, retOutput))
                    request = {"jobId": self.__jobId, "eventRangeID": eventRangeID, 'eventStatus': eventStatus, "output": retOutput}
        if request:
            self.__outputs.put(request)

    def bulkZipOutputs(self, outputs):
        try:
            while True:
                fd, lockfile = self.createAtomicLockFile(self.__zipFileName)
                if fd:
                    break
                time.sleep(0.1)
            for outputMsg in outputs:
                try:
                    eventRangeID, eventStatus, output = outputMsg
                    self.stageOut(eventRangeID, eventStatus, output, retries=0)
                except:
                    self.__tmpLog.warning("Rank %s: error message: %s" % (self.__rank, traceback.format_exc()))
        except:
            self.__tmpLog.warning("Rank %s: error message: %s" % (self.__rank, traceback.format_exc()))
        finally:
            self.releaseAtomicLockFile(fd, lockfile)

    def stop(self):
        self.__stop.set()

    def isFinished(self):
        return self.__isFinished

    def run(self):
        while True:
            try:
                outputs = self.__esJobManager.getOutputs()
                if outputs:
                    self.__tmpLog.debug("Rank %s: getOutputs: %s" % (self.__rank, outputs))
                    if self.__yodaToZip:
                        self.bulkZipOutputs(outputs)
                    else:
                        for outputMsg in outputs:
                            try:
                                eventRangeID, eventStatus, output = outputMsg
                                if self.__threadpool:
                                    self.__tmpLog.debug("Rank %s: add event output to threadpool: %s" % (self.__rank, outputMsg))
                                    self.__threadpool.add_task(self.stageOut, eventRangeID, eventStatus, output, retries=0)
                                else:
                                    self.stageOut(eventRangeID, eventStatus, output, retries=0)
                            except:
                                self.__tmpLog.warning("Rank %s: error message: %s" % (self.__rank, traceback.format_exc()))
                                continue
            except:
                self.__tmpLog.error("Rank %s: Stager Thread failed: %s" % (self.__rank, traceback.format_exc()))
            if self.__stop.isSet():
                if self.__threadpool:
                    self.__tmpLog.warning("Rank %s: wait threadpool to finish" % (self.__rank))
                    self.__threadpool.wait_completion()
                    self.__tmpLog.warning("Rank %s: threadpool finished" % (self.__rank))
                break
            time.sleep(1)
        self.__isFinished = True
