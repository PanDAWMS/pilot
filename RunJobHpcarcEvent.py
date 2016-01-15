# Class definition:
#   RunJobHpcEvent
#   This class is the base class for the HPC Event Server classes.
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

import commands
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback

from RunJobHpcEvent import RunJobHpcEvent
from pUtil import tolog


class RunJobHpcarcEvent(RunJobHpcEvent):

    # private data members
    __runjob = "RunJobHpcarcEvent"                            # String defining the sub class
    __instance = None                           # Boolean used by subclasses to become a Singleton
    #__error = PilotErrors()                     # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """
        super(RunJobHpcarcEvent, self).__init__()

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobHpcarcEvent, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobHpcEvent, self).getRunJobFileName()

    # def argumentParser(self):  <-- see example in RunJob.py

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return False

    def setupYoda(self):
        tolog("setupYoda")
        self.__hpcStatue = 'starting'
        self.updateAllJobsState('starting', self.__hpcStatue)

        status, output = self.prepareHPCJobs()
        if status != 0:
            tolog("Failed to prepare HPC jobs: status %s, output %s" % (status, output))
            self.failAllJobs(0, PilotErrors.ERR_UNKNOWN, self.__jobs, pilotErrorDiag=output)
            return 

        self.__hpcStatus = None
        self.__hpcLog = None

        hpcManager = self.__hpcManager
        hpcJobs = {}
        for jobId in self.__jobs:
            if len(self.__eventRanges[jobId]) > 0:
                hpcJobs[jobId] = self.__jobs[jobId]['hpcJob']
        hpcManager.initJobs(hpcJobs, self.__jobEventRanges)

        hpcManager.setPandaJobStateFile(self.__jobStateFile)
        hpcManager.setStageoutThreads(self.__stageout_threads)
        hpcManager.saveState()
        self.__hpcManager = hpcManager

    def getRankNum():
        if os.environ.has_key('RANK_NUM'):
            tolog("RANK %s" % os.environ['RANK_NUM'])
            return int(os.environ['RANK_NUM'])
        elif os.environ.has_key('SLURM_NODEID'):
            tolog("RANK %s" % os.environ['SLURM_NODEID'])
            return int(os.environ['SLURM_NODEID'])
        return None

if __name__ == "__main__":

    tolog("Starting RunJobHpcarcEvent")

    rank_num = None
    if os.environ.has_key('RANK_NUM'):
        tolog("RANK_NUM(PBS) is %s" % os.environ['RANK_NUM'])
        rank_num = int(os.environ['RANK_NUM'])
    elif os.environ.has_key('SLURM_NODEID'):
        tolog("RANK_NUM(SLURM) %s" % os.environ['SLURM_NODEID'])
        rank_num = int(os.environ['SLURM_NODEID'])

    if not os.environ.has_key('PilotHomeDir'):
        os.environ['PilotHomeDir'] = os.getcwd()

    if True:
        runJob = RunJobHpcarcEvent()
        try:
            runJob.setupHPCEvent(rank_num)
            tolog("RANK_NUM %s" % rank_num)
            if rank_num is None or rank_num == 0:
                runJob.setupHPCManager()
                runJob.getHPCEventJobs()
                runJob.stageInHPCJobs()
                runJob.startHPCJobs()
            else:
                runJob.startHPCSlaveJobs()
        except:
            tolog("RunJobHpcEventException")
            tolog(traceback.format_exc())
            tolog(sys.exc_info()[1])
            tolog(sys.exc_info()[2])
        finally:
            runJob.finishJobs()
            sys.exit(0)
