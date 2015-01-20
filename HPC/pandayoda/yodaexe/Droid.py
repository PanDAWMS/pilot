import commands
import json
import os
import shutil
import sys
import time
import pickle
import signal
from os.path import abspath as _abspath, join as _join
from pandayoda.yodacore import Interaction,Database,Logger
from EventServer.EventServerJobManager import EventServerJobManager


class Droid:
    def __init__(self, globalWorkingDir, localWorkingDir):
        self.__globalWorkingDir = globalWorkingDir
        self.__localWorkingDir = localWorkingDir
        self.__currentDir = None
        self.__comm = Interaction.Requester()
        self.__tmpLog = Logger.Logger()
        self.__esJobManager = None
        self.__rank = self.__comm.getRank()
        self.__tmpLog.info("Rank %s: Global working dir: %s" % (self.__rank, self.__globalWorkingDir))
        self.initWorkingDir()
        self.__tmpLog.info("Rank %s: Current working dir: %s" % (self.__rank, self.__currentDir))

        self.__poolFileCatalog = None
        self.__inputFiles = None
        self.__copyInputFiles = None
        signal.signal(signal.SIGTERM, self.stop)

    def initWorkingDir(self):
        # Create separate working directory for each rank
        curdir = _abspath (self.__localWorkingDir)
        wkdirname = "rank_%s" % str(self.__rank)
        wkdir  = _abspath (_join(curdir,wkdirname))
        if not os.path.exists(wkdir):
             os.makedirs (wkdir)
        os.chdir (wkdir)
        self.__currentDir = wkdir

    def postExecJob(self):
        if self.__copyInputFiles and self.__inputFiles is not None and self.__poolFileCatalog is not None:
            for inputFile in self.__inputFiles:
                localInputFile = os.path.join(os.getcwd(), os.path.basename(inputFile))
                self.__tmpLog.debug("Rank %s: Remove input file: %s" % (self.__rank, localInputFile))
                os.remove(localInputFile)

        if self.__globalWorkingDir != self.__localWorkingDir:
            command = "mv " + self.__currentDir + " " + self.__globalWorkingDir
            self.__tmpLog.debug("Rank %s: copy files from local working directory to global working dir(cmd: %s)" % (self.__rank, command))
            status, output = commands.getstatusoutput(command)
            self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))

    def setup(self, job):
        #try:
        if True:
            self.__poolFileCatalog = job.get('PoolFileCatalog', None)
            self.__inputFiles = job.get('InputFiles', None)
            self.__copyInputFiles = job.get('CopyInputFiles', False)
            if self.__copyInputFiles and self.__inputFiles is not None and self.__poolFileCatalog is not None:
                for inputFile in self.__inputFiles:
                    shutil.copy(inputFile, './')

                pfc_name = os.path.basename(self.__poolFileCatalog)
                pfc_name = os.path.join(os.getcwd(), pfc_name)
                pfc_name_back = pfc_name + ".back"
                shutil.copy2(self.__poolFileCatalog, pfc_name_back)
                with open(pfc_name, 'wt') as pfc_out:
                    with open(pfc_name_back, 'rt') as pfc_in:
                        for line in pfc_in:
                            pfc_out.write(line.replace('HPCWORKINGDIR', os.getcwd()))
                    
                job["AthenaMPCmd"] = job["AthenaMPCmd"].replace('HPCWORKINGDIR', os.getcwd())
            
            self.__esJobManager = EventServerJobManager(self.__rank)
            self.__esJobManager.initMessageThread(socketname='EventService_EventRanges', context='local')
            self.__esJobManager.initTokenExtractorProcess(job["TokenExtractCmd"])
            self.__esJobManager.initAthenaMPProcess(job["AthenaMPCmd"])
            return True, None
        #except Exception, e:
        #    errMsg = "Failed to init EventServerJobManager: %s" % str(e)
        #    self.__esJobManager.terminate()
        #    return False, errMsg

    def getJob(self):
        request = {'Test':'TEST'}
        self.__tmpLog.debug("Rank %s: getJob(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('getJob',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            job = output["job"]
            if statusCode == 0:
                return True, job
        return False, None

    def getEventRanges(self):
        request = {'nRanges': 1}
        self.__tmpLog.debug("Rank %s: getEventRanges(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('getEventRanges',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            eventRanges = output['eventRanges']
            if statusCode == 0:
                return True, eventRanges
        return False, None

    def updateEventRange(self, output):
        try:
            eventRangeID = output.split(",")[1]
        except Exception, e:
            self.__tmpLog.warnning("Rank %s: failed to get eventRangeID from output: %s" % (self.__rank, output))
            self.__tmpLog.warnning("Rank %s: error message: %s" % (self.__rank, str(e)))
        request = {"eventRangeID": eventRangeID,
                   'eventStatus':" finished",
                   "output": output}
        self.__tmpLog.debug("Rank %s: updateEventRange(request: %s)" % (self.__rank, request))
        retStatus, retOutput = self.__comm.sendRequest('updateEventRange',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, retStatus, retOutput))
        if retStatus:
            statusCode = retOutput["StatusCode"]
            if statusCode == 0:
                return True
        return False

    def finishJob(self):
        request = {'state': 'finished'}
        self.__tmpLog.debug("Rank %s: updateJob(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('updateJob',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            if statusCode == 0:
                return True
        return False

    def failedJob(self):
        request = {'state': 'failed'}
        self.__tmpLog.debug("Rank %s: updateJob(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('updateJob',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            if statusCode == 0:
                return True
        return False

    def waitYoda(self):
        self.__tmpLog.debug("Rank %s: WaitYoda" % (self.__rank))
        while True:
            status, output = self.__comm.waitMessage()
            self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
            if status:
                statusCode = output["StatusCode"]
                state = output["State"]
                if statusCode == 0 and state == 'finished':
                    return True
        return True

    def run(self):
        self.__tmpLog.info("Droid Starts")
        status, job = self.getJob()
        self.__tmpLog.info("Rank %s: getJob(%s)" % (self.__rank, job))
        if not status:
            self.__tmpLog.debug("Rank %s: Failed to get job" % self.__rank)
            self.failedJob()
            return -1
        status, output = self.setup(job)
        self.__tmpLog.info("Rank %s: setup job(status:%s, output:%s)" % (self.__rank, status, output))
        if not status:
            self.__tmpLog.debug("Rank %s: Failed to setup job(%s)" % (self.__rank, output))
            self.failedJob()
            return -1

        # main loop
        failedNum = 0
        #self.__tmpLog.info("Rank %s: isDead: %s" % (self.__rank, self.__esJobManager.isDead()))
        while not self.__esJobManager.isDead():
            #self.__tmpLog.info("Rank %s: isDead: %s" % (self.__rank, self.__esJobManager.isDead()))
            #self.__tmpLog.info("Rank %s: isNeedMoreEvents: %s" % (self.__rank, self.__esJobManager.isNeedMoreEvents()))
            if self.__esJobManager.isNeedMoreEvents():
                self.__tmpLog.info("Rank %s: need more events" % self.__rank)
                status, eventRanges = self.getEventRanges()
                # failed to get message again and again
                if not status:
                    fileNum += 1
                    if fileNum > 30:
                        self.__tmpLog.warning("Rank %s: failed to get events more than 30 times. finish job" % self.__rank)
                        self.__esJobManager.insertEventRange("No more events")
                    else:
                        continue
                else:
                    fileNum = 0
                    self.__tmpLog.info("Rank %s: get event ranges(%s)" % (self.__rank, eventRanges))
                    if len(eventRanges) == 0:
                        self.__tmpLog.info("Rank %s: no more events" % self.__rank)
                        self.__esJobManager.insertEventRange("No more events")
                    for eventRange in eventRanges:
                        self.__esJobManager.insertEventRange(eventRange)

            self.__esJobManager.poll()
            output = self.__esJobManager.getOutput()
            if output is not None:
                self.__tmpLog.info("Rank %s: get output(%s)" % (self.__rank, output))
                self.updateEventRange(output)

            time.sleep(2)

        self.__esJobManager.flushMessages()
        output = self.__esJobManager.getOutput()
        while output:
            self.__tmpLog.info("Rank %s: get output(%s)" % (self.__rank, output))
            self.updateEventRange(output)
            output = self.__esJobManager.getOutput()

        self.__tmpLog.info("Rank %s: post exec job" % self.__rank)
        self.postExecJob()
        self.__tmpLog.info("Rank %s: finish job" % self.__rank)
        self.finishJob()
        self.waitYoda()
        return 0

    def stop(self, signum=None, frame=None):
        self.__tmpLog.info('Rank %s: stop signal received' % self.__rank)
        self.__esJobManager.terminate()
        self.__esJobManager.flushMessages()
        output = self.__esJobManager.getOutput()
        while output:
            self.__tmpLog.info("Rank %s: get output(%s)" % (self.__rank, output))
            self.updateEventRange(output)
            output = self.__esJobManager.getOutput()

        self.__tmpLog.info("Rank %s: post exec job" % self.__rank)
        self.postExecJob()
        self.__tmpLog.info("Rank %s: finish job" % self.__rank)
        self.finishJob()

        self.__tmpLog.info('Rank %s: stop' % self.__rank)

    def __del__(self):
        self.__tmpLog.info('Rank %s: __del__ function' % self.__rank)
        #self.__esJobManager.terminate()
        #self.__esJobManager.flushMessages()
        #output = self.__esJobManager.getOutput()
        #while output:
        #    self.__tmpLog.info("Rank %s: get output(%s)" % (self.__rank, output))
        #    self.updateEventRange(output)
        #    output = self.__esJobManager.getOutput()

        #self.__tmpLog.info("Rank %s: post exec job" % self.__rank)
        #self.postExecJob()
        #self.__tmpLog.info("Rank %s: finish job" % self.__rank)
        #self.finishJob()

        self.__tmpLog.info('Rank %s: __del__ function' % self.__rank)

