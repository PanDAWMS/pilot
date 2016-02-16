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

from pandayoda.yodacore import Interaction,Database,Logger
from EventServer.EventServerJobManager import EventServerJobManager
from signal_block.signal_block import block_sig, unblock_sig

import pUtil
from objectstoreSiteMover import objectstoreSiteMover
from Mover import getInitialTracingReport

class Droid(threading.Thread):
    def __init__(self, globalWorkingDir, localWorkingDir, rank=None, nonMPIMode=False, reserveCores=0):
        threading.Thread.__init__(self)
        self.__globalWorkingDir = globalWorkingDir
        self.__localWorkingDir = localWorkingDir
        self.__currentDir = None
        self.__comm = Interaction.Requester(rank=rank, nonMPIMode=nonMPIMode)
        self.__tmpLog = Logger.Logger(filename='Droid.log')
        self.__esJobManager = None
        self.__isFinished = False
        self.__rank = self.__comm.getRank()
        self.__tmpLog.info("Rank %s: Global working dir: %s" % (self.__rank, self.__globalWorkingDir))
        if not os.environ.has_key('PilotHomeDir'):
            os.environ['PilotHomeDir'] = self.__globalWorkingDir

        self.initWorkingDir()
        self.__tmpLog.info("Rank %s: Current working dir: %s" % (self.__rank, self.__currentDir))

        self.__jobId = None
        self.__poolFileCatalog = None
        self.__inputFiles = None
        self.__copyInputFiles = None
        self.__preSetup = None
        self.__postRun = None
        self.__ATHENA_PROC_NUMBER = 1
        self.__firstGetEventRanges = True
        self.__copyOutputToGlobal = False

        self.reserveCores = reserveCores
        self.__hostname = socket.getfqdn()

        self.__outputs = []

        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGQUIT, self.stop)
        signal.signal(signal.SIGSEGV, self.stop)
        signal.signal(signal.SIGXCPU, self.stop)
        signal.signal(signal.SIGUSR1, self.stop)
        signal.signal(signal.SIGBUS, self.stop)

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
            command = "cp -fr " + self.__currentDir + " " + self.__globalWorkingDir
            self.__tmpLog.debug("Rank %s: copy files from local working directory to global working dir(cmd: %s)" % (self.__rank, command))
            status, output = commands.getstatusoutput(command)
            self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))

        if self.__postRun and self.__esJobManager:
            self.__esJobManager.postRun(self.__postRun)

    def setup(self, job):
        try:
            self.__jobId = job.get("JobId", None)
            self.__poolFileCatalog = job.get('PoolFileCatalog', None)
            self.__inputFiles = job.get('InputFiles', None)
            self.__copyInputFiles = job.get('CopyInputFiles', False)
            self.__preSetup = job.get('PreSetup', None)
            self.__postRun = job.get('PostRun', None)
            self.__yodaToOS = job.get('yodaToOS', False)
            self.__copyOutputToGlobal =  job.get('copyOutputToGlobal', False)

            if self.__yodaToOS:
                setup = job.get('setup', None)
                self.__esPath = job.get('esPath', None)
                self.__report =  getInitialTracingReport(userid='Yoda', sitename='Yoda', dsname=None, eventType="objectstore", analysisJob=False, jobId=None, jobDefId=None, dn='Yoda')
                self.__siteMover = objectstoreSiteMover(setup, useTimerCommand=False)

            self.__ATHENA_PROC_NUMBER = int(job.get('ATHENA_PROC_NUMBER', 1))
            self.__ATHENA_PROC_NUMBER -= self.reserveCores
            if self.__ATHENA_PROC_NUMBER < 0:
                self.__ATHENA_PROC_NUMBER = 1
            job["AthenaMPCmd"] = "export ATHENA_PROC_NUMBER=" + str(self.__ATHENA_PROC_NUMBER) + "; " + job["AthenaMPCmd"]
            self.__jobWorkingDir = job.get('GlobalWorkingDir', None)
            if self.__jobWorkingDir:
                self.__jobWorkingDir = os.path.join(self.__jobWorkingDir, 'rank_%s' % self.__rank)
                if not os.path.exists(self.__jobWorkingDir):
                    os.makedirs(self.__jobWorkingDir)
                os.chdir(self.__jobWorkingDir)
                logFile = os.path.join(self.__jobWorkingDir, 'Droid.log')
                logging.basicConfig(filename=logFile, level=logging.DEBUG)
                self.__tmpLog = Logger.Logger()

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
            
            self.__esJobManager = EventServerJobManager(self.__rank, self.__ATHENA_PROC_NUMBER)
            status, output = self.__esJobManager.preSetup(self.__preSetup)
            if status != 0:
                return False, output
            # self.__esJobManager.initMessageThread(socketname='EventService_EventRanges', context='local')
            # self.__esJobManager.initTokenExtractorProcess(job["TokenExtractCmd"])
            # self.__esJobManager.initAthenaMPProcess(job["AthenaMPCmd"])
            ret = self.__esJobManager.init(socketname='EventService_EventRanges', context='local', athenaMPCmd=job["AthenaMPCmd"], tokenExtractorCmd=job["TokenExtractCmd"])
            return True, None
        except:
            errMsg = "Failed to init EventServerJobManager: %s" % str(traceback.format_exc())
            self.__esJobManager.terminate()
            return False, errMsg

    def getJob(self):
        request = {'Test':'TEST', 'rank': self.__rank}
        self.__tmpLog.debug("Rank %s: getJob(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('getJob',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            job = output["job"]
            if statusCode == 0 and job:
                return True, job
        return False, None

    def copyOutput(self, output):
        if self.__copyOutputToGlobal:
            filename = output.split(",")[0]
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
            return 0, output.replace(filename, new_file_name)
        else:
            if self.__localWorkingDir == self.__globalWorkingDir:
                return 0, output

            filename = output.split(",")[0]
            new_file_name = filename.replace(self.__localWorkingDir, self.__globalWorkingDir)
            dirname = os.path.dirname(new_file_name)
            if not os.path.exists(dirname):
                 os.makedirs (dirname)
            shutil.copy(filename, new_file_name)
            os.remove(filename)
            return 0, output.replace(filename, new_file_name)

    def stageOut(self, output):
        filename = output.split(",")[0]
        ret_status, pilotErrorDiag, surl, size, checksum, arch_type = self.__siteMover.put_data(filename, self.__esPath, lfn=os.path.basename(filename), report=self.__report, token=None, experiment='ATLAS')
        if ret_status == 0:
            os.remove(filename)
            return 0, output.replace(filename, surl)
        self.__tmpLog.debug("Failed to stageout %s: %s %s" % (output, ret_status, pilotErrorDiag))
        return ret_status, output

    def getEventRanges(self, nRanges=1):
        #if self.__firstGetEventRanges:
        #    request = {'nRanges': self.__ATHENA_PROC_NUMBER}
        #    self.__firstGetEventRanges = False
        #else:
        #    request = {'nRanges': nRanges}
        request = {'jobId': self.__jobId, 'nRanges': nRanges}
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
        status, output = self.copyOutput(output)
        if status != 0:
            self.__tmpLog.debug("Rank %s: failed to copy output from local working dir to global working dir: %s" % (self.__rank, output))
            return False
        request = {"eventRangeID": eventRangeID,
                   'eventStatus': "finished",
                   "output": output}
        self.__tmpLog.debug("Rank %s: updateEventRange(request: %s)" % (self.__rank, request))
        retStatus, retOutput = self.__comm.sendRequest('updateEventRange',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, retStatus, retOutput))
        if retStatus:
            statusCode = retOutput["StatusCode"]
            if statusCode == 0:
                return True
        return False


    def dumpUpdates(self, outputs):
        timeNow = datetime.datetime.utcnow()
        outFileName = 'rank_' + str(self.__rank) + '_' + timeNow.strftime("%Y-%m-%d-%H-%M-%S") + '.dump'
        outFileName = os.path.join(self.globalWorkingDir, outFileName)
        outFile = open(outFileName,'w')
        for eventRangeID,status,output in outputs:
            outFile.write('{0} {1} {2}\n'.format(eventRangeID,status,output))
        outFile.close()

    def updatePandaEventRanges(self, event_ranges):
        """ Update an event range on the Event Server """
        self.__tmpLog.debug("Updating event ranges..")

        message = ""
        #url = "https://aipanda007.cern.ch:25443/server/panda"
        url = "https://pandaserver.cern.ch:25443/server/panda"
        # eventRanges = [{'eventRangeID': '4001396-1800223966-4426028-1-2', 'eventStatus':'running'}, {'eventRangeID': '4001396-1800223966-4426028-2-2','eventStatus':'running'}]

        node={}
        node['eventRanges']=json.dumps(event_ranges)

        # open connection
        ret = pUtil.httpConnect(node, url, path='.', mode="UPDATEEVENTRANGES")
        # response = json.loads(ret[1])

        status = ret[0]
        if ret[0]: # non-zero return code
            message = "Failed to update event range - error code = %d, error: " % (ret[0], ret[1])
        else:
            response = json.loads(json.dumps(ret[1]))
            status = int(response['StatusCode'])
            message = json.dumps(response['Returns'])

        return status, message

    def updateOutputs(self, signal=False, final=False):
        outputs = self.__esJobManager.getOutputs(signal)
        if outputs:
            self.__outputs +=  outputs
            self.__tmpLog.info("Rank %s: get outputs(%s)" % (self.__rank, outputs))
            if final:
                self.dumpUpdates(outputs)

        outputs = self.__outputs
        if outputs:
            requests = []
            stagedRequests = []
            for outputMsg in outputs:
                try:
                    #eventRangeID = output.split(",")[1]
                    eventRangeID, eventStatus, output = outputMsg
                except Exception, e:
                    self.__tmpLog.warnning("Rank %s: failed to parse output message: %s" % (self.__rank, outputMsg))
                    self.__tmpLog.warnning("Rank %s: error message: %s" % (self.__rank, str(e)))
                    continue
                if eventStatus.startswith("ERR"):
                    request = {"jobId": self.__jobId,
                               "eventRangeID": eventRangeID,
                               'eventStatus': eventStatus,
                               "output": output}
                    stagedRequests.append(request)
                    continue

                copyOutput = True
                if self.__yodaToOS and not final:
                    copyOutput = False
                    status, output = self.stageOut(output)
                    if status != 0:
                        self.__tmpLog.debug("Rank %s: failed to stageout output from local working dir to S3 Objectstore: %s" % (self.__rank, output))
                        copyOutput = True
                    else:
                        request = {"jobId": self.__jobId,
                                   "eventRangeID": eventRangeID,
                                   'eventStatus': 'stagedOut',
                                   "output": output}
                        stagedRequests.append(request)
                if copyOutput:
                    status, output = self.copyOutput(output)
                    if status != 0:
                        self.__tmpLog.debug("Rank %s: failed to copy output from local working dir to global working dir: %s" % (self.__rank, output))
                        continue

                    request = {"jobId": self.__jobId,
                               "eventRangeID": eventRangeID,
                               'eventStatus': eventStatus,
                               "output": output}
                    requests.append(request)
            if stagedRequests:
                eventRanges = []
                for stagedRequest in stagedRequests:
                    eventStatus = stagedRequest['eventStatus']
                    if eventStatus.startswith("ERR"):
                        eventStatus = 'failed'
                        eventRanges.append({'eventRangeID': stagedRequest['eventRangeID'], 'eventStatus': eventStatus})
                    if eventStatus == 'stagedOut':
                        eventRanges.append({'eventRangeID': stagedRequest['eventRangeID'], 'eventStatus': 'finished'})
                status, output = self.updatePandaEventRanges(eventRanges)
                if status == 0:
                    self.__tmpLog.debug("Rank %s: updatePandaEventRanges(status: %s, output: %s)" % (self.__rank, status, output))
                    for stagedRequest in stagedRequests:
                        if not stagedRequest['eventStatus'].startswith("ERR"):
                            stagedRequest['eventStatus'] = 'reported'
            requests += stagedRequests
            if requests:
                self.__tmpLog.debug("Rank %s: updateEventRanges(request: %s)" % (self.__rank, requests))
                retStatus, retOutput = self.__comm.sendRequest('updateEventRanges',requests)
                self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, retStatus, retOutput))
                if retStatus:
                    statusCode = retOutput["StatusCode"]
                    if statusCode == 0:
                        if signal:
                            self.__esJobManager.updatedOutputs(outputs)

            for output in outputs:
                self.__outputs.remove(output)            
            return True
                    
        return False

    def finishJob(self):
        if not self.__isFinished:
            request = {'jobId': self.__jobId, 'state': 'finished'}
            self.__tmpLog.debug("Rank %s: updateJob(request: %s)" % (self.__rank, request))
            status, output = self.__comm.sendRequest('updateJob',request)
            self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
            if status:
                statusCode = output["StatusCode"]

            #self.__comm.disconnect()
            return True
        return False

    def failedJob(self):
        request = {'jobId': self.__jobId, 'state': 'failed'}
        self.__tmpLog.debug("Rank %s: updateJob(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('updateJob',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            if statusCode == 0:
                return True
        return False

    def finishDroid(self):
        request = {'state': 'finished'}
        self.__tmpLog.debug("Rank %s: finishDroid(request: %s)" % (self.__rank, request))
        status, output = self.__comm.sendRequest('finishDroid',request)
        self.__tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.__rank, status, output))
        if status:
            statusCode = output["StatusCode"]
            if statusCode == 0:
                return True
        self.__comm.disconnect()
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

    def runOneJob(self):
        self.__tmpLog.info("Droid Starts to get job")
        status, job = self.getJob()
        self.__tmpLog.info("Rank %s: getJob(%s)" % (self.__rank, job))
        if not status or not job:
            self.__tmpLog.debug("Rank %s: Failed to get job" % self.__rank)
            # self.failedJob()
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
            while self.__esJobManager.isNeedMoreEvents() > 0:
                neededEvents = self.__esJobManager.isNeedMoreEvents()
                self.__tmpLog.info("Rank %s: need %s events" % (self.__rank, neededEvents))
                status, eventRanges = self.getEventRanges(neededEvents)
                # failed to get message again and again
                if not status:
                    failedNum += 1
                    if failedNum > 30:
                        self.__tmpLog.warning("Rank %s: failed to get events more than 30 times. finish job" % self.__rank)
                        self.__esJobManager.insertEventRange("No more events")
                    else:
                        continue
                else:
                    failedNum = 0
                    self.__tmpLog.info("Rank %s: get event ranges(%s)" % (self.__rank, eventRanges))
                    if len(eventRanges) == 0:
                        self.__tmpLog.info("Rank %s: no more events" % self.__rank)
                        self.__esJobManager.insertEventRange("No more events")
                    else:    
                        self.__esJobManager.insertEventRanges(eventRanges)

            self.__esJobManager.poll()
            self.updateOutputs()

            time.sleep(0.001)

        self.__esJobManager.flushMessages()
        self.updateOutputs()

        self.__tmpLog.info("Rank %s: post exec job" % self.__rank)
        self.postExecJob()
        self.__tmpLog.info("Rank %s: finish job" % self.__rank)
        self.finishJob()
        #self.waitYoda()
        return self.__esJobManager.getChildRetStatus()

    def preCheck(self):
        if not os.access('/tmp', os.W_OK):
            self.__tmpLog.info("Rank %s: PreCheck /tmp is readonly" % self.__rank)
            status, output = commands.getstatusoutput("ll /|grep tmp")
            self.__tmpLog.info("Rank %s: tmp dir: %s" % (self.__rank, output))
            return 1
        return 0

    def run(self):
        self.__tmpLog.info("Rank %s: Droid starts on %s" % (self.__rank, self.__hostname))
        if self.preCheck():
            self.__tmpLog.info("Rank %s: Droid failed preCheck, exit" % self.__rank)
            return 1

        while True:
            self.__tmpLog.info("Rank %s: Droid starts to run one job" % self.__rank)
            os.chdir(self.__globalWorkingDir)
            try:
                ret = self.runOneJob()
                if ret != 0:
                    self.__tmpLog.warning("Rank %s: Droid fails to run one job: ret - %s" % (self.__rank, ret))
                    break
            except:
                self.__tmpLog.warning("Rank %s: Droid throws exception when running one job: %s" % (self.__rank, traceback.format_exc()))
                break
            os.chdir(self.__globalWorkingDir)
            self.__tmpLog.info("Rank %s: Droid finishes to run one job" % self.__rank)
        self.finishDroid()
        return 0
            
    def stop(self, signum=None, frame=None):
        self.__tmpLog.info('Rank %s: stop signal received' % self.__rank)
        block_sig(signal.SIGTERM)
        self.__esJobManager.terminate()
        self.__esJobManager.flushMessages()
        self.updateOutputs(signal=True, final=True)

        self.__tmpLog.info("Rank %s: post exec job" % self.__rank)
        self.postExecJob()
        #self.__tmpLog.info("Rank %s: finish job" % self.__rank)
        #self.finishJob()

        self.__tmpLog.info('Rank %s: stop' % self.__rank)
        unblock_sig(signal.SIGTERM)

    def __del_not_use__(self):
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
        self.__esJobManager.flushMessages()
        self.updateOutputs(signal=True, final=True)

        self.__tmpLog.info("Rank %s: post exec job" % self.__rank)
        self.postExecJob()
        self.__tmpLog.info("Rank %s: finish job" % self.__rank)
        self.finishJob()

        self.__tmpLog.info('Rank %s: __del__ function' % self.__rank)

