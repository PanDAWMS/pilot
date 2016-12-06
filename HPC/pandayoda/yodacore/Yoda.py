import commands
import datetime
import json
import logging
import math
import os
import shutil
import sys
import time
import traceback
import threading
import pickle
import signal
from os.path import abspath as _abspath, join as _join

# logging.basicConfig(filename='Yoda.log', level=logging.DEBUG)

import Interaction,Database,Logger
from signal_block.signal_block import block_sig, unblock_sig
#from HPC import EventServer

# main Yoda class
class Yoda(threading.Thread):
    class HelperThread(threading.Thread):
        def __init__(self, logger, helperFunc, **kwds):
            threading.Thread.__init__(self, **kwds)
            self.__log = logger
            self.__func = helperFunc
            self._stop = threading.Event()
            self.__log.debug("HelperThread initialized.")

        def stop(self):
            self._stop.set()

        def stopped(self):
            return self._stop.isSet()

        def run(self):
            try:
                exec_time = None
                while True:
                    if self.stopped():
                        break
                    if exec_time is None or exec_time < time.time() - 60:
                        self.__func()
                        exec_time = time.time()
                    time.sleep(1)
            except:
                self.__log.debug("Exception: HelperThread failed: %s" % traceback.format_exc())


    # constructor
    def __init__(self, globalWorkingDir, localWorkingDir, pilotJob=None, rank=None, nonMPIMode=False, outputDir=None, dumpEventOutputs=False):
        threading.Thread.__init__(self)
        self.globalWorkingDir = globalWorkingDir
        self.localWorkingDir = localWorkingDir
        self.currentDir = None
        # database backend
        self.db = Database.Backend(self.globalWorkingDir)
        # logger
        self.tmpLog = Logger.Logger(filename='Yoda.log')

        # communication channel
        self.comm = Interaction.Receiver(rank=rank, nonMPIMode=nonMPIMode, logger=self.tmpLog)
        self.rank = self.comm.getRank()

        self.tmpLog.info("Global working dir: %s" % self.globalWorkingDir)
        self.initWorkingDir()
        self.tmpLog.info("Current working dir: %s" % self.currentDir)
        self.failed_updates = []
        self.outputDir = outputDir
        self.dumpEventOutputs = dumpEventOutputs

        self.pilotJob = pilotJob

        self.cores = 10
        self.jobs = []

        # jobs which needs more than one rank
        self.jobRanks = []
        self.totalJobRanks = 0
        # jobs which needs less than one rank
        self.jobRanksSmallPiece = []
        self.totalJobRanksSmallPiece = 0
        self.rankJobsTries = {}

        # scheduler policy:
        self.bigJobFirst = True
        self.lastRankForBigJobFirst = int(self.getTotalRanks() * 0.9)

        self.readyEventRanges = []
        self.runningEventRanges = {}
        self.finishedEventRanges = []

        self.readyJobsEventRanges = {}
        self.runningJobsEventRanges = {}
        self.finishedJobsEventRanges = {}
        self.stagedOutJobsEventRanges = {}

        self.updateEventRangesToDBTime = None

        self.jobMetrics = {}
        self.jobsTimestamp = {}
        self.jobsRuningRanks = {}

        self.originSigHandler = {}
        for sig in [signal.SIGTERM, signal.SIGQUIT, signal.SIGSEGV, signal.SIGXCPU, signal.SIGUSR1, signal.SIGBUS]:
            self.originSigHandler[sig] = signal.getsignal(sig)
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGQUIT, self.stop)
        signal.signal(signal.SIGSEGV, self.stop)
        signal.signal(signal.SIGXCPU, self.stopYoda)
        signal.signal(signal.SIGUSR1, self.stopYoda)
        signal.signal(signal.SIGBUS, self.stopYoda)

    def getTotalRanks(self):
        return self.comm.getTotalRanks()

    def initWorkingDir(self):
        # Create separate working directory for each rank
        curdir = _abspath (self.localWorkingDir)
        wkdirname = "rank_%s" % str(self.rank)
        wkdir  = _abspath (_join(curdir,wkdirname))
        if not os.path.exists(wkdir):
             os.makedirs (wkdir)
        os.chdir (wkdir)
        self.currentDir = wkdir

    def postExecJob(self):
        if self.globalWorkingDir != self.localWorkingDir:
            command = "mv " + self.currentDir + " " + self.globalWorkingDir
            self.tmpLog.debug("Rank %s: copy files from local working directory to global working dir(cmd: %s)" % (self.rank, command))
            status, output = commands.getstatusoutput(command)
            self.tmpLog.debug("Rank %s: (status: %s, output: %s)" % (self.rank, status, output))

    # setup 
    def setupJob(self, job):
        try:
            self.job = job
            jobFile = os.path.join(self.globalWorkingDir, 'HPCJob.json')
            tmpFile = open(jobFile, "w")
            #pickle.dump(self.job, tmpFile)
            json.dump(self.job, tmpFile)
            return True, None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to dump job with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg


    # load job
    def loadJob(self):
        try:
            # load job
            tmpFile = open(os.path.join(self.globalWorkingDir, 'HPCJob.json'))
            #self.job = pickle.load(tmpFile)
            self.job = json.load(tmpFile)
            tmpFile.close()
            return True,self.job
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to load job with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg


    # load jobs
    def loadJobs(self):
        try:
            # load jobs
            tmpFile = open(os.path.join(self.globalWorkingDir, 'HPCJobs.json'))
            #self.job = pickle.load(tmpFile)
            self.jobs = json.load(tmpFile)
            tmpFile.close()
            return True,self.jobs
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to load job with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg


    # init job ranks
    def initJobRanks(self):
        try:
            # sort by needed ranks
            neededRanks = {}
            for jobId in self.jobs:
                job = self.jobs[jobId]
                try:
                    self.cores = int(job.get('ATHENA_PROC_NUMBER', 10))
                    if self.cores < 1:
                        self.cores = 10
                except:
                     self.tmpLog.debug("Rank %s: failed to get core count" % (self.rank, traceback.format_exc()))
                if job['neededRanks'] not in neededRanks:
                    neededRanks[job['neededRanks']] = []
                neededRanks[job['neededRanks']].append(jobId)
            keys = neededRanks.keys()
            keys.sort(reverse=True)
            for key in keys:
                self.tmpLog.debug("Rank %s: Needed ranks %s" % (self.rank, key))

                if key < 1:
                    for jobId in neededRanks[key]:
                        self.tmpLog.debug("Rank %s: Adding %s to small piece queue" % (self.rank, jobId))
                        self.totalJobRanksSmallPiece += key
                        self.jobRanksSmallPiece.append(jobId)
                else:
                    for jobId in neededRanks[key]:            
                        # for i in range(int(math.ceil(key))):
                        for i in range(int(key)):
                            self.tmpLog.debug("Rank %s: Adding %s to full rank queue" % (self.rank, jobId))
                            self.jobRanks.append(jobId)
            self.totalJobRanks = len(self.jobRanks)
            self.tmpLog.debug("Rank %s: Jobs in small piece queue(one job is not enough to take the full rank) %s, total needed ranks %s" % (self.rank, self.jobRanksSmallPiece, self.totalJobRanksSmallPiece))
            self.tmpLog.debug("Rank %s: Jobs in full rank queue(one job is long enough to take the full rank) %s, total needed ranks %s" % (self.rank, self.jobRanks, self.totalJobRanks))
            return True,self.jobRanks
        except:
            self.tmpLog.debug("Rank %s: %s" % (self.rank, traceback.format_exc()))
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to load job with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg


    def createEventTable(self):
        try:
            self.db.createEventTable()
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to create event table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    def insertEventRanges(self, eventRanges):
        try:
            self.db.insertEventRanges(eventRanges)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to insert event range to table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    def insertJobsEventRanges(self, eventRanges):
        try:
            self.db.insertJobsEventRanges(eventRanges)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to insert event range to table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    # make event table
    def makeEventTable(self):
        try:
            # load event ranges
            tmpFile = open(os.path.join(self.globalWorkingDir, 'EventRanges.json'))
            eventRangeList = json.load(tmpFile)
            tmpFile.close()
            # setup database
            self.db.setupEventTable(self.job,eventRangeList)
            self.readyJobsEventRanges = eventRangeList
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to make event table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    # make event table
    def makeJobsEventTable(self):
        try:
            # load event ranges
            tmpFile = open(os.path.join(self.globalWorkingDir, 'JobsEventRanges.json'))
            eventRangeList = json.load(tmpFile)
            tmpFile.close()
            # setup database
            # self.db.setupJobsEventTable(self.jobs,eventRangeList)
            self.readyJobsEventRanges = eventRangeList
            for jobId in self.readyJobsEventRanges:
                self.runningJobsEventRanges[jobId] = {}
                self.finishedJobsEventRanges[jobId] = []
                self.stagedOutJobsEventRanges[jobId] = []
            return True,None
        except:
            self.tmpLog.debug("Rank %s: %s" % (self.rank, traceback.format_exc()))
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to make event table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg
        
    def printEventStatus(self):
        try:
            for jobId in self.jobs:
                job = self.jobs[jobId]
                neededRanks = job['neededRanks']
                readyEvents = len(self.readyJobsEventRanges[jobId]) if jobId in self.readyJobsEventRanges else 0
                self.tmpLog.debug("Rank %s: Job %s has %s events, needs %s ranks" % (self.rank, jobId, readyEvents, neededRanks))
            self.tmpLog.debug("Rank %s: Job full rank queue: %s" % (self.rank, self.jobRanks))
            self.tmpLog.debug("Rank %s: Job small piece queue: %s" % (self.rank, self.jobRanksSmallPiece))
            return True, None
        except:
            self.tmpLog.debug("Rank %s: %s" % (self.rank, traceback.format_exc()))
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to make event table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    # inject more events
    def injectEvents(self):
        try:
            # scan new event ranges json files
            all_files = os.listdir(self.globalWorkingDir)
            for file in all_files:
                if file != 'EventRanges.json' and file.endswith("EventRanges.json"):
                    tmpFile = open(os.path.join(self.globalWorkingDir, file))
                    eventRangeList = json.load(tmpFile)
                    tmpFile.close()
                    self.insertEventRanges(eventRangeList)
                    for eventRange in eventRangeList:
                        self.readyEventRanges.append(eventRange)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to inject more event range to table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg


    # inject more events
    def injectJobsEvents(self):
        try:
            # scan new event ranges json files
            all_files = os.listdir(self.globalWorkingDir)
            for file in all_files:
                if file != 'JobsEventRanges.json' and file.endswith("JobsEventRanges.json"):
                    tmpFile = open(os.path.join(self.globalWorkingDir, file))
                    eventRangeList = json.load(tmpFile)
                    tmpFile.close()
                    self.insertJobsEventRanges(eventRangeList)
                    for jobId in eventRangeList:
                        for eventRange in eventRangeList[jobId]:
                            self.readyJobsEventRanges[jobId].append(eventRange)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to inject more event range to table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    def rescheduleJobRanks(self):
        try:
            self.tmpLog.debug("Rank %s: rescheduleJobRanks" % (self.rank))
            numEvents = {}
            for jobId in self.readyJobsEventRanges:
                no = len(self.readyJobsEventRanges[jobId])
                self.tmpLog.debug("Rank %s: Job %s ready events %s"  % (self.rank, jobId, no))
                if no not in numEvents:
                    numEvents[len] = []
                numEvents.append(jobId)
            keys = numEvents.keys()
            keys.sort(reverse=True)
            for key in keys:
                if key < self.cores * 2:
                    continue
                for jobId in numEvents[key]:
                    #for i in range(key/self.cores):
                    self.tmpLog.debug("Rank %s: Adding job %s to small piece queue"  % (self.rank, jobId))
                    self.jobRanksSmallPiece.append(jobId)

            self.tmpLog.debug("Rank %s: Jobs in small piece queue(one job is not enough to take the full rank) %s" % (self.rank, self.jobRanksSmallPiece))
            self.tmpLog.debug("Rank %s: Jobs in full rank queue(one job is long enough to take the full rank, should be empty if reaching here) %s" % (self.rank, self.jobRanks))

            self.printEventStatus()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to reschedule job ranks with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    # get job
    def getJobScheduler(self,params):
        rank = params['rank']
        job = None
        jobId = None
        if int(rank) <= self.lastRankForBigJobFirst:
            self.tmpLog.debug("Rank %s: Big jobs first for rank %s(<=%s the last rank for big job first)"  % (self.rank, rank, self.lastRankForBigJobFirst))
            while len(self.jobRanks):
                jobId = self.jobRanks.pop(0)
                if rank in self.rankJobsTries and jobId in self.rankJobsTries[rank]:
                    self.tmpLog.debug("Rank %s: Job %s already tried on rank %s, will not scheduled to it again."  % (self.rank, jobId, rank))
                    continue
                if len(self.readyJobsEventRanges[jobId]) > 0:
                    job = self.jobs[jobId]
                    break
            if job is None:
                self.tmpLog.debug("Rank %s: no available jobs in full rank queue, try to get job from small piece queue"  % (self.rank))
                while len(self.jobRanksSmallPiece):
                    jobId = self.jobRanksSmallPiece.pop(0)
                    if rank in self.rankJobsTries and jobId in self.rankJobsTries[rank]:
                        self.tmpLog.debug("Rank %s: Job %s already tried on rank %s, will not scheduled to it again."  % (self.rank, jobId, rank))
                        continue
                    if len(self.readyJobsEventRanges[jobId]) > 0:
                        job = self.jobs[jobId]
                        break
        else:
            self.tmpLog.debug("Rank %s: Small jobs first for rank %s(>%s the last rank for big job first)"  % (self.rank, rank, self.lastRankForBigJobFirst))
            while len(self.jobRanksSmallPiece):
                jobId = self.jobRanksSmallPiece.pop()
                if rank in self.rankJobsTries and jobId in self.rankJobsTries[rank]:
                    self.tmpLog.debug("Rank %s: Job %s already tried on rank %s, will not scheduled to it again."  % (self.rank, jobId, rank))
                    continue
                if len(self.readyJobsEventRanges[jobId]) > 0:
                    job = self.jobs[jobId]
                    break
            if job is None:
                while len(self.jobRanks):
                    jobId = self.jobRanks.pop()
                    if rank in self.rankJobsTries and jobId in self.rankJobsTries[rank]:
                        self.tmpLog.debug("Rank %s: Job %s already tried on rank %s, will not scheduled to it again."  % (self.rank, jobId, rank))
                        continue
                    if len(self.readyJobsEventRanges[jobId]) > 0:
                        job = self.jobs[jobId]
                        break

        return jobId, job

    # get job
    def getJob(self,params):
        rank = params['rank']
        jobId, job = self.getJobScheduler(params)
        if job is None:
            ##### not disable reschedule job ranks, it will split jobs to additional ranks
            ##### instead, pilot will download more events then expected
            self.rescheduleJobRanks()
            jobId, job = self.getJobScheduler(params)

        res = {'StatusCode':0,
               'job': job}
        self.tmpLog.debug('res={0}'.format(str(res)))

        if jobId:
            if jobId not in self.jobsRuningRanks:
                self.jobsRuningRanks[jobId] = []
            self.jobsRuningRanks[jobId].append(rank)
            if jobId not in self.jobsTimestamp:
                self.jobsTimestamp[jobId] = {'startTime': time.time(), 'endTime': None}
            if rank not in self.rankJobsTries:
                self.rankJobsTries[rank] = []
            self.rankJobsTries[rank].append(jobId)

        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')


    # update job
    def updateJob(self,params):
        # final heartbeat
        if params['state'] in ['finished','failed']:
            # self.comm.decrementNumRank()
            pass
        # make response
        res = {'StatusCode':0,
               'command':'NULL'}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')


    # finish job
    def finishJob(self,params):
        # final heartbeat
        jobId = params['jobId']
        rank = params['rank']
        if params['state'] in ['finished','failed']:
            # self.comm.decrementNumRank()
            self.jobsRuningRanks[jobId].remove(rank)
            endTime = time.time()
            if self.jobsTimestamp[params['jobId']]['endTime'] is None or self.jobsTimestamp[params['jobId']]['endTime'] < endTime:
                self.jobsTimestamp[params['jobId']]['endTime'] = endTime
        # make response
        res = {'StatusCode':0,
               'command':'NULL'}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')


    # finish droid
    def finishDroid(self,params):
        # final heartbeat
        if params['state'] in ['finished','failed']:
            self.comm.decrementNumRank()
        # make response
        res = {'StatusCode':0,
               'command':'NULL'}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')
        

    # get event ranges
    def getEventRanges_old(self,params):
        # number of event ranges
        if 'nRanges' in params:
            nRanges = int(params['nRanges'])
        else:
            nRanges = 1
        # get event ranges from DB
        try:
            eventRanges = self.db.getEventRanges(nRanges)
        except Exception as e:
            self.tmpLog.debug('db.getEventRanges failed: %s' % str(e))
            res = {'StatusCode':-1,
                   'eventRanges':None}
        else:
            # make response
            res = {'StatusCode':0,
                   'eventRanges':eventRanges}
        # return response
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        # dump updated records
        try:
            self.db.dumpUpdates()
        except Exception as e:
            self.tmpLog.debug('db.dumpUpdates failed: %s' % str(e))


    # get event ranges
    def getEventRanges(self,params):
        jobId = params['jobId']
        # number of event ranges
        if 'nRanges' in params:
            nRanges = int(params['nRanges'])
        else:
            nRanges = 1
        eventRanges = []
        try:
            for i in range(nRanges):
                if len(self.readyJobsEventRanges[jobId]) > 0:
                    eventRange = self.readyJobsEventRanges[jobId].pop(0)
                    eventRanges.append(eventRange)
                    self.runningJobsEventRanges[jobId][eventRange['eventRangeID']] = eventRange
                else:
                    break
        except:
            self.tmpLog.warning("Failed to get event ranges: %s" % traceback.format_exc())
            print self.readyJobsEventRanges
            print self.runningJobsEventRanges

        # make response
        res = {'StatusCode':0,
               'eventRanges':eventRanges}
        # return response
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')


    # update event range
    def updateEventRange_old(self,params):
        # extract parameters
        eventRangeID = params['eventRangeID']
        eventStatus = params['eventStatus']
        output = params['output']
        # update database
        try:
            self.db.updateEventRange(eventRangeID,eventStatus, output)
        except Exception as e:
            self.tmpLog.debug('db.updateEventRange failed: %s' % str(e))
            self.failed_updates.append([eventRangeID,eventStatus, output])
        # make response
        res = {'StatusCode':0}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        # dump updated records
        try:
            self.db.dumpUpdates()
        except Exception as e:
            self.tmpLog.debug('db.dumpUpdates failed: %s' % str(e))


    # update event range
    def updateEventRange(self,params):
        # extract parameters
        jobId = params['jobId']
        eventRangeID = params['eventRangeID']
        eventStatus = params['eventStatus']
        output = params['output']

        if eventRangeID in self.runningJobsEventRanges[jobId]:
            # eventRange = self.runningEventRanges[eventRangeID]
            del self.runningJobsEventRanges[jobId][eventRangeID]
        if eventStatus == 'stagedOut':
            self.stagedOutJobsEventRanges[jobId].append((eventRangeID, eventStatus, output))
        else:
            self.finishedJobsEventRanges[jobId].append((eventRangeID, eventStatus, output))

        # make response
        res = {'StatusCode':0}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')


    # update event ranges
    def updateEventRanges(self,params):
        for param in params:
            # extract parameters
            jobId = param['jobId']
            eventRangeID = param['eventRangeID']
            eventStatus = param['eventStatus']
            output = param['output']

            if eventRangeID in self.runningJobsEventRanges[jobId]:
                # eventRange = self.runningEventRanges[eventRangeID]
                del self.runningJobsEventRanges[jobId][eventRangeID]
            if eventStatus == 'stagedOut':
                self.stagedOutJobsEventRanges[jobId].append((eventRangeID, eventStatus, output))
            else:
                self.finishedJobsEventRanges[jobId].append((eventRangeID, eventStatus, output))

        # make response
        res = {'StatusCode':0}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')


    def updateFailedEventRanges(self):
        for failed_update in self.failed_updates:
            eventRangeID,eventStatus, output = failed_update
            try:
                self.db.updateEventRange(eventRangeID,eventStatus, output)
            except Exception as e:
                self.tmpLog.debug('db.updateEventRange failed: %s' % str(e))


    def updateRunningEventRangesToDB(self):
        try:
            runningEvents = []
            for jobId in self.runningJobsEventRanges:
                for eventRangeID in self.runningJobsEventRanges[jobId]:
                    # self.tmpLog.debug(self.runningEventRanges[eventRangeID])
                    status = 'running'
                    output = None
                    runningEvents.append((eventRangeID, status, output))
            if len(runningEvents):
                self.db.updateEventRanges(runningEvents)
        except Exception as e:
            self.tmpLog.debug('updateRunningEventRangesToDB failed: %s, %s' % (str(e), traceback.format_exc()))

    def dumpUpdates(self, jobId, outputs, type=''):
        #if self.dumpEventOutputs == False:
        #    return
        timeNow = datetime.datetime.utcnow()
        #outFileName = str(jobId) + "_" + timeNow.strftime("%Y-%m-%d-%H-%M-%S-%f") + '.dump' + type
        outFileName = str(jobId) + "_event_status.dump" + type
        outFileName = os.path.join(self.globalWorkingDir, outFileName)
        outFile = open(outFileName + ".new", 'w')
        self.tmpLog.debug("dumpUpdates: dumpFileName %s" % (outFileName))

        metadataFileName = None
        metafd = None
        # if self.dumpEventOutputs:
        if True:
            metadataFileName = 'metadata-' + os.path.basename(outFileName).split('.dump')[0] + '.xml'
            if self.outputDir:
                metadataFileName = os.path.join(self.outputDir, metadataFileName)
            else:
                metadataFileName = os.path.join(self.globalWorkingDir, metadataFileName)


            self.tmpLog.debug("dumpUpdates: outputDir %s, metadataFileName %s" % (self.outputDir, metadataFileName))
            metafd = open(metadataFileName + ".new", "w")
            metafd.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
            metafd.write("<!-- Edited By POOL -->\n")
            metafd.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
            metafd.write("<POOLFILECATALOG>\n")

        for eventRangeID,status,output in outputs:
            outFile.write('{0} {1} {2} {3}\n'.format(str(jobId), str(eventRangeID), str(status), str(output)))

            if status.startswith("ERR"):
                status = 'failed'

            if metafd:
                metafd.write('  <File EventRangeID="%s" Status="%s">\n' % (eventRangeID, status))
                metafd.write("    <physical>\n")
                if isinstance(output, (list, tuple)):
                    for output1 in output:
                        metafd.write('      <pfn filetype="ROOT_All" name="%s"/>\n' % (str(output1)))
                else:
                    for output1 in output.split(",")[:-3]:
                        metafd.write('      <pfn filetype="ROOT_All" name="%s"/>\n' % (str(output1)))
                metafd.write("    </physical>\n")
                metafd.write("  </File>\n")

        outFile.close()
        if metafd:
            metafd.write("</POOLFILECATALOG>\n")
            metafd.close()

        # mv the new file to overwrite the current one
        command = "mv %s.new %s" % (outFileName, outFileName)
        retS, retOut = commands.getstatusoutput(command)
        if retS:
            self.tmpLog.debug('Failed to execute %s: %s' % (command, retOut))

        if metadataFileName:
            command = "mv %s.new %s" % (metadataFileName, metadataFileName)
            retS, retOut = commands.getstatusoutput(command)
            if retS:
                self.tmpLog.debug('Failed to execute %s: %s' % (command, retOut))

    def updateFinishedEventRangesToDB(self):
        try:
            self.tmpLog.debug('start to updateFinishedEventRangesToDB')

            for jobId in self.stagedOutJobsEventRanges:
                if len(self.stagedOutJobsEventRanges[jobId]):
                    self.dumpUpdates(jobId, self.stagedOutJobsEventRanges[jobId], type='.stagedOut')
                    #for i in self.stagedOutJobsEventRanges[jobId]:
                    #    self.stagedOutJobsEventRanges[jobId].remove(i)
                    #self.stagedOutJobsEventRanges[jobId] = []

            for jobId in self.finishedJobsEventRanges:
                if len(self.finishedJobsEventRanges[jobId]):
                    self.dumpUpdates(jobId, self.finishedJobsEventRanges[jobId])
                    #self.db.updateEventRanges(self.finishedEventRanges)
                    #for i in self.finishedJobsEventRanges[jobId]:
                    #    self.finishedJobsEventRanges[jobId].remove(i)
                    #self.finishedJobsEventRanges[jobId] = []
            self.tmpLog.debug('finished to updateFinishedEventRangesToDB')
        except Exception as e:
            self.tmpLog.debug('updateFinishedEventRangesToDB failed: %s, %s' % (str(e), traceback.format_exc()))


    def updateEventRangesToDB(self, force=False, final=False):
        timeNow = time.time()
        # forced or first dump or enough interval
        if force or self.updateEventRangesToDBTime == None or \
            ((timeNow - self.updateEventRangesToDBTime) > 60 * 5):
            self.tmpLog.debug('start to updateEventRangesToDB')
            self.updateEventRangesToDBTime = time.time()
            #if not final:
            #    self.updateRunningEventRangesToDB()
            self.updateFinishedEventRangesToDB()
            self.tmpLog.debug('finished to updateEventRangesToDB')


    def finishDroids(self):
        self.tmpLog.debug('finish Droids')
        # make message
        res = {'StatusCode':0, 'State': 'finished'}
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.sendMessage(res)
        #self.comm.disconnect()

    def collectMetrics(self, ranks):
        metrics = {}
        metricsReport = {}
        for rank in ranks.keys():
            for key in ranks[rank].keys():
                if key in ["setupTime", "runningTime", 'totalTime', "cores", "queuedEvents", "processedEvents", "cpuConsumptionTime", 'avgTimePerEvent']:
                    if key not in metrics:
                        metrics[key] = 0
                    metrics[key] += ranks[rank][key]

        setupTime = []
        runningTime = []
        totalTime = []
        stageoutTime = []
        for rank in ranks.keys():
            setupTime.append(ranks[rank]['setupTime'])
            runningTime.append(ranks[rank]['runningTime'])
            totalTime.append(ranks[rank]['totalTime'])
            stageoutTime.append(ranks[rank]['totalTime'] - ranks[rank]['setupTime'] - ranks[rank]['runningTime'])
        num_ranks = len(ranks.keys())
        if num_ranks < 1:
            num_ranks = 1
        processedEvents = metrics['processedEvents']
        if processedEvents < 1:
            processedEvents = 1

        metricsReport['avgYodaSetupTime'] = metrics['setupTime']/num_ranks
        metricsReport['avgYodaRunningTime'] = metrics['runningTime']/num_ranks
        metricsReport['avgYodaStageoutTime'] = (metrics['totalTime'] - metrics['setupTime'] - metrics['runningTime'])/num_ranks
        metricsReport['avgYodaTotalTime'] = metrics['totalTime']/num_ranks
        metricsReport['maxYodaSetupTime'] = max(setupTime)
        metricsReport['maxYodaRunningTime'] = max(runningTime)
        metricsReport['maxYodaStageoutTime'] = max(stageoutTime)
        metricsReport['maxYodaTotalTime'] = max(totalTime)
        metricsReport['minYodaSetupTime'] = min(setupTime)
        metricsReport['minYodaRunningTime'] = min(runningTime)
        metricsReport['minYodaStageoutTime'] = min(stageoutTime)
        metricsReport['minYodaTotalTime'] = min(totalTime)
        metricsReport['cores'] = metrics['cores']
        metricsReport['cpuConsumptionTime'] = metrics['cpuConsumptionTime']
        metricsReport['totalQueuedEvents'] = metrics['queuedEvents']
        metricsReport['totalProcessedEvents'] = metrics['processedEvents']
        metricsReport['avgTimePerEvent'] = metrics['avgTimePerEvent']/ num_ranks

        for key in metricsReport:
            metricsReport[key] = int(metricsReport[key])
        return metricsReport

    def heartbeat(self, params):
        """
        {"jobId": , "rank": , "startTime": ,"readyTime": , "endTime": , "setupTime": , "totalTime": , "cores": , "processCPUHour": , "totalCPUHour": , "queuedEvents": , "processedEvents": , "cpuConsumptionTime": }
        """
        self.tmpLog.debug('heartbeat')
        jobId = params['jobId']
        rank = params['rank']

        # make response
        res = {'StatusCode':0}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        self.tmpLog.debug('return response')

        if jobId not in self.jobMetrics:
            self.jobMetrics[jobId] = {'ranks': {}, 'collect': {}}

        self.jobMetrics[jobId]['ranks'][rank] = params
        self.jobMetrics[jobId]['collect'] = self.collectMetrics(self.jobMetrics[jobId]['ranks'])

        #self.dumpJobMetrics()


    def dumpJobMetrics(self):
        jobMetricsFileName = "jobMetrics-yoda.json"
        try:
            #outputDir = self.jobs[jobId]["GlobalWorkingDir"]
            outputDir = self.globalWorkingDir
        except:
            self.tmpLog.debug("Failed to get job's global working dir: %s" % (traceback.format_exc()))
            outputDir = self.globalWorkingDir
        jobMetrics = os.path.join(outputDir, jobMetricsFileName)
        self.tmpLog.debug("JobMetrics file: %s" % (jobMetrics + ".new"))
        tmpFile = open(jobMetrics+ ".new", "w")
        json.dump(self.jobMetrics, tmpFile)
        tmpFile.close()

        command = "mv %s.new %s" % (jobMetrics, jobMetrics)
        retS, retOut = commands.getstatusoutput(command)
        if retS:
            self.tmpLog.debug('Failed to execute %s: %s' % (command, retOut))

    def dumpJobsStartTime(self):
        jobsTimestampFileName = "jobsTimestamp-yoda.json"
        outputDir = self.globalWorkingDir
        jobsTimestampFile = os.path.join(outputDir, jobsTimestampFileName)
        self.tmpLog.debug("JobsStartTime file: %s" % (jobsTimestampFile + ".new"))
        tmpFile = open(jobsTimestampFile + ".new", "w")
        json.dump(self.jobsTimestamp, tmpFile)
        tmpFile.close()

        command = "mv %s.new %s" % (jobsTimestampFile, jobsTimestampFile)
        retS, retOut = commands.getstatusoutput(command)
        if retS:
            self.tmpLog.debug('Failed to execute %s: %s' % (command, retOut))

    def helperFunction(self):
        # flush the updated event ranges to db
        self.updateEventRangesToDB(force=True)

        self.dumpJobMetrics()
        self.dumpJobsStartTime()

    # main yoda
    def runYoda(self):
        # get logger
        self.tmpLog.info('start')
        # load job
        self.tmpLog.info('loading job')
        tmpStat,tmpOut = self.loadJobs()
        self.tmpLog.info("loading jobs: (status: %s, output: %s)" %(tmpStat, tmpOut))
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            raise Exception(tmpOut)

        self.tmpLog.info("init job ranks")
        tmpStat,tmpOut = self.initJobRanks()
        self.tmpLog.info("initJobRanks: (status: %s, output: %s)" %(tmpStat, tmpOut))
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            raise Exception(tmpOut)

        # make event table
        self.tmpLog.info('making JobsEventTable')
        tmpStat,tmpOut = self.makeJobsEventTable()
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            raise Exception(tmpOut)

        # print event status
        self.tmpLog.info('print event status')
        tmpStat,tmpOut = self.printEventStatus()

        self.tmpLog.info('Initialize Helper thread')
        helperThread = Yoda.HelperThread(self.tmpLog, self.helperFunction)
        helperThread.start()

        # main loop
        self.tmpLog.info('main loop')
        time_dupmJobMetrics = time.time()
        while self.comm.activeRanks():
            #self.injectEvents()
            # get request
            self.tmpLog.info('waiting requests')
            tmpStat,method,params = self.comm.receiveRequest()
            self.tmpLog.debug("received request: (rank: %s, status: %s, method: %s, params: %s)" %(self.comm.getRequesterRank(),tmpStat,method,params))
            if not tmpStat:
                self.tmpLog.error(method)
                raise Exception(method)
            # execute
            self.tmpLog.debug('rank={0} method={1} param={2}'.format(self.comm.getRequesterRank(),
                                                                method,str(params)))
            if hasattr(self,method):
                methodObj = getattr(self,method)
                try:
                    apply(methodObj,[params])
                except:
                    self.tmpLog.debug("Failed to run function %s: %s" % (method, traceback.format_exc()))
            else:
                self.tmpLog.error('unknown method={0} was requested from rank={1} '.format(method,
                                                                                      self.comm.getRequesterRank()))
        helperThread.stop()
        self.flushMessages()
        #self.updateFailedEventRanges()
        self.updateEventRangesToDB(force=True)
        self.dumpJobMetrics()
        self.dumpJobsStartTime()
        # final dump
        #self.tmpLog.info('final dumping')
        #self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.finishDroids()
        self.tmpLog.info('done')
        

    # main
    def run(self):
        try:
            self.runYoda()
        except:
            self.tmpLog.info("Excpetion to run Yoda: %s" % traceback.format_exc())
            raise

    def flushMessages(self):
        self.tmpLog.info('flush messages')

        while self.comm.activeRanks():
            # get request
            tmpStat,method,params = self.comm.receiveRequest()
            self.tmpLog.debug("received request: (rank: %s, status: %s, method: %s, params: %s)" %(self.comm.getRequesterRank(),tmpStat,method,params))
            if not tmpStat:
                self.tmpLog.error(method)
                raise Exception(method)
            # execute
            self.tmpLog.debug('rank={0} method={1} param={2}'.format(self.comm.getRequesterRank(),
                                                                method,str(params)))
            if hasattr(self,method):
                methodObj = getattr(self,method)
                apply(methodObj,[params])
            else:
                self.tmpLog.error('unknown method={0} was requested from rank={1} '.format(method, self.comm.getRequesterRank()))


    def stopYoda(self, signum=None, frame=None):
        self.tmpLog.info('stopYoda signal %s received' % signum)
        #signal.signal(signum, self.originSigHandler[signum])
        # make message
        res = {'StatusCode':0, 'State': 'signal', 'signum': signum}
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.sendMessage(res)

        self.dumpJobMetrics()
        for jobId in self.jobsTimestamp:
            if self.jobsTimestamp[jobId]['endTime'] is None:
                self.jobsTimestamp[jobId]['endTime'] = time.time()
            if len(self.jobsRuningRanks[jobId]) > 0:
                self.jobsTimestamp[jobId]['endTime'] = time.time()
        self.dumpJobsStartTime()
        self.updateEventRangesToDB(force=True, final=True)

    def stop(self, signum=None, frame=None):
        self.tmpLog.info('stop signal %s received' % signum)
        block_sig(signum)
        signal.siginterrupt(signum, False)
        self.dumpJobMetrics()
        for jobId in self.jobsTimestamp:
            if self.jobsTimestamp[jobId]['endTime'] is None:
                self.jobsTimestamp[jobId]['endTime'] = time.time()
            if len(self.jobsRuningRanks[jobId]) > 0:
                self.jobsTimestamp[jobId]['endTime'] = time.time()
        self.dumpJobsStartTime()
        #self.flushMessages()
        #self.updateFailedEventRanges()
        # final dump
        self.tmpLog.info('final dumping')
        self.updateEventRangesToDB(force=True, final=True)
        #self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.tmpLog.info('stop')
        #signal.siginterrupt(signum, True)
        unblock_sig(signum)

    def getOutputs(self):
        pass


    def __del_not_use__(self):
        self.tmpLog.info('__del__ function')
        self.flushMessages()
        self.updateFailedEventRanges()
        # final dump
        self.tmpLog.info('final dumping')
        self.updateEventRangesToDB(force=True, final=True)
        #self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.tmpLog.info('__del__ function')
