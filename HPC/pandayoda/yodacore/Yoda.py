import commands
import datetime
import json
import logging
import os
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
    
    # constructor
    def __init__(self, globalWorkingDir, localWorkingDir, pilotJob=None, rank=None, nonMPIMode=False):
        threading.Thread.__init__(self)
        self.globalWorkingDir = globalWorkingDir
        self.localWorkingDir = localWorkingDir
        self.currentDir = None
        # communication channel
        self.comm = Interaction.Receiver(rank=rank, nonMPIMode=nonMPIMode)
        self.rank = self.comm.getRank()
        # database backend
        self.db = Database.Backend(self.globalWorkingDir)
        # logger
        self.tmpLog = Logger.Logger(filename='Yoda.log')
        self.tmpLog.info("Global working dir: %s" % self.globalWorkingDir)
        self.initWorkingDir()
        self.tmpLog.info("Current working dir: %s" % self.currentDir)
        self.failed_updates = []

        self.pilotJob = pilotJob

        self.jobs = []
        self.jobRanks = []
        self.readyEventRanges = []
        self.runningEventRanges = {}
        self.finishedEventRanges = []

        self.readyJobsEventRanges = {}
        self.runningJobsEventRanges = {}
        self.finishedJobsEventRanges = {}
        self.stagedOutJobsEventRanges = {}

        self.updateEventRangesToDBTime = None

        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGQUIT, self.stop)
        signal.signal(signal.SIGSEGV, self.stop)
        signal.signal(signal.SIGXCPU, self.stop)
        signal.signal(signal.SIGUSR1, self.stop)
        signal.signal(signal.SIGBUS, self.stop)

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
                if job['neededRanks'] not in neededRanks:
                    neededRanks[job['neededRanks']] = []
                neededRanks[job['neededRanks']].append(jobId)
            keys = neededRanks.keys()
            keys.sort()
            for key in keys:
                for jobId in neededRanks[key]:
                    for i in range(key):
                        self.jobRanks.append(jobId)
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
            numEvents = {}
            for jobId in self.readyJobsEventRanges:
                no = len(self.readyJobsEventRanges[jobId])
                if no not in numEvents:
                    numEvents[len] = []
                numEvents.append(jobId)
            keys = numEvents.keys()
            keys.sort(reverse=True)
            for key in keys:
                for jobId in numEvents[key]:
                    for i in range(key/100):
                        self.jobRanks.append(jobId)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to reschedule job ranks with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg

    # get job
    def getJob(self,params):
        rank = params['rank']
        job = None
        if len(self.jobRanks):
            jobId = self.jobRanks.pop(0)
            job = self.jobs[jobId]
        else:
            self.rescheduleJobRanks()
            if len(self.jobRanks):
                jobId = self.jobRanks.pop(0)
                job = self.jobs[jobId]

        res = {'StatusCode':0,
               'job': job}
        self.tmpLog.debug('res={0}'.format(str(res)))
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
        timeNow = datetime.datetime.utcnow()
        outFileName = str(jobId) + "_" + timeNow.strftime("%Y-%m-%d-%H-%M-%S-%f") + '.dump' + type
        etadataFileName = 'metadata-' + outFileName.split('.dump')[0] + '.xml'
        outFileName = os.path.join(self.globalWorkingDir, outFileName)
        metadataFileName = os.path.join(self.globalWorkingDir, etadataFileName)
        outFile = open(outFileName,'w')

        metafd = open(metadataFileName, "w")
        metafd.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        metafd.write("<!-- Edited By POOL -->\n")
        metafd.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        metafd.write("<POOLFILECATALOG>\n")

        for eventRangeID,status,output in outputs:
            outFile.write('{0} {1} {2} {3}\n'.format(jobId, eventRangeID,status,output))

            metafd.write('  <File EventRangeID="%s">\n' % (eventRangeID))
            metafd.write("    <physical>\n")
            metafd.write('      <pfn filetype="ROOT_All" name="%s"/>\n' % (output))
            metafd.write("    </physical>\n")
            metafd.write("  </File>\n")

        outFile.close()
        metafd.write("</POOLFILECATALOG>\n")
        metafd.close()

    def updateFinishedEventRangesToDB(self):
        try:
            self.tmpLog.debug('start to updateFinishedEventRangesToDB')

            for jobId in self.stagedOutJobsEventRanges:
                if len(self.stagedOutJobsEventRanges[jobId]):
                    self.dumpUpdates(jobId, self.stagedOutJobsEventRanges[jobId], type='.stagedOut')
                    #self.db.updateEventRanges(self.finishedEventRanges)
                    for i in self.stagedOutJobsEventRanges[jobId]:
                        self.stagedOutJobsEventRanges[jobId].remove(i)
                    self.stagedOutJobsEventRanges[jobId] = []

            for jobId in self.finishedJobsEventRanges:
                if len(self.finishedJobsEventRanges[jobId]):
                    self.dumpUpdates(jobId, self.finishedJobsEventRanges[jobId])
                    #self.db.updateEventRanges(self.finishedEventRanges)
                    for i in self.finishedJobsEventRanges[jobId]:
                        self.finishedJobsEventRanges[jobId].remove(i)
                    self.finishedJobsEventRanges[jobId] = []
            self.tmpLog.debug('finished to updateFinishedEventRangesToDB')
        except Exception as e:
            self.tmpLog.debug('updateFinishedEventRangesToDB failed: %s, %s' % (str(e), traceback.format_exc()))


    def updateEventRangesToDB(self, force=False, final=False):
        timeNow = time.time()
        # forced or first dump or enough interval
        if force or self.updateEventRangesToDBTime == None or \
            ((timeNow - self.updateEventRangesToDBTime) > 60 * 2):
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

    # main
    def run(self):
        # get logger
        self.tmpLog.info('start')
        # load job
        self.tmpLog.info('loading job')
        tmpStat,tmpOut = self.loadJobs()
        self.tmpLog.info("loading jobs: (status: %s, output: %s)" %(tmpStat, tmpOut))
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)

        self.tmpLog.info("init job ranks")
        tmpStat,tmpOut = self.initJobRanks()
        self.tmpLog.info("initJobRanks: (status: %s, output: %s)" %(tmpStat, tmpOut))
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)

        # make event table
        self.tmpLog.info('making JobsEventTable')
        tmpStat,tmpOut = self.makeJobsEventTable()
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)

        # main loop
        self.tmpLog.info('main loop')
        while self.comm.activeRanks():
            #self.injectEvents()
            # get request
            self.tmpLog.info('waiting requests')
            tmpStat,method,params = self.comm.receiveRequest()
            self.tmpLog.debug("received request: (rank: %s, status: %s, method: %s, params: %s)" %(self.comm.getRequesterRank(),tmpStat,method,params))
            if not tmpStat:
                self.tmpLog.error(method)
                sys.exit(1)
            # execute
            self.tmpLog.debug('rank={0} method={1} param={2}'.format(self.comm.getRequesterRank(),
                                                                method,str(params)))
            if hasattr(self,method):
                methodObj = getattr(self,method)
                apply(methodObj,[params])
            else:
                self.tmpLog.error('unknown method={0} was requested from rank={1} '.format(method,
                                                                                      self.comm.getRequesterRank()))
            # flush the updated event ranges to db
            self.updateEventRangesToDB(force=False)


        self.flushMessages()
        #self.updateFailedEventRanges()
        self.updateEventRangesToDB(force=True)
        # final dump
        #self.tmpLog.info('final dumping')
        #self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.finishDroids()
        self.tmpLog.info('done')


    def flushMessages(self):
        self.tmpLog.info('flush messages')

        while self.comm.activeRanks():
            # get request
            tmpStat,method,params = self.comm.receiveRequest()
            self.tmpLog.debug("received request: (rank: %s, status: %s, method: %s, params: %s)" %(self.comm.getRequesterRank(),tmpStat,method,params))
            if not tmpStat:
                self.tmpLog.error(method)
                sys.exit(1)
            # execute
            self.tmpLog.debug('rank={0} method={1} param={2}'.format(self.comm.getRequesterRank(),
                                                                method,str(params)))
            if hasattr(self,method):
                methodObj = getattr(self,method)
                apply(methodObj,[params])
            else:
                self.tmpLog.error('unknown method={0} was requested from rank={1} '.format(method, self.comm.getRequesterRank()))


    def stop(self, signum=None, frame=None):
        self.tmpLog.info('stop signal received')
        block_sig(signal.SIGTERM)
        #self.flushMessages()
        #self.updateFailedEventRanges()
        # final dump
        self.tmpLog.info('final dumping')
        self.updateEventRangesToDB(force=True, final=True)
        #self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.tmpLog.info('stop')
        unblock_sig(signal.SIGTERM)


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
