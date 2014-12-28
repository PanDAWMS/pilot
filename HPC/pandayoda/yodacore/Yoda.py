import commands
import json
import os
import sys
import pickle
import signal
from os.path import abspath as _abspath, join as _join
import Interaction,Database,Logger

# main Yoda class
class Yoda:
    
    # constructor
    def __init__(self, globalWorkingDir, localWorkingDir):
        self.globalWorkingDir = globalWorkingDir
        self.localWorkingDir = localWorkingDir
        self.currentDir = None
        # communication channel
        self.comm = Interaction.Receiver()
        self.rank = self.comm.getRank()
        # database backend
        self.db = Database.Backend(self.globalWorkingDir)
        # logger
        self.tmpLog = Logger.Logger()
        self.tmpLog.info("Global working dir: %s" % self.globalWorkingDir)
        self.initWorkingDir()
        self.tmpLog.info("Current working dir: %s" % self.currentDir)
        self.failed_updates = []

        signal.signal(signal.SIGTERM, self.stop)

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

    # make event table
    def makeEventTable(self):
        try:
            # load event ranges
            tmpFile = open(os.path.join(self.globalWorkingDir, 'EventRanges.json'))
            eventRangeList = json.load(tmpFile)
            tmpFile.close()
            # setup database
            self.db.setupEventTable(self.job,eventRangeList)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to make event table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg
        


    # get job
    def getJob(self,params):
        res = {'StatusCode':0,
               'job': self.job}
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)



    # update job
    def updateJob(self,params):
        # final heartbeat
        if params['state'] in ['finished','failed']:
            self.comm.decrementNumRank()
        # make response
        res = {'StatusCode':0,
               'command':'NULL'}
        # return
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        

    # get event ranges
    def getEventRanges(self,params):
        # number of event ranges
        if 'nRanges' in params:
            nRanges = params['nRanges']
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


    # update event range
    def updateEventRange(self,params):
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

    def updateFailedEventRanges(self):
        for failed_update in self.failed_updates:
            eventRangeID,eventStatus, output = failed_update
            try:
                self.db.updateEventRange(eventRangeID,eventStatus, output)
            except Exception as e:
                self.tmpLog.debug('db.updateEventRange failed: %s' % str(e))


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
        tmpStat,tmpOut = self.loadJob()
        self.tmpLog.info("loading job: (status: %s, output: %s)" %(tmpStat, tmpOut))
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)
        # make event table
        self.tmpLog.info('making EventTable')
        tmpStat,tmpOut = self.makeEventTable()
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)
        # main loop
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
                self.tmpLog.error('unknown method={0} was requested from rank={1} '.format(method,
                                                                                      self.comm.getRequesterRank()))
        self.updateFailedEventRanges()
        # final dump
        self.tmpLog.info('final dumping')
        self.db.dumpUpdates(True)
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
        self.flushMessages()
        self.updateFailedEventRanges()
        # final dump
        self.tmpLog.info('final dumping')
        self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.tmpLog.info('stop')


    def getOutputs(self):
        pass


    def __del__(self):
        self.tmpLog.info('__del__ function')
        self.flushMessages()
        self.updateFailedEventRanges()
        # final dump
        self.tmpLog.info('final dumping')
        self.db.dumpUpdates(True)
        self.tmpLog.info("post Exec job")
        self.postExecJob()
        self.tmpLog.info('__del__ function')
