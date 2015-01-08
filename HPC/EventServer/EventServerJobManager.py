import inspect
import commands
import os
import signal
import sys
import time
import Queue
import subprocess
import threading
import yampl
import json

import Logger

class EventServerJobManager():
    class MessageThread(threading.Thread):
        def __init__(self, messageQ, socketname, context, **kwds):
            threading.Thread.__init__(self, **kwds)
            self.__messageQ = messageQ
            self._stop = threading.Event()
            self.__messageSrv = yampl.ServerSocket(socketname, context)

        def send(self, message):
            self.__messageSrv.send_raw(message)

        def stop(self):
            self._stop.set()

        def stopped(self):
            return self._stop.isSet()

        def run(self):
            while True:
                if self.stopped():
                    break
                size, buf = self.__messageSrv.try_recv_raw()
                if size == -1: time.sleep(1)
                else:
                    self.__messageQ.put(buf)

    def __init__(self, rank=None):
        self.__rank = rank
        self.__name = "EventServerJobManager"
        self.__eventRanges = []
        self.__eventRangesStatus = {}
        self.__outputMessage = []
        self.__messageQueue = None
        self.__messageThread = None
        self.__TokenExtractorProcess = None
        self.__athenaMPProcess = None
        self.__athenaMP_isReady = False
        self.__athenaMP_needEvents = 0
        self.__pollTimeout = 5
        self.__log = Logger.Logger()
        self.initSignalHandler()

    def handler(self, signal, frame):
        print "!!FAILED!!3000!! Signal %s is caught" % signal
        self.terminate()
        sys.exit(-1)

    def initSignalHandler(self):
        #signal.signal(signal.SIGTERM, self.handler)
        #signal.signal(signal.SIGQUIT, self.handler)
        #signal.signal(signal.SIGSEGV, self.handler)
        signal.signal(signal.SIGINT, self.handler)

    def initMessageThread(self, socketname='EventService_EventRanges', context='local'):
        self.__log.debug("Rank %s: initMessageThread: socketname: %s, context: %s" %(self.__rank, socketname, context))
        try:
            self.__messageQueue = Queue.Queue()
            self.__messageThread = EventServerJobManager.MessageThread(self.__messageQueue, socketname, context)
            self.__messageThread.start()
        except Exception, e:
            self.__log.warning("Rank %s: Failed to initMessageThread: %s" % (self.__rank, str(e)))
            self.terminate()

    def initTokenExtractorProcess(self, cmd):
        self.__log.debug("Rank %s: initTokenExtractorProcess: %s" % (self.__rank, cmd))
        try:
            #self.__TokenExtractorProcess = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            self.__TokenExtractorProcess = subprocess.Popen(cmd, shell=True)
        except Exception, e:
            self.__log.warning("Rank %s: Failed to initTokenExtractorProcess: %s" % (self.__rank, str(e)))
            self.terminate()

    def initAthenaMPProcess(self, cmd):
        self.__log.debug("Rank %s: initAthenaMPProcess: %s" % (self.__rank, cmd))
        try:
            #self.__athenaMPProcess = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            self.__athenaMPProcess = subprocess.Popen(cmd, shell=True)
        except Exception, e:
            self.__log.warning("Rank %s: Failed to initAthenaMPProcess: %s" % (self.__rank, str(e)))
            self.terminate()

    def insertEventRange(self, message):
        self.__log.debug("Rank %s: insertEventRange to ESJobManager: %s" % (self.__rank, message))
        self.__eventRanges.append(message)
        self.__athenaMP_needEvents -= 1
        if not "No more events" in message:
            eventRangeID = message['eventRangeID']
            if not eventRangeID in self.__eventRangesStatus:
                self.__eventRangesStatus[eventRangeID] = {}
                self.__eventRangesStatus[eventRangeID]['status'] = 'new'
            #eventRanges= eval(message)
            #for eventRange in eventRanges:
            #    eventRangeID = eventRange['eventRangeID']
            #    if not eventRangeID in self.__eventRangesStatus:
            #        self.__eventRangesStatus[eventRangeID] = {}
            #        self.__eventRangesStatus[eventRangeID]['status'] = 'new'

    def getEventRange(self):
        if len(self.__eventRanges) > 0:
            eventRange = self.__eventRanges.pop(0)
            self.__log.debug("Rank %s: getEventRange from ESJobManager(will send to AthenaMP): %s" % (self.__rank, eventRange))
            return eventRange
        return None

    def sendEventRangeToAthenaMP(self, eventRange):
        if "No more events" in eventRange:
            self.__log.debug("Rank %s: sendEventRangeToAthenaMP: %s" % (self.__rank, eventRange))
            self.__messageThread.send(eventRange)
        else:
            eventRangeFormat = json.dumps([eventRange])
            self.__log.debug("Rank %s: sendEventRangeToAthenaMP: %s" % (self.__rank, eventRangeFormat))
            self.__messageThread.send(eventRangeFormat)

            eventRangeID = eventRange['eventRangeID']
            self.__eventRangesStatus[eventRangeID]['status'] = 'processing'
            #eventRanges= eval(eventRange)
            #for eventRange in eventRanges:
            #    eventRangeID = eventRange['eventRangeID']
            #    self.__eventRangesStatus[eventRangeID]['status'] = 'processing'

        self.__athenaMP_isReady = False

    def getOutput(self):
        if len(self.__outputMessage) > 0:
            output = self.__outputMessage.pop(0)
            self.__log.debug("Rank %s: getOutput from ESJobManager(main prog will handle output): %s" % (self.__rank, output))
            return output
        return None

    def getEventRangesStatus(self):
        return self.__eventRangesStatus

    def isDead(self):
        #if self.__TokenExtractorProcess is None or self.__TokenExtractorProcess.poll() is not None or self.__athenaMPProcess is None or self.__athenaMPProcess.poll() is not None or not self.__messageThread.is_alive():
        if self.__TokenExtractorProcess is None or self.__athenaMPProcess is None or self.__athenaMPProcess.poll() is not None or not self.__messageThread.is_alive(): 
            return True
        return False

    def isReady(self):
        #return self.__athenaMP_isReady and self.__athenaMPProcess.poll() is None
        #return self.__athenaMP_needEvents > 0 and self.__athenaMPProcess.poll() is None
        return len(self.__eventRanges) > 0 and self.__athenaMPProcess.poll() is None

    def isNeedMoreEvents(self):
        #return self.__athenaMP_isReady and len(self.__eventRanges) == 0
        return self.__athenaMP_needEvents

    def handleMessage(self):
        try:
            #message = self.__messageQueue.get(True, self.__pollTimeout)
            message = self.__messageQueue.get(False)
        except Queue.Empty:
            return False
        else:
            self.__log.debug("Rank %s: Received message: %s" % (self.__rank, message))
            if "Ready for events" in message:
                self.__athenaMP_isReady = True
                self.__athenaMP_needEvents += 1
            elif message.startswith("/"):
                self.__outputMessage.append(message)
                try:
                    eventRangeID = message.split(',')[0].split('.')[-1]
                    self.__eventRangesStatus[eventRangeID]['status'] = 'Done'
                    self.__eventRangesStatus[eventRangeID]['output'] = message
                except Exception, e:
                    self.__log.warning("Rank %s: output message format is not recognized: %s " % (self.__rank, message))
                    self.__log.warning("Rank %s: %s" % (self.__rank, str(e)))
            return True

    def findChildProcesses(self,pid):
        command = "/bin/ps -e --no-headers -o pid -o ppid -o fname"
        status,output = commands.getstatusoutput(command)
        #print "ps output: %s" % output

        pieces = []
        result = []
        for line in output.split("\n"):
            pieces= line.split()
            try:
                value=int(pieces[1])
            except Exception,e:
                #print "trouble interpreting ps output %s: \n %s" % (e,pieces)
                continue
            if value==pid:
                try:
                    job=int(pieces[0])
                except ValueError,e:
                    #print "trouble interpreting ps output %s: \n %s" % (e,pieces[0])
                    continue
                result.append(job)
        return result

    def getChildren(self, pid):
        self.__childProcs = []
        self.__childProcs.append(pid)
        childProcs = self.findChildProcesses(pid)
        for child in childProcs:
            #print "Child Process found: %s" % child
            self.__childProcs.append(child)
            self.getChildren(child)

    def killProcess(self, pid):
        if pid > -1:
            for process in self.getChildren(pid):
                try:
                    os.kill(int(process), signal.SIGKILL)
                except Exception, e:
                    self.__log.warning("Rank %s: SIGKILL error: %s" % (self.__rank, str(e)))

    def terminate(self):
        self.__log.debug("Rank %s: ESJobManager is terminating" % self.__rank)
        try:
            self.__log.debug("Rank %s: Killing AthenaMP process" % self.__rank)
            os.killpg(self.__athenaMPProcess.pid, signal.SIGTERM)
        except Exception, e:
            self.__log.debug("Rank %s: Failed to kill AthenaMP process: %s" % (self.__rank, str(e)))
        try:
            self.__log.debug("Rank %s: Killing TokenExtractor process" % self.__rank)
            os.killpg(self.__TokenExtractorProcess.pid, signal.SIGTERM)
        except Exception, e:
            self.__log.debug("Rank %s: Failed to kill TokenExtractor Process process: %s" % (self.__rank, str(e)))
        self.__log.debug("Rank %s: Stopping Message Thread" % self.__rank)
        self.__messageThread.stop()

        # Frequently the process is not stopped. So kill them with SIGKILL
        time.sleep(5)
        try:
            if self.__athenaMPProcess is not None and self.__athenaMPProcess.poll() is None:
                self.__log.debug("Rank %s: AthenMP is still running. send SIGKILL" % self.__rank)
                self.killProcess(self.__athenaMPProcess.pid)
            if self.__TokenExtractorProcess is not None and self.__TokenExtractorProcess.poll() is None:
                self.__log.debug("Rank %s: Token Extractor is still running. send SIGKILL" % self.__rank)
                self.killProcess(self.__TokenExtractorProcess.pid)
        except Exception, e:
            self.__log.debug("Rank %s: Failed to kill process: %s" % (self.__rank, str(e)))

    def finish(self):
        self.__log.info("Rank %s: ESJobManager is finishing" % self.__rank)
        self.__log.info("Rank %s: wait AthenaMP to finish" % self.__rank)
        i = 0
        while self.__athenaMPProcess.poll() is None and i < 360:
            i += 1
            self.handleMessage()
            time.sleep(10)
        # make sure to finish all message in the message Queue
        while self.handleMessage():
            pass
        self.terminate()

    def poll(self):
        try:
            if self.isDead():
                self.__log.warning("Rank %s: One Process in ESJobManager is dead." % self.__rank)
                self.terminate()
                return -1

            self.handleMessage()
            while self.isReady():
                self.__log.info("Rank %s: AthenMP is ready." % self.__rank)
                eventRange = self.getEventRange()
                if eventRange is None:
                    return -1
                else:
                    self.__log.info("Rank %s: Process Event: %s" % (self.__rank, eventRange))
                    self.sendEventRangeToAthenaMP(eventRange)
                    if "No more events" in eventRange:
                        self.finish()
                        return 0
        except Exception, e:
            self.__log.warning("Rank %s: Exception happened when polling: %s" % (self.__rank, str(e)))


    def flushMessages(self):
        self.__log.info("Rank %s: ESJobManager flush messages" % self.__rank)
        while self.handleMessage():
            pass
