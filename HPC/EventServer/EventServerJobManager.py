import inspect
import commands
import os
import re
import signal
import sys
import time
import Queue
import multiprocessing
import subprocess
import threading
import json
import traceback

try:
    import yampl
except:
    print "Failed to import yampl: %s" % traceback.format_exc()


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from pandayoda.yodacore import Logger
from FileHandling import getCPUTimes

from signal_block.signal_block import block_sig, unblock_sig

class EventServerJobManager():
    class MessageThread(threading.Thread):
        def __init__(self, messageQ, socketname, context, **kwds):
            threading.Thread.__init__(self, **kwds)
            self.__log = Logger.Logger(filename='EventServiceManager.log')
            self.__messageQ = messageQ
            self._stop = threading.Event()
            try:
                self.__messageSrv = yampl.ServerSocket(socketname, context)
            except:
                self.__log.debug("Exception: failed to start yampl server socket: %s" % traceback.format_exc())

        def send(self, message):
            try:
                self.__messageSrv.send_raw(message)
            except:
                self.__log.debug("Exception: failed to send yampl message: %s" % traceback.format_exc())

        def stop(self):
            self._stop.set()

        def stopped(self):
            return self._stop.isSet()

        def run(self):
            try:
                while True:
                    if self.stopped():
                        break
                    size, buf = self.__messageSrv.try_recv_raw()
                    if size == -1:
                        time.sleep(0.00001)
                    else:
                        self.__messageQ.put(buf)
            except:
                self.__log.debug("Exception: Message Thread failed: %s" % traceback.format_exc())

    def __init__(self, rank=None, ATHENA_PROC_NUMBER=1, workingDir=None):
        self.__rank = rank
        self.__name = "EventServerJobManager"
        self.__eventRanges = []
        self.__eventRangesStatus = {}
        self.__outputMessage = []
        self.__messageQueue = multiprocessing.Queue()
        self.__messageInQueue = multiprocessing.Queue()
        self.__messageThread = None
        self.__TokenExtractorCmd = None
        self.__TokenExtractorProcess = None
        self.__athenaMPProcess = None
        self.__athenaMP_isReady = False
        self.__athenaMP_needEvents = 0
        self.__pollTimeout = 5
        self.__child_pid = None
        self.__child_cpuTime = {}
        if workingDir:
            self.__log = Logger.Logger(filename=os.path.join(workingDir, 'EventServiceManager.log'))
        else:
            self.__log = Logger.Logger(filename='EventServiceManager.log')
        self.__childProcs = []
        self.__isKilled = False

        self.__waitTerminate = False
        self.__waitTerminateTime = 1800
        self.__startTerminateTime = None

        self.__noMoreEvents = False
        self.__insertedMessages = 0
        self.__ATHENA_PROC_NUMBER = int(ATHENA_PROC_NUMBER)
        self.__numOutputs = 0
        self.initSignalHandler()

        self.__childRetStatus = 0
        self.__retry = 0
        self.__errEvent = False

        # accounting
        self.__startTime = time.time()
        self.__readyForEventTime = None
        self.__endTime = None
        self.__startOSTimes = os.times()
        self.__log.debug("Rank %s: startOSTimes: %s" % (self.__rank, self.__startOSTimes))
        self.__endOSTimes = None
        self.__totalQueuedEvents = 0
        self.__totalProcessedEvents = 0

    def handler(self, signal, frame):
        self.__log.debug("!!FAILED!!3000!! Signal %s is caught" % signal)
        self.terminate()
        sys.exit(-1)

    def initSignalHandler(self):
        #signal.signal(signal.SIGTERM, self.handler)
        #signal.signal(signal.SIGQUIT, self.handler)
        #signal.signal(signal.SIGSEGV, self.handler)
        #signal.signal(signal.SIGINT, self.handler)
        pass

    def getSetupTime(self):
        if self.__readyForEventTime:
            ret = self.__readyForEventTime - self.__startTime
        else:
            ret = time.time() - self.__startTime
        return ret

    def getTotalTime(self):
        if self.__endTime:
            ret = self.__endTime - self.__startTime
        else:
            ret = time.time() - self.__startTime
        return ret

    def getCPUConsumptionTimeFromProcPid(self, pid):
        try:
            if not os.path.exists(os.path.join('/proc/', str(pid), 'stat')):
                return 0
            with open(os.path.join('/proc/', str(pid), 'stat'), 'r') as pidfile:
                proctimes = pidfile.readline()
                # get utime from /proc/<pid>/stat, 14 item
                utime = proctimes.split(' ')[13]
                # get stime from proc/<pid>/stat, 15 item
                stime = proctimes.split(' ')[14]
                # count total process used time
                proctotal = int(utime) + int(stime)
            return(float(proctotal))
        except:
            # self.__log.debug("Rank %s: Failed to get cpu consumption time for pid %s: %s" % (self.__rank, pid, traceback.format_exc()))
            return 0

    def getCPUConsumptionTimeFromProc(self):
        cpuConsumptionTime = 0L
        try:
            CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
            if self.__child_pid:
                self.__childProcs = []
                self.getChildren(self.__child_pid)
                for process in self.__childProcs:
                    if process not in self.__child_cpuTime.keys():
                        self.__child_cpuTime[process] = 0
                for process in self.__child_cpuTime.keys():
                    cpuTime = self.getCPUConsumptionTimeFromProcPid(process) / CLOCK_TICKS
                    if cpuTime > self.__child_cpuTime[process]:
                        self.__child_cpuTime[process] = cpuTime
                    cpuConsumptionTime += self.__child_cpuTime[process]
        except:
            self.__log.debug("Rank %s: Failed to get cpu consumption time from proc: %s" % (self.__rank, traceback.format_exc()))
        return cpuConsumptionTime

    def getCPUConsumptionTime(self):
        cpuConsumptionUnit, cpuConsumptionTime, cpuConversionFactor = getCPUTimes(os.getcwd())
        self.__log.debug("Rank %s: cpuConsumptionTime: %s" % (self.__rank, cpuConsumptionTime))
        self.__log.debug("Rank %s: start os.times: %s" % (self.__rank, self.__startOSTimes))
        self.__log.debug("Rank %s: os.times: %s" % (self.__rank, os.times()))
        if cpuConsumptionTime < 10:
            endOSTimes = os.times()
            if self.__endOSTimes:
                endOSTimes = self.__endOSTimes
            cpuConsumptionTime = endOSTimes[2] + endOSTimes[3] - self.__startOSTimes[2] - self.__startOSTimes[3]
            if cpuConsumptionTime < 0:
                cpuConsumptionTime = 0
        procCPUConsumptionTime = self.getCPUConsumptionTimeFromProc()
        self.__log.debug("Rank %s: cpuConsumptionTime from proc: %s" % (self.__rank, procCPUConsumptionTime))
        if self.__isKilled or cpuConsumptionTime < procCPUConsumptionTime / 2:
            cpuConsumptionTime = procCPUConsumptionTime
        self.__log.debug("Rank %s: cpuConsumptionTime: %s" % (self.__rank, cpuConsumptionTime))
        return cpuConsumptionTime

    def getCores(self):
        return self.__ATHENA_PROC_NUMBER

    def getProcessCPUHour(self):
        return (self.getTotalTime() - self.getSetupTime()) * self.getCores()

    def getTotalCPUHour(self):
        return self.getTotalTime() * self.getCores()

    def getTotalQueuedEvents(self):
        return self.__totalQueuedEvents

    def getTotalProcessedEvents(self):
        return self.__totalProcessedEvents

    def getAccountingMetrics(self):
        return {"startTime": self.__startTime,
                "readyTime": self.__readyForEventTime,
                "endTime": self.__endTime,
                "setupTime": self.getSetupTime(),
                "runningTime": self.getTotalTime() - self.getSetupTime(),
                "cores": self.getCores(),
                #"processCPUHour": self.getProcessCPUHour(),
                #"totalCPUHour": self.getTotalCPUHour(),
                "cpuConsumptionTime": self.getCPUConsumptionTime(),
                "queuedEvents": self.getTotalQueuedEvents(),
                "processedEvents": self.getTotalProcessedEvents()}

    def preSetup(self, preSetup):
        if preSetup:
            self.__log.debug("Rank %s: PreSetup: %s" % (self.__rank, preSetup))
            status, output = commands.getstatusoutput(preSetup)
            self.__log.debug("Rank %s: PreSetup status: %s, output: %s" % (self.__rank, status, output))
            return status, output
        else:
            return 0, None

    def postRun(self, postRun):
        if postRun:
            self.__log.debug("Rank %s: postRun: %s" % (self.__rank, postRun))
            status, output = commands.getstatusoutput(postRun)
            self.__log.debug("Rank %s: postRun status: %s, output: %s" % (self.__rank, status, output))

    def initMessageThread(self, socketname='EventService_EventRanges', context='local'):
        self.__log.debug("Rank %s: initMessageThread: socketname: %s, context: %s, workdir: %s" %(self.__rank, socketname, context, os.getcwd()))
        try:
            self.__messageThread = EventServerJobManager.MessageThread(self.__messageQueue, socketname, context)
            self.__messageThread.start()
        except:
            self.__log.warning("Rank %s: Failed to initMessageThread: %s" % (self.__rank, str(traceback.format_exc())))
            self.terminate()

    def initTokenExtractorProcess(self, cmd):
        self.__log.debug("Rank %s: initTokenExtractorProcess: %s, workdir: %s" % (self.__rank, cmd, os.getcwd()))
        try:
            self.__TokenExtractorCmd = cmd
            if cmd:
                self.__TokenExtractorProcess = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stdout, shell=True)
                # self.__TokenExtractorProcess = subprocess.Popen(cmd, shell=True)
                if self.__TokenExtractorProcess.poll() is not None:
                    self.__log.warning("Rank %s: Failed to initTokenExtractorProcess, poll is not None: %s" % (self.__rank, self.__TokenExtractorProcess.poll()))
                    self.terminate()
            else:
                self.__log.debug("Rank %s: TokenExtractor cmd(%s) is None, will not use it" % (self.__rank, cmd))
                self.__TokenExtractorProcess = None
        except:
            self.__log.warning("Rank %s: Failed to initTokenExtractorProcess: %s" % (self.__rank, str(traceback.format_exc())))
            self.terminate()

    def initAthenaMPProcess(self, cmd):
        self.__log.debug("Rank %s: initAthenaMPProcess: %s, workdir: %s" % (self.__rank, cmd, os.getcwd()))
        try:
            self.__athenaMPProcess = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stdout, shell=True)
            # self.__athenaMPProcess = subprocess.Popen(cmd, shell=True)
            if self.__athenaMPProcess.poll() is not None:
                self.__log.warning("Rank %s: Failed to initAthenaMPProcess, poll is not None: %s" % (self.__rank, self.__athenaMPProcess.poll()))
                self.terminate()
        except:
            self.__log.warning("Rank %s: Failed to initAthenaMPProcess: %s" % (self.__rank, str(traceback.format_exc())))
            self.terminate()

    def init(self, socketname='EventService_EventRanges', context='local', athenaMPCmd=None, tokenExtractorCmd=None):
        self.__childRetStatus = 0
        child_pid = os.fork()
        if child_pid == 0:
            # child process
            self.initMessageThread(socketname, context)
            self.initTokenExtractorProcess(tokenExtractorCmd)
            self.initAthenaMPProcess(athenaMPCmd)
            self.__log.debug("Rank %s: Child main loop start" % (self.__rank))
            while True:
                if self.isChildDead():
                   self.__log.warning("Rank %s: Child One Process in ESJobManager is dead." % self.__rank)
                   self.terminateChild()
                   break
                try:
                    message = self.__messageInQueue.get(False)
                    self.__log.debug("Rank %s: Child get message: %s" % (self.__rank, message))
                    if "Stop_Message_Process" in message:
                        self.__log.debug("Rank %s: Child stop" % (self.__rank))
                        break
                    else:
                        self.__messageThread.send(message)
                    #self.__messageInQueue.task_done()
                except Queue.Empty:
                    pass
                except:
                    self.__log.debug("Rank %s: Child Exception: failed to send yampl message: %s" % (self.__rank, traceback.format_exc()))

            self.__log.debug("Rank %s: Child main loop end" % (self.__rank))
            self.terminateChild()
            self.__log.debug("Rank %s: Child terminated" % (self.__rank))
            # sys.exit(0)
            os._exit(0)
        else:
            self.__child_pid = child_pid
            return 0
            
    def insertEventRange(self, message):
        self.__log.debug("Rank %s: insertEventRange to ESJobManager: %s" % (self.__rank, message))
        self.__eventRanges.append(message)
        self.__athenaMP_needEvents -= 1
        self.__insertedMessages += 1
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
        else:
            self.__athenaMP_needEvents = 0
            self.__noMoreEvents = True

    def insertEventRanges(self, messages):
        self.__log.debug("Rank %s: insertEventRanges to ESJobManager: %s" % (self.__rank, messages))
        for message in messages:
            self.__athenaMP_needEvents -= 1
            self.__insertedMessages += 1
            self.__eventRanges.append(message)
            if not "No more events" in message:
                eventRangeID = message['eventRangeID']
                if not eventRangeID in self.__eventRangesStatus:
                    self.__eventRangesStatus[eventRangeID] = {}
                    self.__eventRangesStatus[eventRangeID]['status'] = 'new'
            else:
                self.__athenaMP_needEvents = 0
                self.__noMoreEvents = True

    def getEventRanges(self):
        if len(self.__eventRanges) > 0:
            eventRanges = self.__eventRanges.pop(0)
            self.__log.debug("Rank %s: getEventRanges from ESJobManager(will send to AthenaMP): %s" % (self.__rank, eventRanges))
            return eventRanges
        return None

    def sendEventRangeToAthenaMP(self, eventRanges):
        block_sig(signal.SIGTERM)

        if "No more events" in eventRanges:
            self.__log.debug("Rank %s: sendEventRangeToAthenaMP: %s" % (self.__rank, eventRanges))
            self.__messageInQueue.put(eventRanges)
        else:
            if type(eventRanges) is not list:
                eventRanges = [eventRanges]
            eventRangeFormat = json.dumps(eventRanges)
            self.__log.debug("Rank %s: sendEventRangeToAthenaMP: %s" % (self.__rank, eventRangeFormat))
            self.__messageInQueue.put(eventRangeFormat)
            self.__totalQueuedEvents += 1

            for eventRange in eventRanges:
                eventRangeID = eventRange['eventRangeID']
                self.__eventRangesStatus[eventRangeID]['status'] = 'processing'
                #eventRanges= eval(eventRange)
                #for eventRange in eventRanges:
                #    eventRangeID = eventRange['eventRangeID']
                #    self.__eventRangesStatus[eventRangeID]['status'] = 'processing'

        self.__athenaMP_isReady = False

        unblock_sig(signal.SIGTERM)

    def getOutput(self):
        if len(self.__outputMessage) > 0:
            output = self.__outputMessage.pop(0)
            self.__log.debug("Rank %s: getOutput from ESJobManager(main prog will handle output): %s" % (self.__rank, output))
            return output
        return None

    def getOutputs(self, signal=False):
        outputs = []
        if not signal:
            if len(self.__outputMessage) > 0:
                outputs = self.__outputMessage
                self.__outputMessage = []
                self.__log.debug("Rank %s: getOutputs from ESJobManager(main prog will handle outputs): %s" % (self.__rank, outputs))
                return outputs
        else:
            if len(self.__outputMessage) > 0:
                self.__log.debug("Rank %s: getOutputs signal from ESJobManager(main prog will handle outputs): %s" % (self.__rank, self.__outputMessage))
                return self.__outputMessage
        return None

    def updatedOutputs(self, outputs):
        for output in outputs:
            try:
                self.__outputMessage.remove(output)
            except:
                self.__log.debug("Rank %s: updatedOutputs failed to updated message: %s" % (self.__rank, output))

    def getEventRangesStatus(self):
        return self.__eventRangesStatus

    def isChildDead(self):
        # if self.__TokenExtractorProcess is None or self.__TokenExtractorProcess.poll() is not None or self.__athenaMPProcess is None or self.__athenaMPProcess.poll() is not None or not self.__messageThread.is_alive():
        # if self.__TokenExtractorProcess is None or self.__athenaMPProcess is None or self.__athenaMPProcess.poll() is not None or not self.__messageThread.is_alive(): 
        #     return True
        if (self.__TokenExtractorCmd is not None and self.__TokenExtractorProcess is None) or self.__athenaMPProcess is None:
            self.__log.debug("Rank %s: TokenExtractorProcess: %s, athenaMPProcess: %s" % (self.__rank, self.__TokenExtractorProcess, self.__athenaMPProcess))
            return True
        if self.__athenaMPProcess.poll() is not None:
            self.__log.debug("Rank %s: AthenaMP process dead: %s" % (self.__rank, self.__athenaMPProcess.poll()))
            return True
        if not self.__messageThread.is_alive():
            self.__log.debug("Rank %s: Yampl message thread isAlive: %s" % (self.__rank, self.__messageThread.is_alive()))
            return True
        return False

    def isDead(self):
        if self.__child_pid is None:
            self.__log.debug("Rank %s: Child process id is %s" % (self.__rank, self.__child_pid))
            if self.__endTime is None:
                self.__endTime = time.time()
            return True
        try:
            pid, status = os.waitpid(self.__child_pid, os.WNOHANG)
        except OSError, e:
            self.__log.debug("Rank %s: Exception when checking child process %s: %s" % (self.__rank, self.__child_pid, e))
            if "No child processes" in str(e):
                self.__childRetStatus = 0
                if self.__endTime is None:
                    self.__endTime = time.time()
                return True
        else:
            if pid: # finished
                self.__log.debug("Rank %s: Child process %s finished with status: %s" % (self.__rank, pid, status%255))
                self.__childRetStatus = status%255
                if self.__endTime is None:
                    self.__endTime = time.time()
                return True
        return False

    def getChildRetStatus(self):
        return self.__childRetStatus

    def isReady(self):
        #return self.__athenaMP_isReady and self.__athenaMPProcess.poll() is None
        #return self.__athenaMP_needEvents > 0 and self.__athenaMPProcess.poll() is None
        return len(self.__eventRanges) > 0 and (not self.isDead()) and self.__athenaMP_isReady

    def isNeedMoreEvents(self):
        #return self.__athenaMP_isReady and len(self.__eventRanges) == 0
        #return self.__athenaMP_needEvents
        if self.__noMoreEvents:
            return 0
        neededEvents = int(self.__numOutputs) + int(self.__ATHENA_PROC_NUMBER) - int(self.__insertedMessages)
        if neededEvents > 0:
            return neededEvents
        return self.__athenaMP_needEvents

    def extractErrorMessage(self, msg):
        """ Extract the error message from the AthenaMP message """

        # msg = 'ERR_ATHENAMP_PROCESS 130-2068634812-21368-1-4: Failed to process event range'
        # -> error_acronym = 'ERR_ATHENAMP_PROCESS'
        #    event_range_id = '130-2068634812-21368-1-4'
        #    error_diagnostics = 'Failed to process event range')
        #
        # msg = ERR_ATHENAMP_PARSE "u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', u'startEvent': 5, u'GUID': u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'": Wrong format
        # -> error_acronym = 'ERR_ATHENAMP_PARSE'
        #    event_range = "u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', ..
        #    error_diagnostics = 'Wrong format'
        #    -> event_range_id = '130-2068634812-21368-1-4' (if possible to extract)

        error_acronym = ""
        event_range_id = ""
        error_diagnostics = ""

        # Special error acronym
        if "ERR_ATHENAMP_PARSE" in msg:
            # Note: the event range will be in the msg and not the event range id only 
            pattern = re.compile(r"(ERR\_[A-Z\_]+)\ (.+)\:\ ?(.+)")
            found = re.findall(pattern, msg)
            if len(found) > 0:
                try:
                    error_acronym = found[0][0]
                    event_range = found[0][1] # Note: not the event range id only, but the full event range
                    error_diagnostics = found[0][2]
                except Exception, e:
                    self.__log.error("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "EXTRACTION_FAILURE"
                    error_diagnostics = e
                else:
                    # Can the event range id be extracted?
                    if "eventRangeID" in event_range:
                        pattern = re.compile(r"eventRangeID\'\:\ ?.?\'([0-9\-]+)")
                        found = re.findall(pattern, event_range)
                        if len(found) > 0:
                            try:
                                event_range_id = found[0]
                            except Exception, e:
                                self.__log.error("!!WARNING!!2212!! Failed to extract event_range_id: %s" % (e))
                            else:
                                self.__log.error("Extracted event_range_id: %s" % (event_range_id))
                    else:
                        self.__log.error("!!WARNING!!2213!1 event_range_id not found in event_range: %s" % (event_range))
        else:
            # General error acronym
            pattern = re.compile(r"(ERR\_[A-Z\_]+)\ ([0-9\-]+)\:\ ?(.+)")
            found = re.findall(pattern, msg)
            if len(found) > 0:
                try:
                    error_acronym = found[0][0]
                    event_range_id = found[0][1]
                    error_diagnostics = found[0][2]
                except Exception, e:
                    self.__log.error("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "ERR_EXTRACTION_FAILURE"
                    error_diagnostics = e
            else:
                self.__log.error("!!WARNING!!2212!! Failed to extract AthenaMP message")
                error_acronym = "ERR_EXTRACTION_FAILURE"
                error_diagnostics = msg

        return error_acronym, event_range_id, error_diagnostics

    def handleMessage(self):
        block_sig(signal.SIGTERM)
        try:
            #message = self.__messageQueue.get(True, self.__pollTimeout)
            message = self.__messageQueue.get(False)
            #self.__messageQueue.task_done()
        except Queue.Empty:
            unblock_sig(signal.SIGTERM)
            return False
        else:
            if self.__readyForEventTime is None:
                self.__readyForEventTime = time.time()
            self.__log.debug("Rank %s: Received message: %s" % (self.__rank, message))
            if "Ready for events" in message:
                self.__athenaMP_isReady = True
                self.__athenaMP_needEvents += 1
            elif message.startswith("/"):
                self.__totalProcessedEvents += 1
                self.__numOutputs += 1
                # self.__outputMessage.append(message)
                try:
                    # eventRangeID = message.split(',')[0].split('.')[-1]
                    eventRangeID = message.split(',')[-3].replace("ID:", "").replace("ID: ", "")
                    self.__eventRangesStatus[eventRangeID]['status'] = 'finished'
                    self.__eventRangesStatus[eventRangeID]['output'] = message
                    self.__outputMessage.append((eventRangeID, 'finished', message))
                except Exception, e:
                    self.__log.warning("Rank %s: output message format is not recognized: %s " % (self.__rank, message))
                    self.__log.warning("Rank %s: %s" % (self.__rank, str(e)))
            elif message.startswith('ERR'):
                self.__log.error("Rank %s: Received an error message: %s" % (self.__rank, message))
                error_acronym, eventRangeID, error_diagnostics = self.extractErrorMessage(message)
                if eventRangeID != "":
                    try:
                        self.__log.error("Rank %s: !!WARNING!!2144!! Extracted error acronym %s and error diagnostics \'%s\' for event range %s" % (self.__rank, error_acronym, error_diagnostics, eventRangeID))
                        self.__eventRangesStatus[eventRangeID]['status'] = 'failed'
                        self.__eventRangesStatus[eventRangeID]['output'] = message
                        self.__outputMessage.append((eventRangeID, error_acronym, message))
                    except Exception, e:
                        self.__log.warning("Rank %s: output message format is not recognized: %s " % (self.__rank, message))
                        self.__log.warning("Rank %s: %s" % (self.__rank, str(e)))
                if "FATAL" in error_acronym:
                    self.__log.error("Rank %s: !!WARNING!!2146!! A FATAL error was encountered, prepare to finish" % (self.__rank))
                    self.terminate()
            else:
                self.__log.error("Rank %s: Received an unknown message: %s" % (self.__rank, message))
            unblock_sig(signal.SIGTERM)
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
        #self.__childProcs = []
        if pid not in self.__childProcs:
            self.__childProcs.append(pid)
        childProcs = self.findChildProcesses(pid)
        for child in childProcs:
            #print "Child Process found: %s" % child
            #self.__childProcs.append(child)
            self.getChildren(child)

    def killProcess(self, pid):
        self.__isKilled = True
        if pid > -1:
            self.__childProcs = []
            self.getChildren(pid)
            for process in self.__childProcs:
                try:
                    os.kill(int(process), signal.SIGKILL)
                except:
                    self.__log.warning("Rank %s: SIGKILL error: %s" % (self.__rank, str(traceback.format_exc())))

    def terminateChild(self):
        self.__isKilled = True
        self.__log.debug("Rank %s: ESJobManager Child is terminating" % self.__rank)
        try:
            if self.__athenaMPProcess and self.__athenaMPProcess.poll() is None:
                self.__log.debug("Rank %s: Killing AthenaMP process" % self.__rank)
                os.killpg(self.__athenaMPProcess.pid, signal.SIGTERM)
        except:
            self.__log.debug("Rank %s: Failed to kill AthenaMP process: %s" % (self.__rank, str(traceback.format_exc())))
        try:
            if self.__TokenExtractorProcess and self.__TokenExtractorProcess.poll() is None:
                self.__log.debug("Rank %s: Killing TokenExtractor process" % self.__rank)
                os.killpg(self.__TokenExtractorProcess.pid, signal.SIGTERM)
        except:
            self.__log.debug("Rank %s: Failed to kill TokenExtractor Process process: %s" % (self.__rank, str(traceback.format_exc())))

        # Frequently the process is not stopped. So kill them with SIGKILL
        time.sleep(1)
        try:
            if self.__athenaMPProcess is not None and self.__athenaMPProcess.poll() is None:
                self.__log.debug("Rank %s: AthenMP is still running. send SIGKILL" % self.__rank)
                self.killProcess(self.__athenaMPProcess.pid)
            if self.__TokenExtractorProcess is not None and self.__TokenExtractorProcess.poll() is None:
                self.__log.debug("Rank %s: Token Extractor is still running. send SIGKILL" % self.__rank)
                self.killProcess(self.__TokenExtractorProcess.pid)
        except:
            self.__log.debug("Rank %s: Failed to kill process: %s" % (self.__rank, str(traceback.format_exc())))
        self.__log.debug("Rank %s: Stopping Message Thread" % self.__rank)
        self.__messageThread.stop()

    def terminate(self):
        self.__isKilled = True
        self.__log.debug("Rank %s: ESJobManager is terminating" % self.__rank)
        try:
            self.__messageInQueue.put("Stop_Message_Process")
            time.sleep(2)
            if not self.isDead():
                os.killpg(self.__child_pid, signal.SIGTERM)
        except:
            self.__log.debug("Rank %s: Failed to kill child process: %s" % (self.__rank, str(traceback.format_exc())))

        # Frequently the process is not stopped. So kill them with SIGKILL
        time.sleep(5)
        try:
            if not self.isDead():
                self.killProcess(self.__child_pid)
        except:
            self.__log.debug("Rank %s: Failed to kill child process: %s" % (self.__rank, str(traceback.format_exc())))

    def kill(self):
        self.__isKilled = True
        self.__log.debug("Rank %s: ESJobManager is terminating" % self.__rank)
        try:
            self.__messageInQueue.put("Stop_Message_Process")
            if not self.isDead():
                os.killpg(self.__child_pid, signal.SIGTERM)
        except:
            self.__log.debug("Rank %s: Failed to kill child process: %s" % (self.__rank, str(traceback.format_exc())))

        # Frequently the process is not stopped. So kill them with SIGKILL
        try:
            if not self.isDead():
                self.killProcess(self.__child_pid)
        except:
            self.__log.debug("Rank %s: Failed to kill child process: %s" % (self.__rank, str(traceback.format_exc())))

    def finish(self):
        if self.__waitTerminate and (time.time() - self.__startTerminateTime) < self.__waitTerminateTime:
            pass
        else:
            self.terminate()

    def poll(self):
        try:
            if self.isDead():
                self.__log.warning("Rank %s: One Process in ESJobManager is dead." % self.__rank)
                self.terminate()
                return -1

            while self.handleMessage():
                pass
            if self.__waitTerminate:
                self.finish()
            else:
                while self.isReady():
                    self.__log.info("Rank %s: AthenMP is ready." % self.__rank)
                    eventRanges = self.getEventRanges()
                    if eventRanges is None:
                        return -1
                    else:
                        self.__log.info("Rank %s: Process Event: %s" % (self.__rank, eventRanges))
                        self.sendEventRangeToAthenaMP(eventRanges)
                        if "No more events" in eventRanges:
                            self.__log.info("Rank %s: ESJobManager is finishing" % self.__rank)
                            self.__log.info("Rank %s: wait AthenaMP to finish" % self.__rank)
                            self.__startTerminateTime = time.time()
                            self.__waitTerminate = True
                            return 0
        except:
            self.__log.warning("Rank %s: Exception happened when polling: %s" % (self.__rank, str(traceback.format_exc())))


    def flushMessages(self):
        block_sig(signal.SIGTERM)

        self.__log.info("Rank %s: ESJobManager flush messages" % self.__rank)
        while self.isReady():
            self.__log.info("Rank %s: AthenaMP is ready, send 'No more events' to it." % self.__rank)
            self.sendEventRangeToAthenaMP("No more events")
        while self.handleMessage():
            pass

        unblock_sig(signal.SIGTERM)
