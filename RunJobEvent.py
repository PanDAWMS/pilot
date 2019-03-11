# Class definition:
#   RunJobEvent:  module for receiving and processing events from the Event Service
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJob import RunJob                        # Parent RunJob class
from pUtil import writeToFileWithStatus

# Standard python modules
import os
import re
import sys
import random
import time
import atexit
import signal
import commands
import traceback
import uuid
from optparse import OptionParser
from json import loads, dump
from shutil import copy2
from xml.dom import minidom

# Pilot modules
import Job
import Node
import Site
import pUtil
import RunJobUtilities
import Mover as mover
from JobRecovery import JobRecovery
from FileStateClient import getFilesOfState
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from StoppableThread import StoppableThread
from pUtil import tolog, isAnalysisJob, readpar, createLockFile, getDatasetDict,\
     tailPilotErrorDiag, getExperiment, getEventService,\
     getSiteInformation, getGUID
from FileHandling import getExtension, addToOSTransferDictionary, getCPUTimes, getReplicaDictionaryFromXML, writeFile
from EventRanges import downloadEventRanges, updateEventRange, updateEventRanges
from movers.base import BaseSiteMover
from processes import get_cpu_consumption_time

try:
    from PilotYamplServer import PilotYamplServer as MessageServer
except Exception, e:
    MessageServer = None
    tolog("RunJobEvent caught exception: %s" % str(e))

class RunJobEvent(RunJob):

    # private data members
    __runjob = "RunJobEvent"                     # String defining the sub class
    __instance = None                            # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                      # PilotErrors object
    __errorCode = 0                              # Error code, e.g. set by stage-out method
    __experiment = "ATLAS"                       # Current experiment (can be set with pilot option -F <experiment>)
    __pilotserver = "localhost"                  # Default server
    __pilotport = 88888                          # Default port
    __failureCode = None                         # Set by signal handler when user/batch system kills the job
    __pworkdir = "/tmp"                          # Site work dir used by the parent
    __logguid = None                             # GUID for the log file
    __pilotlogfilename = "pilotlog.txt"          # Default pilotlog filename
    __stageinretry = None                        # Number of stage-in tries
    __stageoutretry = None                       # Number of stage-out tries
    __pilot_initdir = ""                         # location of where the pilot is untarred and started
    __proxycheckFlag = True                      # True (default): perform proxy validity checks, False: no check
    __globalPilotErrorDiag = ""                  # Global pilotErrorDiag used with signal handler (only)
    __globalErrorCode = 0                        # Global error code used with signal handler (only)
    __inputDir = ""                              # Location of input files (source for mv site mover)
    __outputDir = ""                             # Location of output files (destination for mv site mover)
    __taskID = ""                                # TaskID (needed for OS transfer file and eventually for job metrics)
    __event_loop_running = False                 # Is the event loop running?
    __output_files = []                          # A list of all files that have been successfully staged-out, used by createFileMetadata()
    __guid_list = []                             # Keep track of downloaded GUIDs
    __lfn_list = []                              # Keep track of downloaded LFNs
    __eventRange_dictionary = {}                 # eventRange_dictionary[event_range_id] = [path, cpu, wall]
    __eventRangeID_dictionary = {}               # eventRangeID_dictionary[event_range_id] = True (corr. output file has been transferred)
    __stageout_queue = []                        # Queue for files to be staged-out; files are added as they arrive and removed after they have been staged-out
    __pfc_path = ""                              # The path to the pool file catalog
    __message_server_payload = None              # Message server for the payload
    __message_server_prefetcher = None           # Message server for Prefetcher
    __message_thread_payload = None              # Thread for listening to messages from the payload
    __message_thread_prefetcher = None           # Thread for listening to messages from the Prefetcher
    __status = True                              # Global job status; will be set to False if an event range or stage-out fails
    __athenamp_is_ready = False                  # True when an AthenaMP worker is ready to process an event range
    __prefetcher_is_ready = False                # True when Prefetcher is ready to receive an event range
    __prefetcher_has_finished = False            # True when Prefetcher has updated an event range which then should be sent to AthenaMP
    __asyncOutputStager_thread = None            #
    __asyncOutputStager_thread_sleep_time = 600  #
    __analysisJob = False                        # True for analysis job
    __jobSite = None                             # Site object
    __siteInfo = None                            # site information
    __node = None
    __job = None                                 # Job object
    __cache = ""                                 # Cache URL, e.g. used by LSST
    __metadata_filename = ""                     # Full path to the metadata file
    __yamplChannelNamePayload = None             # Yampl channel name used by the payload (AthenaMP)
    __yamplChannelNamePrefetcher = None          # Yampl channel name used by the Prefetcher
    __useEventIndex = True                       # Should Event Index be used? If not, a TAG file will be created
    __tokenextractor_input_list_filenane = ""    #
    __sending_event_range = False                # True while event range is being sent to payload
    __current_event_range = ""                   # Event range being sent to payload
    __updated_lfn = ""                           # Updated LFN sent from the Prefetcher
    __useTokenExtractor = False                  # Should the TE be used?
    __usePrefetcher = False                      # Should the Prefetcher be user
    __inFilePosEvtNum = False                    # Use event number ranges relative to in-file position
    __pandaserver = ""                   # Full PanDA server url incl. port and sub dirs

    # ES zip
    __esToZip = True
    __multipleBuckets = None
    __numBuckets = 1
    __stageoutStorages = None
    __max_wait_for_one_event = 360	# 6 hours, 360 minutes
    __min_events = 1
    __allowPrefetchEvents = True

    # calculate cpu time, os.times() doesn't report correct value for preempted jobs
    __childProcs = []
    __child_cpuTime = {}

    # record processed events
    __nEvents = 0
    __nEventsW = 0
    __nEventsFailed = 0
    __nEventsFailedStagedOut = 0
    __nStageOutFailures = 0
    __nStageOutSuccessAfterFailure = 0
    __isLastStageOutFailed = False

    __eventrangesToBeUpdated = []

    # error fatal code
    __esFatalCode = None
    __isKilled = False

    # external stagout time(time after athenaMP terminated)
    __external_stagout_time = 0

    # allow read/download remote inputs if closest RSE is in downtime
    __allow_remote_inputs = False

    # input files
    __input_files = {}

    # Getter and setter methods

    def getNEvents(self):
        return self.__nEvents, self.__nEventsW, self.__nEventsFailed, self.__nEventsFailedStagedOut

    def getSubStatus(self):
        if not self.__eventRangeID_dictionary:
            return 'no_events'
        if self.__esFatalCode:
            return 'pilot_fatal'
        if self.__nEventsFailed:
            if self.__nEventsFailed < self.__nEventsW:
                return 'partly_failed'
            elif self.__nEventsW == 0:
                return 'all_failed' # 'all_failed'
            else:
                return 'mostly_failed'
        else:
            return 'all_success'

    def getStageOutDetail(self):
        retStr = 'Stageout summary:'
        if 'primary' in self.__stageoutStorages and self.__stageoutStorages['primary']:
           retStr += "primary storage('%s' at '%s'): [success %s, failed %s]" % (self.__stageoutStorages['primary']['activity'],
                                                                                 self.__stageoutStorages['primary']['endpoint'],
                                                                                 self.__stageoutStorages['primary']['success'],
                                                                                 self.__stageoutStorages['primary']['failed'])
        if 'failover' in self.__stageoutStorages and self.__stageoutStorages['failover']:
           retStr += "failover storage('%s' at '%s'): [success %s, failed %s]" % (self.__stageoutStorages['failover']['activity'],
                                                                                  self.__stageoutStorages['failover']['endpoint'],
                                                                                  self.__stageoutStorages['failover']['success'],
                                                                                  self.__stageoutStorages['failover']['failed'])
        return retStr

    def setFinalESStatus(self, job):
        if self.__nEventsW < 1 and self.__nStageOutFailures >= 3:
            job.subStatus = 'pilot_failed'
            job.pilotErrorDiag = "Too many stageout failures. (%s)" % self.getStageOutDetail()
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_ESRECOVERABLE
            job.jobState = "failed"
        elif not self.__eventRangeID_dictionary:
            job.subStatus = 'pilot_noevents'  # 'no_events'
            job.pilotErrorDiag = "Pilot got no events"
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_NOEVENTS
            job.jobState = "failed"
        elif self.__eventRangeID_dictionary and self.__nEventsW < 1:
            job.subStatus = 'pilot_failed'  # 'no_running_events'
            job.pilotErrorDiag = "Pilot didn't run any events"
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_UNKNOWN
            job.jobState = "failed"
        elif self.__esFatalCode:
            job.subStatus = 'pilot_failed'
            job.pilotErrorDiag = "AthenaMP fatal error happened. (%s)" % self.getStageOutDetail()
            job.result[0] = "failed"
            job.result[2] = self.__esFatalCode
            job.jobState = "failed"
        elif self.__nEventsFailed:
            if self.__nEventsW == 0:
                job.subStatus = 'pilot_failed' # all failed
                job.pilotErrorDiag = "All events failed. (%s, other failure: %s)" % (self.getStageOutDetail(), self.__nEventsFailed - self.__nEventsFailedStagedOut)
                job.result[0] = "failed"
                job.result[2] = self.__error.ERR_ESRECOVERABLE
                job.jobState = "failed"
            elif self.__nEventsFailed < self.__nEventsW:
                job.subStatus = 'partly_failed'
                job.pilotErrorDiag = "Part of events failed. (%s, other failure: %s)" % (self.getStageOutDetail(), self.__nEventsFailed - self.__nEventsFailedStagedOut)
                job.result[0] = "failed"
                job.result[2] = self.__error.ERR_ESRECOVERABLE
                job.jobState = "failed"
            else:
                job.subStatus = 'mostly_failed' 
                job.pilotErrorDiag = "Most of events failed. (%s, other failure: %s)" % (self.getStageOutDetail(), self.__nEventsFailed - self.__nEventsFailedStagedOut)
                job.result[0] = "failed"
                job.result[2] = self.__error.ERR_ESRECOVERABLE
                job.jobState = "failed"
        else:
            job.subStatus = 'all_success'
            job.jobState = "finished"
            job.pilotErrorDiag = "AllSuccess. (%s)" % self.getStageOutDetail()

    def getESFatalCode(self):
        return self.__esFatalCode

    def getExperiment(self):
        """ Getter for __experiment """

        return self.__experiment

    def setExperiment(self, experiment):
        """ Setter for __experiment """

        self.__experiment = experiment

    def getPilotServer(self):
        """ Getter for __pilotserver """

        return self.__pilotserver

    def setPilotServer(self, pilotserver):
        """ Setter for __pilotserver """

        self.__pilotserver = pilotserver

    def getPilotPort(self):
        """ Getter for __pilotport """

        return self.__pilotport

    def setPilotPort(self, pilotport):
        """ Setter for __pilotport """

        self.__pilotport = pilotport

    def getFailureCode(self):
        """ Getter for __failureCode """

        return self.__failureCode

    def setFailureCode(self, code):
        """ Setter for __failureCode """

        self.__failureCode = code

    def getParentWorkDir(self):
        """ Getter for __pworkdir """

        return self.__pworkdir

    def setParentWorkDir(self, pworkdir):
        """ Setter for __pworkdir """

        self.__pworkdir = pworkdir
        super(RunJobEvent, self).setParentWorkDir(pworkdir)

    def getLogGUID(self):
        """ Getter for __logguid """

        return self.__logguid

    def setLogGUID(self, logguid):
        """ Setter for __logguid """

        self.__logguid = logguid

    def getPilotLogFilename(self):
        """ Getter for __pilotlogfilename """

        return self.__pilotlogfilename

    def setPilotLogFilename(self, pilotlogfilename):
        """ Setter for __pilotlogfilename """

        self.__pilotlogfilename = pilotlogfilename

    def getStageInRetry(self):
        """ Getter for __stageinretry """

        return self.__stageinretry

    def setStageInRetry(self, stageinretry):
        """ Setter for __stageinretry """

        self.__stageinretry = stageinretry
        super(RunJobEvent, self).setStageInRetry(stageinretry)

    def getStageOutRetry(self):
        """ Getter for __stageoutretry """

        return self.__stageoutretry

    def setStageOutRetry(self, stageoutretry):
        """ Setter for __stageoutretry """

        self.__stageoutretry = stageoutretry

    def getPilotInitDir(self):
        """ Getter for __pilot_initdir """

        return self.__pilot_initdir

    def setPilotInitDir(self, pilot_initdir):
        """ Setter for __pilot_initdir """

        self.__pilot_initdir = pilot_initdir
        super(RunJobEvent, self).setPilotInitDir(pilot_initdir)

    def getProxyCheckFlag(self):
        """ Getter for __proxycheckFlag """

        return self.__proxycheckFlag

    def setProxyCheckFlag(self, proxycheckFlag):
        """ Setter for __proxycheckFlag """

        self.__proxycheckFlag = proxycheckFlag

    def getGlobalPilotErrorDiag(self):
        """ Getter for __globalPilotErrorDiag """

        return self.__globalPilotErrorDiag

    def setGlobalPilotErrorDiag(self, pilotErrorDiag):
        """ Setter for __globalPilotErrorDiag """

        self.__globalPilotErrorDiag = pilotErrorDiag

    def getGlobalErrorCode(self):
        """ Getter for __globalErrorCode """

        return self.__globalErrorCode

    def setGlobalErrorCode(self, code):
        """ Setter for __globalErrorCode """

        self.__globalErrorCode = code

    def getErrorCode(self):
        """ Getter for __errorCode """

        return self.__errorCode

    def setErrorCode(self, code):
        """ Setter for __errorCode """

        self.__errorCode = code

    def getInputDir(self):
        """ Getter for __inputDir """

        return self.__inputDir

    def setInputDir(self, inputDir):
        """ Setter for __inputDir """

        self.__inputDir = inputDir
        super(RunJobEvent, self).setInputDir(inputDir)

    def getOutputDir(self):
        """ Getter for __outputDir """

        return self.__outputDir

    def setOutputDir(self, outputDir):
        """ Setter for __outputDir """

        self.__outputDir = outputDir

    def getEventLoopRunning(self):
        """ Getter for __event_loop_running """

        return self.__event_loop_running

    def setEventLoopRunning(self, event_loop_running):
        """ Setter for __event_loop_running """

        self.__event_loop_running = event_loop_running

    def getOutputFiles(self):
        """ Getter for __output_files """

        return self.__output_files

    def setOutputFiles(self, output_files):
        """ Setter for __output_files """

        self.__output_files = output_files

    def getGUIDList(self):
        """ Getter for __guid_list """

        return self.__guid_list

    def setGUIDList(self, guid_list):
        """ Setter for __guid_list """

        self.__guid_list = guid_list

    def getLFNList(self):
        """ Getter for __lfn_list """

        return self.__lfn_list

    def setLFNList(self, lfn_list):
        """ Setter for __lfn_list """

        self.__lfn_list = lfn_list

    def getUpdatedLFN(self):
        """ Getter for __updated_lfn """

        return self.__updated_lfn

    def setUpdatedLFN(self, updated_lfn):
        """ Setter for __updated_lfn """

        self.__updated_lfn = updated_lfn

    def getEventRangeDictionary(self):
        """ Getter for __eventRange_dictionary """

        return self.__eventRange_dictionary

    def setEventRangeDictionary(self, eventRange_dictionary):
        """ Setter for __eventRange_dictionary """

        self.__eventRange_dictionary = eventRange_dictionary

    def getEventRangeIDDictionary(self):
        """ Getter for __eventRangeID_dictionary """

        return self.__eventRangeID_dictionary

    def setEventRangeIDDictionary(self, eventRangeID_dictionary):
        """ Setter for __eventRangeID_dictionary """

        self.__eventRangeID_dictionary = eventRangeID_dictionary

    def getStageOutQueue(self):
        """ Getter for __stageout_queue """

        return self.__stageout_queue

    def setStageOutQueue(self, stageout_queue):
        """ Setter for __stageout_queue """

        self.__stageout_queue = stageout_queue

    def getPoolFileCatalogPath(self):
        """ Getter for __pfc_path """

        return self.__pfc_path

    def setPoolFileCatalogPath(self, pfc_path):
        """ Setter for __pfc_path """

        self.__pfc_path = pfc_path

    def getMessageServerPayload(self):
        """ Getter for __message_server_payload """

        return self.__message_server_payload

    def setMessageServerPayload(self, message_server):
        """ Setter for __message_server_payload """

        self.__message_server_payload = message_server

    def getMessageServerPrefetcher(self):
        """ Getter for __message_server_prefetcher """

        return self.__message_server_prefetcher

    def setMessageServerPrefetcher(self, message_server):
        """ Setter for __message_server_prefetcher """

        self.__message_server_prefetcher = message_server

    def getMessageThreadPayload(self):
        """ Getter for __message_thread_payload """

        return self.__message_thread_payload

    def setMessageThreadPayload(self, message_thread_payload):
        """ Setter for __message_thread_payload """

        self.__message_thread_payload = message_thread_payload

    def getMessageThreadPrefetcher(self):
        """ Getter for __message_thread_prefetcher """

        return self.__message_thread_prefetcher

    def setMessageThreadPrefetcher(self, message_thread_prefetcher):
        """ Setter for __message_thread_prefetcher """

        self.__message_thread_prefetcher = message_thread_prefetcher

    def isAthenaMPReady(self):
        """ Getter for __athenamp_is_ready """

        return self.__athenamp_is_ready

    def setAthenaMPIsReady(self, athenamp_is_ready):
        """ Setter for __athenamp_is_ready """

        self.__athenamp_is_ready = athenamp_is_ready

    def isPrefetcherReady(self):
        """ Getter for __prefetcher_is_ready """

        return self.__prefetcher_is_ready

    def setPrefetcherIsReady(self, prefetcher_is_ready):
        """ Setter for __prefetcher_is_ready """

        self.__prefetcher_is_ready = prefetcher_is_ready

    def prefetcherHasFinished(self):
        """ Getter for __prefetcher_has_finished """

        return self.__prefetcher_has_finished

    def setPrefetcherHasFinished(self, prefetcher_has_finished):
        """ Setter for __prefetcher_has_finished """

        self.__prefetcher_has_finished = prefetcher_has_finished

    def getAsyncOutputStagerThread(self):
        """ Getter for __asyncOutputStager_thread """

        return self.__asyncOutputStager_thread

    def setAsyncOutputStagerThread(self, asyncOutputStager_thread):
        """ Setter for __asyncOutputStager_thread """

        self.__asyncOutputStager_thread = asyncOutputStager_thread

    def getAnalysisJob(self):
        """ Getter for __analysisJob """

        return self.__analysisJob

    def setAnalysisJob(self, analysisJob):
        """ Setter for __analysisJob """

        self.__analysisJob = analysisJob

    def getCache(self):
        """ Getter for __cache """

        return self.__cache

    def setCache(self, cache):
        """ Setter for __cache """

        self.__cache = cache

    def getMetadataFilename(self):
        """ Getter for __cache """

        return self.__metadata_filename

    def setMetadataFilename(self, event_range_id):
        """ Setter for __metadata_filename """

        self.__metadata_filename = os.path.join(self.__job.workdir, "metadata-%s.xml" % (event_range_id))

    def getJobSite(self):
        """ Getter for __jobSite """

        return self.__jobSite

    def setJobSite(self, jobSite):
        """ Setter for __jobSite """

        self.__jobSite = jobSite

    def setJobNode(self, node):
        self.__node = node

    def getYamplChannelNamePayload(self):
        """ Getter for __yamplChannelNamePayload """

        return self.__yamplChannelNamePayload

    def setYamplChannelNamePayload(self, yamplChannelNamePayload):
        """ Setter for __yamplChannelNamePayload """

        self.__yamplChannelNamePayload = yamplChannelNamePayload

    def getYamplChannelNamePrefetcher(self):
        """ Getter for __yamplChannelNamePrefetcher """

        return self.__yamplChannelNamePrefetcher

    def setYamplChannelNamePrefetcher(self, yamplChannelNamePrefetcher):
        """ Setter for __yamplChannelNamePrefetcher """

        self.__yamplChannelNamePrefetcher = yamplChannelNamePrefetcher

    def getStatus(self):
        """ Getter for __status """

        return self.__status

    def setStatus(self, status):
        """ Setter for __status """

        self.__status = status

    def isSendingEventRange(self):
        """ Getter for __sending_event_range """

        return self.__sending_event_range

    def setSendingEventRange(self, sending_event_range):
        """ Setter for __sending_event_range """

        self.__sending_event_range = sending_event_range

    def getCurrentEventRange(self):
        """ Getter for __current_event_range """

        return self.__current_event_range

    def setCurrentEventRange(self, current_event_range):
        """ Setter for __current_event_range """

        self.__current_event_range = current_event_range

    def getMaxWaitOneEvent(self):
        """ Getter for __max_wait_for_one_event """

        return self.__max_wait_for_one_event

    def getMinEvents(self):
        """ Getter for __min_events """
        return self.__min_events

    def shouldBeAborted(self):
        """ Should the job be aborted? """

        if os.path.exists(os.path.join(self.__job.workdir, "ABORT")):
            return True
        else:
            return False

    def setAbort(self):
        """ Create the ABORT lock file """

        createLockFile(False, self.__job.workdir, lockfile="ABORT")

    def shouldBeKilled(self):
        """ Does the TOBEKILLED lock file exist? """

        path = os.path.join(self.__job.workdir, "TOBEKILLED")
        if os.path.exists(path):
            tolog("path exists: %s" % (path))
            return True
        else:
            tolog("path does not exist: %s" % (path))
            return False

    def setToBeKilled(self):
        """ Create the TOBEKILLED lock file"""

        createLockFile(False, self.__job.workdir, lockfile="TOBEKILLED")

    # Get/setters for the job object

    def getJob(self):
        """ Getter for __job """

        return self.__job

    def setJob(self, job):
        """ Setter for __job """

        self.__job = job

        # Reset the outFilesGuids list since guids will be generated by this module
        self.__job.outFilesGuids = []

    def getJobWorkDir(self):
        """ Getter for workdir """

        return self.__job.workdir

    def setJobWorkDir(self, workdir):
        """ Setter for workdir """

        self.__job.workdir = workdir

    def getJobID(self):
        """ Getter for jobId """

        return self.__job.jobId

    def setJobID(self, jobId):
        """ Setter for jobId """

        self.__job.jobId = jobId

    def getJobDataDir(self):
        """ Getter for datadir """

        return self.__job.datadir

    def setJobDataDir(self, datadir):
        """ Setter for datadir """

        self.__job.datadir = datadir

    def getJobTrf(self):
        """ Getter for trf """

        return self.__job.trf

    def setJobTrf(self, trf):
        """ Setter for trf """

        self.__job.trf = trf

    def getJobResult(self):
        """ Getter for result """

        return self.__job.result

    def setJobResult(self, result, pilot_failed=False):
        """ Setter for result """

        self.__job.result = result
        if pilot_failed:
            self.setFinalESStatus(self.__job)

    def getJobState(self):
        """ Getter for jobState """

        return self.__job.jobState

    def setJobState(self, jobState):
        """ Setter for jobState """

        self.__job.jobState = jobState

    def getJobStates(self):
        """ Getter for job states """

        return self.__job.result

    def setJobStates(self, states):
        """ Setter for job states """

        self.__job.result = states
        self.__job.currentState = states[0]

    def getTaskID(self):
        """ Getter for TaskID """

        return self.__taskID

    def setTaskID(self, taskID):
        """ Setter for taskID """

        self.__taskID = taskID

    def getJobOutFiles(self):
        """ Getter for outFiles """

        return self.__job.outFiles

    def setJobOutFiles(self, outFiles):
        """ Setter for outFiles """

        self.__job.outFiles = outFiles

    def getTokenExtractorInputListFilename(self):
        """ Getter for __tokenextractor_input_list_filenane """

        return self.__tokenextractor_input_list_filenane

    def setTokenExtractorInputListFilename(self, tokenextractor_input_list_filenane):
        """ Setter for __tokenextractor_input_list_filenane """

        self.__tokenextractor_input_list_filenane = tokenextractor_input_list_filenane

    def useEventIndex(self):
        """ Should the Event Index be used? """

        return self.__useEventIndex

    def setUseEventIndex(self, jobPars):
        """ Set the __useEventIndex variable to a boolean value """

        if "--createTAGFileForES" in jobPars:
            value = False
        else:
            value = True
        self.__useEventIndex = value

    def useTokenExtractor(self):
        """ Should the Token Extractor be used? """

        return self.__useTokenExtractor

    def setUseTokenExtractor(self, setup):
        """ Set the __useTokenExtractor variable to a boolean value """
        # Decision is based on info in the setup string

        self.__useTokenExtractor = 'TokenScatterer' in setup or 'UseTokenExtractor=True' in setup.replace("  ","").replace(" ","")

        if self.__useTokenExtractor:
            tolog("Token Extractor is needed")
        else:
            tolog("Token Extractor is not needed")

    def usePrefetcher(self):
        """ Should the Prefetcher be used? """

        return self.__usePrefetcher

    def setUsePrefetcher(self, usePrefetcher):
        """ Set the __usePrefetcher variable to a boolean value """

        self.__usePrefetcher = usePrefetcher

    def getInFilePosEvtNum(self):
        """ Should the event range numbers relative to in-file position be used? """

        return self.__inFilePosEvtNum

    def setInFilePosEvtNum(self, inFilePosEvtNum):
        """ Set the __inFilePosEvtNum variable to a boolean value """

        self.__inFilePosEvtNum = inFilePosEvtNum

    def getPanDAServer(self):
        """ Getter for __pandaserver """

        return self.__pandaserver

    def setPanDAServer(self, pandaserver):
        """ Setter for __pandaserver """

        self.__pandaserver = pandaserver

    def getAllowPrefetchEvents(self):
        return self.__allowPrefetchEvents

    def setAllowPrefetchEvents(self, allowPrefetchEvents):
        self.__allowPrefetchEvents = allowPrefetchEvents

    def init_guid_list(self):
        """ Init guid and lfn list for staged in files"""

        for guid in self.__job.inFilesGuids:
            self.__guid_list.append(guid)
        for lfn in self.__job.inFiles:
            self.__lfn_list.append(lfn)

    def init_input_files(self, job):
        """ Init input files list"""
        self.__input_files = job.get_stagedIn_files()

    def add_input_file(self, scope, name, pfn):
        """ Add a file to input files """
        self.__input_files['%s:%s' % (scope, name)] = pfn

    def get_input_files(self):
        return self.__input_files

    def addPFNsToEventRanges(self, eventRanges):
        """ Add the pfn's to the event ranges """
        # If an event range is file related, we need to add the pfn to the event range

        if self.getInFilePosEvtNum():
            for eventRange in eventRanges:
                key = '%s:%s' % (eventRange['scope'], eventRange['LFN'])
                if key in self.__input_files:
                    eventRange['PFN'] = self.__input_files[key]
                else:
                    eventRange['PFN'] = eventRange['LFN']
       	return eventRanges

    def addPFNToEventRange(self, eventRange):
        """ Add the pfn to an event range """
        # If an event range is file related, we need to add the pfn to the event range

        key = '%s:%s' % (eventRange['scope'], eventRange['LFN'])
        if key in self.__input_files:
            eventRange['PFN'] = self.__input_files[key]
        else:
            eventRange['PFN'] = eventRange['LFN']
        return eventRange

    def setAsyncOutputStagerSleepTime(self, sleep_time=600):
        self.__asyncOutputStager_thread_sleep_time = sleep_time

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        uuidgen = commands.getoutput('uuidgen')
        self.__yamplChannelNamePayload = "EventService_EventRanges-%s" % (uuidgen)
        self.__yamplChannelNamePrefetcher = "EventService_Prefetcher-%s" % (uuidgen)

    # is this necessary? doesn't exist in RunJob
    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobEvent, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobEvent, self).getRunJobFileName()

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return True

    def argumentParser(self):
        """ Argument parser for the RunJob module """

        # Return variables
        appdir = None
        queuename = None
        sitename = None
        workdir = None

        parser = OptionParser()
        parser.add_option("-a", "--appdir", dest="appdir",
                          help="The local path to the applications directory", metavar="APPDIR")
        parser.add_option("-b", "--queuename", dest="queuename",
                          help="Queue name", metavar="QUEUENAME")
        parser.add_option("-d", "--workdir", dest="workdir",
                          help="The local path to the working directory of the payload", metavar="WORKDIR")
        parser.add_option("-g", "--inputdir", dest="inputDir",
                          help="Location of input files to be transferred by the mv site mover", metavar="INPUTDIR")
        parser.add_option("-i", "--logfileguid", dest="logguid",
                          help="Log file guid", metavar="GUID")
        parser.add_option("-k", "--pilotlogfilename", dest="pilotlogfilename",
                          help="The name of the pilot log file", metavar="PILOTLOGFILENAME")
        parser.add_option("-l", "--pilotinitdir", dest="pilot_initdir",
                          help="The local path to the directory where the pilot was launched", metavar="PILOT_INITDIR")
        parser.add_option("-m", "--outputdir", dest="outputDir",
                          help="Destination of output files to be transferred by the mv site mover", metavar="OUTPUTDIR")
        parser.add_option("-o", "--parentworkdir", dest="pworkdir",
                          help="Path to the work directory of the parent process (i.e. the pilot)", metavar="PWORKDIR")
        parser.add_option("-s", "--sitename", dest="sitename",
                          help="The name of the site where the job is to be run", metavar="SITENAME")
        parser.add_option("-w", "--pilotserver", dest="pilotserver",
                          help="The URL of the pilot TCP server (localhost) WILL BE RETIRED", metavar="PILOTSERVER")
        parser.add_option("-p", "--pilotport", dest="pilotport",
                          help="Pilot TCP server port (default: 88888)", metavar="PORT")
        parser.add_option("-t", "--proxycheckflag", dest="proxycheckFlag",
                          help="True (default): perform proxy validity checks, False: no check", metavar="PROXYCHECKFLAG")
        parser.add_option("-x", "--stageinretries", dest="stageinretry",
                          help="The number of stage-in retries", metavar="STAGEINRETRY")
        #parser.add_option("-B", "--filecatalogregistration", dest="fileCatalogRegistration",
        #                  help="True (default): perform file catalog registration, False: no catalog registration", metavar="FILECATALOGREGISTRATION")
        parser.add_option("-E", "--stageoutretries", dest="stageoutretry",
                          help="The number of stage-out retries", metavar="STAGEOUTRETRY")
        parser.add_option("-F", "--experiment", dest="experiment",
                          help="Current experiment (default: ATLAS)", metavar="EXPERIMENT")
        parser.add_option("-H", "--cache", dest="cache",
                          help="Cache URL", metavar="CACHE")
        parser.add_option("-W", "--pandaserver", dest="pandaserver",
                          help="The full URL of the PanDA server (incl. port)", metavar="PANDASERVER")

        # options = {'experiment': 'ATLAS'}
        try:
            (options, args) = parser.parse_args()
        except Exception,e:
            tolog("!!WARNING!!3333!! Exception caught:" % (e))
            print options.experiment
        else:

            if options.appdir:
#                self.__appdir = options.appdir
                appdir = options.appdir
            if options.experiment:
                self.__experiment = options.experiment
            if options.logguid:
                self.__logguid = options.logguid
            if options.inputDir:
                self.__inputDir = options.inputDir
                self.setInputDir(self.__inputDir)
            if options.pilot_initdir:
                self.__pilot_initdir = options.pilot_initdir
                self.setPilotInitDir(self.__pilot_initdir)
            if options.pilotlogfilename:
                self.__pilotlogfilename = options.pilotlogfilename
            if options.pilotserver:
                self.__pilotserver = options.pilotserver
            if options.proxycheckFlag:
                if options.proxycheckFlag.lower() == "false":
                    self.__proxycheckFlag = False
                else:
                    self.__proxycheckFlag = True
            else:
                self.__proxycheckFlag = True
            if options.pworkdir:
                self.__pworkdir = options.pworkdir
                self.setParentWorkDir(self.__pworkdir)
            if options.outputDir:
                self.__outputDir = options.outputDir
            if options.pilotport:
                try:
                    self.__pilotport = int(options.pilotport)
                except Exception, e:
                    tolog("!!WARNING!!3232!! Exception caught: %s" % (e))
# self.__queuename is not needed
            if options.queuename:
                queuename = options.queuename
            if options.sitename:
                sitename = options.sitename
            if options.stageinretry:
                try:
                    self.__stageinretry = int(options.stageinretry)
                    self.setStageInRetry(self.__stageinretry)
                except Exception, e:
                    tolog("!!WARNING!!3232!! Exception caught: %s" % (e))
            if options.stageoutretry:
                try:
                    self.__stageoutretry = int(options.stageoutretry)
                except Exception, e:
                    tolog("!!WARNING!!3232!! Exception caught: %s" % (e))
            if options.workdir:
                workdir = options.workdir
            if options.cache:
                self.__cache = options.cache
            if options.pandaserver:
                self.__pandaserver = options.pandaserver

        # use sitename as queuename if queuename == ""
        if queuename == "":
            queuename = sitename

        return sitename, appdir, workdir, queuename

    def cleanup(self, rf=None):
        """ Cleanup function """
        # 'rf' is a list that will contain the names of the files that could be transferred
        # In case of transfer problems, all remaining files will be found and moved
        # to the data directory for later recovery.

        tolog("********************************************************")
        tolog(" This job ended with (trf,pilot) exit code of (%d,%d)" % (self.__job.result[1], self.__job.result[2]))
        tolog("********************************************************")

        # clean up the pilot wrapper modules
        pUtil.removePyModules(self.__job.workdir)

        if os.path.isdir(self.__job.workdir):
            os.chdir(self.__job.workdir)

            # remove input files from the job workdir
            remFiles = self.__job.inFiles
            for inf in remFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (self.__job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (self.__job.workdir, inf))
                    except Exception,e:
                        tolog("!!WARNING!!3000!! Ignore this Exception when deleting file %s: %s" % (inf, str(e)))
                        pass

            # only remove output files if status is not 'holding'
            # in which case the files should be saved for the job recovery.
            # the job itself must also have finished with a zero trf error code
            # (data will be moved to another directory to keep it out of the log file)

            # always copy the metadata-<jobId>.xml to the site work dir
            # WARNING: this metadata file might contain info about files that were not successfully moved to the SE
            # it will be regenerated by the job recovery for the cases where there are output files in the datadir

            try:
                copy2("%s/metadata-%s.xml" % (self.__job.workdir, self.__job.jobId), "%s/metadata-%s.xml" % (self.__pworkdir, self.__job.jobId))
            except Exception, e:
                tolog("Warning: Could not copy metadata-%s.xml to site work dir - ddm Adder problems will occure in case of job recovery" % \
                          (self.__job.jobId))

            if self.__job.result[0] == 'holding' and self.__job.result[1] == 0:
                try:
                    # create the data directory
                    os.makedirs(self.__job.datadir)
                except OSError, e:
                    tolog("!!WARNING!!3000!! Could not create data directory: %s, %s" % (self.__job.datadir, str(e)))
                else:
                    # find all remaining files in case 'rf' is not empty
                    remaining_files = []
                    moved_files_list = []
                    try:
                        if rf != None:
                            moved_files_list = RunJobUtilities.getFileNamesFromString(rf[1])
                            remaining_files = RunJobUtilities.getRemainingFiles(moved_files_list, self.__job.outFiles)
                    except Exception, e:
                        tolog("!!WARNING!!3000!! Illegal return value from Mover: %s, %s" % (str(rf), str(e)))
                        remaining_files = self.__job.outFiles

                    # move all remaining output files to the data directory
                    nr_moved = 0
                    for _file in remaining_files:
                        try:
                            os.system("mv %s %s" % (_file, self.__job.datadir))
                        except OSError, e:
                            tolog("!!WARNING!!3000!! Failed to move file %s (abort all)" % (_file))
                            break
                        else:
                            nr_moved += 1

                    tolog("Moved %d/%d output file(s) to: %s" % (nr_moved, len(remaining_files), self.__job.datadir))

                    # remove all successfully copied files from the local directory
                    nr_removed = 0
                    for _file in moved_files_list:
                        try:
                            os.system("rm %s" % (_file))
                        except OSError, e:
                            tolog("!!WARNING!!3000!! Failed to remove output file: %s, %s" % (_file, e))
                        else:
                            nr_removed += 1

                    tolog("Removed %d output file(s) from local dir" % (nr_removed))

                    # copy the PoolFileCatalog.xml for non build jobs
                    if not pUtil.isBuildJob(remaining_files):
                        _fname = os.path.join(self.__job.workdir, "PoolFileCatalog.xml")
                        tolog("Copying %s to %s" % (_fname, self.__job.datadir))
                        try:
                            copy2(_fname, self.__job.datadir)
                        except Exception, e:
                            tolog("!!WARNING!!3000!! Could not copy PoolFileCatalog.xml to data dir - expect ddm Adder problems during job recovery")

            # remove all remaining output files from the work directory
            # (a successfully copied file should already have been removed by the Mover)
            rem = False
            for inf in self.__job.outFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (self.__job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (self.__job.workdir, inf))
                    except Exception, e:
                        tolog("!!WARNING!!3000!! Ignore this Exception when deleting file %s: %s" % (inf, e))
                        pass
                    else:
                        tolog("Lingering output file removed: %s" % (inf))
                        rem = True
            if not rem:
                tolog("All output files already removed from local dir")

        tolog("Payload cleanup has finished")

    def sysExit(self, rf=None):
        '''
        wrapper around sys.exit
        rs is the return string from Mover::put_data() containing a list of files that were not transferred
        '''

        self.cleanup(rf=rf)
        sys.stderr.close()
        tolog("RunJobEvent (payload wrapper) has finished")

        # change to sys.exit?
        os._exit(self.__job.result[2]) # pilotExitCode, don't confuse this with the overall pilot exit code,
                                       # which doesn't get reported back to panda server anyway
    def failJob(self, transExitCode, pilotExitCode, job, ins=None, pilotErrorDiag=None, docleanup=True, pilot_failed=True):
        """ set the fail code and exit """

        job.setState(["failed", transExitCode, pilotExitCode])
        if pilot_failed:
            job.subStatus = 'pilot_failed'
        if pilotErrorDiag:
            job.pilotErrorDiag = pilotErrorDiag

        if pilotExitCode in [self.__error.ERR_NOEVENTS, self.__error.ERR_TOOFEWEVENTS]:
            job.subStatus = 'pilot_noevents'

        tolog("Will now update local pilot TCP server")
        rt = RunJobUtilities.updatePilotServer(job, self.__pilotserver, self.__pilotport, final=True)
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)
        if docleanup:
            self.sysExit()

    def getTrfExitInfo(self, exitCode, workdir):
        """ Get the trf exit code and info from job report if possible """

        exitAcronym = ""
        exitMsg = ""

        # does the job report exist?
        extension = getExtension(alternative='pickle')
        if extension.lower() == "json":
            _filename = "jobReport.%s" % (extension)
        else:
            _filename = "jobReportExtract.%s" % (extension)
        filename = os.path.join(workdir, _filename)

        # first backup the jobReport to the job workdir since it will be needed later
        # (the current location will disappear since it will be tarred up in the jobs' log file)
        d = os.path.join(workdir, '..')
        try:
            copy2(filename, os.path.join(d, _filename))
        except Exception, e:
            tolog("Warning: Could not backup %s to %s: %s" % (_filename, d, e))
        else:
            tolog("Backed up %s to %s" % (_filename, d))

        # It might take a short while longer until the job report is created (unknown why)
        count = 1
        max_count = 10
        nap = 5
        found = False
        while count <= max_count:
            if os.path.exists(filename):
                tolog("Found job report: %s" % (filename))
                found = True
                break
            else:
                tolog("Waiting %d s for job report to arrive (#%d/%d)" % (nap, count, max_count))
                time.sleep(nap)
                count += 1

        if found:
            # search for the exit code
            try:
                f = open(filename, "r")
            except Exception, e:
                tolog("!!WARNING!!1112!! Failed to open job report: %s" % (e))
            else:
                if extension.lower() == "json":
                    from json import load
                else:
                    from pickle import load
                data = load(f)

                # extract the exit code and info
                _exitCode = self.extractDictionaryObject("exitCode", data)
                if _exitCode:
                    if _exitCode == 0 and exitCode != 0:
                        tolog("!!WARNING!!1111!! Detected inconsistency in %s: exitcode listed as 0 but original trf exit code was %d (using original error code)" %\
                                  (filename, exitCode))
                    else:
                        exitCode = _exitCode
                _exitAcronym = self.extractDictionaryObject("exitAcronym", data)
                if _exitAcronym:
                    exitAcronym = _exitAcronym
                _exitMsg = self.extractDictionaryObject("exitMsg", data)
                if _exitMsg:
                    exitMsg = _exitMsg

                f.close()

                tolog("Trf exited with:")
                tolog("...exitCode=%d" % (exitCode))
                tolog("...exitAcronym=%s" % (exitAcronym))
                tolog("...exitMsg=%s" % (exitMsg))

                # Ignore special trf error for now
                if (exitCode == 65 and exitAcronym == "TRF_EXEC_FAIL") or (exitCode == 68 and exitAcronym == "TRF_EXEC_LOGERROR") or (exitCode == 66 and exitAcronym == "TRF_EXEC_VALIDATION_FAIL") or (exitCode == 11 and exitAcronym == "TRF_OUTPUT_FILE_ERROR"):
                    exitCode = 0
                    exitAcronym = ""
                    exitMsg = ""
                    tolog("!!WARNING!!3333!! Reset TRF error codes..")
        else:
            tolog("Job report not found: %s" % (filename))

        return exitCode, exitAcronym, exitMsg

    def get_site_info(self):
        if not self.__siteInfo:
            self.__siteInfo = getSiteInformation(self.__experiment)
            jobSite = self.getJobSite()
            queuename = jobSite.computingElement
            self.__siteInfo.setQueueName(queuename)
        return self.__siteInfo

    def resolveConfigItem(self, itemName):
        if not self.__siteInfo:
            self.__siteInfo = self.get_site_info()

        pandaqueue = self.__siteInfo.getQueueName()
        items = self.__siteInfo.resolveItems(pandaqueue, itemName)
        return items[pandaqueue]

    def initESConf(self, job=None):
        try:
            self.__job.outputZipName = os.path.join(self.__job.workdir, "EventService_premerge_%s" % self.__job.jobId)
            self.__job.outputZipEventRangesName = os.path.join(self.__job.workdir, "EventService_premerge_eventranges_%s.txt" % self.__job.jobId)
            catchalls = self.resolveConfigItem('catchall')

            if 'multiple_buckets' in catchalls:
                self.__multipleBuckets = 1
                tolog("Enable multiple_buckets without taskid")
            if 'multiple_buckets_with_taskid' in catchalls:
                self.__multipleBuckets = 2
                tolog("Enable multiple_buckets with taskid")
            if 'disable_multiple_buckets' in catchalls:
                self.__multipleBuckets = None
                tolog("Disable multiple_buckets")

            if "num_buckets=" in catchalls:
                for catchall in catchalls.split(","):
                    if 'num_buckets=' in catchall:
                        name, value = catchall.split('=')
                        self.__numBuckets = int(value)
                        if self.__numBuckets < 1:
                            tolog("Number of buckets %s is smaller than 1, set it to 1" % (self.__numBuckets))
                            self.__numBuckets = 1
                        if self.__numBuckets > 99:
                            tolog("Number of buckets %s is bigger than 99, set it to 99" % (self.__numBuckets))
                            self.__numBuckets = 99
            tolog("Number of buckets is %s" % (self.__numBuckets))

            if 'es_to_zip' in catchalls:
                self.__esToZip = True
            if 'not_es_to_zip' in catchalls:
                self.__esToZip = False
            if job.pandaProxySecretKey is not None and job.pandaProxySecretKey != "":
                self.__esToZip = False
                tolog("Disable tar/zip because job.pandaProxySecretKey is defined")

            pledgedcpu = self.resolveConfigItem('pledgedcpu')
            if pledgedcpu:
                try:
                    if int(pledgedcpu) == -1:
                        self.__asyncOutputStager_thread_sleep_time = 600
                        self.__allowPrefetchEvents = False
                    else:
                        self.__asyncOutputStager_thread_sleep_time = 3600 * 4
                except:
                    tolog("Failed to read pledgedcpu: %s" % traceback.format_exc())

            if 'disable_get_events_before_ready' in catchalls:
                self.__allowPrefetchEvents = False

            zip_time_gap = self.resolveConfigItem('zip_time_gap')
            if not (zip_time_gap is None or zip_time_gap == ''):
                try:
                    self.__asyncOutputStager_thread_sleep_time = int(zip_time_gap)
                except:
                    tolog("Failed to read zip time gap: %s" % traceback.format_exc())

            tolog("Sleep time between staging out: %s" % self.__asyncOutputStager_thread_sleep_time)

            if "max_wait_for_one_event=" in catchalls:
                for catchall in catchalls.split(","):
                    if 'max_wait_for_one_event=' in catchall:
                        name, value = catchall.split('=')
                        self.__max_wait_for_one_event = int(value)
            tolog("Max wait time for one event(minutes): %s" % self.__max_wait_for_one_event)

            if "min_events=" in catchalls:
                for catchall in catchalls.split(","):
                    if 'min_events=' in catchall:
                        name, value = catchall.split('=')
                        self.__min_events = int(value)
            tolog("Minimal events requirement: %s events" % self.__min_events)
        except:
            tolog("Failed to init zip cofnig: %s" % traceback.format_exc())

    def initAllowRemoteInputs(self, job):
        try:
            catchalls = self.resolveConfigItem('catchall')
            if 'allow_remote_inputs' in catchalls:
                self.__allow_remote_inputs = True
                job.setAllowRemoteInputs(self.__allow_remote_inputs)
            tolog("Allow remote inputs: %s" % self.__allow_remote_inputs)
        except:
            tolog("Failed to init allow_remote_inputs config: %s, it will use default value False" % traceback.format_exc())

    def convertToLFNs(self):
        """ Convert the output file names to LFNs """
        # Remove the file paths

        lfns = []
        for f in self.getOutputFiles():
            lfns.append(os.path.basename(f))

        return lfns

    def createFileMetadata(self, outsDict, dsname, datasetDict, sitename):
        """ create the metadata for the output + log files """

        ec = 0

        # get the file sizes and checksums for the local output files
        # WARNING: any errors are lost if occur in getOutputFileInfo()
        ec, pilotErrorDiag, fsize, checksum = pUtil.getOutputFileInfo(list(self.getOutputFiles()), "adler32", skiplog=True, logFile=self.__job.logFile)
        if ec != 0:
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            self.failJob(self.__job.result[1], ec, self.__job, pilotErrorDiag=pilotErrorDiag)

        # Get the correct log guid (a new one is generated for the Job() object, but we need to get it from the -i logguid parameter)
        if self.__logguid:
            guid = self.__logguid
        else:
            guid = self.__job.tarFileGuid

        # Convert the output file list to LFNs
        lfns = self.convertToLFNs()

        # Create preliminary metadata (no metadata yet about log file - added later in pilot.py)
        _fname = "%s/metadata-%s.xml" % (self.__job.workdir, self.__job.jobId)
        tolog("fguids=%s"%str(self.__job.outFilesGuids))

        lfns = []
        self.__job.outFilesGuids = []
        tolog("Reset output file LFN and GUID list (pilot will not report these to the server - xml shoould only contain log file info)")

        try:
            _status = pUtil.PFCxml(self.__experiment, _fname, fnlist=lfns, fguids=self.__job.outFilesGuids, fntag="lfn", alog=self.__job.logFile, alogguid=guid,\
                                       fsize=fsize, checksum=checksum, analJob=self.__analysisJob, logToOS=self.__job.putLogToOS)
        except Exception, e:
            pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
            tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag))
            self.failJob(self.__job.result[1], self.__error.ERR_MISSINGGUID, self.__job, pilotErrorDiag=pilotErrorDiag)
        else:
            if not _status:
                pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                self.failJob(self.__job.result[1], self.__error.ERR_MISSINGGUID, self.__job, pilotErrorDiag=pilotErrorDiag)

        tolog("NOTE: Output file info will not be sent to the server as part of xml metadata")
        tolog("..............................................................................................................")
        tolog("Created %s with:" % (_fname))
        tolog(".. log            : %s (to be transferred)" % (self.__job.logFile))
        tolog(".. log guid       : %s" % (guid))
        tolog(".. out files      : %s" % str(self.__job.outFiles))
        tolog(".. out file guids : %s" % str(self.__job.outFilesGuids))
        tolog(".. fsize          : %s" % str(fsize))
        tolog(".. checksum       : %s" % str(checksum))
        tolog("..............................................................................................................")

        # convert the preliminary metadata-<jobId>.xml file to OutputFiles-<jobId>.xml for NG and for CERNVM
        # note: for CERNVM this is only really needed when CoPilot is used
        if os.environ.has_key('Nordugrid_pilot') or sitename == 'CERNVM':
            if RunJobUtilities.convertMetadata4NG(os.path.join(self.__job.workdir, self.__job.outputFilesXML), _fname, outsDict, dsname, datasetDict):
                tolog("Metadata has been converted to NG/CERNVM format")
            else:
                self.__job.pilotErrorDiag = "Could not convert metadata to NG/CERNVM format"
                tolog("!!WARNING!!1999!! %s" % (self.__job.pilotErrorDiag))

        # try to build a file size and checksum dictionary for the output files
        # outputFileInfo: {'a.dat': (fsize, checksum), ...}
        # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
        # checksum for file a.dat: outputFileInfo['a.dat'][1]
        try:
            # remove the log entries
            _fsize = fsize[1:]
            _checksum = checksum[1:]
            outputFileInfo = dict(zip(self.__job.outFiles, zip(_fsize, _checksum)))
        except Exception, e:
            tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
            outputFileInfo = {}
        else:
            tolog("Output file info dictionary created: %s" % str(outputFileInfo))

        return ec, outputFileInfo

    def createFileMetadata4EventRange(self, outputFile, event_range_id):
        """ Create the metadata for an output file """

        # This function will create a metadata file called metadata-<event_range_id>.xml using file info
        # from PoolFileCatalog.xml
        # Return: ec, pilotErrorDiag, outputFileInfo, fname
        #         where outputFileInfo: {'<full path>/filename.ext': (fsize, checksum, guid), ...}
        #         (dictionary is used for stage-out)
        #         fname is the name of the metadata/XML file containing the file info above

        ec = 0
        pilotErrorDiag = ""
        outputFileInfo = {}

        # Get/assign a guid to the output file
        guid = getGUID()
        if guid == "":
            ec = self.__error.ERR_UUIDGEN
            pilotErrorDiag = "uuidgen failed to produce a guid"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag, None

        guid_list = [guid]
        tolog("Generated GUID %s for file %s" % (guid_list[0], outputFile))

        # Add the new guid to the outFilesGuids list
        self.__job.outFilesGuids.append(guid)

        # Get the file size and checksum for the local output file
        # WARNING: any errors are lost if occur in getOutputFileInfo()
        ec, pilotErrorDiag, fsize_list, checksum_list = pUtil.getOutputFileInfo([outputFile], "adler32", skiplog=True)
        if ec != 0:
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag, None
        else:
            tolog("fsize = %s" % str(fsize_list))
            tolog("checksum = %s" % str(checksum_list))

        # Create the metadata
        try:
            self.setMetadataFilename(event_range_id)
            fname = self.getMetadataFilename()
            tolog("Metadata filename = %s" % (fname))
        except Exception,e:
            tolog("!!WARNING!!2222!! Caught exception: %s" % (e))

        _status = pUtil.PFCxml(self.__experiment, fname, fnlist=[outputFile], fguids=guid_list, fntag="pfn", fsize=fsize_list,\
                                   checksum=checksum_list, analJob=self.__analysisJob)
        if not _status:
            pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag, None

        tolog("..............................................................................................................")
        tolog("Created %s with:" % (fname))
        tolog(".. output file      : %s" % (outputFile))
        tolog(".. output file guid : %s" % str(guid_list))
        tolog(".. fsize            : %s" % str(fsize_list))
        tolog(".. checksum         : %s" % str(checksum_list))
        tolog("..............................................................................................................")

        # Build a file size and checksum dictionary for the output file
        # outputFileInfo: {'a.dat': (fsize, checksum, guid), ...}
        # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
        # checksum for file a.dat: outputFileInfo['a.dat'][1]
        try:
            outputFileInfo = dict(zip([outputFile], zip(fsize_list, checksum_list, guid_list)))
        except Exception, e:
            tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
        else:
            tolog("Output file info dictionary created: %s" % str(outputFileInfo))

        return ec, pilotErrorDiag, outputFileInfo, fname

    def getDatasets(self):
        """ Get the datasets for the output files """

        # Get the default dataset
        if self.__job.destinationDblock and self.__job.destinationDblock[0] != 'NULL' and self.__job.destinationDblock[0] != ' ':
            dsname = self.__job.destinationDblock[0]
        else:
            dsname = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name

        # Create the dataset dictionary
        # (if None, the dsname above will be used for all output files)
        datasetDict = getDatasetDict(self.__job.outFiles, self.__job.destinationDblock, self.__job.logFile, self.__job.logDblock)
        if datasetDict:
            tolog("Dataset dictionary has been verified: %s" % str(datasetDict))
        else:
            tolog("Dataset dictionary could not be verified, output files will go to: %s" % (dsname))

        return dsname, datasetDict

    def stageOut(self, file_list, dsname, datasetDict, outputFileInfo, metadata_fname):
        """ Perform the stage-out """

        ec = 0
        pilotErrorDiag = ""
        os_bucket_id = -1

        rs = "" # return string from put_data with filename in case of transfer error
        tin_0 = os.times()
        try:
            ec, pilotErrorDiag, rf, rs, self.__job.filesNormalStageOut, self.__job.filesAltStageOut, os_bucket_id = mover.mover_put_data("xmlcatalog_file:%s" %\
                                         (metadata_fname), dsname, self.__jobSite.sitename, self.__jobSite.computingElement, analysisJob=self.__analysisJob, pinitdir=self.__pilot_initdir,\
                                         proxycheck=self.__proxycheckFlag, datasetDict=datasetDict, outputDir=self.__outputDir, outputFileInfo=outputFileInfo, stageoutTries=self.__stageoutretry,\
                                         eventService=True, job=self.__job, recoveryWorkDir=self.__job.workdir)
            tin_1 = os.times()
            self.__job.timeStageOut = int(round(tin_1[4] - tin_0[4]))
        except Exception, e:
            tin_1 = os.times()
            self.__job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

            if 'format_exc' in traceback.__all__:
                trace = traceback.format_exc()
                pilotErrorDiag = "Put function can not be called for staging out: %s, %s" % (str(e), trace)
            else:
                tolog("traceback.format_exc() not available in this python version")
                pilotErrorDiag = "Put function can not be called for staging out: %s" % (str(e))
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))

            ec = self.__error.ERR_PUTFUNCNOCALL
            self.__job.setState(["holding", self.__job.result[1], ec])
        else:
            self.__job.pilotErrorDiag = pilotErrorDiag
            if self.__job.pilotErrorDiag != "":
                self.__job.pilotErrorDiag = self.__job.pilotErrorDiag.replace("Put error:", "Objectstore stageout to bucket(%s) error:" % os_bucket_id)
                self.__job.pilotErrorDiag = tailPilotErrorDiag(self.__job.pilotErrorDiag, size=256-len("Objectstore stageout to bucket(%s) error:" % os_bucket_id))

            tolog("Put function returned code: %d" % (ec))
            if ec == 0:
                self.__job.pilotErrorDiag = ""

            if ec != 0:
                # is the job recoverable?
                if self.__error.isRecoverableErrorCode(ec):
                    _state = "holding"
                    _msg = "WARNING"
                else:
                    _state = "failed"
                    _msg = "FAILED"
                tolog("!!%s!!1212!! %s" % (_msg, self.__error.getErrorStr(ec)))

                # set the internal error, to be picked up at the end of the job
                self.setErrorCode(ec)

        return ec, pilotErrorDiag, os_bucket_id

    def getEventRangeID(self, filename):
        """ Return the event range id for the corresponding output file """

        event_range_id = ""
        for event_range in self.__eventRange_dictionary.keys():
            if self.__eventRange_dictionary[event_range][0] == filename:
                event_range_id = event_range
                break

        return event_range_id

    def transferToObjectStore(self, outputFileInfo, metadata_fname):
        """ Transfer the output file to the object store """

        # FORMAT:  outputFileInfo = {'<full path>/filename.ext': (fsize, checksum, guid), ...}
        # The dictionary will only contain info about a single file

        ec = 0
        pilotErrorDiag = ""
        os_bucket_id = -1

        # Get the site information object
        si = getSiteInformation(self.__experiment)

        # Get the queuename - which is only needed if objectstores field is not present in queuedata
        jobSite = self.getJobSite()
        queuename = jobSite.computingElement
        si.setQueueName(queuename)

        # Extract all information from the dictionary
        for path in outputFileInfo.keys():

            fsize = outputFileInfo[path][0]
            checksum = outputFileInfo[path][1]
            guid = outputFileInfo[path][2]

            # First backup some schedconfig fields that need to be modified for the secondary transfer
            copytool_org = readpar('copytool')

            # Temporarily modify the schedconfig fields with values
            tolog("Temporarily modifying queuedata for log file transfer to secondary SE")
            ec = si.replaceQueuedataField("copytool", "objectstore")

            # needs to know source, destination, fsize=0, fchecksum=0, **pdict, from pdict: lfn, guid, logPath
            # where source is local file path and destination is not used, set to empty string

            # Get the dataset name for the output file
            dsname, datasetDict = self.getDatasets()

            # Transfer the file
            ec, pilotErrorDiag, os_bucket_id = self.stageOut([path], dsname, datasetDict, outputFileInfo, metadata_fname)
            if ec == 0:
                os_ddmendpoint = si.getObjectstoreDDMEndpointFromBucketID(os_bucket_id)
                if os_ddmendpoint != "":
                    tolog("Files were transferred to objectstore ddm_endpoint=%s with os_bucket_id=%d" % (os_ddmendpoint, os_bucket_id))

                    # Add the transferred file to the OS transfer file
                    addToOSTransferDictionary(os.path.basename(path), self.__pilot_initdir, os_bucket_id, os_ddmendpoint)
                else:
                    tolog("!!WARNING!!5656!! OS DDM endpoint unknown - cannot add bucket id to OS transfer dictionary")

            # Finally restore the modified schedconfig fields
            tolog("Restoring queuedata fields")
            _ec = si.replaceQueuedataField("copytool", copytool_org)

        return ec, pilotErrorDiag, os_bucket_id

    def getTransientPathConvention(self, pathConvention=None):
        # To switch to use 'transient' scope for all ES files, we use a pathConvention which is 1000 + pathConvention
        return 1000 if pathConvention is None else pathConvention + 1000

    def get_storage_endpoint_and_id(self, ddmconf, siteInfo, pandaqueue, activity):
        tolog("[get_storage_endpoint_and_id] looking for associated storages with activity: %s" % (activity))
        associate_storages = siteInfo.resolvePandaAssociatedStorages(pandaqueue).get(pandaqueue, {})
        endpoints = associate_storages.get(activity, [])
        if endpoints:
            tolog("[get_storage_endpoint_and_id] found associated storages %s with activity: %s" % (endpoints, activity))
            return endpoints[0], ddmconf.get(endpoints[0], {}).get('id', -1)
        else:
            return None, None

    def is_blacklisted(self, endpoint):
        ddm_blacklisting_endpoints = []
        self.__siteInfo = self.get_site_info()
        ddm_blacklisting = self.__siteInfo.resolveDDMBlacklistingConf()
        for ddm_blacklisting_endpoint in ddm_blacklisting.keys():
            if 'w' in ddm_blacklisting[ddm_blacklisting_endpoint] \
                and 'mode' in ddm_blacklisting[ddm_blacklisting_endpoint]['w'] \
                and 'OFF' in ddm_blacklisting[ddm_blacklisting_endpoint]['w']['mode']:
                ddm_blacklisting_endpoints.append(ddm_blacklisting_endpoint)

        tolog("Blacklisted ddm endpoints: %s" % ddm_blacklisting_endpoints)
        if endpoint in ddm_blacklisting_endpoints:
            return True
        return False

    def resolve_os_access_keys(self, ddmconf, endpoint):
        storage_type = ddmconf.get(endpoint, {}).get('type', {})
        if storage_type and storage_type in ['OS_ES', 'OS_LOGS']:
            protocols = ddmconf.get(endpoint, {}).get('rprotocols', {})
            access_key = None
            secret_key = None
            is_secure = None
            for protocol in protocols:
                if 'w' in protocols[protocol].get('activities', []) or 'write_wan' in protocols[protocol].get('activities', []):
                    settings = protocols[protocol].get('settings', {})
                    access_key = settings.get('access_key', None)
                    secret_key = settings.get('secret_key', None)
                    is_secure = settings.get('is_secure', None)
                    if access_key is None or len(access_key) == 0\
                        or secret_key is None or len(secret_key) == 0\
                        or is_secure is None:
                        continue
                    else:
                        keyPair = self.__siteInfo.getSecurityKey(secret_key, access_key)
                        if "privateKey" not in keyPair or keyPair["privateKey"] is None:
                            tolog("Failed to get the keyPair for S3 objectstore from panda")
                        else:
                            return 0, {"publicKey": keyPair["publicKey"], "privateKey": keyPair["privateKey"], "is_secure": str(is_secure)}
        return -1, None

    def reolve_stageout_endpoints(self):
        storages = {'primary': None, 'failover': None}

        self.__siteInfo = self.get_site_info()

        # resolve accepted OS DDMEndpoints

        pandaqueue = self.__siteInfo.getQueueName() # FIX ME LATER
        ddmconf = self.__siteInfo.resolveDDMConf([])

        endpoint, storageId, activity = None, None, "es_events"
        if endpoint is None or storageId is None or storageId == -1:
            activity = "es_events" ## pilot log special/second transfer
            tolog("[reolve_stageout_endpoint] no osddms defined, looking for associated storages with activity: %s" % (activity))
            endpoint, storageId = self.get_storage_endpoint_and_id(ddmconf, self.__siteInfo, pandaqueue, activity)
            tolog("[reolve_stageout_endpoint] found associated storages with activity(%s): endpoint: %s, storageId: %s" % (activity, endpoint, storageId))
            if endpoint is None or storageId is None or storageId == -1:
                std_activity = 'pw'
                tolog("[reolve_stageout_endpoint] no ddm endpoints defined for %s, looking for associated storages with activity: %s" % (activity, std_activity))
                endpoint, storageId = self.get_storage_endpoint_and_id(ddmconf, self.__siteInfo, pandaqueue, std_activity)
                tolog("[reolve_stageout_endpoint] found associated storages with activity(%s): endpoint: %s, storageId: %s" % (activity, endpoint, storageId))
                activity = std_activity

        if not (endpoint is None or storageId is None or storageId == -1 or self.is_blacklisted(endpoint)):
            storage_type = ddmconf.get(endpoint, {}).get('type', {})
            if storage_type and storage_type in ['OS_ES', 'OS_LOGS']:
                ret_code, access_keys = self.resolve_os_access_keys(ddmconf, endpoint)
                if ret_code:
                    tolog("[reolve_stageout_endpoint] Failed to resolve os access keys for endpoint: %s, %s" % (endpoint, access_keys))
                    storages['primary'] = {'endpoint': endpoint, 'storageId': storageId, 'activity': activity, 'continousErrors': 0, 'success': 0, 'failed': 0}
                else:
                    storages['primary'] = {'endpoint': endpoint, 'storageId': storageId, 'activity': activity, 'continousErrors': 0, 'success': 0, 'failed': 0}
                    storages['primary']['access_keys'] = access_keys
            else:
                storages['primary'] = {'endpoint': endpoint, 'storageId': storageId, 'activity': activity, 'continousErrors': 0, 'success': 0, 'failed': 0}


        # resolve failover storages
        activity = "es_failover" ## pilot log special/second transfer
        tolog("[reolve_stageout_endpoint] looking for associated storages with activity: %s" % (activity))
        endpoint, storageId = self.get_storage_endpoint_and_id(ddmconf, self.__siteInfo, pandaqueue, activity)
        tolog("[reolve_stageout_endpoint] found associated storages with activity(%s): endpoint: %s, storageId: %s" % (activity, endpoint, storageId))
        if not (endpoint is None or storageId is None or storageId == -1 or self.is_blacklisted(endpoint)):
            storage_type = ddmconf.get(endpoint, {}).get('type', {})
            if storage_type and storage_type in ['OS_ES', 'OS_LOGS']:
                ret_code, access_keys = self.resolve_os_access_keys(ddmconf, endpoint)
                if ret_code:
                    tolog("[reolve_stageout_endpoint] Failed to resolve os access keys for endpoint: %s, %s" % (endpoint, access_keys))
                    storages['failover'] = {'endpoint': endpoint, 'storageId': storageId, 'activity': activity, 'continousErrors': 0, 'success': 0, 'failed': 0}
                else:
                    storages['failover'] = {'endpoint': endpoint, 'storageId': storageId, 'activity': activity, 'continousErrors': 0, 'success': 0, 'failed': 0}
                    storages['failover']['access_keys'] = access_keys
            else:
                storages['failover'] = {'endpoint': endpoint, 'storageId': storageId, 'activity': activity, 'continousErrors': 0, 'success': 0, 'failed': 0}

        tolog("[reolve_stageout_endpoint] resolved storages: primary: %s, failover: %s" % (storages['primary']['endpoint'] if storages['primary'] else None,
                                                                                           storages['failover']['endpoint'] if storages['failover'] else None))

        if storages['primary'] is None and storages['failover'] is None:
            return PilotErrors.ERR_NOSTORAGE, "Failed to reolve_stageout_endpoint: no associate storages: %s" % storages
        return 0, storages

    def stage_out_es(self, job, event_range_id, file_paths, pathConvention=None):
        """
        event_range_id: event range id as a string.
        file_paths: List of file paths.

        In ES, for one event range id, more can one output files can be produced.
        Only all these files are staged out successfully, the ret_code can be 0 and then the event range can be marked as finished.
        If one file in the list fails, the event range should be marked as failed.
        """
        tolog("To stage out event %s: %s" % (event_range_id, file_paths))

        if self.__stageoutStorages is None or\
            self.__stageoutStorages['primary'] and self.is_blacklisted(self.__stageoutStorages['primary']['endpoint']) or\
            self.__stageoutStorages['failover'] and self.is_blacklisted(self.__stageoutStorages['failover']['endpoint']):
            ret, storages = self.reolve_stageout_endpoints()
            if ret:
                 tolog("[stage_out_es] Failed to resolve stageout endpoints: %s, %s" % (ret, storages))
                 return PilotErrors.ERR_NOSTORAGE, "[stage_out_es] Failed to resolve stageout endpoints: %s, %s" % (ret, storages), None
            else:
                self.__stageoutStorages = storages

        ret_code, ret_str, os_bucket_id = PilotErrors.ERR_STAGEOUTFAILED, "Stageout failed", -1
        if self.__stageoutStorages['primary'] and (self.__stageoutStorages['primary']['continousErrors'] < 3 or random.randint(1, self.__stageoutStorages['primary']['continousErrors']) <2):
            tolog("[stage_out_es] Trying to stageout with primary storage: %s" % (self.__stageoutStorages['primary']['endpoint']))
            try:
                ret_code, ret_str, os_bucket_id = self.stage_out_es_real(job, event_range_id, file_paths, pathConvention=pathConvention, storage=self.__stageoutStorages['primary'])
            except Exception, e:
                tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
            if ret_code == 0:
                tolog("[stage_out_es] Successful to stageout to primary storage: %s" % (self.__stageoutStorages['primary']['endpoint']))
                self.__stageoutStorages['primary']['continousErrors'] = 0
                self.__stageoutStorages['primary']['success'] += 1
                return ret_code, ret_str, os_bucket_id
            else:
                tolog("[stage_out_es] Failed to stageout to primary storage(%s): %s, %s" % (self.__stageoutStorages['primary']['endpoint'], ret_code, ret_str))
                self.__stageoutStorages['primary']['continousErrors'] += 1
                self.__stageoutStorages['primary']['failed'] += 1
        else:
            tolog("[stage_out_es] Primary storage(%s) is not available or reached 3 times countinous errors(continousErrors:%s)" %
                  (self.__stageoutStorages['primary'], self.__stageoutStorages['primary']['continousErrors']))

        ret_code, ret_str, os_bucket_id = PilotErrors.ERR_STAGEOUTFAILED, "Stageout failed", -1
        if self.__stageoutStorages['failover']:
            tolog("[stage_out_es] Failover storage is defined. Trying to stageout with failover storage: %s" % (self.__stageoutStorages['failover']['endpoint']))
            try:
                ret_code, ret_str, os_bucket_id = self.stage_out_es_real(job, event_range_id, file_paths, pathConvention=pathConvention, storage=self.__stageoutStorages['failover'])
            except Exception, e:
                tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
            if ret_code == 0:
                tolog("[stage_out_es] Successful to stageout to failover storage: %s" % (self.__stageoutStorages['failover']['endpoint']))
                self.__stageoutStorages['failover']['continousErrors'] = 0
                self.__stageoutStorages['failover']['success'] += 1
                return ret_code, ret_str, os_bucket_id
            else:
                tolog("[stage_out_es] Failed to stageout to failover storage(%s): %s, %s" % (self.__stageoutStorages['failover']['endpoint'], ret_code, ret_str))
                self.__stageoutStorages['failover']['continousErrors'] += 1
                self.__stageoutStorages['failover']['failed'] += 1
        else:
            tolog("[stage_out_es] Failover storage(%s) is not available" % (self.__stageoutStorages['failover']))

        return ret_code, ret_str, os_bucket_id

    def stage_out_es_real(self, job, event_range_id, file_paths, pathConvention=None, storage=None):
        tolog("[stage-out-os] ddmendpoints %s,  storageId %s with activity %s" % (storage['endpoint'], storage['storageId'], storage['activity']))
        if 'access_keys' in storage:
            os.environ['S3_ACCESS_KEY'] = storage['access_keys']["publicKey"]
            os.environ['S3_SECRET_KEY'] = storage['access_keys']["privateKey"]
            os.environ['S3_IS_SECURE'] = storage['access_keys']['is_secure']
            tolog("[stage-out] [%s] resolved ddmendpoint=%s for es transfer with access key %s" % (storage['activity'], storage['endpoint'], os.environ.get('S3_ACCESS_KEY', None)))
        else:
            if 'S3_ACCESS_KEY' in os.environ:
                del os.environ['S3_ACCESS_KEY']
            if 'S3_SECRET_KEY' in os.environ:
                del os.environ['S3_SECRET_KEY']
            if 'S3_IS_SECURE' in os.environ:
                del os.environ['S3_IS_SECURE']

        try:
            osPublicKey = self.__siteInfo.getObjectstoresField("os_access_key", os_bucket_name="eventservice")
        except:
            tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
            osPublicKey = ""
        try:
            osPrivateKey = self.__siteInfo.getObjectstoresField("os_secret_key", os_bucket_name="eventservice")
        except:
            tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
            osPrivateKey = ""

        files = []
        for file_path in file_paths:
            file_dict = {'lfn': os.path.basename(file_path),
                         'pfn': file_path,
                         'dataset': job.destinationDblock[0],
                         'scope': 'transient',
                         'eventRangeId': event_range_id,
                         'storageId': storage['storageId'],
                         'pathConvention': self.getTransientPathConvention(pathConvention),
                         'ddmendpoint': storage['endpoint'],
                         'pandaProxySecretKey': job.pandaProxySecretKey,
                         'jobId':job.jobId,
                         'osPrivateKey':osPrivateKey,
                         'osPublicKey':osPublicKey
                         }
            finfo = Job.FileSpec(type='output', **file_dict)
            tolog(finfo)
            files.append(finfo)
            job.addStageOutESFiles(finfo)

        ret_code, ret_str, os_bucket_id = mover.put_data_es(job, jobSite=self.getJobSite(), stageoutTries=2, files=files, workDir=None, activity=storage['activity'], pinitdir=self.__pilot_initdir)
        if os_bucket_id is None or os_bucket_id == 0:
            os_bucket_id = -1
        return ret_code, ret_str, os_bucket_id


    def zipOutput(self, event_range_id, paths, output_name=None):
        """ Transfer the output file to the zip file """

        # FORMAT:  outputFileInfo = {'<full path>/filename.ext': (fsize, checksum, guid), ...}
        # The dictionary will only contain info about a single file

        ec = 0
        pilotErrorDiag = ""

        # Extract all information from the dictionary
        for path in paths:
            if output_name:
                command = "tar -rf " + output_name + " --directory=%s %s" %(os.path.dirname(path), os.path.basename(path))
            else:
                command = "tar -rf " + self.__job.outputZipName + " --directory=%s %s" %(os.path.dirname(path), os.path.basename(path))
            tolog("Adding file to zip: %s" % command)
            ec, pilotErrorDiag = commands.getstatusoutput(command)
            tolog("status: %s, output: %s\n" % (ec, pilotErrorDiag))
            if ec:
                tolog("Failed to zip %s: %s, %s" % (path, ec, pilotErrorDiag))
                return ec, pilotErrorDiag

        # tolog("Adding event range to zip event range file: %s %s" % (event_range_id, paths))
        handler = open(self.__job.outputZipEventRangesName, "a")
        handler.write("%s %s\n" % (event_range_id, paths))
        handler.close()

        return ec, pilotErrorDiag

    def getPathConvention(self, taskId, jobId):
        # __multipleBuckets:
        # 1: final path will be atlaseventservice_<pathConvention>
        # 2: final path will be atlaseventservice_<taskid>_<pathConvention>

        # __numBuckets is from 1 to 99

        # @returns: 
        #  if multiple buckets with task id: 100 + int(jobid) % __numBuckets
        #  if multiple buckets without task id: int(jobid) % __numBuckets

        if self.__multipleBuckets:
            if self.__multipleBuckets == 1:
                return int(jobId) % self.__numBuckets
            if self.__multipleBuckets == 2:
                return int(jobId) % self.__numBuckets + 100

        return None

    def checkSoftMessage(self, msg=None):
        job = self.getJob()
        if (msg and "tobekilled" in msg) or (job and pUtil.checkLockFile(job.workdir, "JOBWILLBEKILLED")):
            self.__isKilled = True
            tolog("The PanDA server has issued a hard kill command for this job - AthenaMP will be killed (current event range will be aborted)")
            self.setAbort()
            self.setToBeKilled()
            if job:
                job.subStatus = 'pilot_killed'
        if (msg and "softkill" in msg) or (job and pUtil.checkLockFile(job.workdir, "SOFTKILL")):
            tolog("The PanDA server has issued a soft kill command for this job - current event range will be allowed to finish")
            self.__isKilled = True
            self.sendMessage("No more events")
            if job:
                job.subStatus = 'softkilled'

    def stageOutZipFiles_new(self, output_name=None, output_eventRanges=None, output_eventRange_id=None):
        if not self.__esToZip:
            tolog("ES to zip is not configured")
            return 0, "ES to zip is not configured.", -1
        else:
            tolog("ES to zip is configured, will start to stage out zipped es file to objectstore")

        if not output_name:
            tolog("output name is None")
            return -1, "output name is None", -1
        if not output_eventRanges:
            tolog("output event range is None")
            return -1, "output event range is None", -1

        if not os.path.exists(output_name):
            tolog("Zip file %s doesn't exist, will not continue" % output_name)
            return -1, "Zip file doesn't exist", -1

        ec = 0
        pilotErrorDiag = ""
        os_bucket_id = -1

        if os.environ.has_key('Nordugrid_pilot'):
            outputDir = os.path.dirname(os.path.dirname(output_name))
            tolog("Copying tar/zip file %s to %s" % (output_name, os.path.join(outputDir, os.path.basename(output_name))))
            os.rename(output_name, os.path.join(outputDir, os.path.basename(output_name)))
            output_eventranges_file = os.path.join(outputDir, os.path.basename(output_name).replace(".tar", "_eventranges"))
            tolog("Creating eventranges file %s" % (output_eventranges_file))
            with open(output_eventranges_file) as handle:
                for eventRange in output_eventRanges:
                    handle.write("%s %s\n" % (eventRange, output_eventRanges[eventRange]))
            return 0, None

        pathConvention = self.getPathConvention(self.__job.taskID, self.__job.jobId)
        tolog("pathConvention: %s" % pathConvention)

        try:
            ec, pilotErrorDiag, os_bucket_id = self.stage_out_es(self.__job, output_eventRange_id, [output_name], pathConvention=pathConvention)
        except Exception, e:
            tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
        else:
            tolog("Adding %s to output file list" % (output_name))
            self.__output_files.append(output_name)
            # tolog("output_files = %s" % (self.__output_files))
            errorCode = None
            if ec == 0:
                status = 'finished'
                self.__isLastStageOutFailed = False
            else:
                status = 'failed'
                self.__isLastStageOutFailed = True
                errorCode = self.__error.ERR_STAGEOUTFAILED

                # Update the global status field in case of failure
                # self.setStatus(False)

            if errorCode:
                eventRanges = []
                for eventRangeID in output_eventRanges:
                    self.__nEventsFailed += 1
                    self.__nEventsFailedStagedOut += 1
                    eventRanges.append({'eventRangeID': eventRangeID, 'eventStatus': status, 'objstoreID': os_bucket_id, 'errorCode': errorCode})

                for chunkEventRanges in pUtil.chunks(eventRanges, 100):
                    # tolog("Update event ranges: %s" % chunkEventRanges)
                    status, output = updateEventRanges(chunkEventRanges, jobId = self.__job.jobId, url=self.getPanDAServer(), version=1, pandaProxySecretKey = self.__job.pandaProxySecretKey)
                    tolog("Update Event ranges status: %s, output: %s" % (status, output))
                    self.checkSoftMessage(output)
                self.__nStageOutFailures += 1
                self.__nStageOutSuccessAfterFailure = 0
            else:
                filesize = os.path.getsize(output_name)
                baseMover = BaseSiteMover()
                checksum, checksum_type = baseMover.calc_file_checksum(output_name)

                eventRanges = []
                for eventRangeID in output_eventRanges:
                    self.__nEventsW += 1
                    eventRanges.append({'eventRangeID': eventRangeID, 'eventStatus': status})

                numEvents = len(eventRanges)
                for chunkEventRanges in pUtil.chunks(eventRanges, 1000):
                    # tolog("Update event ranges: %s" % chunkEventRanges)
                    transientPathConvention = self.getTransientPathConvention(pathConvention)
                    if not transientPathConvention is None:
                        event_status = [{'eventRanges': chunkEventRanges, 'zipFile': {'lfn': os.path.basename(output_name), 'objstoreID': os_bucket_id, 'fsize': filesize, checksum_type: checksum, 'numEvents': numEvents, 'pathConvention': transientPathConvention}}]
                    else:
                        event_status = [{'eventRanges': chunkEventRanges, 'zipFile': {'lfn': os.path.basename(output_name), 'objstoreID': os_bucket_id, 'fsize': filesize, checksum_type: checksum, 'numEvents': numEvents}}]
                    status, output = updateEventRanges(event_status, url=self.getPanDAServer(), version=1, jobId = self.__job.jobId, pandaProxySecretKey = self.__job.pandaProxySecretKey)
                    tolog("Update Event ranges status: %s, output: %s" % (status, output))
                    if str(status) != '0':
                        tolog("Failed to update event ranges, keep it to re-update later")
                        self.__eventrangesToBeUpdated.append(event_status)
                    self.checkSoftMessage(output)
                self.__nStageOutSuccessAfterFailure += 1
                if self.__nStageOutSuccessAfterFailure > 10:
                    self.__nStageOutFailures = 0

    @mover.use_newmover(stageOutZipFiles_new)
    def stageOutZipFiles(self, output_name=None, output_eventRanges=None, output_eventRange_id=None):
        if not self.__esToZip:
            tolog("ES to zip is not configured")
            return 0, "ES to zip is not configured.", -1
        else:
            tolog("ES to zip is configured, will start to stage out zipped es file to objectstore")

        if not output_name:
            tolog("output name is None")
            return -1, "output name is None", -1
        if not output_eventRanges:
            tolog("output event range is None")
            return -1, "output event range is None", -1

        if not os.path.exists(output_name):
            tolog("Zip file %s doesn't exist, will not continue" % output_name)
            return -1, "Zip file doesn't exist", -1

        ec = 0
        pilotErrorDiag = ""
        os_bucket_id = -1

        if os.environ.has_key('Nordugrid_pilot'):
            outputDir = os.path.dirname(os.path.dirname(output_name))
            tolog("Copying tar/zip file %s to %s" % (output_name, os.path.join(outputDir, os.path.basename(output_name))))
            os.rename(output_name, os.path.join(outputDir, os.path.basename(output_name)))
            output_eventranges_file = os.path.join(outputDir, os.path.basename(output_name).replace(".tar", "_eventranges"))
            tolog("Creating eventranges file %s" % (output_eventranges))
            with open(output_eventranges_file) as handle:
                for eventRange in output_eventRanges:
                    handle.write("%s %s\n" % (eventRange, output_eventRanges[eventRange]))
            return 0, None

        tolog("Creating metadata for file %s and event range id %s" % (output_name, output_eventRange_id))
        ec, pilotErrorDiag, outputFileInfo, metadata_fname = self.createFileMetadata4EventRange(output_name, output_eventRange_id)
        if ec == 0:
            try:
                ec, pilotErrorDiag, os_bucket_id = self.transferToObjectStore(outputFileInfo, metadata_fname)
            except Exception, e:
                tolog("!!WARNING!!2222!! Caught exception: %s" % (e))
            else:
                tolog("Adding %s to output file list" % (output_name))
                self.__output_files.append(output_name)
                # tolog("output_files = %s" % (self.__output_files))
                errorCode = None
                if ec == 0:
                    status = 'finished'
                else:
                    status = 'failed'
                    errorCode = self.__error.ERR_STAGEOUTFAILED

                    # Update the global status field in case of failure
                    # self.setStatus(False)

            # Note: the rec pilot must update the server appropriately
            if errorCode:
                eventRanges = []
                for eventRangeID in output_eventRanges:
                    self.__nEventsFailed += 1
                    self.__nEventsFailedStagedOut += 1
                    eventRanges.append({'eventRangeID': eventRangeID, 'eventStatus': status, 'objstoreID': os_bucket_id, 'errorCode': errorCode})

                for chunkEventRanges in pUtil.chunks(eventRanges, 100):
                    # tolog("Update event ranges: %s" % chunkEventRanges)
                    status, output = updateEventRanges(chunkEventRanges, jobId = self.__job.jobId, url=self.getPanDAServer(), version=1, pandaProxySecretKey = self.__job.pandaProxySecretKey)
                    tolog("Update Event ranges status: %s, output: %s" % (status, output))
                    self.checkSoftMessage(output)
            else:
                eventRanges = []
                for eventRangeID in output_eventRanges:
                    self.__nEventsW += 1
                    eventRanges.append({'eventRangeID': eventRangeID, 'eventStatus': status})

                for chunkEventRanges in pUtil.chunks(eventRanges, 100):
                    # tolog("Update event ranges: %s" % chunkEventRanges)
                    event_status = [{'eventRanges': chunkEventRanges, 'zipFile': {'lfn': os.path.basename(output_name), 'objstoreID': os_bucket_id}}]
                    status, output = updateEventRanges(event_status, jobId = self.__job.jobId, url=self.getPanDAServer(), version=1, pandaProxySecretKey=self.__job.pandaProxySecretKey)
                    tolog("Update Event ranges status: %s, output: %s" % (status, output))
                    self.checkSoftMessage(output)
        else:
            tolog("!!WARNING!!1112!! Failed to create file metadata: %d, %s" % (ec, pilotErrorDiag))

    def syncStagedOutESFileStatus(self):
        try:
            tolog("Synchronizing staged out es files status to local file")
            job = self.getJob()
            retFiles = job.getStagedOutESFiles()

            esFilesStatus = "metadata_stagedOut_ES_%s.json" % job.jobId
            esFilesStatus = os.path.join(job.workdir, esFilesStatus)
            esFilesStatus_pre = esFilesStatus + ".pre"
            with open(esFilesStatus_pre, 'w') as fb:
                dump(retFiles, fb)
            os.rename(esFilesStatus_pre, esFilesStatus)
        except:
            tolog("Failed to sync staged out es files status to local file: %s" % traceback.format_exc())

    def startMessageThreadPayload(self):
        """ Start the message thread for the payload """

        self.__message_thread_payload.start()

    def stopMessageThreadPayload(self):
        """ Stop the message thread for the payload """

        self.__message_thread_payload.stop()

    def startMessageThreadPrefetcher(self):
        """ Start the message thread for the prefetcher """

        self.__message_thread_prefetcher.start()

    def stopMessageThreadPrefetcher(self):
        """ Stop the message thread for the prefetcher """

        self.__message_thread_prefetcher.stop()

    def joinMessageThreadPayload(self):
        """ Join the message thread for the payload """

        self.__message_thread_payload.join()

    def joinMessageThreadPrefetcher(self):
        """ Join the message thread for the prefetcher """

        self.__message_thread_prefetcher.join()

    def startAsyncOutputStagerThread(self):
        """ Start the asynchronous output stager thread """

        self.__asyncOutputStager_thread.start()

    def stopAsyncOutputStagerThread(self):
        """ Stop the asynchronous output stager thread """

        self.__asyncOutputStager_thread.stop()

    def joinAsyncOutputStagerThread(self):
        """ Join the asynchronous output stager thread """

        self.__asyncOutputStager_thread.join()

    def updateRemainingEventRanges(self):
        try:
            eventrangesToBeUpdated = self.__eventrangesToBeUpdated
            self.__eventrangesToBeUpdated = []
            for i in range(len(eventrangesToBeUpdated)):
                event_ranges_status = eventrangesToBeUpdated.pop(0)
                status, output = updateEventRanges(event_ranges_status, url=self.getPanDAServer(), version=1, jobId = self.__job.jobId, pandaProxySecretKey = self.__job.pandaProxySecretKey)
                tolog("Update Event ranges status: %s, output: %s" % (status, output))
                if str(status) != '0':
                    tolog("Failed to update event ranges, keep it to re-update later")
                    self.__eventrangesToBeUpdated.append(event_status)
                self.checkSoftMessage(output)
        except:
            tolog("!!WARNING!!2222!! Failed to updateRemainingEventRanges: %s" % (traceback.format_exc()))

    def asynchronousOutputStager_new(self):
        """ Transfer output files to stage-out area asynchronously """

        # Note: this is run as a thread

        sleep_time = 60
        finished_first_upload = False
        first_observe_iskilled = None
        run_time = time.time()
        update_event_ranges_time = time.time()
        tolog("Asynchronous output stager thread initiated")
        while not self.__asyncOutputStager_thread.stopped():
          try:
            if finished_first_upload:
                sleep_time = self.__asyncOutputStager_thread_sleep_time
            if self.__isLastStageOutFailed:
                sleep_time = 600
            if self.__isKilled:
                sleep_time = 5 * 60
                if first_observe_iskilled is None:
                    first_observe_iskilled = True

            if self.__eventrangesToBeUpdated and time.time() > update_event_ranges_time + 1800:
                update_event_ranges_time = time.time()
                self.updateRemainingEventRanges()

            if len(self.__stageout_queue) > 0 and (time.time() > run_time + sleep_time or first_observe_iskilled):
                tolog('Sleeped time: %s, is killed: %s' % (sleep_time, self.__isKilled))
                if first_observe_iskilled:
                    first_observe_iskilled = False
                if not finished_first_upload and len(self.__stageout_queue) < self.__job.coreCount:
                    tolog("Wait 1 minute for every core to finish one event.")
                    time.sleep(60)
                tolog("Asynchronous output stager thread working")
                run_time = time.time()
                if not self.__esToZip:
                    for paths in self.__stageout_queue:
                        # Create the output file metadata (will be sent to server)
                        tolog("Preparing to stage-out file %s" % (paths))
                        event_range_id = self.getEventRangeID(paths)
                        if event_range_id == "":
                            tolog("!!WARNING!!1111!! Did not find the event range for file %s in the event range dictionary" % (paths))
                        else:
                            try:
                                ec, pilotErrorDiag, os_bucket_id = self.stage_out_es(self.__job, event_range_id, paths)
                            except Exception, e:
                                tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
                                tolog("Removing %s from stage-out queue to prevent endless loop" % (paths))
                                self.__stageout_queue.remove(paths)
                            else:
                                tolog("Removing %s from stage-out queue" % (paths))
                                self.__stageout_queue.remove(paths)
                                tolog("Adding %s to output file list" % (paths))
                                for fpath in paths:
                                    self.__output_files.append(fpath)
                                # tolog("output_files = %s" % (self.__output_files))
                                errorCode = None
                                if ec == 0:
                                    status = 'finished'
                                    self.__nEventsW += 1
                                else:
                                    status = 'failed'
                                    self.__nEventsFailed += 1
                                    self.__nEventsFailedStagedOut += 1
                                    errorCode = self.__error.ERR_STAGEOUTFAILED

                                    # Update the global status field in case of failure
                                    self.setStatus(False)

                                try:
                                    # Time to update the server
                                    msg = updateEventRange(event_range_id, self.__eventRange_dictionary[event_range_id], self.__job.jobId, status=status, os_bucket_id=os_bucket_id, errorCode=errorCode, pandaProxySecretKey=self.__job.pandaProxySecretKey)

                                    # Did the updateEventRange back channel contain an instruction?
                                    if msg == "tobekilled":
                                        tolog("The PanDA server has issued a hard kill command for this job - AthenaMP will be killed (current event range will be aborted)")
                                        self.setAbort()
                                        self.setToBeKilled()
                                        job = self.getJob()
                                        if job:
                                            job.subStatus = 'pilot_killed'
                                    if msg == "softkill":
                                        tolog("The PanDA server has issued a soft kill command for this job - current event range will be allowed to finish")
                                        self.sendMessage("No more events")
                                        self.setAbort()
                                        job = self.getJob()
                                        if job:
                                            job.subStatus = 'pilot_killed'
                                except:
                                    tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
                else:
                    output_name = None
                    output_eventRange_id = None
                    output_eventRanges = {}
                    while len(self.__stageout_queue) > 0:
                        paths = self.__stageout_queue.pop()
                        #tolog("Pop %s from stage-out queue" % (paths))

                        # Create the output file metadata (will be sent to server)
                        tolog("Preparing to stage-out file %s" % (paths))
                        event_range_id = self.getEventRangeID(paths)
                        if event_range_id == "":
                            tolog("!!WARNING!!1111!! Did not find the event range for file %s in the event range dictionary" % (paths))
                        else:
                            if not output_name:
                                output_name = "%s_%s.tar" % (self.__job.outputZipName,event_range_id)
                                output_eventRange_id = event_range_id
                            try:
                                status, output = self.zipOutput(event_range_id, paths, output_name)
                            except:
                                tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
                            else:
                                output_eventRanges[event_range_id] = paths
                                # tolog("Adding %s to output file list" % (paths))
                                for fpath in paths:
                                    self.__output_files.append(fpath)
                                # tolog("output_files = %s" % (self.__output_files))
                    tolog("Files %s are zipped to %s" % (output_eventRanges, output_name))
                    self.stageOutZipFiles_new(output_name, output_eventRanges, output_eventRange_id)
                    finished_first_upload = True
                self.syncStagedOutESFileStatus()
            time.sleep(1)
          except:
               tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
        self.updateRemainingEventRanges()
        tolog("Asynchronous output stager thread has been stopped")

    @mover.use_newmover(asynchronousOutputStager_new)
    def asynchronousOutputStager(self):
        """ Transfer output files to stage-out area asynchronously """

        # Note: this is run as a thread
        sleep_time = self.__asyncOutputStager_thread_sleep_time
        run_time = time.time()
        tolog("Asynchronous output stager thread initiated")
        while not self.__asyncOutputStager_thread.stopped():
          try:
            sleep_time = self.__asyncOutputStager_thread_sleep_time
            if len(self.__stageout_queue) > 0 and time.time() > run_time + sleep_time:
                tolog("Asynchronous output stager thread working")
                run_time = time.time()
                if not self.__esToZip:
                    for paths in self.__stageout_queue:
                        # Create the output file metadata (will be sent to server)
                        tolog("Preparing to stage-out file %s" % (paths))
                        event_range_id = self.getEventRangeID(paths)
                        if event_range_id == "":
                            tolog("!!WARNING!!1111!! Did not find the event range for file %s in the event range dictionary" % (paths))
                        else:
                            ec = None
                            pilotErrorDiag = None
                            os_bucket_id = None
                            for f in paths:
                                tolog("Creating metadata for file %s and event range id %s" % (f, event_range_id))
                                ec, pilotErrorDiag, outputFileInfo, metadata_fname = self.createFileMetadata4EventRange(f, event_range_id)
                                if ec == 0:
                                    try:
                                        ec, pilotErrorDiag, os_bucket_id = self.transferToObjectStore(outputFileInfo, metadata_fname)
                                    except Exception, e:
                                        tolog("!!WARNING!!2222!! Caught exception: %s" % (e))
                                        ec = self.__error.ERR_STAGEOUTFAILED
                                        pilotErrorDiag = "Objectstore stageout error: %s" % str(e)

                                if not ec == 0:
                                    break

                            tolog("Removing %s from stage-out queue" % (paths))
                            self.__stageout_queue.remove(paths)
                            tolog("Adding %s to output file list" % (paths))
                            for fpath in paths:
                                self.__output_files.append(fpath)
                            # tolog("output_files = %s" % (self.__output_files))
                            errorCode = None
                            if ec == 0:
                                status = 'finished'
                                self.__nEventsW += 1
                            else:
                                status = 'failed'
                                self.__nEventsFailed += 1
                                self.__nEventsFailedStagedOut += 1
                                errorCode = self.__error.ERR_STAGEOUTFAILED

                                # Update the global status field in case of failure
                                self.setStatus(False)

                                # Note: the rec pilot must update the server appropriately

                            try:
                                # Time to update the server
                                msg = updateEventRange(event_range_id, self.__eventRange_dictionary[event_range_id], self.__job.jobId, status=status, os_bucket_id=os_bucket_id, errorCode=errorCode, pandaProxySecretKey=self.__job.pandaProxySecretKey)

                                # Did the updateEventRange back channel contain an instruction?
                                if msg == "tobekilled":
                                    tolog("The PanDA server has issued a hard kill command for this job - AthenaMP will be killed (current event range will be aborted)")
                                    self.setAbort()
                                    self.setToBeKilled()
                                    job = self.getJob()
                                    if job:
                                        job.subStatus = 'pilot_killed'
                                if msg == "softkill":
                                    tolog("The PanDA server has issued a soft kill command for this job - current event range will be allowed to finish")
                                    self.sendMessage("No more events")
                                    self.setAbort()
                                    job = self.getJob()
                                    if job:
                                        job.subStatus = 'pilot_killed'
                            except:
                                tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
                else:
                    output_name = None
                    output_eventRange_id = None
                    output_eventRanges = {}
                    while len(self.__stageout_queue) > 0:
                        paths = self.__stageout_queue.pop()
                        tolog("Pop %s from stage-out queue" % (paths))

                        # Create the output file metadata (will be sent to server)
                        tolog("Preparing to stage-out file %s" % (paths))
                        event_range_id = self.getEventRangeID(paths)
                        if event_range_id == "":
                            tolog("!!WARNING!!1111!! Did not find the event range for file %s in the event range dictionary" % (paths))
                        else:
                            if not output_name:
                                output_name = "%s_%s.tar" % (self.__job.outputZipName,event_range_id)
                                output_eventRange_id = event_range_id
                            try:
                                status, output = self.zipOutput(event_range_id, paths, output_name)
                            except:
                                tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
                            else:
                                output_eventRanges[event_range_id] = paths
                                tolog("Adding %s to output file list" % (paths))
                                for fpath in paths:
                                    self.__output_files.append(fpath)
                                # tolog("output_files = %s" % (self.__output_files))
                    tolog("Files %s are zipped to %s" % (output_eventRanges, output_name))
                    self.stageOutZipFiles(output_name, output_eventRanges, output_eventRange_id)

            time.sleep(1)
          except:
               tolog("!!WARNING!!2222!! Caught exception: %s" % (traceback.format_exc()))
        tolog("Asynchronous output stager thread has been stopped")

    def payloadListener(self):
        """ Listen for messages from the payload """

        # Note: this is run as a thread

        # Listen for messages as long as the thread is not stopped
        while not self.__message_thread_payload.stopped():

            try:
                # Receive a message
                tolog("Waiting for a new message")
                size, buf = self.__message_server_payload.receive()
                while size == -1 and not self.__message_thread_payload.stopped():
                    time.sleep(0.1)
                    size, buf = self.__message_server_payload.receive()
                tolog("Received new message from Payload: %s" % (buf))

                max_wait = 600
                i = 0
                if self.__sending_event_range:
                    tolog("Will wait for current event range to finish being sent (pilot not yet ready to process new request)")
                while self.__sending_event_range:
                    # Wait until previous send event range has completed (to avoid racing condition), but wait maximum 60 seconds then fail job
                    time.sleep(0.1)
                    if i > max_wait:
                        # Abort with error
                        buf = "ERR_FATAL_STUCK_SENDING %s: Stuck sending event range to payload; new message: %s" % (self.__current_event_range, buf)
                        break
                    i += 1
                if i > 0:
                    tolog("Delayed %d s for send message to complete" % (i*10))

#                if not "Ready for" in buf:
#                    if self.__eventRangeID_dictionary.keys():
#                        try:
#                            keys = self.__eventRangeID_dictionary.keys()
#                            key = keys[0]
#                            tolog("Faking error for range = %s" % (key))
#                            buf = "ERR_TE_RANGE %s: Range contains wrong positional number 5001" % (key)
#                        except Exception,e:
#                            tolog("No event ranges yet:%s" % (e))
#                    #buf = "ERR_TE_FATAL Range-2: CURL curl_easy_perform() failed! Couldn't resolve host name"
#                    #buf = "ERR_TE_FATAL 5211313-2452346274-2058479689-3-8: URL No tokens for GUID 00224B03-8005-E849-BCD5-D8F8F764B630"

                # Interpret the message and take the appropriate action
                if "Ready for events" in buf:
                    buf = ""
                    #tolog("AthenaMP is ready for events")
                    self.__athenamp_is_ready = True

                elif buf.startswith('/'):
                    # tolog("Received file and process info from client: %s" % (buf))

                    self.__nEvents += 1

                    # Extract the information from the message
                    paths, event_range_id, cpu, wall = self.interpretMessage(buf)
                    if paths and paths not in self.__stageout_queue:
                        # Add the extracted info to the event range dictionary
                        self.__eventRange_dictionary[event_range_id] = [paths, cpu, wall]

                        # Add the file to the stage-out queue
                        self.__stageout_queue.append(paths)
                        # tolog("File %s has been added to the stage-out queue (length = %d)" % (paths, len(self.__stageout_queue)))

                elif buf.startswith('['):
                    tolog("Received an updated event range message from Prefetcher: %s" % (buf))
                    self.__current_event_range = buf

                    # Set the boolean to True since Prefetcher is now ready (finished with the current event range)
                    runJob.setPrefetcherIsReady(True)

                elif buf.startswith('ERR'):
                    tolog("Received an error message: %s" % (buf))

                    # Extract the error acronym and the error diagnostics
                    error_acronym, event_range_id, error_diagnostics = self.extractErrorMessage(buf)
                    if event_range_id != "":
                        tolog("!!WARNING!!2144!! Extracted error acronym %s and error diagnostics \'%s\' for event range %s" % (error_acronym, error_diagnostics, event_range_id))

                        error_code = None
                        event_status = 'fatal'
                        # Was the error fatal? If so, the pilot should abort
                        if "FATAL" in error_acronym:
                            tolog("!!WARNING!!2146!! A FATAL error was encountered, prepare to finish")

                            # Fail the job
                            if error_acronym == "ERR_TE_FATAL" and "URL Error" in error_diagnostics:
                                error_code = self.__error.ERR_TEBADURL
                            elif error_acronym == "ERR_TE_FATAL" and "resolve host name" in error_diagnostics:
                                error_code = self.__error.ERR_TEHOSTNAME
                            elif error_acronym == "ERR_TE_FATAL" and "Invalid GUID length" in error_diagnostics:
                                error_code = self.__error.ERR_TEINVALIDGUID
                            elif error_acronym == "ERR_TE_FATAL" and "No tokens for GUID" in error_diagnostics:
                                error_code = self.__error.ERR_TEWRONGGUID
                            elif error_acronym == "ERR_TE_FATAL":
                                error_code = self.__error.ERR_TEFATAL
                            else:
                                error_code = self.__error.ERR_ESFATAL
                            self.__esFatalCode = error_code
                        else:
                            error_code = self.__error.ERR_UNKNOWN

                        # Time to update the server
                        self.__nEventsFailed += 1
                        msg = updateEventRange(event_range_id, [], self.__job.jobId, status=event_status, errorCode=error_code, pandaProxySecretKey=self.__job.pandaProxySecretKey)
                        if msg != "":
                            tolog("!!WARNING!!2145!! Problem with updating event range: %s" % (msg))
                        else:
                            tolog("Updated server for failed event range")

                        if error_code:
                            result = ["failed", 0, error_code]
                            tolog("Setting error code: %d" % (error_code))
                            self.setJobResult(result, pilot_failed=True)

                            # ..
                    else:
                        tolog("!!WARNING!!2245!! Extracted error acronym %s and error diagnostics \'%s\' (event range could not be extracted - cannot update server)" % (error_acronym, error_diagnostics))

                else:
                    tolog("Pilot received message:%s" % buf)
            except Exception, e:
                tolog("Caught exception:%s" % e)
            time.sleep(0.1)

        tolog("Payload listener has finished")

    def prefetcherListener(self):
        """ Listen for messages from the prefetcher """

        # Note: this is run as a thread

        # Listen for messages as long as the thread is not stopped
        while not self.__message_thread_prefetcher.stopped():

            try:
                # Receive a message
                # tolog("Waiting for a new message")
                size, buf = self.__message_server_prefetcher.receive()
                while size == -1 and not self.__message_thread_prefetcher.stopped():
                    time.sleep(1)
                    size, buf = self.__message_server_prefetcher.receive()
                tolog("Received new message from Prefetcher: %s" % (buf))

                # Interpret the message and take the appropriate action
                if "Ready for events" in buf:
                    buf = ""
                    tolog("Prefetcher is ready for an event range")
                    # Set the boolean to True since Prefetcher is now ready to receive a new event range
                    self.__prefetcher_is_ready = True

                elif buf.startswith('/'):
                    tolog("Received an updated LFN path from Prefetcher: %s" % (buf))
                    # /home/tmp/Panda_Pilot_87984_1490352234/PandaJob_3301909532_1490352238/athenaMP-workers-EVNTMerge-None/worker_0/localRange.pool.root_000.10982162-3301909532-8861875445-1-5,ID:10982162-3301909532-8861875445-1-5,CPU:0,WALL:0
                    self.__updated_lfn = buf.split(',')[0]

                    # Set the booleans to True since Prefetcher has finished and is now ready to start again
                    runJob.setPrefetcherIsReady(True)
                    self.__prefetcher_has_finished = True

                elif buf.startswith('ERR'):
                    tolog("Received an error message: %s" % (buf))

                    # Extract the error acronym and the error diagnostics
                    error_acronym, event_range_id, error_diagnostics = self.extractErrorMessage(buf)
                    if event_range_id != "":
                        tolog("!!WARNING!!2144!! Extracted error acronym %s and error diagnostics \'%s\' for event range %s" % (error_acronym, error_diagnostics, event_range_id))

                        error_code = None
                        event_status = 'fatal'
                        # Was the error fatal? If so, the pilot should abort
                        if "FATAL" in error_acronym:
                            tolog("!!WARNING!!2146!! A FATAL error was encountered, prepare to finish")

                            # Fail the job
                            if error_acronym == "ERR_TE_FATAL" and "URL Error" in error_diagnostics:
                                error_code = self.__error.ERR_TEBADURL
                            elif error_acronym == "ERR_TE_FATAL" and "resolve host name" in error_diagnostics:
                                error_code = self.__error.ERR_TEHOSTNAME
                            elif error_acronym == "ERR_TE_FATAL" and "Invalid GUID length" in error_diagnostics:
                                error_code = self.__error.ERR_TEINVALIDGUID
                            elif error_acronym == "ERR_TE_FATAL" and "No tokens for GUID" in error_diagnostics:
                                error_code = self.__error.ERR_TEWRONGGUID
                            elif error_acronym == "ERR_TE_FATAL":
                                error_code = self.__error.ERR_TEFATAL
                            else:
                                error_code = self.__error.ERR_ESFATAL
                            self.__esFatalCode = error_code
                        else:
                            error_code = self.__error.ERR_UNKNOWN

                        # Time to update the server
                        self.__nEventsFailed += 1
                        msg = updateEventRange(event_range_id, [], self.__job.jobId, status=event_status, errorCode=error_code)
                        if msg != "":
                            tolog("!!WARNING!!2145!! Problem with updating event range: %s" % (msg))
                        else:
                            tolog("Updated server for failed event range")

                        if error_code:
                            # result = ["failed", 0, error_code]
                            tolog("Error code: %d, send 'No more events' to stop AthenaMP" % (error_code))
                            # self.setJobResult(result, pilot_failed=True)
                            self.sendMessage("No more events")
                    else:
                        tolog("!!WARNING!!2245!! Extracted error acronym %s and error diagnostics \'%s\' (event range could not be extracted - cannot update server)" % (error_acronym, error_diagnostics))

                else:
                    tolog("Pilot received message:%s" % buf)
            except Exception, e:
                tolog("Caught exception:%s" % e)
            time.sleep(1)

        tolog("Prefetcher listener has finished")

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
                    tolog("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
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
                                tolog("!!WARNING!!2212!! Failed to extract event_range_id: %s" % (e))
                            else:
                                tolog("Extracted event_range_id: %s" % (event_range_id))
                    else:
                        tolog("!!WARNING!!2213!1 event_range_id not found in event_range: %s" % (event_range))
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
                    tolog("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "EXTRACTION_FAILURE"
                    error_diagnostics = e
            else:
                tolog("!!WARNING!!2212!! Failed to extract AthenaMP message")
                error_acronym = "EXTRACTION_FAILURE"
                error_diagnostics = msg

        return error_acronym, event_range_id, error_diagnostics

    def correctFileName(self, path, event_range_id):
        """ Correct the output file name if necessary """

        # Make sure the output file name follows the format OUTPUT_FILENAME_FROM_JOBDEF.EVENT_RANGE_ID

        outputFileName = self.__job.outFiles[0]
        if outputFileName != "":
            fname = os.path.basename(path)
            dirname = os.path.dirname(path)

            constructedFileName = outputFileName + "." + event_range_id
            if fname == constructedFileName:
                tolog("Output file name verified")
            else:
                tolog("Output file name does not follow convension: OUTPUT_FILENAME_FROM_JOBDEF.EVENT_RANGE_ID: %s" % (fname))
                fname = constructedFileName
                _path = os.path.join(dirname, fname)
                cmd = "mv %s %s" % (path, _path)
                out = commands.getoutput(cmd)
                path = _path
                tolog("Corrected output file name: %s" % (path))

        return path

    def interpretMessage(self, msg):
        """ Interpret a yampl message containing file and processing info """

        # The message is assumed to have the following format
        # Format: "<file_path1>,<file_path2>, .. ,ID:<event_range_id>,CPU:<number_in_sec>,WALL:<number_in_sec>"
        # Return: [paths], event_range_id, cpu time (s), wall time (s)

        paths = []
        event_range_id = ""
        cpu = ""
        wall = ""

        if "," in msg:
            for message in msg.split(","):
                if not ":" in message:
                    paths.append(message)
                elif message.startswith("ID"):
                    event_range_id = message.split(":")[1]
                elif message.startswith("CPU"):
                    cpu = message.split(":")[1]
                elif message.startswith("WALL"):
                    wall = message.split(":")[1]
                else:
                    tolog("!!WARNING!!3535!! Unsupported identifier: %s" % (message))

            # Correct for older format where ID was not present. In this case, the last 'path' is actually the event_range_id
            if not "ID" in msg:
                event_range_id = paths.pop()
        else:
            tolog("!!WARNING!!1122!! Unknown yampl message format: missing commas: %s" % (msg))

        return paths, event_range_id, cpu, wall

    def getTokenExtractorInputListEntry(self, input_file_guid, input_filename):
        """ Prepare the guid and filename string for the token extractor file with the proper format """

        return "%s,PFN:%s\n" % (input_file_guid.upper(), input_filename)

    def getTokenExtractorProcess(self, thisExperiment, setup, input_file, input_file_guid, stdout=None, stderr=None, url=""):
        """ Execute the TokenExtractor """

        options = ""

        # Should the event index be used or should a tag file be used?
        if url == "" and self.__useEventIndex:
            tolog("!!WARNING!!5656!! Event index URL not specified (switching off event index mode)")
            self.__useEventIndex = False

        if not self.__useEventIndex:
            # In this case, the input file is the tag file
            # First create a file with format: <guid>,PFN:<input_tag_file>
            filename = os.path.join(os.getcwd(), "tokenextractor_input_list.txt")
            self.setTokenExtractorInputListFilename(filename) # needed later when we add the files from the event ranges
            s = self.getTokenExtractorInputListEntry(input_file_guid, input_file)
            status = writeToFileWithStatus(filename, s)

            # Define the options
            options += "-v --source %s" % (filename)

        else:
            # In this case the input file is an EVT file
            # Define the options
            options = '-v -e -s \"%s\"' % (url)

        # Define the command
        cmd = "%s TokenExtractor %s" % (setup, options)

        # Execute and return the TokenExtractor subprocess object
        return self.getSubprocess(thisExperiment, cmd, stdout=stdout, stderr=stderr)

    def getPrefetcherProcess(self, thisExperiment, setup, input_file, stdout=None, stderr=None):
        """ Execute the Prefetcher """
        # The input file corresponds to a remote input file (full path)

        # Prefix of the local file names
        prefix = "localRange.pool.root"
        options = "'--inputEVNTFile' %s '--outputEVNT_MRGFile' %s '--eventService=True' '--preExec' 'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"'" % (input_file, prefix, self.__yamplChannelNamePrefetcher)

        # Define the command
        cmd = "%s export ATHENA_PROC_NUMBER=1; EVNTMerge_tf.py %s" % (setup, options)

        # Execute and return the Prefetcher subprocess object
        return self.getSubprocess(thisExperiment, cmd, stdout=stdout, stderr=stderr)

    def createMessageServer(self, prefetcher=False):
        """ Create the message server socket object """
        # Create a message server for the payload by default, otherwise for Prefetcher if prefetcher is set

        status = False

        # Create the server socket
        if MessageServer:
            if prefetcher:
                self.__message_server_prefetcher = MessageServer(socketname=self.__yamplChannelNamePrefetcher, context='local')

                # is the server alive?
                if not self.__message_server_prefetcher.alive():
                    # destroy the object
                    tolog("!!WARNING!!3333!! Message server for Prefetcher is not alive")
                    self.__message_server_prefetcher = None
                else:
                    status = True
            else:
                self.__message_server_payload = MessageServer(socketname=self.__yamplChannelNamePayload, context='local')

                # is the server alive?
                if not self.__message_server_payload.alive():
                    # destroy the object
                    tolog("!!WARNING!!3333!! Message server for the payload is not alive")
                    self.__message_server_payload = None
                else:
                    status = True
        else:
            tolog("!!WARNING!!3333!! MessageServer object is not available")

        return status

    def getTAGFileInfo(self, inFiles, guids):
        """ Extract the TAG file from the input files list """

        # Note: assume that there is only one TAG file
        tag_file = ""
        guid = ""
        i = -1

        if len(inFiles) == len(guids):
            for f in inFiles:
                i += 1
                if ".TAG." in f:
                    tag_file = f
                    break
            i = -1
            for f in inFiles:
                i += 1
                if not ".TAG." in f: # fix this, just added 'not' to get theother guid - won't work of course in thelong run
                    guid = guids[i]
                    break
        else:
            tolog("!!WARNING!!2121!! Input file list not same length as guid list")

        return tag_file, guid

    def sendMessage(self, message, prefetcher=False):
        """ Send a message """
        # Message will be sent to the payload by default, or to the Prefetcher in case prefetcher is set

        # Filter away unwanted fields
        if "scope" in message:
            # First replace an ' with " since loads() cannot handle ' signs properly
            # Then convert to a list and get the 0th element (there should be only one)
            try:
                #_msg = loads(message.replace("'",'"'))[0]
                _msg = loads(message.replace("'",'"').replace('u"','"'))[0]
            except Exception, e:
                tolog("!!WARNING!!2233!! Caught exception: %s" % (e))
            else:
                # _msg = {u'eventRangeID': u'79-2161071668-11456-1011-1', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1020, u'startEvent': 1011, u'scope': u'mc12_8TeV', u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}
                # Now remove the "scope" key/value
                scope = _msg.pop("scope")
                # Convert back to a string
                message = str([_msg])

        if prefetcher:
            self.__message_server_prefetcher.send(message)
            label = "Prefetcher"
        else:
            self.__message_server_payload.send(message)
            label = "the payload"
        tolog("Sent %s to %s" % (message, label))

    def getPoolFileCatalog(self, dsname, tokens, workdir, dbh, DBReleaseIsAvailable,\
                               scope_dict, filesizeIn, checksumIn, thisExperiment=None, inFilesGuids=None, lfnList=None, ddmEndPointIn=None):
        """ Wrapper function for the actual getPoolFileCatalog function in Mover """

        # This function is a wrapper to the actual getPoolFileCatalog() in Mover, but also contains SURL to TURL conversion

        file_info_dictionary = {}

        from SiteMover import SiteMover
        sitemover = SiteMover()

        # Is the inFilesGuids list populated (ie the case of the initial PFC creation) or
        # should the __guid_list be used (ie for files downloaded via server messages)?
        # (same logic for lfnList)
        if not inFilesGuids:
            inFilesGuids = self.__guid_list
        if not lfnList:
            lfnList = self.__lfn_list

        # Create the PFC
        ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic, surl_filetype_dictionary, copytool_dictionary = mover.getPoolFileCatalog("", inFilesGuids, lfnList, self.__pilot_initdir,\
                                                                                                  self.__analysisJob, tokens, workdir, dbh,\
                                                                                                  DBReleaseIsAvailable, scope_dict, filesizeIn, checksumIn,\
                                                                                                  sitemover, thisExperiment=thisExperiment, ddmEndPointIn=ddmEndPointIn,\
                                                                                                  pfc_name=self.getPoolFileCatalogPath())
        if ec != 0:
            tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))
        else:
            # Create the file dictionaries needed for the TURL conversion
            file_nr = 0
            fileInfoDic = {}
            dsdict = {}
            xmldoc = minidom.parseString(xml_from_PFC)
            fileList = xmldoc.getElementsByTagName("File")
            for thisfile in fileList: # note that there should only ever be one file
                surl = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
                guid = inFilesGuids[file_nr]
#                guid = self.__guid_list[file_nr]
                # Fill the file info dictionary (ignore the file size and checksum values since they are irrelevant for the TURL conversion - set to 0)
                fileInfoDic[file_nr] = (guid, surl, 0, 0)
                if not dsdict.has_key(dsname): dsdict[dsname] = []
                dsdict[dsname].append(os.path.basename(surl))
                file_nr += 1

            transferType = ""
            sitename = ""
            usect = False
            eventService = True

            # Create a TURL based PFC
            #tokens_dictionary = {} # not needed here, so set it to an empty dictionary
            #ec, pilotErrorDiag, createdPFCTURL, usect, dummy = mover.PFC4TURLs(self.__analysisJob, transferType, fileInfoDic, self.getPoolFileCatalogPath(),\
            #                                                                sitemover, sitename, usect, dsdict, eventService, tokens_dictionary, sitename, "", lfnList, scope_dict, self.__experiment)
            #if ec != 0:
            #    tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

            # Finally return the TURL based PFC
            #if ec == 0:
            #    file_info_dictionary = mover.getFileInfoDictionaryFromXML(self.getPoolFileCatalogPath())

        return ec, pilotErrorDiag, file_info_dictionary

    def createPoolFileCatalog_new(self, inFiles, scopeIn, inFilesGuids, tokens, filesizeIn, checksumIn, thisExperiment, workdir, ddmEndPointIn):
        """In the new mover, the catalog file is created by get_data_new. we don't need to create poolFileCatalog again"""
        self.setPoolFileCatalogPath(os.path.join(self.__job.workdir, "PFC.xml"))
        tolog("Using PFC path: %s" % (self.getPoolFileCatalogPath()))

        return 0, "", {}

    @mover.use_newmover(createPoolFileCatalog_new)
    def createPoolFileCatalog(self, inFiles, scopeIn, inFilesGuids, tokens, filesizeIn, checksumIn, thisExperiment, workdir, ddmEndPointIn):
        """ Create the Pool File Catalog """

        # Note: this method is only used for the initial PFC needed to start AthenaMP

        # Create the scope dictionary
        scope_dict = {}
        n = 0
        for lfn in inFiles:
            scope_dict[lfn] = scopeIn[n]
            n += 1

        tolog("Using scope dictionary for initial PFC: %s" % str(scope_dict))

        dsname = 'dummy_dsname' # not used by getPoolFileCatalog()
        dbh = None
        DBReleaseIsAvailable = False

        # Get the TURL based PFC
        ec, pilotErrorDiag, file_info_dictionary = self.getPoolFileCatalog(dsname, tokens, workdir, dbh, DBReleaseIsAvailable, scope_dict,\
                                                           filesizeIn, checksumIn, thisExperiment=thisExperiment, inFilesGuids=inFilesGuids, lfnList=inFiles, ddmEndPointIn=ddmEndPointIn)
        if ec != 0:
            tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

        return ec, pilotErrorDiag, file_info_dictionary

    def createPoolFileCatalogFromMessage(self, message, thisExperiment):
        """ Prepare and create the PFC using file/guid info from the event range message """

        # Note: the PFC created by this function will only contain a single LFN
        # while the intial PFC can contain multiple LFNs

        # WARNING!!!!!!!!!!!!!!!!!!!!!!
        # Consider rewrite: this function should append an entry into the xml, not replace the entire xml file

        ec = 0
        pilotErrorDiag = ""
        file_info_dictionary = {}

        # Reset the guid and lfn lists
#        self.__guid_list = []
#        self.__lfn_list = []

        if not "No more events" in message:
            # Convert string to list
            msg = loads(message)

            # Get the LFN and GUID (there is only one LFN/GUID per event range)
            try:
                # Must convert unicode strings to normal strings or the catalog lookups will fail
                lfn = str(msg[0]['LFN'])
                guid = str(msg[0]['GUID'])
                scope = str(msg[0]['scope'])
            except Exception, e:
                ec = -1
                pilotErrorDiag = "Failed to extract LFN from event range: %s" % (e)
                tolog("!!WARNING!!3434!! %s" % (pilotErrorDiag))
            else:
                # Has the file already been used? (If so, the PFC already exists)
                if guid in self.__guid_list:
                    tolog("PFC for GUID in downloaded event range has already been created")
                else:
                    self.__guid_list.append(guid)
                    self.__lfn_list.append(lfn)

                    tolog("Updating PFC for lfn=%s, guid=%s, scope=%s" % (lfn, guid, scope))

                    # Create the PFC (includes replica lookup over multiple catalogs)
                    scope_dict = { lfn : scope }
                    tokens = ['NULL']
                    filesizeIn = ['']
                    checksumIn = ['']
                    ddmEndPointIn = ['']
                    dsname = 'dummy_dsname' # not used by getPoolFileCatalog()
                    workdir = os.getcwd()
                    dbh = None
                    DBReleaseIsAvailable = False

                    ec, pilotErrorDiag, file_info_dictionary = self.getPoolFileCatalog(dsname, tokens, workdir, dbh, DBReleaseIsAvailable,\
                                                                              scope_dict, filesizeIn, checksumIn, thisExperiment=thisExperiment, ddmEndPointIn=ddmEndPointIn)
                    if ec != 0:
                        tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

        return ec, pilotErrorDiag, file_info_dictionary

    def stageInForEventRanges_new(self, eventRanges, job):
        """"Stagein files for event ranges"""
        staged_input_files = job.get_stagedIn_files()
        for key in staged_input_files:
            if key not in self.__input_files:
                self.__input_files[key] = staged_input_files[key]
        tolog("Files already staged in: %s" % self.__input_files)

        files = []
        for eventRange in eventRanges:
            f = {'scope': eventRange['scope'], 'lfn': eventRange['LFN'], 'guid': eventRange['GUID']}
            if f not in files:
                key = '%s:%s' % (f['scope'], f['lfn'])
                if key not in self.__input_files:
                    files.append(f)

        if files:
            tolog("Will stagein files for events: %s" % files)
            to_stagein_files = job.get_stagein_requests(files, allowRemoteInputs=self.__allow_remote_inputs)
            super(RunJobEvent, self).stageIn(self.getJob(), self.getJobSite(), to_stagein_files)

            staged_input_files = job.get_stagedIn_files(to_stagein_files)
            for f in files:
                key = '%s:%s' % (f['scope'], f['lfn'])
                if key not in staged_input_files:
                    ec = PilotErrors.ERR_STAGEINFAILED
                    pilotErrorDiag = "Failed to stage in file for %s" % key
                    return ec, pilotErrorDiag, {}
                else:
                    self.__input_files[key] = staged_input_files[key]

        return 0, "", {}

    @mover.use_newmover(stageInForEventRanges_new)
    def stageInForEventRanges(self, eventRanges, job):
        return 0, "", {}

    def getEventRangeFilesDictionary(self, event_ranges, eventRangeFilesDictionary):
        """ Build and return the event ranges dictionary out of the event_ranges dictinoary """

        # Format: eventRangeFilesDictionary = { guid: [lfn, is_added_to_token_extractor_file_list (boolean)], .. }
        for event_range in event_ranges:
            guid = event_range['GUID']
            lfn = event_range['LFN']
            if not eventRangeFilesDictionary.has_key(guid):
                eventRangeFilesDictionary[guid] = [lfn, False]

        return eventRangeFilesDictionary

    def updateTokenExtractorInputFile(self, eventRangeFilesDictionary, input_tag_file):
        """ Add the new file info to the token extractor file list """

        for guid in eventRangeFilesDictionary.keys():
            lfn = input_tag_file #eventRangeFilesDictionary[guid][0]
            already_added = eventRangeFilesDictionary[guid][1]
            if not already_added:
                s = self.getTokenExtractorInputListEntry(guid, lfn)
                filename = self.getTokenExtractorInputListFilename()
                status = writeToFileWithStatus(filename, s, attribute='a')
                if not status:
                    tolog("!!WARNING!!2233!! Failed to update %s" % (filename))
                else:
                    eventRangeFilesDictionary[guid][1] = True

        return eventRangeFilesDictionary

    def extractEventRangeIDs(self, event_ranges):
        """ Extract the eventRangeID's from the event ranges """

        eventRangeIDs = []
        for event_range in event_ranges:
            eventRangeIDs.append(event_range['eventRangeID'])

        return eventRangeIDs

    def areAllOutputFilesTransferred(self):
        """ Verify whether all files have been staged out or not """

        status = True
        for eventRangeID in self.__eventRangeID_dictionary.keys():
            if self.__eventRangeID_dictionary[eventRangeID] == False:
                status = False
                break

        return status

    def addEventRangeIDsToDictionary(self, currentEventRangeIDs):
        """ Add the latest eventRangeIDs list to the total event range id dictionary """

        # The eventRangeID_dictionary is used to keep track of which output files have been returned from AthenaMP
        # (eventRangeID_dictionary[eventRangeID] = False means that the corresponding output file has not been created/transferred yet)
        # This is necessary since otherwise the pilot will not know what has been processed completely when the "No more events"
        # message arrives from the server

        for eventRangeID in currentEventRangeIDs:
            if not self.__eventRangeID_dictionary.has_key(eventRangeID):
                self.__eventRangeID_dictionary[eventRangeID] = False

    def getProperInputFileName(self, input_files):
        """ Return the first non TAG file name in the input file list """

        # AthenaMP needs to know the name of an input file to be able to start
        # An Event Service job might also have a TAG file in the input file list
        # but AthenaMP cannot start with that file, so identify the proper name and return it

        filename = ""
        for f in input_files:
            if ".TAG." in f:
                continue
            else:
                filename = f
                break

        return filename

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
            self.getChildren(child)

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
            #tolog("Failed to get cpu consumption time for pid %s: %s" % (pid, traceback.format_exc()))
            return 0

    def getCPUConsumptionTimeFromProc(self, processId):
        cpuConsumptionTime = 0L
        try:
            CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
            if processId:
                self.__childProcs = []
                self.__child_cpuTime = {}
                self.getChildren(processId)
                for process in self.__childProcs:
                    if process not in self.__child_cpuTime.keys():
                        self.__child_cpuTime[process] = 0
                for process in self.__child_cpuTime.keys():
                    cpuTime = self.getCPUConsumptionTimeFromProcPid(process) / CLOCK_TICKS
                    # if cpuTime > self.__child_cpuTime[process]:
                    # process can return a small value if it's killed
                    self.__child_cpuTime[process] = cpuTime
                    cpuConsumptionTime += self.__child_cpuTime[process]
            tolog("cpuConsumptionTime for %s: %s" % (processId, cpuConsumptionTime))
        except:
            tolog("Failed to get cpu consumption time from proc: %s" % (traceback.format_exc()))
        return cpuConsumptionTime

    def updateRunCommand(self, runCommand):
        """ Update the run command with additional options """

        # AthenaMP needs to know where exactly is the PFC
        # runCommand += " '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (self.getPoolFileCatalogPath())

        # Tell AthenaMP the name of the yampl channel
        if "PILOT_EVENTRANGECHANNEL" in runCommand:
            runCommand = "export PILOT_EVENTRANGECHANNEL=\"%s\"; " % (self.getYamplChannelNamePayload()) + runCommand
        elif not "--preExec" in runCommand:
            runCommand += " --preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\'" % (self.getYamplChannelNamePayload())
        else:
            if "import jobproperties as jps" in runCommand:
                runCommand = runCommand.replace("import jobproperties as jps;", "import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\";" % (self.getYamplChannelNamePayload()))
            else:
                if "--preExec " in runCommand:
                    runCommand = runCommand.replace("--preExec ", "--preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % (self.getYamplChannelNamePayload()))
                else:
                    tolog("!!WARNING!!43431! --preExec has an unknown format - expected \'--preExec \"\' or \"--preExec \'\", got: %s" % (runCommand))

        return runCommand

    def stopThreads(self, tokenExtractorProcess, prefetcherProcess, tokenextractor_stdout, tokenextractor_stderr, prefetcher_stdout, prefetcher_stderr):
        """ Stop all threads and close output streams """

        self.stopAsyncOutputStagerThread()
        self.joinAsyncOutputStagerThread()
        self.stopMessageThreadPayload()
        self.joinMessageThreadPayload()
        if self.usePrefetcher():
            self.stopMessageThreadPrefetcher()
            self.joinMessageThreadPrefetcher()
        if tokenExtractorProcess:
            # tokenExtractorProcess.kill()
            os.killpg(os.getpgid(tokenExtractorProcess.pid), signal.SIGTERM)
        if prefetcherProcess:
            # prefetcherProcess.kill()
            os.killpg(os.getpgid(prefetcherProcess.pid), signal.SIGTERM)

        # Close stdout/err streams
        if tokenextractor_stdout:
            tokenextractor_stdout.close()
        if tokenextractor_stderr:
            tokenextractor_stderr.close()
        if prefetcher_stdout:
            prefetcher_stderr.close()
        if prefetcher_stdout:
            prefetcher_stderr.close()


    def stopPrefetcher(self, prefetcherProcess, prefetcher_stdout, prefetcher_stderr):
        """ Stop Prefetcher thread and close output steams """

        if prefetcherProcess:
            os.killpg(os.getpgid(prefetcherProcess.pid), signal.SIGTERM)
            # prefetcherProcess.kill()

        # Close stdout/err streams
        if prefetcher_stdout:
            prefetcher_stderr.close()
        if prefetcher_stdout:
            prefetcher_stderr.close()


    def checkStageOutFailures(self):
        # if there are two many stageout failures, stop
        if self.__nStageOutFailures > 0:
            tolog("Continous stageout failures: %s" % self.__nStageOutFailures)
        if self.__nStageOutFailures >= 3:
            tolog("Too many stageout failures, send 'No more events' to AthenaMP")
            self.sendMessage("No more events")
        if self.__stageoutStorages:
            if ((self.__stageoutStorages['primary'] is None or 
                 self.__stageoutStorages['primary'] and self.__stageoutStorages['primary']['continousErrors'] >= 3) and 
                (self.__stageoutStorages['failover'] is None or 
                 self.__stageoutStorages['failover'] and self.__stageoutStorages['failover']['continousErrors'] >= 3)):
                 tolog("Too many stageout failures, send 'No more events' to AthenaMP")
                 self.sendMessage("No more events")


    def updateJobParsForBrackets(self, jobPars, inputFiles):
        """ Replace problematic bracket lists with full file names """
        # jobPars = .. --inputEVNTFile=EVNT.01416937._[000003,000004].pool.root ..
        # ->
        # jobPars = .. --inputEVNTFile=EVNT.01416937._000003.pool.root,EVNT.01416937._000004.pool.root ..
        # Note: this function should only be used for testing purposes - although there appears to be a bug in the Sim_tf seen with multiple input files

        # Extract the inputEVNTFile from the jobPars
        if "--inputEVNTFile" in jobPars:
            found_items = re.findall(r'\S+', jobPars)

            pattern = r"\'?\-\-inputEVNTFile\=(.+)\'?"
            for item in found_items:
                found = re.findall(pattern, item)
                if len(found) > 0:
                    inputfile_list = found[0]

                    # Did it find any input EVNT files? If so, does the extracted string contain any brackets?
                    if inputfile_list != "" and "[" in inputfile_list:
                        if inputfile_list.endswith("\'"):
                            inputfile_list = inputfile_list[:-1]
                        tolog("Found bracket list: {0}".format(inputfile_list))

                        # Replace the extracted string with the full input file list
                        l = ",".join(inputFiles)
                        jobPars = jobPars.replace(inputfile_list, l)
                        tolog("Updated jobPars={0}".format(jobPars))
                    break

        return jobPars


# main process starts here
if __name__ == "__main__":

    tolog("Starting RunJobEvent")

    if not os.environ.has_key('PilotHomeDir'):
        os.environ['PilotHomeDir'] = os.getcwd()

    # Get error handler
    error = PilotErrors()

    # Get runJob object
    runJob = RunJobEvent()

    # Define a new parent group
    os.setpgrp()

    # Protect the runEvent code with exception handling
    hP_ret = False
    try:
        # always use this filename as the new jobDef module name
        import newJobDef

        jobSite = Site.Site()
        jobSite.setSiteInfo(runJob.argumentParser())

        # Reassign workdir for this job
        jobSite.workdir = jobSite.wntmpdir

        # Done with setting jobSite data members, not save the object so that the runJob methods have access to it
        runJob.setJobSite(jobSite)

        if runJob.getPilotLogFilename() != "":
            pUtil.setPilotlogFilename(runJob.getPilotLogFilename())

        # Set node info
        node = Node.Node()
        node.setNodeName(os.uname()[1])
        node.collectWNInfo(jobSite.workdir)
        runJob.setJobNode(node)

        # Redirect stderr
        sys.stderr = open("%s/runevent.stderr" % (jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)

        # Get the experiment object
        thisExperiment = getExperiment(runJob.getExperiment())
        tolog("runEvent will serve experiment: %s" % (thisExperiment.getExperiment()))

        # Get the event service object using the experiment name (since it can be experiment specific)
        thisEventService = getEventService(runJob.getExperiment())

        JR = JobRecovery()
        try:
            job = Job.Job()
            # init whether it allows to read/download inputs remotely
            # should be executed before setJobDef(), because setJobDef() sets job.inData
            runJob.initAllowRemoteInputs(job)
            job.setJobDef(newJobDef.job)
            job.workdir = jobSite.workdir
            job.experiment = runJob.getExperiment()
            # figure out and set payload file names
            job.setPayloadName(thisExperiment.getPayloadName(job))
            # reset the default job output file list which is anyway not correct
            logGUID = newJobDef.job.get('logGUID', "")
            if logGUID != "NULL" and logGUID != "":
                job.tarFileGuid = logGUID

            job.outFiles = []
            runJob.setOutputFiles(job.outFiles)
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            runJob.failJob(0, error.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag)
        runJob.setJob(job)

        # Should the Event Index be used?
        runJob.setUseEventIndex(job.jobPars)

        # Update problematic bracket lists
        job.jobPars = runJob.updateJobParsForBrackets(job.jobPars, job.inFiles)

        # Set the taskID
        runJob.setTaskID(job.taskID)
        tolog("taskID = %s" % (runJob.getTaskID()))

        # Prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        runJob.setJobDataDir(runJob.getParentWorkDir() + "/PandaJob_%s_data" % (job.jobId))

        # Register cleanup function
        atexit.register(runJob.cleanup, job)

        # To trigger an exception so that the SIGTERM signal can trigger cleanup function to run
        # because by default signal terminates process without cleanup.
        def sig2exc(sig, frm):
            """ signal handler """

            error = PilotErrors()
            runJob.setGlobalPilotErrorDiag("!!FAILED!!3000!! SIGTERM Signal %s is caught in child pid=%d!\n" % (sig, os.getpid()))
            tolog(runJob.getGlobalPilotErrorDiag())
            if sig == signal.SIGTERM:
                runJob.setGlobalErrorCode(error.ERR_SIGTERM)
            elif sig == signal.SIGQUIT:
                runJob.setGlobalErrorCode(error.ERR_SIGQUIT)
            elif sig == signal.SIGSEGV:
                runJob.setGlobalErrorCode(error.ERR_SIGSEGV)
            elif sig == signal.SIGXCPU:
                runJob.setGlobalErrorCode(error.ERR_SIGXCPU)
            elif sig == signal.SIGBUS:
                runJob.setGlobalErrorCode(error.ERR_SIGBUS)
            elif sig == signal.SIGUSR1:
                runJob.setGlobalErrorCode(error.ERR_SIGUSR1)
            else:
                runJob.setGlobalErrorCode(error.ERR_KILLSIGNAL)
            runJob.setFailureCode(runJob.getGlobalErrorCode())

            runJob.setAsyncOutputStagerSleepTime(sleep_time=0)
            runJob.asynchronousOutputStager()

            # print to stderr
            print >> sys.stderr, runJob.getGlobalPilotErrorDiag()
            raise SystemError(sig)

        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGUSR1, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)

        # See if it's an analysis job or not
        trf = runJob.getJobTrf()
        analysisJob = isAnalysisJob(trf.split(",")[0])
        runJob.setAnalysisJob(analysisJob)

        runJob.initESConf(job)

        # Create a message server object (global message_server)
        if runJob.createMessageServer():
            tolog("The message server for the payload is alive")
        else:
            pilotErrorDiag = "The message server for the payload could not be created, cannot continue"
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            job.result[2] = PilotErrors.ERR_ESMESSAGESERVER
            runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

        runJob.setUsePrefetcher(job.usePrefetcher)
        if runJob.usePrefetcher():
            if runJob.createMessageServer(prefetcher=True):
                tolog("The message server for Prefetcher is alive")
            else:
                tolog("!!WARNING!!1111!! The message server for Prefetcher could not be created, cannot use Prefetcher")
        else:
            tolog("!!WARNING!!1111!! The message server for Prefetcher could not be created, cannot use Prefetcher")

        runJob.setInFilePosEvtNum(job.inFilePosEvtNum)
        if runJob.getInFilePosEvtNum():
            tolog("Event number ranges relative to in-file position will be used")

        # Setup starts here ................................................................................

        # Update the job state file
        job.jobState = "setup"
        runJob.setJobState(job.jobState)
        _retjs = JR.updateJobStateTest(runJob.getJob(), jobSite, node, mode="test")

        # Send [especially] the process group back to the pilot
        job.setState([job.jobState, 0, 0])
        runJob.setJobState(job.result)
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # Prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = runJob.setup(job, jobSite, thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)
        tolog("Setup has finished successfully")
        runJob.setJob(job)

        # Job has been updated, display it again
        job.displayJob()

        # Stage-in .........................................................................................

        # Launch the benchmark, let it execute during stage-in
        benchmark_subprocess = runJob.getBenchmarkSubprocess(node, job.coreCount, job.workdir, jobSite.sitename)

        # Update the job state
        tolog("Setting stage-in state until all input files have been copied")
        job.jobState = "stagein"
        job.setState([job.jobState, 0, 0])
        runJob.setJobState(job.jobState)
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # Update copysetup[in] for production jobs if brokerage has decided that remote I/O should be used
        if job.transferType == 'direct' or job.transferType == 'fax':
            tolog('Brokerage has set transfer type to \"%s\" (remote I/O will be attempted for input files)' % (job.transferType))
            RunJobUtilities.updateCopysetups('', transferType=job.transferType)
            si = getSiteInformation(runJob.getExperiment())
            si.updateDirectAccess(job.transferType)

        # Stage-in all input files (if necessary)
        # Note: for Prefetcher, no file transfer is necessary but only the movers.stagein_real() function knows the
        # full path to the input. This function places the path in the fileState file from where it can be read
        job, ins, statusPFCTurl, usedFAXandDirectIO = runJob.stageIn(job, jobSite, analysisJob, pfc_name="PFC.xml")
        if job.result[2] != 0:
            tolog("Failing job with ec: %d" % (job.result[2]))
            runJob.failJob(0, job.result[2], job, ins=ins, pilotErrorDiag=job.pilotErrorDiag)
        runJob.setJob(job)

        # Initialize the intput files dictionary
        runJob.init_input_files(job)

        # Already staged in files should be in the guid list and lfn list
        runJob.init_guid_list()

        # The LFN:s in the input file list need to be replaced with full paths in direct access mode
        # (will be replaced in updateRunCommandList() below, first get the replica dictionary)
        if job.transferType == 'direct':
            # Populate the full_paths_dictionary
            full_paths_dictionary = getReplicaDictionaryFromXML(job.workdir, pfc_name="PFC.xml")
        else:
            full_paths_dictionary = None

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix options but use --usePFCTurl
        hasInput = job.inFiles != ['']
        runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, runJob.getParentWorkDir(), job.jobId,\
                                                              statusPFCTurl, analysisJob, usedFAXandDirectIO, hasInput,\
                                                              job.prodDBlockToken,\
                                                              full_paths_dictionary=full_paths_dictionary)
        tolog("runCommandList=%s"%str(runCommandList))

        if not os.environ.has_key('ATHENA_PROC_NUMBER') and 'ATHENA_PROC_NUMBER' not in runCommandList[0]:
            runCommandList[0] = 'export ATHENA_PROC_NUMBER=1; %s' % (runCommandList[0])

        # (stage-in ends here) .............................................................................

        # Loop until the benchmark subprocess has finished
        if benchmark_subprocess:
            max_count = 6
            _sleep = 15
            count = 0
            while benchmark_subprocess.poll() is None:
                if count >= max_count:
                    benchmark_subprocess.send_signal(signal.SIGUSR1)
                    tolog("Terminated the benchmark since it ran for longer than %d s" % (max_count*_sleep))
                else:
                    count += 1

                    # Take a short nap
                    tolog("Benchmark suite has not finished yet, taking a %d s nap (iteration #%d/%d)" % \
                          (_sleep, count, max_count))
                    time.sleep(15)

        # Prepare XML for input files to be read by the Event Server

        # runEvent determines the physical file replica(s) to be used as the source for input event data
        # It determines this from the input dataset/file info provided in the PanDA job spec

        # threading starts here ............................................................................

        # update the job state file
        job.jobState = "running"
        runJob.setJobState(job.jobState)
        job.setState([job.jobState, 0, 0])
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        event_loop_running = True
        payload_running = False

        # Create and start the stage-out thread which will run in an infinite loop until it is stopped
        asyncOutputStager_thread = StoppableThread(name='asynchronousOutputStager', target=runJob.asynchronousOutputStager)
        runJob.setAsyncOutputStagerThread(asyncOutputStager_thread)
        runJob.startAsyncOutputStagerThread()

        # Create and start the message listener threads
        message_thread_payload = StoppableThread(name='payloadListener', target=runJob.payloadListener)
        runJob.setMessageThreadPayload(message_thread_payload)
        runJob.startMessageThreadPayload()
        if runJob.usePrefetcher():
            message_thread_prefetcher = StoppableThread(name='prefetcherListener', target=runJob.prefetcherListener)
            runJob.setMessageThreadPrefetcher(message_thread_prefetcher)
            runJob.startMessageThreadPrefetcher()
        else:
            message_thread_prefetcher = None

        # threading ends here ..............................................................................

        # service tools starts here ........................................................................

        # Should the token extractor be used?
        runJob.setUseTokenExtractor(runCommandList[0])

        # Stdout/err file objects
        tokenextractor_stdout = None
        tokenextractor_stderr = None
        prefetcher_stdout = None
        prefetcher_stderr = None
        athenamp_stdout = None
        athenamp_stderr = None

        # Create and start the TokenExtractor

        # Extract the proper setup string from the run command in case the token extractor should be used
        if runJob.useTokenExtractor():
            setupString = thisEventService.extractSetup(runCommandList[0], job.trf)
            tolog("The Token Extractor will be setup using: %s" % (setupString))

            # Create the file objects
            tokenextractor_stdout, tokenextractor_stderr = runJob.getStdoutStderrFileObjects(stdoutName="tokenextractor_stdout.txt", stderrName="tokenextractor_stderr.txt")

            # In case the event index is not to be used, we need to create a TAG file
            if not runJob.useEventIndex():
                input_file, tag_file_guid = runJob.createTAGFile(runCommandList[0], job.trf, job.inFiles, "MakeRunEventCollection.py")
                input_file_guid = job.inFilesGuids[0]

                if input_file == "" or input_file_guid == "":
                    pilotErrorDiag = "Required TAG file/guid could not be identified"
                    tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))

                    # Stop threads
                    runJob.stopAsyncOutputStagerThread()
                    runJob.joinAsyncOutputStagerThread()
                    runJob.stopMessageThreadPayload()
                    runJob.joinMessageThreadPayload()
                    if runJob.usePrefetcher():
                        runJob.stopMessageThreadPrefetcher()
                        runJob.joinMessageThreadPrefetcher()

                    # Set error code
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_ESRECOVERABLE
                    runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)
            else:
                input_file = job.inFiles[0]
                input_file_guid = job.inFilesGuids[0]

            # Get the Token Extractor process
            tolog("Will use input file %s for the TokenExtractor" % (input_file))
            tokenExtractorProcess = runJob.getTokenExtractorProcess(thisExperiment, setupString, input_file, input_file_guid,\
                                                                    stdout=tokenextractor_stdout, stderr=tokenextractor_stderr,\
                                                                    url=thisEventService.getEventIndexURL())
        else:
            setupString = None
            tokenextractor_stdout = None
            tokenextractor_stderr = None
            tokenExtractorProcess = None

        # Create and start the Prefetcher

        # Extract the proper setup string from the run command in case the Prefetcher should be used
        if runJob.usePrefetcher():
            setupString = thisEventService.extractSetup(runCommandList[0], job.trf)
            if "export ATLAS_LOCAL_ROOT_BASE" not in setupString:
                setupString = "export ATLAS_LOCAL_ROOT_BASE=%s/atlas.cern.ch/repo/ATLASLocalRootBase;" % thisExperiment.getCVMFSPath() + setupString
            tolog("The Prefetcher will be setup using: %s" % (setupString))

            #job.transferType = 'direct'
            #RunJobUtilities.updateCopysetups('', transferType=job.transferType)
            #si = getSiteInformation(runJob.getExperiment())
            #si.updateDirectAccess(job.transferType)

            # Create the file objects
            prefetcher_stdout, prefetcher_stderr = runJob.getStdoutStderrFileObjects(stdoutName="prefetcher_stdout.txt", stderrName="prefetcher_stderr.txt")

            # Get the full path to the input file from the fileState file
            if job.transferType == "direct":
                state = "direct_access"
            else:
                state = "prefetch"
            input_files = getFilesOfState(runJob.getParentWorkDir(), job.jobId, ftype="input", state=state)
            if input_files == "":
                pilotErrorDiag = "Did not find any turls in fileState file"
                tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))

                # Set error code
                job.result[0] = "failed"
                job.result[2] = error.ERR_ESRECOVERABLE
                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

            input_file = ""
            infiles = input_files.split(",")
            for infile in infiles:
                for inFileLfn in job.inFiles:
                    if inFileLfn in infile:
                        input_file += infile + " "
                        break
                else:
                    pilotErrorDiag = "Did not find turl for lfn=%s in fileState file" % (inFileLfn)
                    tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))

                    # Set error code
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_ESRECOVERABLE
                    runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

            if input_file.endswith(" "):
                input_file = input_file[:-1]

            if input_file == "":
                pilotErrorDiag = "Did not find turl for any lfn in fileState file"
                tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))

                # Set error code
                job.result[0] = "failed"
                job.result[2] = error.ERR_ESRECOVERABLE
                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

            # Get the Prefetcher process
            prefetcherProcess = runJob.getPrefetcherProcess(thisExperiment, setupString, input_file=input_file, \
                                                                    stdout=prefetcher_stdout, stderr=prefetcher_stderr)
            if not prefetcherProcess:
                tolog("!!WARNING!!1234!! Prefetcher could not be started - will run without it")
                runJob.setUsePrefetcher(False)
            else:
                tolog("Prefetcher is running")
        else:
            prefetcher_stdout = None
            prefetcher_stderr = None
            prefetcherProcess = None

        # service tools ends here ..........................................................................


        # Create the file objects
        athenamp_stdout, athenamp_stderr = runJob.getStdoutStderrFileObjects(stdoutName="athena_stdout.txt", stderrName="athena_stderr.txt")

        # Remove the 1>.. 2>.. bit from the command string (not needed since Popen will handle the streams)
        if " 1>" in runCommandList[0] and " 2>" in runCommandList[0]:
            runCommandList[0] = runCommandList[0][:runCommandList[0].find(' 1>')]

        # AthenaMP needs the PFC when it is launched (initial PFC using info from job definition)
        # The returned file info dictionary contains the TURL for the input file. AthenaMP needs to know the full path for the --inputEvgenFile option
        # If Prefetcher is used, a turl based PFC will already have been created (in Mover.py)

        runJob.setPoolFileCatalogPath(os.path.join(job.workdir, "PFC.xml"))
        tolog("Using PFC path: %s" % (runJob.getPoolFileCatalogPath()))

        if not runJob.usePrefetcher():
            ec, pilotErrorDiag, file_info_dictionary = runJob.createPoolFileCatalog(job.inFiles, job.scopeIn, job.inFilesGuids, job.prodDBlockToken,\
                                                                                    job.filesizeIn, job.checksumIn, thisExperiment, runJob.getParentWorkDir(), job.ddmEndPointIn)
            if ec != 0:
                tolog("!!WARNING!!4440!! Failed to create initial PFC - cannot continue, will stop all threads")

                # Stop threads
                runJob.stopThreads(tokenExtractorProcess, prefetcherProcess, tokenextractor_stdout, tokenextractor_stderr, prefetcher_stdout, prefetcher_stderr)

                job.result[0] = "failed"
                job.result[2] = error.ERR_ESRECOVERABLE
                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

        # Update the run command with additional options
        runCommandList[0] = runJob.updateRunCommand(runCommandList[0])

        # Now update LFN(s) with the full path(s) to the input file(s) using the TURL
        if runJob.usePrefetcher():
            _fname = runJob.getPoolFileCatalogPath()
            runCommandList[0] = runJob.replaceLFNsWithTURLs(runCommandList[0], _fname, job.inFiles)

        # download event ranges before athenaMP
        # Pilot will download some event ranges from the Event Server
        catchalls = runJob.resolveConfigItem('catchall')
        first_event_ranges = None
        try:
            job.coreCount = int(job.coreCount)
        except:
            pass
        if runJob.getAllowPrefetchEvents():
            numRanges = max(job.coreCount, runJob.getMinEvents())
            message = downloadEventRanges(job.jobId, job.jobsetID, job.taskID, job.pandaProxySecretKey, numRanges=numRanges, url=runJob.getPanDAServer())
            # Create a list of event ranges from the downloaded message
            first_event_ranges = runJob.extractEventRanges(message)
            if first_event_ranges is None or first_event_ranges == []:
                tolog("No more events. will finish this job directly")
                runJob.failJob(0, error.ERR_NOEVENTS, job, pilotErrorDiag="No events before start AthenaMP")
            if len(first_event_ranges) < runJob.getMinEvents():
                tolog("Got less events(%s events) than minimal requirement(%s events). will finish this job directly" % (len(first_event_ranges), runJob.getMinEvents()))
                runJob.failJob(0, error.ERR_TOOFEWEVENTS, job, pilotErrorDiag="Got less events(%s events) than minimal requirement(%s events)" % (len(first_event_ranges), runJob.getMinEvents()))

            # Get the current list of eventRangeIDs
            currentEventRangeIDs = runJob.extractEventRangeIDs(first_event_ranges)

            # Store the current event range id's in the total event range id dictionary
            runJob.addEventRangeIDsToDictionary(currentEventRangeIDs)

        # Create and start the AthenaMP process
        t0 = os.times()
        path = os.path.join(job.workdir, 't0_times.txt')
        if writeFile(path, str(t0)):
            tolog("Wrote %s to file %s" % (str(t0), path))
        else:
            tolog("!!WARNING!!3344!! Failed to write t0 to file, will not be able to calculate CPU consumption time on the fly")

        athenaMPProcess = runJob.getSubprocess(thisExperiment, runCommandList[0], stdout=athenamp_stdout, stderr=athenamp_stderr)

        if athenaMPProcess:
             path = os.path.join(job.workdir, 'cpid.txt')
             if writeFile(path, str(athenaMPProcess.pid)):
                 tolog("Wrote cpid=%s to file %s" % (athenaMPProcess.pid, path))

        # Start the utility if required
        utility_subprocess = runJob.getUtilitySubprocess(thisExperiment, runCommandList[0], athenaMPProcess.pid, job)
        utility_subprocess_launches = 1

        # Main loop ........................................................................................

        tolog("Entering monitoring loop")

        k = 0
        max_wait = runJob.getMaxWaitOneEvent()
        nap = 0.1
        eventRangeFilesDictionary = {}
        time_to_calculate_cuptime = time.time()
        while True:
            # calculate cpu time, if it's killed, this value will be reported.
            # should not use os.times(). os.times() collects cputime at the end of a process.
            # When a process is running, os.times() returns a very small value.
            if time_to_calculate_cuptime < time.time() - 2 * 60:
                time_to_calculate_cuptime = time.time()
                job.cpuConsumptionTime = runJob.getCPUConsumptionTimeFromProc(athenaMPProcess.pid)
                job.subStatus = runJob.getSubStatus()
                job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut = runJob.getNEvents()
                tolog("nevents = %s, neventsW = %s, neventsFailed = %s, nEventsFailedStagedOut=%s" % (job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut))
                # agreed to only report stagedout events to panda
                job.nEvents = job.nEventsW
                rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=False)
                runJob.checkSoftMessage()

            runJob.checkStageOutFailures()

            # if the AthenaMP workers are ready for event processing, download some event ranges
            # the boolean will be set to true in the listener after the "Ready for events" message is received from the client
            if runJob.isAthenaMPReady():

                if first_event_ranges:
                    event_ranges = first_event_ranges
                    first_event_ranges = None
                else:
                    # Pilot will download some event ranges from the Event Server
                    message = downloadEventRanges(job.jobId, job.jobsetID, job.taskID, job.pandaProxySecretKey, numRanges=job.coreCount, url=runJob.getPanDAServer())

                    # Create a list of event ranges from the downloaded message
                    event_ranges = runJob.extractEventRanges(message)

                # Are there any event ranges?
                if event_ranges == []:
                    tolog("No more events")
                    runJob.sendMessage("No more events")
                    break

                # Get the current list of eventRangeIDs
                currentEventRangeIDs = runJob.extractEventRangeIDs(event_ranges)

                # Store the current event range id's in the total event range id dictionary
                runJob.addEventRangeIDsToDictionary(currentEventRangeIDs)

                if not runJob.usePrefetcher():
                    ec, pilotErrorDiag, dummy = runJob.stageInForEventRanges(event_ranges, job)
                    if ec != 0:
                        tolog("Failed to stagein some files for event ranges - cannot continue, will stop: %s" % pilotErrorDiag)
                        runJob.sendMessage("No more events")
                        break

                # if event range is file related position, add pfn to it (Prefetcher aware)
                # tolog("event_ranges=%s"%str(event_ranges))
                event_ranges = runJob.addPFNsToEventRanges(event_ranges)

                # Update the token extractor file list and keep track of added guids to the file list (not needed for Event Index)
                if not runJob.useEventIndex():
                    eventRangeFilesDictionary = runJob.getEventRangeFilesDictionary(event_ranges, eventRangeFilesDictionary)
                    if runJob.useTokenExtractor():
                        eventRangeFilesDictionary = runJob.updateTokenExtractorInputFile(eventRangeFilesDictionary, input_file)

                ### # Create a new PFC for the current event ranges
                ### ec, pilotErrorDiag, dummy = runJob.createPoolFileCatalogFromMessage(message, thisExperiment)
                ### if ec != 0:
                ###    tolog("!!WARNING!!4444!! Failed to create PFC - cannot continue, will stop all threads")
                ###    runJob.sendMessage("No more events")
                ###    break

                # Loop over the event ranges and call AthenaMP for each event range
                i = 0
                j = 0
                for event_range in event_ranges:

                    # do not continue if the abort has been set
                    if runJob.shouldBeAborted():
                        tolog("Aborting event range loop")
                        break

                    # Send the downloaded event ranges to the Prefetcher, who will update the message before it is sent to AthenaMP
                    if runJob.usePrefetcher():

                        while True:
                            
                            # Set the boolean to false until Prefetcher has finished updating the event range (if used)
                            runJob.setPrefetcherHasFinished(False)
                            runJob.setUpdatedLFN("") # forget about any previously updated LFN
                            
                            prefetcherAttempts = 0
                            maxPrefetcherAttempts = 3
                            l = 0
                            while prefetcherAttempts < maxPrefetcherAttempts:
                                # Loop until Prefetcher is ready to process an event range
                                if runJob.isPrefetcherReady():
                                    tolog("Sending event range to Prefetcher")
                                    runJob.sendMessage(str([event_range]), prefetcher=True)
                                    # need to get the updated event range back from Prefetcher
                                    tolog("Waiting for Prefetcher reply")
                                    count = 0
                                    maxCount = 180
                                    while not runJob.prefetcherHasFinished():
                                        time.sleep(1)
                                        if count > maxCount:
                                            tolog("Prefetcher has not replied for %d seconds - restarting it" % maxCount)
                                            # Stop Prefetcher
                                            runJob.stopPrefetcher(prefetcherProcess, prefetcher_stdout, prefetcher_stderr)
                                            # Create new message server for new Prefetcher
                                            prefetcherUuid = uuid.uuid4()
                                            runJob.setYamplChannelNamePrefetcher("EventService_Prefetcher-%s" % (prefetcherUuid))
                                            runJob.createMessageServer(prefetcher=True)
                                            # Start new Prefetcher process
                                            prefetcher_stdout, prefetcher_stderr = runJob.getStdoutStderrFileObjects(stdoutName="prefetcher_stdout_%s.txt" % prefetcherUuid,
                                                                                                                     stderrName="prefetcher_stderr_%s.txt" % prefetcherUuid)
                                            prefetcherProcess = runJob.getPrefetcherProcess(thisExperiment, setupString, input_file=input_file, stdout=prefetcher_stdout, stderr=prefetcher_stderr)
                                            prefetcherAttempts += 1
                                            runJob.setPrefetcherIsReady(False)
                                            l = 0
                                            break
                                        count += 1
                                    else:
                                        # Prefetcher has finished, stop trying to restart it
                                        break
                                else:
                                    time.sleep(1)
                                    if l%10 == 0:
                                        tolog("Prefetcher waiting loop iteration #%d" % (l))
                                    l += 1

                                    # Is Prefetcher still running?
                                    if prefetcherProcess.poll() is not None:
                                        job.pilotErrorDiag = "Prefetcher finished prematurely"
                                        job.result[0] = "failed"
                                        job.result[2] = error.ERR_ESPREFETCHERDIED
                                        tolog("!!WARNING!!2228!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                                        break
                            else:
                                # Reached maximum Prefetcher restart attempts, abort job
                                pilotErrorDiag = "Prefetcher has not replied after %d restarts - aborting" % (maxPrefetcherAttempts)
                                tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))
                                # Stop threads
                                runJob.stopThreads(tokenExtractorProcess, prefetcherProcess, tokenextractor_stdout, tokenextractor_stderr, prefetcher_stdout, prefetcher_stderr)
                                job.result[0] = "failed"
                                job.result[2] = error.ERR_ESRECOVERABLE
                                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

                            if job.result[0] == "failed":
                                tolog("Picked up an error: aborting")
                                break

                            tolog("Original event_range=%s"%str(event_range))
                            
                            # Prefetcher should now have sent back the updated LFN
                            # Update the event_range
                            event_range['LFN'] = runJob.getUpdatedLFN()
                            event_range['PFN'] = runJob.getUpdatedLFN()

                            # Add the PFN (local file path)
                            # event_range = runJob.addPFNToEventRange(event_range)

                            # Update the positional event numbers
                            event_range['lastEvent'] = 1 + event_range['lastEvent'] - event_range['startEvent']
                            event_range['startEvent'] = 1
                            tolog("Updated event_range=%s"%str(event_range))
                
                            # Pilot can continue to send the updated event range to AthenaMP
                            break

                    if job.result[0] == "failed":
                        tolog("Picked up an error: aborting")
                        break

                    # Send the event range to AthenaMP
                    # tolog("Sending a new event range to AthenaMP (id=%s)" % (currentEventRangeIDs[j]))
                    runJob.setSendingEventRange(True)
                    runJob.setCurrentEventRange(currentEventRangeIDs[j])
                    runJob.sendMessage(str([event_range]))
                    runJob.setSendingEventRange(False)

                    # Set the boolean to false until AthenaMP is again ready for processing more events
                    runJob.setAthenaMPIsReady(False)

                    # Wait until AthenaMP is ready to receive another event range
                    w = 0
                    while not runJob.isAthenaMPReady():
                        # calculate cpu time
                        if time_to_calculate_cuptime < time.time() - 2 * 60:
                            time_to_calculate_cuptime = time.time()
                            job.cpuConsumptionTime = runJob.getCPUConsumptionTimeFromProc(athenaMPProcess.pid)
                            job.subStatus = runJob.getSubStatus()
                            job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut = runJob.getNEvents()
                            tolog("nevents = %s, neventsW = %s, neventsFailed = %s, nEventsFailedStagedOut=%s" % (job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut))
                            # agreed to only report stagedout events to panda
                            job.nEvents = job.nEventsW
                            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=False)

                        # do not continue if the abort has been set
                        if runJob.shouldBeAborted():
                            tolog("Aborting AthenaMP loop")
                            break

                        # Take a nap
                        if i%600 == 0:
                            tolog("Event range loop iteration #%d" % (i/600))
                        i += 1
                        w += 1
                        time.sleep(nap)

                        # Is AthenaMP still running?
                        if athenaMPProcess.poll() is not None:
                            tolog("AthenaMP appears to have finished (aborting event processing loop for this event range)")
                            break

                        if runJob.isAthenaMPReady():
                            # tolog("AthenaMP is ready for new event range")
                            w = 0
                            break
                        if w * nap > max_wait * 60:
                            tolog("AthanaMP has been stuck for %s minutes, will kill AthenaMP" % max_wait)
                            # athenaMPProcess.kill()
                            os.killpg(os.getpgid(athenaMPProcess.pid), signal.SIGTERM)

                            job.pilotErrorDiag = "AthenaMP has been stuck for %s minutes" % max_wait
                            job.result[0] = "failed"
                            job.result[2] = error.ERR_ESATHENAMPDIED

                        # Make sure that the utility subprocess is still running
                        if utility_subprocess:
                            if not utility_subprocess.poll() is None:
                                # If poll() returns anything but None it means that the subprocess has ended - which it should not have done by itself
                                # Unless it was killed by the Monitor along with all other subprocesses
                                if not os.path.exists(os.path.join(job.workdir, "MEMORYEXCEEDED")):
                                    if utility_subprocess_launches <= 5:
                                        tolog("!!WARNING!!4343!! Dectected crashed utility subprocess - will restart it")
                                        utility_subprocess = runJob.getUtilitySubprocess(thisExperiment, runCommandList[0], athenaMPProcess.pid, job)
                                        utility_subprocess_launches += 1
                                    elif utility_subprocess_launches <= 6:
                                        tolog("!!WARNING!!4343!! Dectected crashed utility subprocess - too many restarts, will not restart again")
                                        utility_subprocess_launches += 1
                                        utility_subprocess = None
                                    else:
                                        pass
                                else:
                                    tolog("Detected lockfile MEMORYEXCEEDED: will not restart utility")
                                    utility_subprocess = None

                        # Make sure that the token extractor is still running
                        if runJob.useTokenExtractor():
                            if not tokenExtractorProcess.poll() is None:
                                max_wait = 0
                                job.pilotErrorDiag = "Token Extractor has crashed"
                                job.result[0] = "failed"
                                job.result[2] = error.ERR_TEFATAL
                                tolog("!!WARNING!!2322!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                                break

                    # Is AthenaMP still running?
                    if athenaMPProcess.poll() is not None:
                        tolog("AthenaMP has finished (aborting event range loop for current event ranges)")
                        break

                    # Was there a fatal error in the inner loop?
                    if job.result[0] == "failed":
                        job.jobState = job.result[0]
                        runJob.setFinalESStatus(job)
                        runJob.setJobState(job.jobState)
                        tolog("Detected a failure - aborting event range loop")
                        break

                    j += 1

                # Is AthenaMP still running?
                if athenaMPProcess.poll() is not None:
                    tolog("AthenaMP has finished (aborting event range loop)")
                    break

            else:
                # do not continue if the abort has been set
                if runJob.shouldBeAborted():
                    tolog("Aborting AthenaMP waiting loop")
                    break

                time.sleep(0.1)

                if k%600 == 0:
                    tolog("AthenaMP waiting loop iteration #%d" % (k/600))
                k += 1

                # Is AthenaMP still running?
                if athenaMPProcess.poll() is not None:
                    job.pilotErrorDiag = "AthenaMP finished prematurely"
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_ESATHENAMPDIED
                    tolog("!!WARNING!!2222!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                    break

                # Make sure that the utility subprocess is still running
                if utility_subprocess:
                    if not utility_subprocess.poll() is None:
                        # If poll() returns anything but None it means that the subprocess has ended - which it should not have done by itself
                        if utility_subprocess_launches <= 5:
                            tolog("!!WARNING!!4343!! Dectected crashed utility subprocess - will restart it")
                            utility_subprocess = runJob.getUtilitySubprocess(thisExperiment, runCommandList[0], athenaMPProcess.pid, job)
                            utility_subprocess_launches += 1
                        elif utility_subprocess_launches <= 6:
                            tolog("!!WARNING!!4343!! Dectected crashed utility subprocess - too many restarts, will not restart again")
                            utility_subprocess = None
                        else:
                            pass

                # Make sure that the token extractor is still running
                if runJob.useTokenExtractor():
                    if not tokenExtractorProcess.poll() is None:
                        max_wait = 0
                        job.pilotErrorDiag = "Token Extractor has crashed"
                        job.result[0] = "failed"
                        job.result[2] = error.ERR_TEFATAL
                        tolog("!!WARNING!!2322!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                        break

        # Wait for AthenaMP to finish
        kill = False
        tolog("Will now wait for AthenaMP to finish")
        if runJob.shouldBeKilled():
            # athenaMPProcess.kill()
            os.killpg(os.getpgid(athenaMPProcess.pid), signal.SIGTERM)

            tolog("(Kill signal SIGTERM sent to AthenaMP - jobReport might get lost)")
            job.pilotErrorDiag = "Pilot was instructed by server to kill AthenaMP"
            job.result[0] = "failed"
            job.result[2] = error.ERR_ESKILLEDBYSERVER
            tolog("!!WARNING!!2323!! %s" % (job.pilotErrorDiag))
            kill = True
        else:
            i = 0
            while athenaMPProcess.poll() is None:
                tolog("Waiting for AthenaMP to finish (#%d)" % (i))
                if i > max_wait:
                    # Stop AthenaMP
                    tolog("Waited long enough - Stopping AthenaMP process")
                    # athenaMPProcess.kill()
                    os.killpg(os.getpgid(athenaMPProcess.pid), signal.SIGTERM)

                    tolog("(Kill signal SIGTERM sent to AthenaMP - jobReport might get lost)")
                    kill = True
                    break
                if runJob.shouldBeKilled():
                    # athenaMPProcess.kill()
                    os.killpg(os.getpgid(athenaMPProcess.pid), signal.SIGTERM)

                    tolog("(Kill signal SIGTERM sent to AthenaMP - jobReport might get lost)")
                    job.pilotErrorDiag = "Pilot was instructed by server to kill AthenaMP"
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_ESKILLEDBYSERVER
                    tolog("!!WARNING!!2323!! %s" % (job.pilotErrorDiag))
                    kill = True
                    break
                time.sleep(60)
                i += 1

        if runJob.getESFatalCode():
            job.result[2] = runJob.getESFatalCode()
            job.result[0] = "failed"
            job.pilotErrorDiag = "AthenaMP has some fatal errors"

        if not kill:
            tolog("AthenaMP has finished")

        athenaMP_finished_at = time.time()
        job.subStatus = runJob.getSubStatus()
        job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut = runJob.getNEvents()

        # agreed to only report stagedout events to panda
        job.nEvents = job.nEventsW

        t1 = os.times()
        tolog("t1 = %s" % str(t1))

        # Try to get the cpu time from the jobReport
        job.cpuConsumptionUnit, cpuConsumptionTime, job.cpuConversionFactor = getCPUTimes(job.workdir)
        if cpuConsumptionTime == 0:
            tolog("!!WARNING!!3434!! Falling back to less accurate os.times() measurement of CPU time")
            cpuconsumptiontime = get_cpu_consumption_time(t0)
            job.cpuConsumptionTime = int(cpuconsumptiontime)
            job.cpuConsumptionUnit = 's'
            job.cpuConversionFactor = 1.0

        if cpuConsumptionTime > 0:
            # if payload is killed, cpu time returned from os.times() will not be correct.
            job.cpuConsumptionTime = cpuConsumptionTime
        tolog("Job CPU usage: %s %s" % (job.cpuConsumptionTime, job.cpuConsumptionUnit))
        tolog("Job CPU conversion factor: %1.10f" % (job.cpuConversionFactor))
        job.timeExe = int(round(t1[4] - t0[4]))

        # Stop the utility
        if utility_subprocess:
            utility_subprocess.send_signal(signal.SIGUSR1)
            tolog("Terminated the utility subprocess")

            _nap = 10
            tolog("Taking a short nap (%d s) to allow the utility to finish writing to the summary file" % (_nap))
            time.sleep(_nap)

            # Copy the output JSON to the pilots init dir
            _path = os.path.join(job.workdir, thisExperiment.getUtilityJSONFilename())
            if os.path.exists(_path):
                try:
                    copy2(_path, runJob.getPilotInitDir())
                except Exception, e:
                    tolog("!!WARNING!!2222!! Caught exception while trying to copy JSON files: %s" % (e))
                else:
                    tolog("Copied %s to pilot init dir" % (_path))
            else:
                tolog("File %s was not created" % (_path))

        # Do not stop the stageout thread until all output files have been transferred
        starttime = time.time()
        maxtime = 30*60

        runJob.setAsyncOutputStagerSleepTime(sleep_time=0)

        while not runJob.areAllOutputFilesTransferred():
            if len(runJob.getStageOutQueue()) == 0:
                tolog("No files in stage-out queue, no point in waiting for transfers since AthenaMP has finished (job is failed)")
                break

            tolog("Will wait for a maximum of %d seconds for file transfers to finish (so far waited %d seconds)" % (maxtime, time.time() - starttime))
            tolog("stage-out queue: %s" % (runJob.getStageOutQueue()))
            if (len(runJob.getStageOutQueue())) > 0 and (time.time() - starttime > maxtime):
                tolog("Aborting stage-out thread (timeout)")
                break
            time.sleep(30)

        job.external_stageout_time = time.time() - athenaMP_finished_at

        job.subStatus = runJob.getSubStatus()
        job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut = runJob.getNEvents()
        tolog("nevents = %s, neventsW = %s, neventsFailed = %s, nEventsFailedStagedOut=%s" % (job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut))
        # agreed to only report stagedout events to panda
        job.nEvents = job.nEventsW

        # replace the default job output file list which is anyway not correct
        # (it is only used by AthenaMP for generating output file names)
#        job.outFiles = output_files
#        runJob.setJobOutFiles(job.outFiles)
#        tolog("output_files = %s" % (output_files))

        # Get the datasets for the output files
        dsname, datasetDict = runJob.getDatasets()
        tolog("dsname = %s" % (dsname))
        tolog("datasetDict = %s" % (datasetDict))

        # Create the output file dictionary needed for generating the metadata
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, job.workdir, fullpath=True)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            # runJob.failJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)
            tolog("Missing output file: %s" % pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # Create metadata for all successfully staged-out output files (include the log file as well, even if it has not been created yet)
        ec, outputFileInfo = runJob.createFileMetadata(outsDict, dsname, datasetDict, jobSite.sitename)
        if ec:
            tolog("Failed to create metadata for all output files: %s" % job.pilotErrorDiag)
            # runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)

        if prefetcherProcess:
            tolog("Killing Prefetcher process")
            # prefetcherProcess.kill()
            os.killpg(os.getpgid(prefetcherProcess.pid), signal.SIGTERM)

        tolog("Stopping stage-out thread")
        runJob.stopAsyncOutputStagerThread()
        runJob.joinAsyncOutputStagerThread()
#        asyncOutputStager_thread.stop()
#        asyncOutputStager_thread.join()
#        runJob.setAsyncOutputStagerThread(asyncOutputStager_thread)

        # Stop Token Extractor
#        if tokenExtractorProcess:
#            tolog("Stopping Token Extractor process")
#            tokenExtractorProcess.kill()
#            tolog("(Kill signal SIGTERM sent)")
#        else:
#            tolog("No Token Extractor process running")

        # Close stdout/err streams
        if tokenextractor_stdout:
            tokenextractor_stdout.close()
        if tokenextractor_stderr:
            tokenextractor_stderr.close()
        if prefetcher_stdout:
            prefetcher_stdout.close()
        if prefetcher_stderr:
            prefetcher_stderr.close()
        if athenamp_stdout:
            athenamp_stdout.close()
        if athenamp_stderr:
            athenamp_stderr.close()

        tolog("Stopping message threads")
        runJob.stopMessageThreadPayload()
        runJob.joinMessageThreadPayload()
        if runJob.usePrefetcher():
            runJob.stopMessageThreadPrefetcher()
            runJob.joinMessageThreadPrefetcher()

        # Rename the metadata produced by the payload
        # if not pUtil.isBuildJob(outs):
        runJob.moveTrfMetadata(job.workdir, job.jobId)

        # Check the job report for any exit code that should replace the res_tuple[0]
        res0, exitAcronym, exitMsg = runJob.getTrfExitInfo(0, job.workdir)
        res = (res0, exitMsg, exitMsg)

        # If payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        # Payload error handling
        ed = ErrorDiagnosis()
        job = ed.interpretPayload(job, res, False, 0, runCommandList, runJob.getFailureCode())
        if job.result[1] != 0 or job.result[2] != 0:
            tolog("Payload failed: job.result[1]=%s, job.result[2]=%s" % (job.result[1], job.result[2]))
            # runJob.failJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag)
        runJob.setJob(job)

        # wrap up ..........................................................................................

        errorCode = runJob.getErrorCode()

        # Check for special failure condition
        if job.result[2] == error.ERR_ESKILLEDBYSERVER:
            tolog("Killed by server")
            job.jobState = "failed"
        elif job.result[2]:
            tolog("Detected some error: %s" % str(job.result))
            job.jobState = "failed"
        else:
            if not runJob.getStatus() or errorCode != 0:
                tolog("Detected at least one transfer failure, job will be set to failed")
                job.jobState = "failed"
                job.result[2] = errorCode
            else:
                eventRangeIdDic = runJob.getEventRangeIDDictionary()
                if not eventRangeIdDic.keys():
                    job.subStatus = 'pilot_noevents'
                    job.pilotErrorDiag = "Pilot got no events"
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_NOEVENTS
                    job.jobState = "failed"
                else:
                    tolog("No transfer failures detected, job will be set to finished")
                    job.jobState = "finished"
        job.subStatus = runJob.getSubStatus()
        runJob.setFinalESStatus(job)
        job.setState([job.jobState, job.result[1], job.result[2]])
        runJob.setJobState(job.jobState)
        job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut = runJob.getNEvents()
        tolog("nevents = %s, neventsW = %s, neventsFailed = %s, nEventsFailedStagedOut=%s" % (job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut))
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)

        tolog("Done")
        runJob.sysExit(job)

    except Exception, errorMsg:

        job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut = runJob.getNEvents()
        tolog("nevents = %s, neventsW = %s, neventsFailed = %s, nEventsFailedStagedOut=%s" % (job.nEvents, job.nEventsW, job.nEventsFailed, job.nEventsFailedStagedOut))
        job.subStatus = runJob.getSubStatus()
        runJob.setFinalESStatus(job)
        # agreed to only report stagedout events to panda
        job.nEvents = job.nEventsW

        error = PilotErrors()

        if runJob.getGlobalPilotErrorDiag() != "":
            pilotErrorDiag = "Exception caught in RunJobEvent: %s" % (runJob.getGlobalPilotErrorDiag())
        else:
            pilotErrorDiag = "Exception caught in RunJobEvent: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (str(e), pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in RunJobEvent: %s" % str(e)
                tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))

        job = Job.Job()
        job.setJobDef(newJobDef.job)
        job.pilotErrorDiag = pilotErrorDiag
        job.result[0] = "failed"
        if runJob.getGlobalErrorCode() != 0:
            job.result[2] = runJob.getGlobalErrorCode()
        else:
            job.result[2] = error.ERR_RUNEVENTEXC
        tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag, docleanup=False, pilot_failed=False)

    # end of RunJobEvent
