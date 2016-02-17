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

# Import relevant python/pilot modules
# Pilot modules
import Job
import Node
import Site
import pUtil
import RunJobUtilities
import Mover as mover

from ThreadPool import ThreadPool
from RunJob import RunJob              # Parent RunJob class
from JobState import JobState
from JobRecovery import JobRecovery
from PilotErrors import PilotErrors
from ErrorDiagnosis import ErrorDiagnosis
from pUtil import tolog, getExperiment, isAnalysisJob, httpConnect, createPoolFileCatalog, getSiteInformation, getDatasetDict
from objectstoreSiteMover import objectstoreSiteMover
from Mover import getFilePathForObjectStore, getInitialTracingReport
from PandaServerClient import PandaServerClient

from GetJob import GetJob
from EventStager import EventStager
from HPC.HPCManager import HPCManager

class RunJobHpcEvent(RunJob):

    # private data members
    __runjob = "RunJobHpcEvent"                            # String defining the sub class
    __instance = None                           # Boolean used by subclasses to become a Singleton
    #__error = PilotErrors()                     # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass
        self.__output_es_files = []
        self.__eventRanges = {}
        self.__failedStageOuts = []
        self.__hpcManager = None
        self.__stageout_threads = 1
        self.__userid = None

        self.__stageinretry = 1
        self.__siteInfo = None
        # multi-jobs
        self.__firstJob = True
        self.__firstJobId = None
        self.__pilotWorkingDir = None
        self.__jobs = {}
        self.__jobEventRanges = {}
        self.__nJobs = 1
        self.__hpcMode = 'normal'
        self.__hpcStatue = 'starting'
        self.__hpcCoreCount = 0
        self.__hpcEventRanges = 0
        self.__neededEventRanges = 0
        self.__avail_files = {}
        self.__avail_tag_files = {}

        # event Stager
        self.__eventStager = None
        self.__yoda_to_os = False

        # for recovery
        self.__jobStateFile = None


    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobHpcEvent, cls).__new__(cls, *args, **kwargs)

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
        
    def setupHPCEvent(self, rank=None):
        self.__jobSite = Site.Site()
        self.__jobSite.setSiteInfo(self.argumentParser())
        self.__logguid = None
        ## For HPC job, we don't need to reassign the workdir
        # reassign workdir for this job
        self.__jobSite.workdir = self.__jobSite.wntmpdir
        if not os.path.exists(self.__jobSite.workdir):
            os.makedirs(self.__jobSite.workdir)


        tolog("runJobHPCEvent.getPilotLogFilename=%s"% self.getPilotLogFilename())
        if self.getPilotLogFilename() != "":
            pilotLogFilename = self.getPilotLogFilename()
            if rank:
                pilotLogFilename = '%s.%s' % (pilotLogFilename, rank)
            tolog("runJobHPCEvent.setPilotLogFilename=%s"% pilotLogFilename)
            pUtil.setPilotlogFilename(pilotLogFilename)

        # set node info
        self.__node = Node.Node()
        self.__node.setNodeName(os.uname()[1])
        self.__node.collectWNInfo(self.__jobSite.workdir)

        # redirect stderr
        #sys.stderr = open("%s/runJobHPCEvent.stderr" % (self.__jobSite.workdir), "w")

        self.__pilotWorkingDir = self.getParentWorkDir()
        tolog("Pilot workdir is: %s" % self.__pilotWorkingDir)
        os.chdir(self.__pilotWorkingDir)
        tolog("Current job workdir is: %s" % os.getcwd())
        # self.__jobSite.workdir = self.__pilotWorkingDir
        tolog("Site workdir is: %s" % self.__jobSite.workdir)

        # get the experiment object
        self.__thisExperiment = getExperiment(self.getExperiment())
        tolog("runEvent will serve experiment: %s" % (self.__thisExperiment.getExperiment()))
        self.__siteInfo = getSiteInformation(self.getExperiment())

    def getDefaultResources(self):
        siteInfo = self.__siteInfo
        catchalls = siteInfo.readpar("catchall")
        values = {}
        res = {}
        if "yoda_to_os" in catchalls:
            res['yoda_to_os'] = True
        else:
            res['yoda_to_os'] = False
        self.__yoda_to_os = res['yoda_to_os']

        if "copyOutputToGlobal" in catchalls:
            res['copyOutputToGlobal'] = True
        else:
            res['copyOutputToGlobal'] = False

        for catchall in catchalls.split(","):
            if '=' in catchall:
                values[catchall.split('=')[0]] = catchall.split('=')[1]

        res['queue'] = values.get('queue', 'regular')
        res['mppwidth'] = values.get('mppwidth', 48)
        res['mppnppn'] = values.get('mppnppn', 1)
        res['walltime_m'] = values.get('walltime_m', 30)
        res['ATHENA_PROC_NUMBER'] = values.get('ATHENA_PROC_NUMBER', 23)
        res['max_nodes'] = values.get('max_nodes', 3)
        res['min_walltime_m'] = values.get('min_walltime_m', 20)
        res['max_walltime_m'] = values.get('max_walltime_m', 30)
        res['nodes'] = values.get('nodes', 2)
        res['min_nodes'] = values.get('min_nodes', 2)
        res['cpu_per_node'] = values.get('cpu_per_node', 24)
        res['partition'] = values.get('partition', None)
        res['repo'] = values.get('repo', None)
        res['max_events'] = values.get('max_events', 10000)
        res['initialtime_m'] = values.get('initialtime_m', 15)
        res['time_per_event_m'] = values.get('time_per_event_m', 10)
        res['mode'] = values.get('mode', 'normal')
        res['backfill_queue'] = values.get('backfill_queue', 'regular')
        res['stageout_threads'] = int(values.get('stageout_threads', 4))
        res['copy_input_files'] = values.get('copy_input_files', 'false').lower()
        res['plugin'] =  values.get('plugin', 'pbs').lower()
        res['localWorkingDir'] =  values.get('localWorkingDir', None)
        res['parallel_jobs'] = values.get('parallel_jobs', 1)
        res['events_limit_per_job'] = values.get('events_limit_per_job', 1000)

        siteInfo = getSiteInformation(self.getExperiment())
        # get the copy tool
        setup = siteInfo.getCopySetup(stageIn=False)
        tolog("Copy Setup: %s" % (setup))
        espath = getFilePathForObjectStore(filetype="eventservice")
        tolog("ES path: %s" % (espath))

        res['setup'] = setup
        res['esPath'] = espath

        return res

    def getYodaSetup(self):
        siteInfo = self.__siteInfo
        envsetup = siteInfo.readpar("envsetup")
        setupPath = os.path.dirname(envsetup)
        yodaSetup = os.path.join(setupPath, 'yodasetup.sh')
        if os.path.exists(yodaSetup):
            setup = ""
            f = open(yodaSetup)
            for line in f:
                setup += line + "\n"
            f.close()
            return setup
        return None

    def setupHPCManager(self):
        logFileName = None
        tolog("runJobHPCEvent.getPilotLogFilename=%s"% self.getPilotLogFilename())
        if self.getPilotLogFilename() != "":
            logFileName = self.getPilotLogFilename()

        defRes = self.getDefaultResources()
        if defRes['copy_input_files'] == 'true' and defRes['localWorkingDir']:
            self.__copyInputFiles = True
        else:
            self.__copyInputFiles = False
        self.__nJobs = defRes['parallel_jobs']
        self.__stageout_threads = defRes['stageout_threads']

        tolog("Setup HPC Manager")
        hpcManager = HPCManager(globalWorkingDir=self.__pilotWorkingDir, localWorkingDir=defRes['localWorkingDir'], logFileName=logFileName, copyInputFiles=self.__copyInputFiles)

        #jobStateFile = '%s/jobState-%s.pickle' % (self.__pilotWorkingDir, self.__job.jobId)
        #hpcManager.setPandaJobStateFile(jobStateFile)
        self.__hpcMode = "HPC_" + hpcManager.getMode(defRes)
        self.__hpcStatue = 'waitingResource'
        pluginName = defRes.get('plugin', 'pbs')
        hpcManager.setupPlugin(pluginName)

        tolog("Get Yoda setup")
        yodaSetup = self.getYodaSetup()
        tolog("Yoda setup: %s" % yodaSetup)
        hpcManager.setLocalSetup(yodaSetup)

        tolog("HPC Manager getting free resouces")
        hpcManager.getFreeResources(defRes)
        self.__hpcStatue = 'gettingJobs'

        tolog("HPC Manager getting needed events number")
        self.__hpcEventRanges = hpcManager.getEventsNumber()
        tolog("HPC Manager needs events: %s, max_events: %s; use the smallest one." % (self.__hpcEventRanges, defRes['max_events']))
        if self.__hpcEventRanges > int(defRes['max_events']):
            self.__hpcEventRanges = int(defRes['max_events'])
        self.__neededEventRanges = self.__hpcEventRanges
        self.__hpcManager = hpcManager
        tolog("HPC Manager setup finished")

    def setupJob(self, job, data):
        tolog("setupJob")
        try:
            job.coreCount = 0
            job.hpcEvent = True
            if self.__firstJob:
                job.workdir = self.__jobSite.workdir
                self.__firstJob = False
            else:
                # job.mkJobWorkdir(self.__pilotWorkingDir)
                pass
            job.experiment = self.getExperiment()
            # figure out and set payload file names
            job.setPayloadName(self.__thisExperiment.getPayloadName(job))
            # reset the default job output file list which is anyway not correct
            job.outFiles = []
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            
            self.failOneJob(0, PilotErrors.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag, final=True, updatePanda=False)
            return -1

        current_dir = os.getcwd()
        os.chdir(job.workdir)

        tolog("Switch from current dir %s to job %s workdir %s" % (current_dir, job.jobId, job.workdir))

        self.__userid = job.prodUserID
        self.__jobs[job.jobId] = {'job': job}
        # prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        job.datadir = self.__pilotWorkingDir + "/PandaJob_%s_data" % (job.jobId)

        # See if it's an analysis job or not
        trf = job.trf
        self.__jobs[job.jobId]['analysisJob'] = isAnalysisJob(trf.split(",")[0])

        # Setup starts here ................................................................................

        # Update the job state file
        job.jobState = "starting"
        job.setHpcStatus('init')


        # Send [especially] the process group back to the pilot
        job.setState([job.jobState, 0, 0])
        job.jobState = job.result
        rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), self.getPilotPort())

        JR = JobRecovery(pshttpurl='https://pandaserver.cern.ch', pilot_initdir=job.workdir)
        JR.updateJobStateTest(job, self.__jobSite, self.__node, mode="test")
        JR.updatePandaServer(job, self.__jobSite, self.__node, 25443)

        # prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = self.setup(job, self.__jobSite, self.__thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            self.failOneJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag, final=True, updatePanda=False)
            return -1
        tolog("Setup has finished successfully")


        # job has been updated, display it again
        job.displayJob()
        tolog("RunCommandList: %s" % runCommandList)
        tolog("Multi_trf: %s" % multi_trf)
        self.__jobs[job.jobId]['job'] = job
        self.__jobs[job.jobId]['JR'] = JR
        self.__jobs[job.jobId]['runCommandList'] = runCommandList
        self.__jobs[job.jobId]['multi_trf'] = multi_trf

        tolog("Get Event Ranges for job %s" % job.jobId)
        eventRanges = self.getJobEventRanges(job, numRanges=self.__neededEventRanges)
        self.__neededEventRanges = self.__neededEventRanges - len(eventRanges)
        tolog("Get %s Event ranges for job %s" % (len(eventRanges), job.jobId))
        self.__eventRanges[job.jobId] = {}
        self.__jobEventRanges[job.jobId] = eventRanges 
        for eventRange in eventRanges:
            self.__eventRanges[job.jobId][eventRange['eventRangeID']] = 'new'

        # backup job file
        filename = os.path.join(self.__pilotWorkingDir, "Job_%s.json" % job.jobId)
        content = {'workdir': job.workdir, 'data': data, 'experiment': self.getExperiment(), 'runCommandList': runCommandList}
        with open(filename, 'w') as outputFile:
            json.dump(content, outputFile)

        # copy queue data
        try:
            copy_src = self.__siteInfo.getQueuedataFileName()
            copy_dest = os.path.join(job.workdir, os.path.basename(copy_src))
            tolog("Copy %s to %s" % (copy_src, copy_dest))
            shutil.copyfile(copy_src, copy_dest)
        except:
            tolog("Failed to copy queuedata to job working dir: %s" % (traceback.format_exc()))

        tolog("Switch back from job %s workdir %s to current dir %s" % (job.jobId, job.workdir, current_dir))
        os.chdir(current_dir)
        return 0


    def getHPCEventJobFromPanda(self):
        try:
            tolog("Switch to pilot working dir: %s" % self.__pilotWorkingDir)
            os.chdir(self.__pilotWorkingDir)
            tolog("Get new job from Panda")
            getJob = GetJob(self.__pilotWorkingDir, self.__node, self.__siteInfo, self.__jobSite)
            job, data, errLog = getJob.getNewJob()
            if not job:
                if "No job received from jobDispatcher" in errLog or "Dispatcher has no jobs" in errLog:
                    errorText = "!!FINISHED!!0!!Dispatcher has no jobs"
                else:
                    errorText = "!!FAILED!!1999!!%s" % (errLog)
                tolog(errorText)

                # remove the site workdir before exiting
                # pUtil.writeExitCode(thisSite.workdir, error.ERR_GENERALERROR)
                # raise SystemError(1111)
                #pUtil.fastCleanup(self.__jobSite.workdir, self.__pilotWorkingDir, True)
                return -1
            else:
                tolog("download job definition id: %s" % (job.jobDefinitionID))
                # verify any contradicting job definition parameters here
                try:
                    ec, pilotErrorDiag = self.__thisExperiment.postGetJobActions(job)
                    if ec == 0:
                        tolog("postGetJobActions: OK")
                        # return ec
                    else:
                        tolog("!!WARNING!!1231!! Post getJob() actions encountered a problem - job will fail")
                        try:
                            # job must be failed correctly
                            pUtil.tolog("Updating PanDA server for the failed job (error code %d)" % (ec))
                            job.jobState = 'failed'
                            job.setState([job.jobState, 0, ec])
                            # note: job.workdir has not been created yet so cannot create log file
                            pilotErrorDiag = "Post getjob actions failed - workdir does not exist, cannot create job log, see batch log"
                            tolog("!!WARNING!!2233!! Work dir has not been created yet so cannot create job log in this case - refer to batch log")

                            JR = JobRecovery(pshttpurl='https://pandaserver.cern.ch', pilot_initdir=job.workdir)
                            JR.updatePandaServer(job, self.__jobSite, self.__node, 25443) 
                            #pUtil.fastCleanup(self.__jobSite.workdir, self.__pilotWorkingDir, True)
                            return ec
                        except Exception, e:
                            pUtil.tolog("Caught exception: %s" % (e))
                            return ec
                except Exception, e:
                    pUtil.tolog("Caught exception: %s" % (e))
                    return -1
        except:
            tolog("Failed to get job: %s" % (traceback.format_exc()))
            return -1

        self.setupJob(job, data)
        self.updateJobState(job, 'starting', '', final=False)
        return 0

    def getHPCEventJobFromEnv(self):
        tolog("getHPCEventJobFromEnv")
        try:
            # always use this filename as the new jobDef module name
            import newJobDef
            job = Job.Job()
            job.setJobDef(newJobDef.job)
            logGUID = newJobDef.job.get('logGUID', "")
            if logGUID != "NULL" and logGUID != "":
                job.tarFileGuid = logGUID

            if self.__firstJob:
                job.workdir = self.__jobSite.workdir
                self.__firstJob = False

            self.__firstJobId = job.jobId
            filename = os.path.join(self.__pilotWorkingDir, "Job_%s.json" % job.jobId)
            content = {'workdir': job.workdir, 'data': newJobDef.job, 'experiment': self.__thisExperiment.getExperiment()}
            with open(filename, 'w') as outputFile:
                json.dump(content, outputFile)

            self.__jobStateFile = '%s/jobState-%s.pickle' % (self.__pilotWorkingDir, job.jobId)
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            self.failOneJob(0, PilotErrors.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag, final=True, updatePanda=False)
            return -1

        self.setupJob(job, newJobDef.job)
        self.updateJobState(job, 'starting', '', final=False)
        return 0

    def updateJobState(self, job, jobState, hpcState, final=False, updatePanda=True):
        job.setMode(self.__hpcMode)
        job.jobState = jobState
        job.setState([job.jobState, 0, 0])
        job.setHpcStatus(hpcState)
        if job.pilotErrorDiag and len(job.pilotErrorDiag.strip()) == 0:
            job.pilotErrorDiag = None
        JR = self.__jobs[job.jobId]['JR']

        pilotErrorDiag = job.pilotErrorDiag

        JR.updateJobStateTest(job, self.__jobSite, self.__node, mode="test")
        rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), self.getPilotPort(), final=final)
        if updatePanda:
            JR.updatePandaServer(job, self.__jobSite, self.__node, 25443)
        job.pilotErrorDiag = pilotErrorDiag

    def updateAllJobsState(self, jobState, hpcState, final=False, updatePanda=False):
        for jobId in self.__jobs:
            self.updateJobState(self.__jobs[jobId]['job'], jobState, hpcState, final=final, updatePanda=updatePanda)

    def getHPCEventJobs(self):
        self.getHPCEventJobFromEnv()
        tolog("NJobs: %s" % self.__nJobs)
        while self.__neededEventRanges > 0 and (len(self.__jobs.keys()) < int(self.__nJobs)):
            tolog("Len(jobs): %s" % len(self.__jobs.keys()))
            tolog("NJobs: %s" % self.__nJobs)
            try:
                ret = self.getHPCEventJobFromPanda()
                if ret != 0:
                    tolog("Failed to get a job from panda.")
                    break
            except:
                tolog("Failed to get job: %s" % (traceback.format_exc()))
                break

        self.__hpcStatue = ''
        #self.updateAllJobsState('starting', self.__hpcStatue)
            

    def stageInOneJob(self, job, jobSite, analysisJob, avail_files={}, pfc_name="PoolFileCatalog.xml"):
        """ Perform the stage-in """

        current_dir = os.getcwd()
        os.chdir(job.workdir)
        tolog("Start to stage in input files for job %s" % job.jobId)
        tolog("Switch from current dir %s to job %s workdir %s" % (current_dir, job.jobId, job.workdir))

        real_stagein = False
        for lfn in job.inFiles:
            if not (lfn in self.__avail_files):
                real_stagein = True
        if not real_stagein:
            tolog("All files for job %s have copies locally, will try to copy locally" % job.jobId)
            for lfn in job.inFiles:
                try:
                    copy_src = self.__avail_files[lfn]
                    copy_dest = os.path.join(job.workdir, lfn)
                    tolog("Copy %s to %s" % (copy_src, copy_dest))
                    shutil.copyfile(copy_src, copy_dest)
                except:
                    tolog("Failed to copy file: %s" % traceback.format_exc())
                    real_stagein = True
                    break
        if not real_stagein:
            tolog("All files for job %s copied locally" % job.jobId)
            tolog("Switch back from job %s workdir %s to current dir %s" % (job.jobId, job.workdir, current_dir))
            os.chdir(current_dir)
            return job, job.inFiles, None, None

        ec = 0
        statusPFCTurl = None
        usedFAXandDirectIO = False

        # Prepare the input files (remove non-valid names) if there are any
        ins, job.filesizeIn, job.checksumIn = RunJobUtilities.prepareInFiles(job.inFiles, job.filesizeIn, job.checksumIn)
        if ins:
            tolog("Preparing for get command")

            # Get the file access info (only useCT is needed here)
            useCT, oldPrefix, newPrefix = pUtil.getFileAccessInfo()

            # Transfer input files
            tin_0 = os.times()
            ec, job.pilotErrorDiag, statusPFCTurl, FAX_dictionary = \
                mover.get_data(job, jobSite, ins, self.__stageinretry, analysisJob=analysisJob, usect=useCT,\
                               pinitdir=self.getPilotInitDir(), proxycheck=False, inputDir='', workDir=job.workdir, pfc_name=pfc_name)
            if ec != 0:
                job.result[2] = ec
            tin_1 = os.times()
            job.timeStageIn = int(round(tin_1[4] - tin_0[4]))

            # Extract any FAX info from the dictionary
            if FAX_dictionary.has_key('N_filesWithoutFAX'):
                job.filesWithoutFAX = FAX_dictionary['N_filesWithoutFAX']
            if FAX_dictionary.has_key('N_filesWithFAX'):
                job.filesWithFAX = FAX_dictionary['N_filesWithFAX']
            if FAX_dictionary.has_key('bytesWithoutFAX'):
                job.bytesWithoutFAX = FAX_dictionary['bytesWithoutFAX']
            if FAX_dictionary.has_key('bytesWithFAX'):
                job.bytesWithFAX = FAX_dictionary['bytesWithFAX']
            if FAX_dictionary.has_key('usedFAXandDirectIO'):
                usedFAXandDirectIO = FAX_dictionary['usedFAXandDirectIO']

        tolog("Switch back from job %s workdir %s to current dir %s" % (job.jobId, job.workdir, current_dir))
        os.chdir(current_dir)

        if ec == 0:
            for inFile in ins:
                self.__avail_files[inFile] = os.path.join(job.workdir, inFile)
        return job, ins, statusPFCTurl, usedFAXandDirectIO

    def failOneJob(self, transExitCode, pilotExitCode, job, ins=None, pilotErrorDiag=None, docleanup=True, final=True, updatePanda=False):
        """ set the fail code and exit """

        current_dir = os.getcwd()
        if pilotExitCode and job.attemptNr < 4 and job.eventServiceMerge:
            pilotExitCode = PilotErrors.ERR_ESRECOVERABLE
        job.setState(["failed", transExitCode, pilotExitCode])
        if pilotErrorDiag:
            job.pilotErrorDiag = pilotErrorDiag
        tolog("Job %s failed. Will now update local pilot TCP server" % job.jobId)
        self.updateJobState(job, 'failed', 'failed', final=final, updatePanda=updatePanda)
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        self.cleanup(job)
        sys.stderr.close()
        tolog("Job %s has failed" % job.jobId)
        os.chdir(current_dir)

    def failAllJobs(self, transExitCode, pilotExitCode, jobs, pilotErrorDiag=None, docleanup=True, updatePanda=False):
        firstJob = None
        for jobId in jobs:
            if self.__firstJobId and (jobId == self.__firstJobId):
                firstJob = jobs[jobId]['job']
                continue
            job = jobs[jobId]['job']
            self.failOneJob(transExitCode, pilotExitCode, job, ins=job.inFiles, pilotErrorDiag=pilotErrorDiag, updatePanda=updatePanda)
        if firstJob:
            self.failOneJob(transExitCode, pilotExitCode, firstJob, ins=firstJob.inFiles, pilotErrorDiag=pilotErrorDiag, updatePanda=updatePanda)
        os._exit(pilotExitCode)

    def stageInHPCJobs(self):
        tolog("Setting stage-in state until all input files have been copied")
        jobResult = 0
        pilotErrorDiag = ""
        self.__avail_files = {}
        failedJobIds = []
        for jobId in self.__jobs:
            try:
                job = self.__jobs[jobId]['job']
                # self.updateJobState(job, 'transferring', '')
                self.updateJobState(job, 'starting', '')

                # stage-in all input files (if necessary)
                jobRet, ins, statusPFCTurl, usedFAXandDirectIO = self.stageInOneJob(job, self.__jobSite, self.__jobs[job.jobId]['analysisJob'], self.__avail_files, pfc_name="PFC.xml")
                if jobRet.result[2] != 0:
                    tolog("Failing job with ec: %d" % (jobRet.result[2]))
                    jobResult = jobRet.result[2]
                    pilotErrorDiag = job.pilotErrorDiag
                    failedJobIds.append(jobId)
                    self.failOneJob(0, jobResult, job, ins=job.inFiles, pilotErrorDiag=pilotErrorDiag, updatePanda=True)
                    continue
                    #break
                job.displayJob()
                self.__jobs[job.jobId]['job'] = job
            except:
                tolog("stageInHPCJobsException")
                try:
                    tolog(traceback.format_exc())
                except:
                    tolog("Failed to print traceback")

        for jobId in failedJobIds:
            del self.__jobs[jobId]
        #if jobResult != 0:
        #    self.failAllJobs(0, jobResult, self.__jobs, pilotErrorDiag=pilotErrorDiag)


    def updateEventRange(self, event_range_id, status='finished'):
        """ Update an event range on the Event Server """
        tolog("Updating an event range..")

        message = ""
        # url = "https://aipanda007.cern.ch:25443/server/panda"
        url = "https://pandaserver.cern.ch:25443/server/panda"
        node = {}
        node['eventRangeID'] = event_range_id

        # node['cpu'] =  eventRangeList[1]
        # node['wall'] = eventRangeList[2]
        node['eventStatus'] = status
        # tolog("node = %s" % str(node))

        # open connection
        ret = httpConnect(node, url, path=self.__pilotWorkingDir, mode="UPDATEEVENTRANGE")
        # response = ret[1]

        if ret[0]: # non-zero return code
            message = "Failed to update event range - error code = %d" % (ret[0])
        else:
            message = ""

        return ret[0], message


    def getJobEventRanges(self, job, numRanges=2):
        """ Download event ranges from the Event Server """
        tolog("Server: Downloading new event ranges..")

        if os.environ.has_key('EventRanges') and os.path.exists(os.environ['EventRanges']):
            try:
                with open(os.environ['EventRanges']) as json_file:
                    events = json.load(json_file)
                    tolog(events)
                    return events
            except:
                tolog('Failed to open event ranges json file: %s' % traceback.format_exc())

        message = ""
        # url = "https://aipanda007.cern.ch:25443/server/panda"
        url = "https://pandaserver.cern.ch:25443/server/panda"

        node = {}
        node['pandaID'] = job.jobId
        node['jobsetID'] = job.jobsetID
        node['taskID'] = job.taskID
        node['nRanges'] = numRanges

        # open connection
        ret = httpConnect(node, url, path=os.getcwd(), mode="GETEVENTRANGES")
        response = ret[1]

        if ret[0]: # non-zero return code
            message = "Failed to download event range - error code = %d" % (ret[0])
            tolog(message)
            return []
        else:
            message = response['eventRanges']
            return json.loads(message)


    def updateHPCEventRanges(self):
        for jobId in self.__eventRanges:
            for eventRangeID in self.__eventRanges[jobId]:
                if self.__eventRanges[jobId][eventRangeID] == 'stagedOut' or self.__eventRanges[jobId][eventRangeID] == 'failed':
                    if self.__eventRanges[jobId][eventRangeID] == 'stagedOut':
                        eventStatus = 'finished'
                    else:
                        eventStatus = 'failed'
                    try:
                        ret, message = self.updateEventRange(eventRangeID, eventStatus)
                    except Exception, e:
                        tolog("Failed to update event range: %s, %s, exception: %s " % (eventRangeID, eventStatus, str(e)))
                    else:
                        if ret == 0:
                            self.__eventRanges[jobId][eventRangeID] = "Done"
                        else:
                            tolog("Failed to update event range: %s" % eventRangeID)


    def prepareHPCJob(self, job):
        tolog("Prepare for job %s" % job.jobId)
        current_dir = os.getcwd()
        os.chdir(job.workdir)
        tolog("Switch from current dir %s to job %s workdir %s" % (current_dir, job.jobId, job.workdir))

        #print self.__runCommandList
        #print self.getParentWorkDir()
        #print self.__job.workdir
        # 1. input files
        inputFiles = []
        inputFilesGlobal = []
        for inputFile in job.inFiles:
            #inputFiles.append(os.path.join(self.__job.workdir, inputFile))
            inputFilesGlobal.append(os.path.join(job.workdir, inputFile))
            inputFiles.append(os.path.join('HPCWORKINGDIR', inputFile))
        inputFileDict = dict(zip(job.inFilesGuids, inputFilesGlobal))
        self.__jobs[job.jobId]['inputFilesGlobal'] = inputFilesGlobal

        tagFiles = {}
        EventFiles = {}
        for guid in inputFileDict:
            if '.TAG.' in inputFileDict[guid]:
                tagFiles[guid] = inputFileDict[guid]
            elif not "DBRelease" in inputFileDict[guid]:
                EventFiles[guid] = {}
                EventFiles[guid]['file'] = inputFileDict[guid]

        # 2. create TAG file
        jobRunCmd = self.__jobs[job.jobId]['runCommandList'][0]
	usingTokenExtractor = 'TokenScatterer' in jobRunCmd or 'UseTokenExtractor=True' in jobRunCmd.replace("  ","").replace(" ","")
        if usingTokenExtractor:
            for guid in EventFiles:
                local_copy = False
                if guid in self.__avail_tag_files:
                    local_copy = True
                    try:
                        tolog("Copy TAG file from %s to %s" % (self.__avail_tag_files[guid]['TAG_path'], job.workdir))
                        shutil.copy(self.__avail_tag_files[guid]['TAG_path'], job.workdir)
                    except:
                        tolog("Failed to copy %s to %s" % (self.__avail_tag_files[guid]['TAG_path'], job.workdir))
                        local_copy = False
                if local_copy:
                    tolog("Tag file for %s already copied locally. Will not create it again" % guid)
                    EventFiles[guid]['TAG'] = self.__avail_tag_files[guid]['TAG']
                    EventFiles[guid]['TAG_guid'] = self.__avail_tag_files[guid]['TAG_guid']
                else:
                    tolog("Tag file for %s does not exist. Will create it." % guid)
                    inFiles = [EventFiles[guid]['file']]
                    input_tag_file, input_tag_file_guid = self.createTAGFile(self.__jobs[job.jobId]['runCommandList'][0], job.trf, inFiles, "MakeRunEventCollection.py")
                    if input_tag_file != "" and input_tag_file_guid != "":
                        tolog("Will run TokenExtractor on file %s" % (input_tag_file))
                        EventFiles[guid]['TAG'] = input_tag_file
                        EventFiles[guid]['TAG_guid'] = input_tag_file_guid
                        self.__avail_tag_files[guid] = {'TAG': input_tag_file, 'TAG_path': os.path.join(job.workdir, input_tag_file), 'TAG_guid': input_tag_file_guid}
                    else:
                        # only for current test
                        if len(tagFiles)>0:
                            EventFiles[guid]['TAG_guid'] = tagFiles.keys()[0]
                            EventFiles[guid]['TAG'] = tagFiles[tagFiles.keys()[0]]
                        else:
                            return -1, "Failed to create the TAG file", None

        # 3. create Pool File Catalog
        inputFileDict = dict(zip(job.inFilesGuids, inputFilesGlobal))
        poolFileCatalog = os.path.join(job.workdir, "PoolFileCatalog_HPC.xml")
        createPoolFileCatalog(inputFileDict, poolFileCatalog)
        inputFileDictTemp = dict(zip(job.inFilesGuids, inputFiles))
        poolFileCatalogTemp = os.path.join(job.workdir, "PoolFileCatalog_Temp.xml")
        poolFileCatalogTempName = "HPCWORKINGDIR/PoolFileCatalog_Temp.xml"
        createPoolFileCatalog(inputFileDictTemp, poolFileCatalogTemp)
        self.__jobs[job.jobId]['poolFileCatalog'] = poolFileCatalog
        self.__jobs[job.jobId]['poolFileCatalogTemp'] = poolFileCatalogTemp
        self.__jobs[job.jobId]['poolFileCatalogTempName'] = poolFileCatalogTempName

        # 4. getSetupCommand
        setupCommand = self.stripSetupCommand(self.__jobs[job.jobId]['runCommandList'][0], job.trf)
        _cmd = re.search('(source.+\;)', setupCommand)
        source_setup = None
        if _cmd:
            setup = _cmd.group(1)
            source_setup = setup.split(";")[0]
            #setupCommand = setupCommand.replace(source_setup, source_setup + " --cmtextratags=ATLAS,useDBRelease")
            # for test, asetup has a bug
            #new_source_setup = source_setup.split("cmtsite/asetup.sh")[0] + "setup-19.2.0-quick.sh"
            #setupCommand = setupCommand.replace(source_setup, new_source_setup)
        tolog("setup command: " + setupCommand)

        # 5. check if release-compact.tgz exists. If it exists, use it.
        preSetup = None
        postRun = None
        # yoda_setup_command = 'export USING_COMPACT=1; %s' % source_setup

        # 6. AthenaMP command
        runCommandList_0 = self.__jobs[job.jobId]['runCommandList'][0]
        runCommandList_0 = 'export USING_COMPACT=1; %s' % runCommandList_0
        # if yoda_setup_command:
        #     runCommandList_0 = self.__jobs[job.jobId]['runCommandList'][0]
        #     runCommandList_0 = runCommandList_0.replace(source_setup, yoda_setup_command)
        if not self.__copyInputFiles:
            jobInputFileList = None
            jobInputFileList = inputFilesGlobal[0]
            command_list = runCommandList_0.split(" ")
            command_list_new = []
            for command_part in command_list:
                if command_part.startswith("--input"):
                    command_arg = command_part.split("=")[0]
                    command_part_new = command_arg + "=" + jobInputFileList
                    command_list_new.append(command_part_new)
                else:
                    command_list_new.append(command_part)
            runCommandList_0 = " ".join(command_list_new)


            runCommandList_0 += " '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (poolFileCatalog)
        else:
            runCommandList_0 += " '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (poolFileCatalogTempName)

        # should not have --DBRelease and UserFrontier.py in HPC
        runCommandList_0 = runCommandList_0.replace("--DBRelease=current", "")
        if 'RecJobTransforms/UseFrontier.py,' in runCommandList_0:
            runCommandList_0 = runCommandList_0.replace('RecJobTransforms/UseFrontier.py,', '')
        if ',RecJobTransforms/UseFrontier.py' in runCommandList_0:
            runCommandList_0 = runCommandList_0.replace(',RecJobTransforms/UseFrontier.py', '')
        if ' --postInclude=RecJobTransforms/UseFrontier.py ' in runCommandList_0:
            runCommandList_0 = runCommandList_0.replace(' --postInclude=RecJobTransforms/UseFrontier.py ', ' ')
        if '--postInclude "default:RecJobTransforms/UseFrontier.py"' in runCommandList_0:
            runCommandList_0 = runCommandList_0.replace('--postInclude "default:RecJobTransforms/UseFrontier.py"', ' ')

        runCommandList_0 += " 1>athenaMP_stdout.txt 2>athenaMP_stderr.txt"
        runCommandList_0 = runCommandList_0.replace(";;", ";")
        #self.__jobs[job.jobId]['runCommandList'][0] = runCommandList_0

        # 7. Token Extractor file list
        # in the token extractor file list, the guid is the Event guid, not the tag guid.
        if usingTokenExtractor:
            tagFile_list = os.path.join(job.workdir, "TokenExtractor_filelist")
            handle = open(tagFile_list, 'w')
            for guid in EventFiles:
                tagFile = EventFiles[guid]['TAG']
                line = guid + ",PFN:" + tagFile + "\n"
                handle.write(line)
            handle.close()
            self.__jobs[job.jobId]['tagFile_list'] = tagFile_list
        else:
            self.__jobs[job.jobId]['tagFile_list'] = None

        # 8. Token Extractor command
        if usingTokenExtractor:
            setup = setupCommand
            tokenExtractorCmd = setup + ";" + " TokenExtractor -v  --source " + tagFile_list + " 1>tokenExtract_stdout.txt 2>tokenExtract_stderr.txt"
            tokenExtractorCmd = tokenExtractorCmd.replace(";;", ";")
            self.__jobs[job.jobId]['tokenExtractorCmd'] = tokenExtractorCmd
        else:
            self.__jobs[job.jobId]['tokenExtractorCmd'] = None

        os.chdir(current_dir)
        tolog("Switch back from job %s workdir %s to current dir %s" % (job.jobId, job.workdir, current_dir))

        return 0, None, {"TokenExtractCmd": self.__jobs[job.jobId]['tokenExtractorCmd'], "AthenaMPCmd": runCommandList_0, "PreSetup": preSetup, "PostRun": postRun, 'PoolFileCatalog': poolFileCatalog, 'InputFiles': inputFilesGlobal, 'GlobalWorkingDir': job.workdir}

    def prepareHPCJobs(self):
        for jobId in self.__jobs:
            status, output, hpcJob = self.prepareHPCJob(self.__jobs[jobId]['job'])
            tolog("HPC Job %s: %s " % (jobId, hpcJob))
            if status == 0:
                self.__jobs[jobId]['hpcJob'] = hpcJob
            else:
                return status, output
        return 0, None

    def getJobDatasets(self, job):
        """ Get the datasets for the output files """

        # Get the default dataset
        if job.destinationDblock and job.destinationDblock[0] != 'NULL' and job.destinationDblock[0] != ' ':
            dsname = job.destinationDblock[0]
        else:
            dsname = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name

        # Create the dataset dictionary
        # (if None, the dsname above will be used for all output files)
        datasetDict = getDatasetDict(job.outFiles, job.destinationDblock, job.logFile, job.logDblock)
        if datasetDict:
            tolog("Dataset dictionary has been verified: %s" % str(datasetDict))
        else:
            tolog("Dataset dictionary could not be verified, output files will go to: %s" % (dsname))

        return dsname, datasetDict

    def setupJobStageOutHPCEvent(self, job):
        if job.prodDBlockTokenForOutput is not None and len(job.prodDBlockTokenForOutput) > 0 and job.prodDBlockTokenForOutput[0] != 'NULL':
            siteInfo = getSiteInformation(self.getExperiment())
            objectstore_orig = siteInfo.readpar("objectstore")
            #siteInfo.replaceQueuedataField("objectstore", self.__job.prodDBlockTokenForOutput[0])
            espath = getFilePathForObjectStore(filetype="eventservice")
        else:
            #siteInfo = getSiteInformation(self.getExperiment())
            #objectstore = siteInfo.readpar("objectstore")
            espath = getFilePathForObjectStore(filetype="eventservice")
        espath = getFilePathForObjectStore(filetype="eventservice")
        tolog("EventServer objectstore path: " + espath)

        siteInfo = getSiteInformation(self.getExperiment())
        # get the copy tool
        setup = siteInfo.getCopySetup(stageIn=False)
        tolog("Copy Setup: %s" % (setup))

        dsname, datasetDict = self.getJobDatasets(job)
        report = getInitialTracingReport(userid=job.prodUserID, sitename=self.__jobSite.sitename, dsname=dsname, eventType="objectstore", analysisJob=self.__analysisJob, jobId=self.__job.jobId, jobDefId=self.__job.jobDefinitionID, dn=self.__job.prodUserID)
        self.__siteMover = objectstoreSiteMover(setup)


    def stageOutHPCEvent(self, output_info):
        eventRangeID, status, output = output_info
        self.__output_es_files.append(output)

        if status == 'failed':
            try:
                self.__eventRanges[eventRangeID] = 'failed'
            except Exception, e:
                tolog("!!WARNING!!2233!! update %s:%s threw an exception: %s" % (eventRangeID, 'failed', e))
        if status == 'finished':
            status, pilotErrorDiag, surl, size, checksum, self.arch_type = self.__siteMover.put_data(output, self.__espath, lfn=os.path.basename(output), report=self.__report, token=self.__job.destinationDBlockToken, experiment=self.__job.experiment)
            if status == 0:
                try:
                    #self.updateEventRange(eventRangeID)
                    self.__eventRanges[eventRangeID] = 'stagedOut'
                    tolog("Remove staged out output file: %s" % output)
                    os.remove(output)
                except Exception, e:
                    tolog("!!WARNING!!2233!! remove ouput file threw an exception: %s" % (e))
                    #self.__failedStageOuts.append(output_info)
                else:
                    tolog("remove output file has returned")
            else:
                tolog("!!WARNING!!1164!! Failed to upload file to objectstore: %d, %s" % (status, pilotErrorDiag))
                self.__failedStageOuts.append(output_info)


    def startHPCJobs(self):
        tolog("startHPCJobs")
        self.__hpcStatue = 'starting'
        self.updateAllJobsState('starting', self.__hpcStatue)

        status, output = self.prepareHPCJobs()
        if status != 0:
            tolog("Failed to prepare HPC jobs: status %s, output %s" % (status, output))
            self.failAllJobs(0, PilotErrors.ERR_UNKNOWN, self.__jobs, pilotErrorDiag=output)
            return 

        # setup stage out
        #self.setupStageOutHPCEvent()

        self.__hpcStatus = None
        self.__hpcLog = None

        hpcManager = self.__hpcManager
        totalCores = hpcManager.getCoreCount()
        totalJobs = len(self.__jobs.keys())
        if totalJobs < 1:
            totalJobs = 1
        avgCores = totalCores / totalJobs
        hpcJobs = {}
        for jobId in self.__jobs:
            self.__jobs[jobId]['job'].coreCount = avgCores
            if len(self.__eventRanges[jobId]) > 0:
                hpcJobs[jobId] = self.__jobs[jobId]['hpcJob']
        hpcManager.initJobs(hpcJobs, self.__jobEventRanges)

        totalCores = hpcManager.getCoreCount()
        avgCores = totalCores / totalJobs
        for jobId in self.__jobs:
            self.__jobs[jobId]['job'].coreCount = avgCores

        if hpcManager.isLocalProcess():
            self.__hpcStatue = 'running'
            self.updateAllJobsState('running', self.__hpcStatue)

        tolog("Submit HPC job")
        hpcManager.submit()
        tolog("Submitted HPC job")

        hpcManager.setPandaJobStateFile(self.__jobStateFile)
        #self.__stageout_threads = defRes['stageout_threads']
        hpcManager.setStageoutThreads(self.__stageout_threads)
        hpcManager.saveState()
        self.__hpcManager = hpcManager

    def startHPCSlaveJobs(self):
        tolog("Setup HPC Manager")
        hpcManager = HPCManager(globalWorkingDir=self.__pilotWorkingDir)
        tolog("Submit HPC job")
        hpcManager.submit()
        tolog("Submitted HPC job")

    def runHPCEvent(self):
        tolog("runHPCEvent")
        threadpool = ThreadPool(self.__stageout_threads)
        hpcManager = self.__hpcManager

        try:
            old_state = None
            time_start = time.time()
            while not hpcManager.isFinished():
                state = hpcManager.poll()
                self.__job.setHpcStatus(state)
                if old_state is None or old_state != state or time.time() > (time_start + 60*10):
                    old_state = state
                    time_start = time.time()
                    tolog("HPCManager Job stat: %s" % state)
                    self.__JR.updateJobStateTest(self.__job, self.__jobSite, self.__node, mode="test")
                    rt = RunJobUtilities.updatePilotServer(self.__job, self.getPilotServer(), self.getPilotPort())
                    self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)

                if state and state == 'Running':
                    self.__job.jobState = "running"
                    self.__job.setState([self.__job.jobState, 0, 0])
                if state and state == 'Complete':
                    break
                outputs = hpcManager.getOutputs()
                for output in outputs:
                    #self.stageOutHPCEvent(output)
                    threadpool.add_task(self.stageOutHPCEvent, output)

                time.sleep(30)
                self.updateHPCEventRanges()

            tolog("HPCManager Job Finished")
            self.__job.setHpcStatus('stagingOut')
            rt = RunJobUtilities.updatePilotServer(self.__job, self.getPilotServer(), self.getPilotPort())
            self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)
        except:
            tolog("RunHPCEvent failed: %s" % traceback.format_exc())

        for i in range(3):
            try:
                tolog("HPC Stage out outputs retry %s" % i)
                hpcManager.flushOutputs()
                outputs = hpcManager.getOutputs()
                for output in outputs:
                    #self.stageOutHPCEvent(output)
                    threadpool.add_task(self.stageOutHPCEvent, output)

                self.updateHPCEventRanges()
                threadpool.wait_completion()
                self.updateHPCEventRanges()
            except:
                tolog("RunHPCEvent stageout outputs retry %s failed: %s" % (i, traceback.format_exc()))

        for i in range(3):
            try:
                tolog("HPC Stage out failed outputs retry %s" % i)
                failedStageOuts = self.__failedStageOuts
                self.__failedStageOuts = []
                for failedStageOut in failedStageOuts:
                    threadpool.add_task(self.stageOutHPCEvent, failedStageOut)
                threadpool.wait_completion()
                self.updateHPCEventRanges()
            except:
                tolog("RunHPCEvent stageout failed outputs retry %s failed: %s" % (i, traceback.format_exc()))

        self.__job.setHpcStatus('finished')
        self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)
        self.__hpcStatus, self.__hpcLog = hpcManager.checkHPCJobLog()
        tolog("HPC job log status: %s, job log error: %s" % (self.__hpcStatus, self.__hpcLog))
        

    def startEventStager(self):
        try:
            tolog("Starting Event Stager")
            # get the copy tool
            setup = self.__siteInfo.getCopySetup(stageIn=False)

            espath = getFilePathForObjectStore(filetype="eventservice")
            token = None
            
            self.__eventStager = EventStager(workDir=self.__pilotWorkingDir, setup=setup, esPath=espath, token=token, experiment=self.getExperiment(), userid=self.__userid, sitename=self.__jobSite.sitename, threads=self.__stageout_threads, outputDir=self.getOutputDir(), yodaToOS=self.__yoda_to_os)
            self.__eventStager.start()
        except:
            tolog("Failed to start Event Stager: %s" % traceback.format_exc())

    def getEventStagerLog(self):
        try:
            return self.__eventStager.getLog()
        except:
            tolog("Failed to get Event Stager Log: %s" % traceback.format_exc())
            return None

    def updateEventRangesFromStager(self):
        tolog("updateEventRangesFromStager")
        all_files = os.listdir(self.__pilotWorkingDir)
        for file in all_files:
            if file.endswith(".staged.reported"):
                filename = os.path.join(self.__pilotWorkingDir, file)
                handle = open(filename)
                for output_info in handle:
                    if len(output_info)<2:
                        continue
                    jobId, eventRangeID, status, output = output_info.split(" ")
                    if jobId not in self.__eventRanges:
                        self.__eventRanges[jobId] = {}
                    self.__eventRanges[jobId][eventRangeID] = 'Done'
                handle.close()
            if file.endswith(".staged"):
                filename = os.path.join(self.__pilotWorkingDir, file)
                handle = open(filename)
                for output_info in handle:
                    jobId, eventRangeID, status, output = output_info.split(" ")
                    self.__eventRanges[jobId][eventRangeID] = 'stagedOut'
                handle.close()
                self.updateHPCEventRanges()
                os.rename(file, file + ".reported")

    def monitorEventStager(self):
        try:
            self.__eventStager.monitor()
            self.updateEventRangesFromStager()
        except:
            tolog("Failed to monitor Event Stager: %s" % traceback.format_exc())

    def finishEventStager(self):
        try:
            self.__eventStager.finish()
        except:
            tolog("failed to finish event ranges %s" % traceback.format_exc())

    def isEventStagerFinished(self):
        try:
            return self.__eventStager.isFinished()
        except:
            tolog("Failed in check Event Stager status: %s" % traceback.format_exc())
            return False

    def runHPCEventJobsWithEventStager(self):
        tolog("runHPCEventWithEventStager")
        hpcManager = self.__hpcManager
        self.startEventStager()

        try:
            old_state = None
            time_start = time.time()
            state = hpcManager.poll()
            self.__hpcStatue = state
            self.updateAllJobsState('starting', self.__hpcStatue, updatePanda=True)

            while not hpcManager.isFinished():
                state = hpcManager.poll()
                self.__hpcStatue = state
                if old_state is None or old_state != state or time.time() > (time_start + 60*10):
                    old_state = state
                    time_start = time.time()
                    tolog("HPCManager Job stat: %s" % state)
                    if state and state == 'Running':
                        self.updateAllJobsState('running', self.__hpcStatue, updatePanda=True)

                if state and state == 'Complete':
                    break

                self.monitorEventStager()
                time.sleep(30)

            tolog("HPCManager Job Finished")
            self.__hpcStatue = 'stagingOut'
            self.updateAllJobsState('running', self.__hpcStatue)
        except:
            tolog("RunHPCEvent failed: %s" % traceback.format_exc())

        try:
            self.finishEventStager()
            while not self.isEventStagerFinished():
                self.monitorEventStager()
                time.sleep(30)

            eventStager_log = self.getEventStagerLog()
            for jobId in self.__jobs:
                job_workdir = self.__jobs[jobId]['job'].workdir
                tolog("Copy event stager log %s to job %s work dir %s" % (eventStager_log, jobId, job_workdir))
                shutil.copy(eventStager_log, job_workdir)
        except:
            tolog("Waiting EventStager: %s" % traceback.format_exc())

        try:
            hpcManager.postRun()
        except:
            tolog("HPCManager postRun: %s" % traceback.format_exc())

        self.__hpcStatue = 'finished'
        self.updateAllJobsState('running', self.__hpcStatue)
        self.__hpcStatus, self.__hpcLog = hpcManager.checkHPCJobLog()
        tolog("HPC job log status: %s, job log error: %s" % (self.__hpcStatus, self.__hpcLog))


    def check_unmonitored_jobs(self):
        all_jobs = {}
        all_files = os.listdir(self.__pilotWorkingDir)
        for file in all_files:
            if re.search('Job_[0-9]+.json', file):
                filename = os.path.join(self.__pilotWorkingDir, file)
                jobId = file.replace("Job_", "").replace(".json", "")
                all_jobs[jobId] = filename
        pUtil.tolog("Found jobs: %s" % all_jobs)
        for jobId in all_jobs:
            try:
                with open(all_jobs[jobId]) as inputFile:
                    content = json.load(inputFile)
                job = Job.Job()
                job.setJobDef(content['data'])
                job.workdir = content['workdir']
                job.experiment = content['experiment']
                runCommandList = content.get('runCommandList', [])
                logGUID = content['data'].get('logGUID', "")
                if logGUID != "NULL" and logGUID != "":
                    job.tarFileGuid = logGUID
                if job.prodUserID:
                    self.__userid = job.prodUserID
                job.outFiles = []

                if (not job.workdir) or (not os.path.exists(job.workdir)):
                    pUtil.tolog("Job %s work dir %s doesn't exit, will not add it to monitor" % (job.jobId, job.workdir))
                    continue
                if jobId not in self.__jobs:
                    self.__jobs[jobId] = {}
                self.__jobs[jobId]['job'] = job
                self.__jobs[job.jobId]['JR'] = JobRecovery(pshttpurl='https://pandaserver.cern.ch', pilot_initdir=job.workdir)
                self.__jobs[job.jobId]['runCommandList'] = runCommandList
                self.__eventRanges[job.jobId] = {}
            except:
                pUtil.tolog("Failed to load unmonitored job %s: %s" % (jobId, traceback.format_exc()))

    def recoveryJobs(self):
        tolog("Start to recovery job.")
        job_state_file = self.getJobStateFile()
        JS = JobState()
        JS.get(job_state_file)
        _job, _site, _node, _recoveryAttempt = JS.decode()
        #self.__job = _job
        self.__jobs[_job.jobId] = {'job': _job}
        self.__jobSite = _site

        # set node info
        self.__node = Node.Node()
        self.__node.setNodeName(os.uname()[1])
        self.__node.collectWNInfo(self.__jobSite.workdir)

        tolog("The job state is %s" % _job.jobState)
        if _job.jobState in ['starting', 'transfering']:
            tolog("The job hasn't started to run, will not recover it. Just finish it.")
            return False

        os.chdir(self.__jobSite.workdir)
        self.__jobs[_job.jobId]['JR'] = JobRecovery(pshttpurl='https://pandaserver.cern.ch', pilot_initdir=_job.workdir)
        self.__pilotWorkingDir = os.path.dirname(_job.workdir)
        self.__siteInfo = getSiteInformation(self.getExperiment())
        self.__userid = "HPCEventRecovery"

        self.check_unmonitored_jobs()

        return True

    def recoveryHPCManager(self):
        logFileName = None
        tolog("Recover Lost HPC Event job")
        tolog("runJobHPCEvent.getPilotLogFilename=%s"% self.getPilotLogFilename())
        if self.getPilotLogFilename() != "":
            logFileName = self.getPilotLogFilename()
        hpcManager = HPCManager(globalWorkingDir=self.__pilotWorkingDir, logFileName=logFileName)
        hpcManager.recoveryState()
        self.__hpcManager = hpcManager
        self.__stageout_threads = hpcManager.getStageoutThreads()

    def finishOneJob(self, job):
        tolog("Finishing job %s" % job.jobId)
        current_dir = os.getcwd()
        os.chdir(job.workdir)
        tolog("Switch from current dir %s to job %s workdir %s" % (current_dir, job.jobId, job.workdir))

        pilotErrorDiag = ""
        if job.inFiles:
            ec = pUtil.removeFiles(job.workdir, job.inFiles)
        #if self.__output_es_files:
        #    ec = pUtil.removeFiles("/", self.__output_es_files)


        errorCode = PilotErrors.ERR_UNKNOWN
        if job.attemptNr < 4:
            errorCode = PilotErrors.ERR_ESRECOVERABLE

        if (not job.jobId in self.__eventRanges) or len(self.__eventRanges[job.jobId]) == 0:
            tolog("Cannot get event ranges")
            pilotErrorDiag = "Cannot get event ranges"
            # self.failOneJob(0, errorCode, job, pilotErrorDiag="Cannot get event ranges", final=True, updatePanda=False)
            # return -1
        else:
            if job.jobId in self.__eventRanges:
                eventRanges = self.__eventRanges[job.jobId]
                # check whether all event ranges are handled
                tolog("Total event ranges: %s" % len(eventRanges))
                not_handled_events = eventRanges.values().count('new')
                tolog("Not handled events: %s" % not_handled_events)
                done_events = eventRanges.values().count('Done')
                tolog("Finished events: %s" % done_events)
                stagedOut_events = eventRanges.values().count('stagedOut')
                tolog("stagedOut but not updated to panda server events: %s" % stagedOut_events)
                if done_events + stagedOut_events:
                    errorCode = PilotErrors.ERR_ESRECOVERABLE
                if not_handled_events + stagedOut_events:
                    tolog("Not all event ranges are handled. failed job")
                    # self.failOneJob(0, errorCode, job, pilotErrorDiag="Not All events are handled(total:%s, left:%s)" % (len(eventRanges), not_handled_events + stagedOut_events), final=True, updatePanda=False)
                    # return -1
                    pilotErrorDiag="Not All events are handled(total:%s, left:%s)" % (len(eventRanges), not_handled_events + stagedOut_events)

        dsname, datasetDict = self.getJobDatasets(job)
        tolog("dsname = %s" % (dsname))
        tolog("datasetDict = %s" % (datasetDict))

        # Create the output file dictionary needed for generating the metadata
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, job.workdir, fullpath=True)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            # self.failOneJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)
            errorCode = ec
            pilotErrorDiag += pilotErrorDiag
        tolog("outsDict: %s" % str(outsDict))

        # Create metadata for all successfully staged-out output files (include the log file as well, even if it has not been created yet)
        ec, job, outputFileInfo = self.createFileMetadata([], job, outsDict, dsname, datasetDict, self.__jobSite.sitename)
        if ec:
            # self.failOneJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)
            errorCode = ec
            pilotErrorDiag += job.pilotErrorDiag
            self.failOneJob(0, ec, job, pilotErrorDiag=pilotErrorDiag, final=True, updatePanda=False)
            return -1

        # Rename the metadata produced by the payload
        # if not pUtil.isBuildJob(outs):
        self.moveTrfMetadata(job.workdir, job.jobId)

        # Check the job report for any exit code that should replace the res_tuple[0]
        res0, exitAcronym, exitMsg = self.getTrfExitInfo(0, job.workdir)
        res = (res0, exitMsg, exitMsg)

        # Payload error handling
        ed = ErrorDiagnosis()
        job = ed.interpretPayload(job, res, False, 0, self.__jobs[job.jobId]['runCommandList'], self.getFailureCode())
        if job.result[1] != 0 or job.result[2] != 0:
            self.failOneJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag, final=True, updatePanda=False)
            return -1

        job.jobState = "finished"
        job.setState([job.jobState, 0, 0])
        job.jobState = job.result
        rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), self.getPilotPort(), final=True)

        tolog("Panda Job %s Done" % job.jobId)
        #self.sysExit(self.__job)
        self.cleanup(job)
        tolog("Switch back from job %s workdir %s to current dir %s" % (job.jobId, job.workdir, current_dir))
        os.chdir(current_dir)
        tolog("Finished job %s" % job.jobId)

    def copyLogFilesToJob(self):
        found_dirs = {}
        found_files = {}
        all_files = os.listdir(self.__pilotWorkingDir)
        for file in all_files:
            path = os.path.join(self.__pilotWorkingDir, file)
            if os.path.isdir(path):
                if file not in ['HPC', 'lib', 'radical', 'saga'] and not file.startswith("PandaJob_"):
                    if file == 'rank_0' or not file.startswith('ranksd_'):
                        found_dirs[file] = path
                        tolog("Found log dir %s" % path)
            else:
                if not (file.endswith(".py") or file.endswith(".pyc")):
                    found_files[file] = path
                    tolog("Found log file %s" % path)

        for jobId in self.__jobs:
            job = self.__jobs[jobId]['job']
            tolog("Copy log files to job %s work dir %s" % (jobId, job.workdir))
            for file in found_dirs:
                path = found_dirs[file]
                dest_dir = os.path.join(job.workdir, file)
                try:
                    pUtil.recursive_overwrite(path, dest_dir)
                except:
                    tolog("Failed to copy %s to %s: %s" % (path, dest_dir, traceback.format_exc()))
            for file in found_files:
                #if '.dump.' in file and not file.startswith(str(jobId)):
                #    continue
                path = found_files[file]
                dest_dir = os.path.join(job.workdir, file)
                try:
                    pUtil.recursive_overwrite(path, dest_dir)
                except:
                    tolog("Failed to copy %s to %s: %s" % (path, dest_dir, traceback.format_exc()))

    def finishJobs(self):
        try:
            self.__hpcManager.finishJob()
        except:
            tolog(sys.exc_info()[1])
            tolog(sys.exc_info()[2])

        try:
            tolog("Copying Log files to Job working dir")
            self.copyLogFilesToJob()
        except:
            tolog("Failed to copy log files to job working dir: %s" % (traceback.format_exc()))

        # If payload leaves the input files, delete them explicitly
        firstJob = None
        for jobId in self.__jobs:
            try:
                if self.__firstJobId and (jobId == self.__firstJobId):
                    firstJob = self.__jobs[jobId]['job']
                    continue

                job = self.__jobs[jobId]['job']
                self.finishOneJob(job)
            except:
                tolog("Failed to finish one job %s: %s" % (job.jobId, traceback.format_exc()))
        if firstJob:
            try:
                self.finishOneJob(firstJob)
            except:
                tolog("Failed to finish the first job %s: %s" % (firstJob.jobId, traceback.format_exc()))
        time.sleep(1)

if __name__ == "__main__":

    tolog("Starting RunJobHpcEvent")

    if not os.environ.has_key('PilotHomeDir'):
        os.environ['PilotHomeDir'] = os.getcwd()

    # define a new parent group
    os.setpgrp()

    runJob = RunJobHpcEvent()
    try:
        runJob.setupHPCEvent()
        if runJob.getRecovery():
            # recovery job
            runJob.recoveryJobs()
            runJob.recoveryHPCManager()
            #runJob.runHPCEvent()
            runJob.runHPCEventJobsWithEventStager()
        else:
            runJob.setupHPCManager()
            runJob.getHPCEventJobs()
            runJob.stageInHPCJobs()
            runJob.startHPCJobs()
            #runJob.runHPCEvent()
            runJob.runHPCEventJobsWithEventStager()
    except:
        tolog("RunJobHpcEventException")
        tolog(traceback.format_exc())
        tolog(sys.exc_info()[1])
        tolog(sys.exc_info()[2])
    finally:
        runJob.finishJobs()
