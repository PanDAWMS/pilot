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
import sys
import time

# Import relevant python/pilot modules
# Pilot modules
import Job
import Node
import Site
import pUtil
import RunJobUtilities

from ThreadPool import ThreadPool
from RunJob import RunJob              # Parent RunJob class
from JobRecovery import JobRecovery
from PilotErrors import PilotErrors
from ErrorDiagnosis import ErrorDiagnosis
from pUtil import tolog, getExperiment, isAnalysisJob, httpConnect, createPoolFileCatalog, getSiteInformation, getDatasetDict
from objectstoreSiteMover import objectstoreSiteMover
from Mover import getFilePathForObjectStore, getInitialTracingReport
from PandaServerClient import PandaServerClient

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
        self._hpcManager = None

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJob, cls).__new__(cls, *args, **kwargs)

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

        
    def setupHPCEvent(self):
        self.__jobSite = Site.Site()
        self.__jobSite.setSiteInfo(self.argumentParser())
        ## For HPC job, we don't need to reassign the workdir
        # reassign workdir for this job
        self.__jobSite.workdir = self.__jobSite.wntmpdir
        if not os.path.exists(self.__jobSite.workdir):
            os.makedirs(self.__jobSite.workdir)


        tolog("runJobHPCEvent.getPilotLogFilename=%s"% self.getPilotLogFilename())
        if self.getPilotLogFilename() != "":
            pUtil.setPilotlogFilename(self.getPilotLogFilename())

        # set node info
        self.__node = Node.Node()
        self.__node.setNodeName(os.uname()[1])
        self.__node.collectWNInfo(self.__jobSite.workdir)

        # redirect stderr
        #sys.stderr = open("%s/runJobHPCEvent.stderr" % (self.__jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % self.__jobSite.workdir)

        # get the experiment object
        self.__thisExperiment = getExperiment(self.getExperiment())
        tolog("runEvent will serve experiment: %s" % (self.__thisExperiment.getExperiment()))


    def getHPCEventJobFromPanda(self):
        pass

    def getHPCEventJobFromEnv(self):
        tolog("getHPCEventJobFromEnv")
        try:
            # always use this filename as the new jobDef module name
            import newJobDef
            job = Job.Job()
            job.setJobDef(newJobDef.job)
            job.coreCount = 0
            job.workdir = self.__jobSite.workdir
            job.experiment = self.getExperiment()
            # figure out and set payload file names
            job.setPayloadName(self.__thisExperiment.getPayloadName(job))
            # reset the default job output file list which is anyway not correct
            job.outFiles = []
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            self.failJob(0, PilotErrors.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag)

        self.__job = job
        # prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        self.__job.datadir = self.getParentWorkDir() + "/PandaJob_%s_data" % (job.jobId)

        # See if it's an analysis job or not
        trf = self.__job.trf
        self.__analysisJob = isAnalysisJob(trf.split(",")[0])

        # Setup starts here ................................................................................

        # Update the job state file
        self.__job.jobState = "starting"
        self.__job.setHpcStatus('init')


        # Send [especially] the process group back to the pilot
        self.__job.setState([self.__job.jobState, 0, 0])
        self.__job.jobState = self.__job.result
        rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), runJob.getPilotPort())

        self.__JR = JobRecovery(pshttpurl='https://pandaserver.cern.ch', pilot_initdir=self.__job.workdir)
        self.__JR.updateJobStateTest(self.__job, self.__jobSite, self.__node, mode="test")
        self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)

        # prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = self.setup(self.__job, self.__jobSite, self.__thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            self.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)
        tolog("Setup has finished successfully")
        self.__job =  job
        self.__runCommandList = runCommandList
        self.__multi_trf = multi_trf

        # job has been updated, display it again
        self.__job.displayJob()
        tolog("RunCommandList: %s" % self.__runCommandList)
        tolog("Multi_trf: %s" % self.__multi_trf)


    def stageInHPCEvent(self):
        tolog("Setting stage-in state until all input files have been copied")
        self.__job.jobState = "transferring"
        self.__job.setState([self.__job.jobState, 0, 0])
        rt = RunJobUtilities.updatePilotServer(self.__job, self.getPilotServer(), self.getPilotPort())
        self.__JR.updateJobStateTest(self.__job, self.__jobSite, self.__node, mode="test")

        # stage-in all input files (if necessary)
        job, ins, statusPFCTurl, usedFAXandDirectIO = self.stageIn(self.__job, self.__jobSite, self.__analysisJob, pfc_name="PFC.xml")
        if job.result[2] != 0:
            tolog("Failing job with ec: %d" % (job.result[2]))
            self.failJob(0, job.result[2], job, ins=ins, pilotErrorDiag=job.pilotErrorDiag)
        self.__job = job
        self.__job.displayJob()

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
        #if self.__job.inFiles != ['']:
        #    runCommandList = RunJobUtilities.updateRunCommandList(self.__runCommandList, self.getParentWorkDir(), self.__job.jobId, statusPFCTurl, self.__analysisJob, usedFAXandDirectIO)
        #    tolog("New runCommandList: %s" % runCommandList)

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
        ret = httpConnect(node, url, path=self.__job.workdir, mode="UPDATEEVENTRANGE")
        # response = ret[1]

        if ret[0]: # non-zero return code
            message = "Failed to update event range - error code = %d" % (ret[0])
        else:
            message = ""

        return ret[0], message


    def getEventRanges(self, numRanges=2):
        """ Download event ranges from the Event Server """
        tolog("Server: Downloading new event ranges..")

        message = ""
        # url = "https://aipanda007.cern.ch:25443/server/panda"
        url = "https://pandaserver.cern.ch:25443/server/panda"

        node = {}
        node['pandaID'] = self.__job.jobId
        node['jobsetID'] = self.__job.jobsetID
        node['taskID'] = self.__job.taskID
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
        for eventRangeID in self.__eventRanges:
            if self.__eventRanges[eventRangeID] == 'stagedOut':
                try:
                    ret, message = self.updateEventRange(eventRangeID)
                except Exception, e:
                    tolog("Failed to update event range: %s, exception: %s " % (eventRangeID, str(e)))
                else:
                    if ret == 0:
                        self.__eventRanges[eventRangeID] = "Done"
                    else:
                        tolog("Failed to update event range: %s" % eventRangeID)


    def prepareHPCJob(self):
        #print self.__runCommandList
        #print self.getParentWorkDir()
        #print self.__job.workdir
        # 1. input files
        inputFiles = []
        inputFilesGlobal = []
        for inputFile in self.__job.inFiles:
            #inputFiles.append(os.path.join(self.__job.workdir, inputFile))
            inputFilesGlobal.append(os.path.join(self.__job.workdir, inputFile))
            inputFiles.append(os.path.join('HPCWORKINGDIR', inputFile))
        inputFileDict = dict(zip(self.__job.inFilesGuids, inputFilesGlobal))
        self.__inputFilesGlobal = inputFilesGlobal

        tagFiles = {}
        EventFiles = {}
        for guid in inputFileDict:
            if '.TAG.' in inputFileDict[guid]:
                tagFiles[guid] = inputFileDict[guid]
            elif not "DBRelease" in inputFileDict[guid]:
                EventFiles[guid] = {}
                EventFiles[guid]['file'] = inputFileDict[guid]

        # 2. create TAG file
        for guid in EventFiles:
            inFiles = [EventFiles[guid]['file']]
            input_tag_file, input_tag_file_guid = self.createTAGFile(self.__runCommandList[0], self.__job.trf, inFiles, "MakeRunEventCollection.py")
            if input_tag_file != "" and input_tag_file_guid != "":
                tolog("Will run TokenExtractor on file %s" % (input_tag_file))
                EventFiles[guid]['TAG'] = input_tag_file
                EventFiles[guid]['TAG_guid'] = input_tag_file_guid
            else:
                # only for current test
                if len(tagFiles)>0:
                    EventFiles[guid]['TAG_guid'] = tagFiles.keys()[0]
                    EventFiles[guid]['TAG'] = tagFiles[tagFiles.keys()[0]]
                else:
                    return -1, "Failed to create the TAG file", None

        # 3. create Pool File Catalog
        inputFileDict = dict(zip(self.__job.inFilesGuids, inputFilesGlobal))
        self.__poolFileCatalog = os.path.join(self.__job.workdir, "PoolFileCatalog_HPC.xml")
        createPoolFileCatalog(inputFileDict, self.__job.inFiles, self.__poolFileCatalog)
        inputFileDictTemp = dict(zip(self.__job.inFilesGuids, inputFiles))
        self.__poolFileCatalogTemp = os.path.join(self.__job.workdir, "PoolFileCatalog_Temp.xml")
        self.__poolFileCatalogTempName = "HPCWORKINGDIR/PoolFileCatalog_Temp.xml"
        createPoolFileCatalog(inputFileDictTemp, self.__job.inFiles, self.__poolFileCatalogTemp)

        # 4. getSetupCommand
        setupCommand = self.stripSetupCommand(self.__runCommandList[0], self.__job.trf)
        _cmd = re.search('(source.+\;)', setupCommand)
        if _cmd:
            setup = _cmd.group(1)
            source_setup = setup.split(";")[0]
            #setupCommand = setupCommand.replace(source_setup, source_setup + " --cmtextratags=ATLAS,useDBRelease")
            # for test, asetup has a bug
            #new_source_setup = source_setup.split("cmtsite/asetup.sh")[0] + "setup-19.2.0-quick.sh"
            #setupCommand = setupCommand.replace(source_setup, new_source_setup)
        tolog("setup command: " + setupCommand)

        # 5. AthenaMP command
        if not self.__copyInputFiles:
            jobInputFileList = None
            jobInputFileList = ','.join(inputFilesGlobal)
            #for inputFile in self.__job.inFiles:
            #    jobInputFileList = ','.join(os.path.join(self.__job.workdir, inputFile))
            #    self.__runCommandList[0] = self.__runCommandList[0].replace(inputFile, os.path.join(self.__job.workdir, inputFile))
            command_list = self.__runCommandList[0].split(" ")
            command_list_new = []
            for command_part in command_list:
                if command_part.startswith("--input"):
                    command_arg = command_part.split("=")[0]
                    command_part_new = command_arg + "=" + jobInputFileList
                    command_list_new.append(command_part_new)
                else:
                    command_list_new.append(command_part)
            self.__runCommandList[0] = " ".join(command_list_new)

            self.__runCommandList[0] += ' --preExec \'from G4AtlasApps.SimFlags import simFlags;simFlags.RunNumber=222222;from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.Strategy="TokenScatterer";from AthenaCommon.AppMgr import ServiceMgr as svcMgr;from AthenaServices.AthenaServicesConf import OutputStreamSequencerSvc;outputStreamSequencerSvc = OutputStreamSequencerSvc();outputStreamSequencerSvc.SequenceIncidentName = "NextEventRange";outputStreamSequencerSvc.IgnoreInputFileBoundary = True;svcMgr += outputStreamSequencerSvc\' '
            self.__runCommandList[0] += " '--skipFileValidation' '--checkEventCount=False' '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (self.__poolFileCatalog)
        else:
            self.__runCommandList[0] += ' --preExec \'from G4AtlasApps.SimFlags import simFlags;simFlags.RunNumber=222222;from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.Strategy="TokenScatterer";from AthenaCommon.AppMgr import ServiceMgr as svcMgr;from AthenaServices.AthenaServicesConf import OutputStreamSequencerSvc;outputStreamSequencerSvc = OutputStreamSequencerSvc();outputStreamSequencerSvc.SequenceIncidentName = "NextEventRange";outputStreamSequencerSvc.IgnoreInputFileBoundary = True;svcMgr += outputStreamSequencerSvc\' '
            self.__runCommandList[0] += " '--skipFileValidation' '--checkEventCount=False' '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (self.__poolFileCatalogTempName)

        # should not have --DBRelease and UserFrontier.py in HPC
        self.__runCommandList[0] = self.__runCommandList[0].replace("--DBRelease=current", "")
        if 'RecJobTransforms/UseFrontier.py,' in self.__runCommandList[0]:
            self.__runCommandList[0] = self.__runCommandList[0].replace('RecJobTransforms/UseFrontier.py,', '')
        if ',RecJobTransforms/UseFrontier.py' in self.__runCommandList[0]:
            self.__runCommandList[0] = self.__runCommandList[0].replace(',RecJobTransforms/UseFrontier.py', '')
        if ' --postInclude=RecJobTransforms/UseFrontier.py ' in self.__runCommandList[0]:
            self.__runCommandList[0] = self.__runCommandList[0].replace(' --postInclude=RecJobTransforms/UseFrontier.py ', ' ')

        #self.__runCommandList[0] = self.__runCommandList[0].replace(source_setup, source_setup + " --cmtextratags=ATLAS,useDBRelease --skipFileValidation --checkEventCount=False")
        # for tests, asetup has a bug
        #self.__runCommandList[0] = self.__runCommandList[0].replace(source_setup, new_source_setup)

        self.__runCommandList[0] += " 1>athenaMP_stdout.txt 2>athenaMP_stderr.txt"
        self.__runCommandList[0] = self.__runCommandList[0].replace(";;", ";")
  
        # 6. Token Extractor file list
        # in the token extractor file list, the guid is the Event guid, not the tag guid.
        self.__tagFile = os.path.join(self.__job.workdir, "TokenExtractor_filelist")
        handle = open(self.__tagFile, 'w')
        for guid in EventFiles:
            tagFile = EventFiles[guid]['TAG']
            line = guid + ",PFN:" + tagFile + "\n"
            handle.write(line)
        handle.close()

        # 7. Token Extractor command
        setup = setupCommand
        self.__tokenExtractorCmd = setup + ";" + " TokenExtractor -v  --source " + self.__tagFile + " 1>tokenExtract_stdout.txt 2>tokenExtract_stderr.txt"
        self.__tokenExtractorCmd = self.__tokenExtractorCmd.replace(";;", ";")
        # special case
        #self.__tokenExtractorCmd = "export LD_LIBRARY_PATH="+source_setup.split("cmtsite/asetup.sh")[0].strip().split(" ")[1]+"/patch/ldpatch/:$LD_LIBRARY_PATH; " + self.__tokenExtractorCmd

        return 0, None, {"TokenExtractCmd": self.__tokenExtractorCmd, "AthenaMPCmd": self.__runCommandList[0]}

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

    def setupStageOutHPCEvent(self):
        if self.__job.prodDBlockTokenForOutput is not None and len(self.__job.prodDBlockTokenForOutput) > 0 and self.__job.prodDBlockTokenForOutput[0] != 'NULL':
            siteInfo = getSiteInformation(self.getExperiment())
            objectstore_orig = siteInfo.readpar("objectstore")
            #siteInfo.replaceQueuedataField("objectstore", self.__job.prodDBlockTokenForOutput[0])
            espath = getFilePathForObjectStore(filetype="eventservice")
        else:
            #siteInfo = getSiteInformation(self.getExperiment())
            #objectstore = siteInfo.readpar("objectstore")
            espath = getFilePathForObjectStore(filetype="eventservice")
        self.__espath = getFilePathForObjectStore(filetype="eventservice")
        tolog("EventServer objectstore path: " + espath)

        siteInfo = getSiteInformation(self.getExperiment())
        # get the copy tool
        setup = siteInfo.getCopySetup(stageIn=False)
        tolog("Copy Setup: %s" % (setup))

        dsname, datasetDict = self.getDatasets()
        self.__report = getInitialTracingReport(userid=self.__job.prodUserID, sitename=self.__jobSite.sitename, dsname=dsname, eventType="objectstore", analysisJob=self.__analysisJob, jobId=self.__job.jobId, jobDefId=self.__job.jobDefinitionID, dn=self.__job.prodUserID)
        self.__siteMover = objectstoreSiteMover(setup)


    def stageOutHPCEvent(self, output_info):
        eventRangeID, status, output = output_info
        self.__output_es_files.append(output)

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

    def getDefaultResources(self):
        siteInfo = getSiteInformation(self.getExperiment())
        catchalls = siteInfo.readpar("catchall")
        values = {}
        for catchall in catchalls.split(","):
            if '=' in catchall:
                values[catchall.split('=')[0]] = catchall.split('=')[1]
        res = {}
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
        return res

    def runHPCEvent(self):
        tolog("runHPCEvent")
        self.__job.jobState = "running"
        self.__job.setState([self.__job.jobState, 0, 0])
        self.__job.pilotErrorDiag = None
        rt = RunJobUtilities.updatePilotServer(self.__job, self.getPilotServer(), self.getPilotPort())
        self.__JR.updateJobStateTest(self.__job, self.__jobSite, self.__node, mode="test")

        defRes = self.getDefaultResources()
        if defRes['copy_input_files'] == 'true':
            self.__copyInputFiles = True
        else:
            self.__copyInputFiles = False

        status, output, hpcJob = self.prepareHPCJob()
        if status == 0:
            tolog("HPC Job: %s " % hpcJob)
        else:
            tolog("failed to create the Tag file")
            self.failJob(0, PilotErrors.ERR_UNKNOWN, self.__job, pilotErrorDiag=output)
            return 


        self.__hpcStatus = None
        self.__hpcLog = None

        logFileName = None
        tolog("runJobHPCEvent.getPilotLogFilename=%s"% self.getPilotLogFilename())
        if self.getPilotLogFilename() != "":
            logFileName = self.getPilotLogFilename()
        hpcManager = HPCManager(globalWorkingDir=self.__job.workdir, logFileName=logFileName, poolFileCatalog=self.__poolFileCatalogTemp, inputFiles=self.__inputFilesGlobal, copyInputFiles=self.__copyInputFiles)

        self.__hpcManager = hpcManager
        self.HPCMode = "HPC_" + hpcManager.getMode(defRes)
        self.__job.setMode(self.HPCMode)
        self.__job.setHpcStatus('waitingResource')
        rt = RunJobUtilities.updatePilotServer(self.__job, self.getPilotServer(), self.getPilotPort())
        self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)

        hpcManager.getFreeResources(defRes)
        self.__job.coreCount = hpcManager.getCoreCount()
        self.__job.setHpcStatus('gettingEvents')
        rt = RunJobUtilities.updatePilotServer(self.__job, self.getPilotServer(), self.getPilotPort())
        self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)

        numRanges = hpcManager.getEventsNumber()
        tolog("HPC Manager needs events: %s, max_events: %s; use the smallest one." % (numRanges, defRes['max_events']))
        if numRanges > int(defRes['max_events']):
            numRanges = int(defRes['max_events'])
        eventRanges = self.getEventRanges(numRanges=numRanges)
        #tolog("Event Ranges: %s " % eventRanges)
        if len(eventRanges) == 0:
            tolog("Get no Event ranges. return")
            return
        for eventRange in eventRanges:
            self.__eventRanges[eventRange['eventRangeID']] = 'new'

        # setup stage out
        self.setupStageOutHPCEvent()

        hpcManager.initJob(hpcJob)
        hpcManager.initEventRanges(eventRanges)
        
        hpcManager.submit()
        threadpool = ThreadPool(defRes['stageout_threads'])

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

        outputs = hpcManager.getOutputs()
        for output in outputs:
            #self.stageOutHPCEvent(output)
            threadpool.add_task(self.stageOutHPCEvent, output)

        self.updateHPCEventRanges()
        threadpool.wait_completion()
        self.updateHPCEventRanges()


        if len(self.__failedStageOuts) > 0:
            tolog("HPC Stage out retry 1")
            half_stageout_threads = defRes['stageout_threads'] / 2
            if half_stageout_threads < 1:
                half_stageout_threads = 1
            threadpool = ThreadPool(half_stageout_threads)
            failedStageOuts = self.__failedStageOuts
            self.__failedStageOuts = []
            for failedStageOut in failedStageOuts:
                threadpool.add_task(self.stageOutHPCEvent, failedStageOut)
            threadpool.wait_completion()
            self.updateHPCEventRanges()

        if len(self.__failedStageOuts) > 0:
            tolog("HPC Stage out retry 2")
            threadpool = ThreadPool(1)
            failedStageOuts = self.__failedStageOuts
            self.__failedStageOuts = []
            for failedStageOut in failedStageOuts:
                threadpool.add_task(self.stageOutHPCEvent, failedStageOut)
            threadpool.wait_completion()
            self.updateHPCEventRanges()

        self.__job.setHpcStatus('finished')
        self.__JR.updatePandaServer(self.__job, self.__jobSite, self.__node, 25443)
        self.__hpcStatus, self.__hpcLog = hpcManager.checkHPCJobLog()
        tolog("HPC job log status: %s, job log error: %s" % (self.__hpcStatus, self.__hpcLog))
        

    def finishJob(self):
        try:
            self.__hpcManager.finishJob()
        except:
            tolog(sys.exc_info()[1])
            tolog(sys.exc_info()[2])

        # If payload leaves the input files, delete them explicitly
        if self.__job.inFiles:
            ec = pUtil.removeFiles(self.__job.workdir, self.__job.inFiles)
        #if self.__output_es_files:
        #    ec = pUtil.removeFiles("/", self.__output_es_files)


        errorCode = PilotErrors.ERR_UNKNOWN
        if self.__job.attemptNr < 4:
            errorCode = PilotErrors.ERR_ESRECOVERABLE

        #check HPC job status
        #if self.__hpcStatus:
        #    self.failJob(0, 1220, self.__job, pilotErrorDiag="HPC job failed")

        if len(self.__eventRanges) == 0:
            tolog("Cannot get event ranges")
            self.failJob(0, errorCode, self.__job, pilotErrorDiag="Cannot get event ranges")

        # check whether all event ranges are handled
        tolog("Total event ranges: %s" % len(self.__eventRanges))
        not_handled_events = self.__eventRanges.values().count('new')
        tolog("Not handled events: %s" % not_handled_events)
        done_events = self.__eventRanges.values().count('Done')
        tolog("Finished events: %s" % done_events)
        stagedOut_events = self.__eventRanges.values().count('stagedOut')
        tolog("stagedOut but not updated to panda server events: %s" % stagedOut_events)
        if done_events + stagedOut_events:
            errorCode = PilotErrors.ERR_ESRECOVERABLE
        if not_handled_events + stagedOut_events:
            tolog("Not all event ranges are handled. failed job")
            self.failJob(0, errorCode, self.__job, pilotErrorDiag="Not All events are handled(total:%s, left:%s)" % (len(self.__eventRanges), not_handled_events + stagedOut_events))

        dsname, datasetDict = self.getDatasets()
        tolog("dsname = %s" % (dsname))
        tolog("datasetDict = %s" % (datasetDict))

        # Create the output file dictionary needed for generating the metadata
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(self.__job.outFiles, self.__job.logFile, self.__job.workdir, fullpath=True)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            self.failJob(self.__job.result[1], ec, self.__job, pilotErrorDiag=pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # Create metadata for all successfully staged-out output files (include the log file as well, even if it has not been created yet)
        ec, job, outputFileInfo = self.createFileMetadata([], self.__job, outsDict, dsname, datasetDict, self.__jobSite.sitename)
        if ec:
            self.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)

        # Rename the metadata produced by the payload
        # if not pUtil.isBuildJob(outs):
        self.moveTrfMetadata(self.__job.workdir, self.__job.jobId)

        # Check the job report for any exit code that should replace the res_tuple[0]
        res0, exitAcronym, exitMsg = self.getTrfExitInfo(0, self.__job.workdir)
        res = (res0, exitMsg, exitMsg)

        # Payload error handling
        ed = ErrorDiagnosis()
        job = ed.interpretPayload(self.__job, res, False, 0, self.__runCommandList, self.getFailureCode())
        if job.result[1] != 0 or job.result[2] != 0:
            self.failJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag)
        self.__job = job

        job.jobState = "finished"
        job.setState([job.jobState, 0, 0])
        job.jobState = job.result
        rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), self.getPilotPort(), final=True)

        tolog("Done")
        self.sysExit(self.__job)


if __name__ == "__main__":

    tolog("Starting RunJobHpcEvent")

    if not os.environ.has_key('PilotHomeDir'):
        os.environ['PilotHomeDir'] = os.getcwd()

    # define a new parent group
    os.setpgrp()

    runJob = RunJobHpcEvent()
    try:
        runJob.setupHPCEvent()
        runJob.getHPCEventJobFromEnv()
        runJob.stageInHPCEvent()
        runJob.runHPCEvent()
    except:
        tolog(sys.exc_info()[1])
        tolog(sys.exc_info()[2])
    finally:
        runJob.finishJob()
