import os
import commands
import time
import pUtil

class Job:
    """ Job definition """

    def __init__(self):
        self.jobId = '0'                   # panda job id
        self.homePackage = None            # package name
        self.trf = None                    # trf name
        self.inFiles = None                # list of input files
        self.dispatchDblock = None         #
        self.prodDBlockToken = []          # used to send file info to the pilot (if input files should be directly accessed or not)
        self.prodDBlockTokenForOutput = [] # used for object store info
        self.prodDBlocks = []              # contains the correct container or dataset name for the traces
        self.dispatchDBlockToken = []      # used to send space tokens to the pilot (for input files)
        self.dispatchDBlockTokenForOut = None # used for chirp file destination, including server name
        self.destinationDBlockToken = []   # used to send space tokens to the pilot (for output files)
        self.outFiles = []                 # list of output files
        self.destinationDblock = []        # datasets for output files
        self.inFilesGuids = []             # list of input file guids
        self.outFilesGuids = []            # these guids are usually unknown till the job is done
        self.logFile = None                #
        self.tarFileGuid = pUtil.getGUID() # guid for the tarball of the job workdir 
        self.logDblock = None              #
        self.jobPars = None                # Job parameters defining the execution of the job
        self.exeErrorCode = 0              # payload error code
        self.exeErrorDiag = ""             # payload error diagnostic, potentially more detailed error text than std error
        self.pilotErrorDiag = None         # detailed error diag
        self.release = None                # software release string
        self.result = ["Unknown", 0, 0]    # the first digit is the transExitCode, and second one is the pilotErrorCode
        self.action = ""                   # place holder for "tobekilled" command from dispatcher
        self.workdir = None                # workdir for this job, usually under site.workdir
        self.siteworkdir = None            # workdir for the pilot (site.workdir)
        self.logMsgFiles = []              # list of log files that need to be reported back to panda server at the end of a job
        self.newDirNM = ""                 #
        self.datadir = ""                  # path to recovery datadir
        self.finalstate = ""               # final job state (either "failed" or "finished")
        self.attemptNr = -1                # attempt number for this job
        self.output_latereg = "None"       # control variable for late registration by job recovery algo
        self.output_fields = None          # - " -
        self.log_latereg = "None"          # - " -
        self.log_field = None              # - " -
        self.destinationSE = ""            #
        self.fileDestinationSE = ""        # SE info for CMS
        self.payload = "payload"           # payload name, e.g. "athena"
        self.stdout = "payload_stdout.txt" # payload stdout filename, default "%s_stdout.txt" % (self.payload)
        self.stderr = "payload_stderr.txt" # payload stdout filename, default "%s_stderr.txt" % (self.payload)
        self.spsetup = None                # special setup string for xrdcp systems
        self.prodUserID = ""               # user id
        self.cpuConsumptionTime = 0        # time spent during payload execution
        self.cpuConsumptionUnit = None     #
        self.cpuConversionFactor = 0       #
        self.maxCpuCount = 0               # defines what is a looping job (seconds)
        self.maxDiskCount = 21             # max input file size [GB] (server default 0)
        self.processingType = "NULL"       # alternatively 'reprocessing', used to increase max input file size
        self.prodSourceLabel = ""          # job label, e.g. 'user', 'test', 'rc_test', 'ddm', 'software', 'ptest'
        self.nEvents = 0                   # number of processed events (read)
        self.nEventsW = 0                  # number of processed events (written)
        self.realDatasetsIn = None         # dataset name(s) for input file(s)
        self.cmtconfig = None              # CMTCONFIG value from the task definition
        self.jobState = None               # Current job state (for definition, see JobRecovery class)
        self.fileStateDictionary = None    # Dictionary for current file states (for definition, see JobRecovery class)
        self.outputFilesXML = "OutputFiles.xml" # XML metadata related to output files for NG / CERNVM
        self.transferType = None           # Brokerage may decide to have input files transferred with remote I/O (set to 'direct' in that case)
        self.jobDefinitionID = None        # Job definition id forwarded to the DQ2 tracing server
        self.cloud = ""                    # The cloud the job belongs to
        self.credname = 'None'             #
        self.myproxy = 'None'              #
        self.taskID = ""                   # The task that this job belongs to
        self.isPilotResubmissionRequired = False # Pilot-controlled resubmission
        self.filesizeIn = []               # Input file sizes from the dispatcher
        self.checksumIn = []               # Input file checksums from the dispatcher
        self.debug = ""                    # debug = True will trigger the pilot to send stdout tail on job update
        self.currentState = ""             # Basically the same as result[0] but includes states like "stagein", "stageout"
        self.vmPeakMax = 0                 # Maximum value of vmPeak
        self.vmPeakMean = 0                # Average value of vmPeak
        self.RSSMean = 0                   # Average value of RSS
        self.JEM = "NO"                    # JEM usage (YES/NO), default: NO
        self.filesWithoutFAX = 0           # Number of files normally staged in (only reported to jobMetrics in FAX mode)
        self.filesWithFAX = 0              # Number of files staged in by FAX (only reported to jobMetrics in FAX mode)
        self.filesNormalStageOut = 0       # Number of files normally staged out (only reported to jobMetrics in alt stage-out mode)
        self.filesAltStageOut = 0          # Number of files staged out to alternative SE (only reported to jobMetrics in alt stage-out mode)
        self.bytesWithoutFAX = 0           # Total size of files transferred without FAX (only reported to jobMetrics in FAX mode)
        self.bytesWithFAX = 0              # Total size of files transferred with FAX (only reported to jobMetrics in FAX mode)
        self.scopeIn = []                  # Rucio scope for in files
        self.scopeOut = []                 # Rucio scope for out files
        self.scopeLog = []                 # Rucio scope for log file
        self.experiment = "undefined"      # Which experiment this job belongs to
        self.coreCount = None              # Number of cores as requested by the task
        self.pgrp = 0                      # Process group (RunJob* subprocess)
        self.sourceSite = ""               # Keep track of the original source site of the job (useful for overflow jobs to get to the proper FAX redirector)

        # event service objects
        self.eventService = False          # True for event service jobs
        self.eventServiceMerge = False     # True for event service merge jobs
        self.eventRanges = None            # Event ranges dictionary
        self.jobsetID = None               # Event range job set ID
#        self.eventRangeID = None           # Set for event service jobs
#        self.startEvent = None             # Set for event service jobs
#        self.lastEvent = None              # Set for event service jobs
#        self.lfn = None                    # LFNs of input files to be read by the Event Server (NOT by the pilot)
#        self.guid = None                   # GUIDs of input files to be read by the Event Server (NOT by the pilot)
        # self.attemptNr = ""              # (defined above)

        # job mode, for example, HPC_normal, HPC_backfill
        self.mode = None
        self.hpcStatus = None
        self.refreshNow = False

        # walltime counting for various steps
        self.timeSetup = 0
        self.timeGetJob = 0
        self.timeStageIn = 0
        self.timeExe = 0
        self.timeStageOut = 0
        self.timeCleanUp = 0

    def displayJob(self):
        """ dump job specifics """

        pUtil.tolog("Dumping job specifics")
        if self.spsetup and self.spsetup != "":
            _spsetup = self.spsetup
        else:
            _spsetup = "(not defined)"
        pUtil.tolog("\nPandaID=%s\nRelease=%s\nhomePackage=%s\ntrfName=%s\ninputFiles=%s\nrealDatasetsIn=%s\nfilesizeIn=%s\nchecksumIn=%s\nprodDBlocks=%s\nprodDBlockToken=%s\nprodDBlockTokenForOutput=%s\ndispatchDblock=%s\ndispatchDBlockToken=%s\ndispatchDBlockTokenForOut=%s\ndestinationDBlockToken=%s\noutputFiles=%s\ndestinationDblock=%s\nlogFile=%s\nlogFileDblock=%s\njobPars=%s\nThe job state=%s\nJob workdir=%s\nTarFileGuid=%s\noutFilesGuids=%s\ndestinationSE=%s\nfileDestinationSE=%s\nprodSourceLabel=%s\nspsetup=%s\ncredname=%s\nmyproxy=%s\ncloud=%s\ntaskID=%s\nprodUserID=%s\ndebug=%s\ntransferType=%s\nscopeIn=%s\scopeOut=%s\nscopeLog=%s" %\
                    (self.jobId, self.release, self.homePackage, self.trf, self.inFiles, self.realDatasetsIn, self.filesizeIn, self.checksumIn, self.prodDBlocks, self.prodDBlockToken, self.prodDBlockTokenForOutput, self.dispatchDblock, self.dispatchDBlockToken, self.dispatchDBlockTokenForOut, self.destinationDBlockToken, self.outFiles, self.destinationDblock, self.logFile, self.logDblock, self.jobPars, self.result, self.workdir, self.tarFileGuid, self.outFilesGuids, self.destinationSE, self.fileDestinationSE, self.prodSourceLabel, _spsetup, self.credname, self.myproxy, self.cloud, self.taskID, self.prodUserID, self.debug, self.transferType, self.scopeIn, self.scopeOut, self.scopeLog))

    def mkJobWorkdir(self, sitewd):
        """ create the job workdir under pilot workdir """

        ec = 0
        errorText = ""
        if not self.workdir:
            self.workdir = "%s/PandaJob_%s_%s" % (sitewd, self.jobId, str(int(time.time())))
        if not os.path.isdir(self.workdir):
            try:
                # note: do not set permissions in makedirs since they will not come out correctly, 0770 -> 0750
                os.makedirs(self.workdir)
                os.chmod(self.workdir, 0770)
            except OSError,e:
                errorText = "!!FAILED!!2999!! Exception caught in mkJobWorkdir: %s" % str(e)
                pUtil.tolog(errorText)
                ec = -1
        return ec, errorText
        
    def setPayloadName(self, payload):
        """ set the payload name and its stdout/err file names """
        self.payload = payload
        self.stdout = "%s_stdout.txt" % (self.payload)
        self.stderr = "%s_stderr.txt" % (self.payload)

    def setState(self, jobresult=[]):
        '''job status is a list of [state,transexitcode,pilotErrorCode]'''
        self.result = jobresult
        self.currentState = jobresult[0]

    def getState(self):
        '''returns jobId, job status and time stamp'''
        return self.jobId, self.result, pUtil.timeStamp()

    def setMode(self, mode):
        self.mode = mode

    def getMode(self, mode):
        return self.mode

    def setHpcStatus(self, hpcStatus):
        self.hpcStatus = hpcStatus

    def getHpcStatus(self):
        return self.hpcStatus

    def setJobDef(self, data):
        """ set values for a job object from a dictionary data
        which is usually from cgi messages from panda server """

        self.jobId = data.get('PandaID', '0')
        self.taskID = data.get('taskID', '')

        self.outputFilesXML = "OutputFiles-%s.xml" % (self.jobId)

        self.homePackage = data.get('homepackage', '')
        self.trf = data.get('transformation', '')

        try:
            self.jobDefinitionID = int(data.get('jobDefinitionID', ''))
        except:
            self.jobDefinitionID = ''

        try:
            self.cloud = data.get('cloud', '')
        except:
            self.cloud = ''

        # get the input files
        inFiles = data.get('inFiles', '')
        self.inFiles = inFiles.split(",")

        realDatasetsIn = data.get('realDatasetsIn', '')
        self.realDatasetsIn = realDatasetsIn.split(",")

        filesizeIn = data.get('fsize', '')
        self.filesizeIn = filesizeIn.split(",")

        checksumIn = data.get('checksum', '')
        self.checksumIn = checksumIn.split(",")

        dispatchDblock = data.get('dispatchDblock', '')
        self.dispatchDblock = dispatchDblock.split(",")

        prodDBlocks = data.get('prodDBlocks', '')
        self.prodDBlocks = prodDBlocks.split(",")

        prodDBlockToken = data.get('prodDBlockToken', '')
        self.prodDBlockToken = prodDBlockToken.split(",")

        prodDBlockTokenForOutput = data.get('prodDBlockTokenForOutput', '')
        self.prodDBlockTokenForOutput = prodDBlockTokenForOutput.split(",")

        dispatchDBlockToken = data.get('dispatchDBlockToken', '')
        self.dispatchDBlockToken = dispatchDBlockToken.split(",") 

        dispatchDBlockTokenForOut = data.get('dispatchDBlockTokenForOut', '')
        self.dispatchDBlockTokenForOut = dispatchDBlockTokenForOut.split(",") 

        destinationDBlockToken = data.get('destinationDBlockToken', '')
        self.destinationDBlockToken = destinationDBlockToken.split(",") 
        
        logFile = data.get('logFile', '')
        self.logFile = logFile

        self.prodUserID = data.get('prodUserID', '')

        self.credname = data.get('credname', 'None')
        self.myproxy = data.get('myproxy', 'None')

        outFiles = data.get('outFiles', '')

        self.attemptNr = int(data.get('attemptNr', -1))

        if data.has_key('GUID'):
            self.inFilesGuids = data['GUID'].split(",")
        else:
            self.inFilesGuids = []

        if data.has_key('processingType'):
            self.processingType = str(data['processingType'])
#            self.processingType = 'nightlies'
        else:
            # use default
            pass

        # Event Service variables
        if data.has_key('eventService'):
            if data.get('eventService', '').lower() == "true":
                self.eventService = True
            else:
                self.eventService = False
            pUtil.tolog("eventService = %s" % str(self.eventService))
        else:
            pUtil.tolog("Normal job (not an eventService job)")
        if data.has_key('eventRanges'):
            self.eventRanges = data.get('eventRanges', None)
        if data.has_key('jobsetID'):
            self.jobsetID = data.get('jobsetID', None)
            pUtil.tolog("jobsetID=%s" % (self.jobsetID))
        if not self.eventService and self.processingType == "evtest":
            pUtil.tolog("Turning on Event Service for processing type = %s" % (self.processingType))
            self.eventService = True

        # Event Service Merge variables
        if data.has_key('eventServiceMerge'):
            if data.get('eventServiceMerge', '').lower() == "true":
                self.eventServiceMerge = True
            else:
                self.eventServiceMerge = False
            pUtil.tolog("eventServiceMerge = %s" % str(self.eventServiceMerge))

        # Event Service merge job
        if self.workdir and data.has_key('eventServiceMerge') and data['eventServiceMerge'].lower() == "true":
            if data.has_key('writeToFile'):
                writeToFile = data['writeToFile']
                esFileDictionary, orderedFnameList = pUtil.createESFileDictionary(writeToFile)
                pUtil.tolog("esFileDictionary=%s" % (esFileDictionary))
                pUtil.tolog("orderedFnameList=%s" % (orderedFnameList))
                if esFileDictionary != {}:
                    ec, fnames = pUtil.writeToInputFile(self.workdir, esFileDictionary, orderedFnameList)
                    if ec == 0:
                        data['jobPars'] = pUtil.updateJobPars(data['jobPars'], fnames)

        # HPC job staus
        if data.has_key('mode'):
            self.mode = data.get("mode", None)
        if data.has_key('hpcStatus'):
            self.hpcStatus = data.get('hpcStatus', None)

#        self.eventRangeID = data.get('eventRangeID', None)
#        self.startEvent = data.get('startEvent', None)
#        self.lastEvent = data.get('lastEvent', None)
#        pUtil.tolog("eventRangeID = %s" % str(self.eventRangeID))
#        pUtil.tolog("startEvent = %s" % str(self.startEvent))
#        pUtil.tolog("lastEvent = %s" % str(self.lastEvent))
#        if data.has_key('lfn'):
#            self.lfn = data['lfn'].split(",")
#        else:
#            self.lfn = []
#        if data.has_key('guid'):
#            self.guid = data['guid'].split(",")
#        else:
#            self.guid = []

        # Rucio scopes
        if data.has_key('scopeIn'):
            self.scopeIn = data['scopeIn'].split(",")
        else:
            self.scopeIn = []
        if data.has_key('scopeOut'):
            self.scopeOut = data['scopeOut'].split(",")
        else:
            self.scopeOut = []
        if data.has_key('scopeLog'):
            self.scopeLog = data['scopeLog'].split(",")
        else:
            self.scopeLog = []

        self.maxCpuCount = int(data.get('maxCpuCount', 0))
        self.transferType = data.get('transferType', '')
#PN        self.transferType = 'direct'

        if data.has_key('maxDiskCount'):
            _tmp = int(data['maxDiskCount'])
            if _tmp != 0 and _tmp != self.maxDiskCount:
                self.maxDiskCount = _tmp
        else:
            # use default
            pass

        if data.has_key('cmtConfig'):
            self.cmtconfig = str(data['cmtConfig'])
        else:
            # use default
            pass

        if data.has_key('coreCount'):
            self.coreCount = str(data['coreCount'])
        else:
            # use default
            pass

        if data.has_key('sourceSite'):
            self.sourceSite = str(data['sourceSite'])
        else:
            # use default
            pass

        self.debug = data.get('debug', 'False')
        self.prodSourceLabel = data.get('prodSourceLabel', '')
        destinationDblock = data.get('destinationDblock', '')


        # PN tmp
#        skip = False
#        if data.has_key('eventServiceMerge'):
#            if data['eventServiceMerge'] == 'True':
#                skip = True

        # figure out the real output files and log files and their destinationDblock right here
        outfList = outFiles.split(",")
        pUtil.tolog("outfList = %s" % (outfList))
        outfdbList = destinationDblock.split(",")
        pUtil.tolog("outfdbList = %s" % (outfdbList))
        outs = []
        outdb = []
        logFileDblock = ''
        # keep track of log file index in the original file output list
        i_log = -1
        for i in range(len(outfList)):
            if outfList[i] == logFile:
                logFileDblock = outfdbList[i]
                i_log = i
            else:
#                if not skip: #PN tmp
                outs.append(outfList[i])
                outdb.append(outfdbList[i])

        # put the space token for the log file at the end of the list
        if i_log != -1:
            try:
                spacetoken_log = self.destinationDBlockToken[i_log]
                self.destinationDBlockToken.remove(spacetoken_log)
                self.destinationDBlockToken.append(spacetoken_log)
            except Exception, e:
                pUtil.tolog("!!WARNING!!2999!! Could not rearrange destinationDBlockToken list: %s" % str(e))
            else:
                pUtil.tolog("destinationDBlockToken = %s" % (self.destinationDBlockToken))
        # put the chirp server info for the log file at the end of the list
        # note: any NULL value corresponding to a log file will automatically be handled
        if i_log != -1 and self.dispatchDBlockTokenForOut != None and self.dispatchDBlockTokenForOut != []:
            try:
                chirpserver_log = self.dispatchDBlockTokenForOut[i_log]
                self.dispatchDBlockTokenForOut.remove(chirpserver_log)
                self.dispatchDBlockTokenForOut.append(chirpserver_log)
            except Exception, e:
                pUtil.tolog("!!WARNING!!2999!! Could not rearrange dispatchDBlockTokenForOut list: %s" % str(e))
            else:
                pUtil.tolog("dispatchDBlockTokenForOut = %s" % (self.dispatchDBlockTokenForOut))

        pUtil.tolog("logFileDblock = %s" % (logFileDblock))
        self.outFiles = outs
        self.destinationDblock = outdb
        self.logDblock = logFileDblock

        self.jobPars = data.get('jobPars', '')
        # for accessmode testing: self.jobPars += " --accessmode=direct"

        # for jem testing: self.jobPars += ' --enable-jem --jem-config \"a=1;\"'
        if "--pfnList" in self.jobPars:
            import re

            # extract any additional input files from the job parameters and add them to the input file list

            pattern = re.compile(r"\-\-pfnList\=(\S+)")
            pfnSearch = pattern.search(jobPars)
            if pfnSearch:
                # found pfnList
                _pfnList = pfnSearch.group(1)
                if _pfnList:
                    pfnList = _pfnList.split(",")

                    # add the pfnList files to the input file list
                    self.inFiles += _localInFiles.split(",")
                    pUtil.tolog("Added local files from pfnList to input file list")

                    # remove the pfnList directive from the job parameters
                    txt_to_remove = "--pfnList=" + _pfnList
                    if txt_to_remove in self.jobPars:
                        self.jobPars = self.jobPars.replace(txt_to_remove, "")
                        pUtil.tolog('Removed "%s" from job parameters' % (txt_to_remove))
                    else:
                        pUtil.tolog('!!WARNING!!3999!! Failed to remove "%s" from job parameters: %s (cannot remove --pfnList from job parameters)"' % (txt_to_remove, self.jobPars))
                else:
                    pUtil.tolog("!!WARNING!!3999!! Pattern search failed: pfnSearch=%s (cannot remove --pfnList from job parameters)" % str(pfnSearch))

        self.release = data.get('swRelease', '')
        self.destinationSE = data.get('destinationSE', '')
        self.fileDestinationSE = data.get('fileDestinationSE', '')
