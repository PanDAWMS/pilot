import os
import time
import pUtil

from subprocess import Popen, PIPE, STDOUT

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
        self.jobDefinitionID = None        # Job definition id forwarded to the Rucio tracing server
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
        self.ddmEndPointIn = []            #
        self.ddmEndPointOut = []           #
#        self.ddmEndPointOutAlt = []       #
        self.ddmEndPointLog = []           #
        self.cloneJob = ""                 # Is the job cloned? Allowed values: 'runonce', 'storeonce'
        self.allowNoOutput = []            # Used to disregard empty files from jobReport
        self.logBucketID = -1              # To keep track of which OS bucket the log was stored in

        # event service objects
        self.eventService = False          # True for event service jobs
        self.eventServiceMerge = False     # True for event service merge jobs
        self.eventRanges = None            # Event ranges dictionary
        self.jobsetID = None               # Event range job set ID
        self.pandaProxySecretKey = None    # pandaproxy secret key
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

        self.inData = []  # validated structured data of input files ( aggregated inFiles, ddmEndPointIn, scopeIn, filesizeIn and others...)
        self.outData = [] # structured data of output files (similar to inData)
        self.logData = [] # structured data of log files (similar to inData)

        self.accessmode = "" # accessmode=direct,copy: Should direct i/o be used for input files of this job


    def displayJob(self):
        """ dump job specifics """

        pUtil.tolog("Dumping job specifics")
        if self.spsetup and self.spsetup != "":
            _spsetup = self.spsetup
        else:
            _spsetup = "(not defined)"
        pUtil.tolog("\nPandaID=%s\nRelease=%s\nhomePackage=%s\ntrfName=%s\ninputFiles=%s\nrealDatasetsIn=%s\nfilesizeIn=%s\nchecksumIn=%s\nprodDBlocks=%s\nprodDBlockToken=%s\nprodDBlockTokenForOutput=%s\ndispatchDblock=%s\ndispatchDBlockToken=%s\ndispatchDBlockTokenForOut=%s\ndestinationDBlockToken=%s\noutputFiles=%s\ndestinationDblock=%s\nlogFile=%s\nlogFileDblock=%s\njobPars=%s\nThe job state=%s\nJob workdir=%s\nTarFileGuid=%s\noutFilesGuids=%s\ndestinationSE=%s\nfileDestinationSE=%s\nprodSourceLabel=%s\nspsetup=%s\ncredname=%s\nmyproxy=%s\ncloud=%s\ntaskID=%s\nprodUserID=%s\ndebug=%s\ntransferType=%s\nscopeIn=%s\nscopeOut=%s\nscopeLog=%s" %\
                    (self.jobId, self.release, self.homePackage, self.trf, self.inFiles, self.realDatasetsIn, self.filesizeIn, self.checksumIn, self.prodDBlocks, self.prodDBlockToken, self.prodDBlockTokenForOutput, self.dispatchDblock, self.dispatchDBlockToken, self.dispatchDBlockTokenForOut, self.destinationDBlockToken, self.outFiles, self.destinationDblock, self.logFile, self.logDblock, self.jobPars, self.result, self.workdir, self.tarFileGuid, self.outFilesGuids, self.destinationSE, self.fileDestinationSE, self.prodSourceLabel, _spsetup, self.credname, self.myproxy, self.cloud, self.taskID, self.prodUserID, self.debug, self.transferType, self.scopeIn, self.scopeOut, self.scopeLog))
        pUtil.tolog("ddmEndPointIn=%s" % self.ddmEndPointIn)
        pUtil.tolog("ddmEndPointOut=%s" % self.ddmEndPointOut)
        pUtil.tolog("ddmEndPointLog=%s" % self.ddmEndPointLog)
        pUtil.tolog("cloneJob=%s" % self.cloneJob)
        pUtil.tolog("allowNoOutput=%s" % self.allowNoOutput)

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

        self.outputFilesXML = "OutputFiles-%s.xml" % self.jobId

        self.homePackage = data.get('homepackage', '')
        self.trf = data.get('transformation', '')

        try:
            self.jobDefinitionID = int(data.get('jobDefinitionID', ''))
        except:
            self.jobDefinitionID = ''

        self.cloud = data.get('cloud', '')

        # get the input files
        self.inFiles = data.get('inFiles', '').split(',')
        self.realDatasetsIn = data.get('realDatasetsIn', '').split(',')
        self.filesizeIn = data.get('fsize', '').split(',')
        self.checksumIn = data.get('checksum', '').split(',')

        self.dispatchDblock = data.get('dispatchDblock', '').split(',')
        self.prodDBlocks = data.get('prodDBlocks', '').split(',')

        self.prodDBlockToken = data.get('prodDBlockToken', '').split(',')
        self.prodDBlockTokenForOutput = data.get('prodDBlockTokenForOutput', '').split(',')

        self.dispatchDBlockToken = data.get('dispatchDBlockToken', '').split(',')
        self.dispatchDBlockTokenForOut = data.get('dispatchDBlockTokenForOut', '').split(',')

        self.destinationDBlockToken = data.get('destinationDBlockToken', '').split(',')

        self.ddmEndPointIn = data.get('ddmEndPointIn', '').split(',') if data.get('ddmEndPointIn') else []
        self.ddmEndPointOut = data.get('ddmEndPointOut', '').split(',') if data.get('ddmEndPointOut') else []
        self.allowNoOutput = data.get('allowNoOutput', '').split(',') if data.get('allowNoOutput') else []

        self.cloneJob = data.get('cloneJob', '')
        self.logFile = data.get('logFile', '')
        self.prodUserID = data.get('prodUserID', '')

        self.credname = data.get('credname', 'None')
        self.myproxy = data.get('myproxy', 'None')

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
        self.eventService = data.get('eventService', '').lower() == "true"

        if self.eventService:
            pUtil.tolog("eventService = %s" % self.eventService)
        else:
            pUtil.tolog("Normal job (not an eventService job)")

        self.eventRanges = data.get('eventRanges')
        self.jobsetID = data.get('jobsetID')

        pUtil.tolog("jobsetID=%s" % self.jobsetID)

        self.pandaProxySecretKey = data.get('pandaProxySecretKey')

        if not self.eventService and self.processingType == "evtest":
            pUtil.tolog("Turning on Event Service for processing type = %s" % self.processingType)
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
        # Overwrite the coreCount value with ATHENA_PROC_NUMBER if it is set
        if os.environ.has_key('ATHENA_PROC_NUMBER'):
            try:
                self.coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
            except Exception, e:
                pUtil.tolog("ATHENA_PROC_NUMBER is not properly set: %s (will use existing job.coreCount value)" % (e))

        if data.has_key('sourceSite'):
            self.sourceSite = str(data['sourceSite'])
        else:
            # use default
            pass

        self.debug = data.get('debug', 'False')
        self.prodSourceLabel = data.get('prodSourceLabel', '')

        # PN tmp
#        skip = False
#        if data.has_key('eventServiceMerge'):
#            if data['eventServiceMerge'] == 'True':
#                skip = True

        # figure out the real output files and log files and their destinationDblock right here

        outfList = data.get('outFiles', '').split(',')
        outfdbList = data.get('destinationDblock', '').split(',')

        pUtil.tolog("outfList = %s" % outfList)
        pUtil.tolog("outfdbList = %s" % outfdbList)

        outs = []
        outdb = []
        outddm = []
        logddm = []
        logFileDblock = ""
        # keep track of log file index in the original file output list
        i_log = -1
        for i in range(len(outfList)):
            if outfList[i] == self.logFile:
                logFileDblock = outfdbList[i]
                logddm = [ self.ddmEndPointOut[i] ]
                i_log = i
            else:
                outs.append(outfList[i])
                outdb.append(outfdbList[i])
                outddm.append(self.ddmEndPointOut[i])

        # put the space token for the log file at the end of the list
        if i_log != -1:
            try:
                spacetoken_log = self.destinationDBlockToken[i_log]
                self.destinationDBlockToken.remove(spacetoken_log)
                self.destinationDBlockToken.append(spacetoken_log)
            except Exception, e:
                pUtil.tolog("!!WARNING!!2999!! Could not rearrange destinationDBlockToken list: %s" % str(e))
            else:
                pUtil.tolog("destinationDBlockToken = %s" % self.destinationDBlockToken)
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
                pUtil.tolog("dispatchDBlockTokenForOut = %s" % self.dispatchDBlockTokenForOut)

        pUtil.tolog("logFileDblock = %s" % logFileDblock)

        self.outFiles = outs
        self.destinationDblock = outdb
        self.logDblock = logFileDblock
        self.ddmEndPointOut = outddm
        self.ddmEndPointLog = logddm

        pUtil.tolog("Updated ddmEndPointOut=%s" % self.ddmEndPointOut)
        pUtil.tolog("Updated ddmEndPointLog=%s" % self.ddmEndPointLog)

        self.jobPars = data.get('jobPars', '')

        # for accessmode testing: self.jobPars += " --accessmode=direct"

        self.accessmode = ""
        if self.transferType == 'direct': # enable direct access mode
            self.accessmode = 'direct'

        # job input options overwrite any Job settings
        if '--accessmode=direct' in self.jobPars: # fix me later
            self.accessmode = 'direct'
        if '--accessmode=copy' in self.jobPars:   # fix me later
            self.accessmode = 'copy'

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
                    self.inFiles += _localInFiles.split(",") # broken code? _localInFiles is not defined above
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

        # prepare structured inData, outData, logData:
        # old self.inFiles and etc list values may contain [''] - value in case of empty input,
        # that's why new fileds are introduced to avoid breaking current logic
        # later on inFiles and other "splitted" values should be replaced by combined aggregated structure (inData and etc)

        # process infiles properties
        self.inData = []

        # format: [(data[source_key], FileInfo.attr_name),]
        # if second argument=None or not specified then ignore processing of related source_key
        in_keys = [('inFiles', 'lfn'),
                   ('dispatchDblock', 'dispatchDblock'), ('dispatchDBlockToken', 'dispatchDBlockToken'),
                   ('realDatasetsIn', 'dataset'), ('GUID', 'guid'),
                   ('fsize', 'filesize'), ('checksum', 'checksum'), ('scopeIn', 'scope'),
                   ('prodDBlocks', 'prodDBlock'), ('prodDBlockToken', 'prodDBlockToken'),
                   ('ddmEndPointIn', 'ddmendpoint')]

        kmap = dict([k[0], k[1]] for k in in_keys if not isinstance(k, str))
        ksources = dict([k, data.get(k, '').split(',') if data.get(k) else []] for k in kmap)

        for ind, lfn in enumerate(ksources.get('inFiles', [])):
            if lfn in ['', 'NULL']:
                continue
            idat = {} # form data
            for k, attrname in kmap.iteritems():
                idat[attrname] = ksources[k][ind] if len(ksources[k]) > ind else None
            finfo = FileSpec(type='input', **idat)
            self.inData.append(finfo)

        # process outfiles properties
        self.outData = []
        self.logData = []

        # normally attributes names of outfile parametes should be mapped to inputfile corresponding ones, e.g. destinationDblock -> dispatchDblock -> Dblock, but the names kept as is just to simplify logic migration and avoid confusions
        out_keys = [('outFiles', 'lfn'),
                    ('destinationDblock', 'destinationDblock'),
                    ('destinationDBlockToken', 'destinationDBlockToken'),
                    ('realDatasets', 'dataset'),
                    ('scopeOut', 'scope'),
                    ('fileDestinationSE', 'fileDestinationSE'),
                    ('dispatchDBlockTokenForOut', 'dispatchDBlockTokenForOut'),
                    ('ddmEndPointOut', 'ddmendpoint'),
                    ('prodDBlockTokenForOutput', 'prodDBlockTokenForOutput') # exposed only for eventservice related job
                   ]

        kmap = dict([k[0], k[1]] for k in out_keys if not isinstance(k, str))
        ksources = dict([k, data.get(k, '').split(',') if data.get(k) else []] for k in kmap)

        #log_entry = ['logFile', 'logGUID', 'scopeLog'] # log specific values

        # fix scopeOut of log file: to be fixed properly on Panda side: just hard patched here
        logFile = data.get('logFile')
        if ksources['scopeOut'] and logFile:
            scopeOut = []
            for lfn in ksources.get('outFiles', []):
                if lfn == logFile:
                    scopeOut.append(data.get('scopeLog'))
                else:
                    if not ksources['scopeOut']:
                        raise Exception('Failed to extract scopeOut parameter from Job structure sent by Panda, please check input format!')
                    scopeOut.append(ksources['scopeOut'].pop(0))
            ksources['scopeOut'] = scopeOut

        for ind, lfn in enumerate(ksources['outFiles']):
            if lfn in ['', 'NULL']:
                continue
            idat = {} # form data
            for k, attrname in kmap.iteritems():
                idat[attrname] = ksources[k][ind] if len(ksources[k]) > ind else None

            idat['type'] = 'output'
            ref_dat = self.outData
            if lfn == logFile: # log file case
                idat['type'] = 'log'
                idat['guid'] = data.get('logGUID')
                ref_dat = self.logData

            finfo = FileSpec(**idat)
            ref_dat.append(finfo)


    def isAnalysisJob(self):
        """
            Determine whether the job is an analysis job or not
            normally this should be set in constructor as a property of Job class
            specified the type of job explicitly (?)
        """

        #return pUtil.isAnalysisJob(self.trf)
        #copied from pUtil.isAnalysisJob to isolate the logic amd move outside pUtil

        is_analysis = False

        trf = self.trf.split(',') if self.trf else [] # properly convert to list (normally should be done in setJobDef())
        if trf and trf[0] and (trf[0].startswith('https://') or trf[0].startswith('http://')): # check 1st entry
            is_analysis = True

        if self.prodSourceLabel == "software": # logic extracted from the sources, to be verified
            is_analysis = False

        # apply addons checks later if need

        return is_analysis

    def isBuildJob(self):
        """
        Check if the job is a build job
        (i.e. check if the job only has one output file that is a lib file)
        """

        ofiles = self.outData
        is_build_job = len(ofiles) == 1 and '.lib.' in ofiles[0].lfn
        return is_build_job


    #def getDatasets(self):
    #    """ get the datasets for the output files """
    #
    #    # get the default dataset
    #    if self.destinationDblock and self.destinationDblock[0] not in ['NULL', ' ']:
    #        dsname = self.destinationDblock[0]
    #    else:
    #        dsname = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name
    #
    #    # create the dataset dictionary
    #    # (if None, the dsname above will be used for all output files)
    #    datasetDict = getDatasetDict(self.outFiles, self.destinationDblock, self.logFile, self.logDblock)
    #    if datasetDict:
    #        pUtil.tolog("Dataset dictionary has been verified")
    #    else:
    #        pUtil.tolog("Dataset dictionary could not be verified, output files will go to: %s" % dsname)
    #
    #    return dsname, datasetDict
    #
    #
    #def ___getDatasetDict(self): # don't use this function: do use structured self.outData & self.outLog instead
    #    """
    #        Create a dataset dictionary: old cleaned function to verify the logic
    #
    #        :logic:
    #            - validate self.outputFiles, self.destinationDblock: to have same length and contains valid data
    #            - join self.outputFiles, self.destinationDblock into the dict
    #            - add (self.logFile, self.logFileDblock) in the dict
    #    """
    #
    #    datasetDict = None
    #
    #    # verify that the lists are of equal size
    #    if len(self.outputFiles) != len(self.destinationDblock):
    #        pUtil.tolog("WARNING: Lists are not of same length: %s, %s" % (self.outputFiles, self.destinationDblock))
    #    elif not len(outputFiles):
    #        pUtil.tolog("No output files for this job (outputFiles has zero length)")
    #    elif not len(destinationDblock):
    #        pUtil.tolog("WARNING: destinationDblock has zero length")
    #    else:
    #        # verify that list contains valid entries
    #        for _list in [self.outputFiles, self.destinationDblock]:
    #            for _entry in _list:
    #                if _entry in ["NULL", "", " ", None]:
    #                    pUtil.tolog("!!WARNING!!2999!! Found non-valid entry in list: %s" % _list)
    #                    return None
    #
    #        # build the dictionary
    #        try:
    #            datasetDict = dict(zip(self.outputFiles, self.destinationDblock))
    #        except Exception, e:
    #            pUtil.tolog("!!WARNING!!2999!! Exception caught in getDatasetDict(): %s" % e)
    #            return None
    #
    #        # add the log file info
    #        datasetDict[self.logFile] = self.logFileDblock
    #
    #    return datasetDict
    #
    #
    #def __getDatasetName(datasetDict, lfn, pdsname): # don't use this function
    #    """
    #        Get the dataset name: old function to validate and refactor the logic
    #        :return: dsname, dsname_for_traces
    #
    #        :logic:
    #                - dsname_for_traces = datasetDict.get(lfn, pdsname)
    #                - dsname = dsname_for_traces.replace('_sub[0-9]+', '')
    #    """
    #
    #    if datasetDict and lfn not in datasetDict:
    #        pUtil.tolog("!!WARNING!!2999!! Could not get dsname from datasetDict for lfn=%s: dict=%s .. will use default dsname=%s" % (lfn, datasetDict, pdsname))
    #
    #    # get the dataset name from the dictionary
    #    datasetDict = datasetDict or {}
    #    dsname = datasetDict.get(lfn, pdsname)
    #
    #    # save the original dsname for the tracing report
    #    dsname_report = dsname
    #
    #    # remove any _subNNN parts from the dataset name (from now on dsname will only be used to create SE destination paths)
    #    m = re.match('\S+(\_sub[0-9]+)', dsname)
    #    if m:
    #        dsname = dsname.replace(m.group(1), '')
    #        tolog("Removed _subNNN part=%s from the dataset, updated dsname=%s" % (match.group(1), dsname))
    #    else:
    #        tolog("Found no _subNNN string in the dataset name")
    #
    #    tolog("File %s will go to dataset %s" % (lfn, dsname))
    #
    #    return dsname, dsname_report


    def __repr__(self):
        """ smart info for debuggin/printing Job content """

        ret = []
        ret.append("Job.jobId=%s" % self.jobId)
        for k in ['inData', 'outData', 'logData']:
            ret.append("Job.%s = %s" % (k, getattr(self, k, None)))

        return '\n'.join(ret)


    def _print_files(self, key): # quick stub to be checked later

        files = [os.path.join(self.workdir or '', e.lfn) for e in getattr(self, key, [])]
        pUtil.tolog("%s file(s): %s" % (key, files))
        cmd = 'ls -la %s' % ' '.join(files)
        msg = "do EXEC cmd=%s" % cmd
        c = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
        output = c.communicate()[0]
        pUtil.tolog(msg + '\n' + output)

    def print_infiles(self):
        return self._print_files('inData')

    def print_outfiles(self):
        return self._print_files('outData')

    def print_logfiles(self):
        return self._print_files('logData')


    def _sync_outdata(self):
        """
            old logic integration function
              - extend job.outData with new entries from job.outFiles if any
              - do populate guids values
              - init dataset value (destinationDblock) if not set
        """

        # group outData by lfn
        data = dict([e.lfn, e] for e in self.outData)

        # build structure (join data) from splitted attributes
        extra = []
        for i, lfn in enumerate(self.outFiles):
            if lfn not in data: # found new entry
                kw = {'type':'output'}
                #kw['destinationDBlockToken'] = self.destinationDBlockToken[i] # not used by new Movers
                kw['destinationDblock'] = self.destinationDblock[i]
                kw['scope'] = self.scopeOut[i]
                kw['ddmendpoint'] = self.ddmEndPointOut[i] if i<len(self.ddmEndPointOut) else self.ddmEndPointOut[0]
                kw['guid'] = self.outFilesGuids[i] # outFilesGuids must be coherent with outFiles, otherwise logic corrupted
                spec = FileSpec(lfn=lfn, **kw)
                extra.append(spec)
            else: # sync data
                spec = data[lfn]
                spec.guid = self.outFilesGuids[i]
        if extra:
            pUtil.tolog('Job._sync_outdata(): found extra output files to be added for stage-out: extra=%s' % extra)
            self.outData.append(extra)

        # init dataset value (destinationDblock) if not set
        rand_dsn = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name
        for spec in self.outData:
            if not spec.destinationDblock or spec.destinationDblock == 'NULL':
                spec.destinationDblock = self.outData[0].destinationDblock or rand_dsn


class FileSpec(object):

    _infile_keys =  ['lfn', 'ddmendpoint', 'type',
                    'dataset', 'scope',
                    'dispatchDblock', 'dispatchDBlockToken',
                    'guid', 'filesize', 'checksum',
                    'prodDBlock', 'prodDBlockToken',
                    ]


    _outfile_keys = ['lfn', 'ddmendpoint', 'type',
                    'dataset', 'scope',
                    'destinationDblock', 'destinationDBlockToken',
                    'fileDestinationSE',
                    'dispatchDBlockTokenForOut',
                    'prodDBlockTokenForOutput', # exposed only for eventservice related job
                    ]

    _local_keys = ['type', 'status', 'replicas', 'surl', 'turl', 'mtime']

    def __init__(self, **kwargs):

        attributes = self._infile_keys + self._outfile_keys + self._local_keys
        for k in attributes:
            setattr(self, k, kwargs.get(k, getattr(self, k, None)))

        self.filesize = int(getattr(self, 'filesize', 0) or 0)
        self.replicas = []

    def __repr__(self):
        obj = dict((name, getattr(self, name)) for name in sorted(dir(self)) if not name.startswith('_') and not callable(getattr(self, name)))
        return "%s" % obj

    def get_checksum(self):
        """
        :return: checksum, checksum_type
        """

        cc = (self.checksum or '').split(':')
        if len(cc) != 2:
            return self.checksum, None
        checksum_type, checksum = cc
        cmap = {'ad':'adler32', 'md':'md5'}
        checksum_type = cmap.get(checksum_type, checksum_type)

        return checksum, checksum_type

    def is_directaccess(self):

        is_rootfile = '.root' in self.lfn

        exclude_pattern = ['.tar.gz', '.lib.tgz', '.raw.']
        for e in exclude_pattern:
            if e in self.lfn:
                is_rootfile = False
                break

        if not is_rootfile:
            return False

        return self.prodDBlockToken != 'local' and is_rootfile
