# Class definition:
#   Experiment
#   This class is the main experiment class; ATLAS etc will inherit from this class
#   Instances are generated with ExperimentFactory
#   Subclasses should implement all needed methods prototyped in this class
#   Note: not compatible with Singleton Design Pattern due to the subclassing

import os
import re
import commands

from PilotErrors import PilotErrors
from pUtil import tolog                    # Dump to pilot log
from pUtil import readpar                  # Used to read values from the schedconfig DB (queuedata)
from pUtil import getCmtconfig             # cmtconfig (move to subclass)
from pUtil import getDirectAccessDic       # Get the direct access dictionary
from pUtil import isBuildJob               # Is the current job a build job?
from pUtil import remove                   # Used to remove redundant file before log file creation

class Experiment(object):

    # private data members
    __experiment = "generic"               # String defining the experiment
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object
    __doFileLookups = False                # True for LFC based file lookups (basically a dummy data member here since singleton object is static)

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def getJobExecutionCommand(self):
        """ Define and test the command(s) that will be used to execute the payload """
        # E.g. cmd = "source <path>/setup.sh; <path>/python "

        cmd = ""

        return cmd

    def getFileLookups(self):
        """ Return the file lookup boolean """

        return self.__doFileLookups

    def doFileLookups(self, doFileLookups):
        """ Update the file lookups boolean """

        # Only implement this method if class really wants to update the __doFileLookups boolean
        # ATLAS wants to implement this, but not CMS
        # Method is used by Mover
        # self.__doFileLookups = doFileLookups
        pass

    def willDoAlternativeFileLookups(self):
        """ Should file lookups be done using alternative methods? """

        # E.g. in the migration period where LFC lookups are halted in favour of other methods in the DQ2/Rucio API
        # (for ATLAS), this method could be useful. See the usage in Mover::getReplicaDictionary() which is called
        # after Experiment::willDoFileLookups() defined above. The motivation is that direct LFC calls are not to be
        # used any longer by the pilot, and in the migration period the actual LFC calls will be done in the DQ2/Rucio
        # API. Eventually this API will switch to alternative file lookups.

        return False

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return self.__doFileLookups

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    def getFileCatalog(self):
        """ Return the default file catalog to use (e.g. for replica lookups) """
        # See usage in Mover.py

        # e.g. 'lfc://prod-lfc-atlas.cern.ch:/grid/atlas'
        return ""

    # Additional optional methods
    # These methods are optional and can be left as they are here, or modified according to special needs

    def removeRedundantFiles(self, workdir):
        """ Remove redundant files and directories """

        # List of files and directories to be removed from work directory prior to log file creation
        # Make sure that any large files or directories that are not wanted in the log file are included in this list
        dir_list = [
                    "buildJob*",
                    "external",
                    "fort.*",
                    "home",
                    "python",
                    "share",
                    "workdir",
                    "*.py",
                    "*.pyc",
                    "*.root*",
                    "JEM",
                    "tmp*",
                    "*.tmp",
                    "*.TMP",
                    "scratch",
                    ]
    
        for _dir in dir_list: 
            files = glob(os.path.join(workdir, _dir))
            rc = remove(files)
            if not rc:
                tolog("IGNORE: Failed to remove redundant file(s): %s" % (files))

    def getPayloadName(self, job):
        """ Set a suitable name for the payload stdout """

        # The payload <name> gets translated into <name>_stdout.txt
        # which is the name of the stdout file produced by the payload execution
        # (essentially commands.getoutput("<setup>; <payload executable> [options] > <name>_stdout.txt"))

        # The job object can be used to create more precise stdout names (see e.g. the ATLASExperiment implementation)

        return "payload"

    def isOutOfMemory(self, **kwargs):
        """ Try to identify out of memory errors in the stderr/out """

        return False

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """

        return 0

    def specialChecks(self, **kwargs):
        """ Implement special checks here """
        # Return False if fatal failure, otherwise return True
        # The pilot will abort if this method returns a False

        status = False

        tolog("No special checks for \'%s\'" % (self.__experiment))

        return True # obviously change this to 'status' once implemented
    
    def setINDS(self, realDatasetsIn):
        """ Extract the dataset as set by pathena option --inDS and set the INDS environmental variable """
        # Needed by pathena (move to ATLASExperiment later)

        inDS = ""
        for ds in realDatasetsIn:
            if "DBRelease" not in ds and ".lib." not in ds:
                inDS = ds
                break
        if inDS != "":
            tolog("Setting INDS env variable to: %s" % (inDS))
            os.environ['INDS'] = inDS
        else:
            tolog("INDS unknown")

    def getValidBaseURLs(self, order=None):
        """ Return list of valid base URLs """
        # if order is defined, return given item first
        # e.g. order=http://atlpan.web.cern.ch/atlpan -> ['http://atlpan.web.cern.ch/atlpan', ...]

        validBaseURLs = []
        _validBaseURLs = ["http://www.usatlas.bnl.gov",\
                          "https://www.usatlas.bnl.gov",\
                          "http://pandaserver.cern.ch",\
                          "http://atlpan.web.cern.ch/atlpan",\
                          "https://atlpan.web.cern.ch/atlpan",\
                          "http://common-analysis-framework.cern.ch",\
                          "http://classis01.roma1.infn.it",\
                          "http://atlas-install.roma1.infn.it",\
                          "http://homepages.physik.uni-muenchen.de/~Johannes.Ebke"]

        if order:
            validBaseURLs.append(order)
            for url in _validBaseURLs:
                if url != order:
                    validBaseURLs.append(url)
        else:
            validBaseURLs = _validBaseURLs

        tolog("getValidBaseURLs will return: %s" % str(validBaseURLs))
        return validBaseURLs

    def downloadTrf(self, wgetCommand, jobTrf):
        """ Download the trf """

        status = False
        pilotErrorDiag = ""
        cmd = "%s %s" % (wgetCommand, jobTrf)
        trial = 1
        max_trials = 3

        # try to download the trf a maximum of 3 times
        while trial <= max_trials:
            tolog("Executing command [Trial %d/%d]: %s" % (trial, max_trials, cmd))
            ec, rets = commands.getstatusoutput(cmd)
            if not rets:
                rets = "(None)"
            if ec != 0:
                from futil import check_syserr
                check_syserr(ec, rets)
                pilotErrorDiag = "wget command failed: %d, %s" % (ec, rets)
                tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
                if trial == max_trials:
                    tolog("!!FAILED!!3000!! Could not download trf: %s" % (rets))
                    status = False
                    break
                else:
                    tolog("Will try again after 60s..")
                    from time import sleep
                    sleep(60)
            else:
                pilotErrorDiag = ""
                tolog("wget command returned: %s" % (rets))
                status = True
                break
            trial += 1

        return status, pilotErrorDiag

    def getAnalysisTrf(self, wgetCommand, origTRF, pilot_initdir):
        """ Get the trf to be used for analysis jobs """

        pilotErrorDiag = ""
        trfName = origTRF.split('/')[-1]
        tolog("trfName = %s" % (trfName))
        origBaseURL = ""

        # Copy trf from pilot init dir if distributed with pilot code
        fname = os.path.join(pilot_initdir, trfName)
        status = False
        if os.path.exists(fname):
            from shutil import copy2
            try:
                copy2(fname, os.getcwd())
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not copy trf from pilot init dir: %s" % str(e))
            else:
                tolog("Copied trf (%s) from pilot init dir" % (fname))
                status = True

        # Download trf
        if not status:
            # verify the base URL
            for baseURL in self.getValidBaseURLs():
                if origTRF.startswith(baseURL):
                    origBaseURL = baseURL
                    break

            if origBaseURL == "":
                pilotErrorDiag = "Invalid base URL: %s" % (origTRF)
                return self.__error.ERR_TRFDOWNLOAD, pilotErrorDiag, ""
            else:
                tolog("Verified the trf base URL: %s" % (origBaseURL))

            # try to download from the required location, if not - switch to backup
            for baseURL in self.getValidBaseURLs(order=origBaseURL):
                trf = re.sub(origBaseURL, baseURL, origTRF)
                tolog("Attempting to download trf: %s" % (trf))
                status, pilotErrorDiag = self.downloadTrf(wgetCommand, trf)
                if status:
                    break

            if not status:
                return self.__error.ERR_TRFDOWNLOAD, pilotErrorDiag, ""

            tolog("Successfully downloaded trf")

        tolog("Changing permission of %s to 0755" % (trfName))
        try:
            os.chmod(trfName, 0755)
        except Exception, e:
            pilotErrorDiag = "Failed to chmod %s: %s" % (trfName, str(e))
            return self.__error.ERR_CHMODTRF, pilotErrorDiag, ""

        return 0, pilotErrorDiag, trfName

    def getAnalysisRunCommand(self, job, jobSite, trfName):
        """ Get the run command for analysis jobs """
        # The run command is used to setup up runAthena/runGen

        from RunJobUtilities import updateCopysetups

        ec = 0
        pilotErrorDiag = ""
        run_command = ""

        # get the queuedata info
        # (directAccess info is stored in the copysetup variable)
        region = readpar('region')

        # get relevant file transfer info
        dInfo, useCopyTool, useDirectAccess, useFileStager, oldPrefix, newPrefix, copysetup, usePFCTurl, lfcHost =\
               self.getFileTransferInfo(job.transferType, isBuildJob(job.outFiles))

        # extract the setup file from copysetup (and verify that it exists)
        _copysetup = self.getSetupFromCopysetup(copysetup)
        if _copysetup != "" and os.path.exists(_copysetup):
            run_command = 'source %s;' % (_copysetup)

        # add the user proxy
        if os.environ.has_key('X509_USER_PROXY'):
            run_command += 'export X509_USER_PROXY=%s;' % os.environ['X509_USER_PROXY']
        else:
            tolog("Could not add user proxy to the run command (proxy does not exist)")

        # set up analysis trf
        run_command += './%s %s' % (trfName, job.jobPars)

#PN
#        job.jobPars += ' --accessmode=filestager'

        # add options for file stager if necessary
        if dInfo:
            # in case of forced usePFCTurl
            if usePFCTurl and not '--usePFCTurl' in run_command:
                oldPrefix = ""
                newPrefix = ""
                run_command += ' --usePFCTurl'
                tolog("reset old/newPrefix (forced TURL mode (1))")

            # sort out when directIn and useFileStager options should be used
            if useDirectAccess and '--directIn' not in job.jobPars and '--directIn' not in run_command:
                run_command += ' --directIn'
            if useFileStager and '--useFileStager' not in job.jobPars:
                run_command += ' --useFileStager'
            # old style copysetups will contain oldPrefix and newPrefix needed for the old style remote I/O
            if oldPrefix != "" and newPrefix != "":
                run_command += ' --oldPrefix "%s" --newPrefix %s' % (oldPrefix, newPrefix)
            else:
                # --directIn should be used in combination with --usePFCTurl, but not --old/newPrefix and --lfcHost
                if usePFCTurl and not '--usePFCTurl' in run_command:
                    run_command += ' --usePFCTurl'

        if job.transferType == 'direct':
            # update the copysetup
            # transferType is only needed if copysetup does not contain remote I/O info
            updateCopysetups(run_command, transferType=job.transferType, useCT=False, directIn=useDirectAccess, useFileStager=useFileStager)

        # add options for file stager if necessary (ignore if transferType = direct)
        if "accessmode" in job.jobPars and job.transferType != 'direct':
            accessmode_useCT = None
            accessmode_useFileStager = None
            accessmode_directIn = None
            _accessmode_dic = { "--accessmode=copy":["copy-to-scratch mode", ""],
                                "--accessmode=direct":["direct access mode", " --directIn"],
                                "--accessmode=filestager":["direct access / file stager mode", " --directIn --useFileStager"]}
            # update run_command according to jobPars
            for _mode in _accessmode_dic.keys():
                if _mode in job.jobPars:
                    # any accessmode set in jobPars should overrule schedconfig
                    tolog("Enforcing %s" % (_accessmode_dic[_mode][0]))
                    if _mode == "--accessmode=copy":
                        # make sure direct access and file stager get turned off 
                        usePFCTurl = False
                        accessmode_useCT = True
                        accessmode_directIn = False
                        accessmode_useFileStager = False
                    elif _mode == "--accessmode=direct":
                        # make sure copy-to-scratch and file stager get turned off
                        usePFCTurl = True
                        accessmode_useCT = False
                        accessmode_directIn = True
                        accessmode_useFileStager = False
                    else:
                        # make sure file stager gets turned on
                        usePFCTurl = False
                        accessmode_useCT = False
                        accessmode_directIn = True
                        accessmode_useFileStager = True

                    # update run_command (do not send the accessmode switch to runAthena)
                    run_command += _accessmode_dic[_mode][1]
                    if _mode in run_command:
                        run_command = run_command.replace(_mode, "")

            if "directIn" in run_command and not dInfo:
                if not usePFCTurl:
                    usePFCTurl = True
                    tolog("WARNING: accessmode mode specified directIn but direct access mode is not specified in copysetup (will attempt to create TURL based PFC later)")
                if not "usePFCTurl" in run_command:
                    run_command += ' --usePFCTurl'

            # need to add proxy if not there already
            if ("--directIn" in run_command or "--useFileStager" in run_command) and not "export X509_USER_PROXY" in run_command:
                if os.environ.has_key('X509_USER_PROXY'):
                    run_command = run_command.replace("./%s" % (trfName), "export X509_USER_PROXY=%s;./%s" % (os.environ['X509_USER_PROXY'], trfName))
                else:
                    tolog("Did not add user proxy to the run command (proxy does not exist)")

            # add the lfcHost if not there already
            if not "--lfcHost" in run_command and lfcHost != "":
                run_command += " --lfcHost %s" % (lfcHost)

            # update the copysetup
            updateCopysetups(run_command, transferType=None, useCT=accessmode_useCT, directIn=accessmode_directIn, useFileStager=accessmode_useFileStager)

        # add guids and lfc host when needed
        if region == 'US' and jobSite.dq2url != "" and lfcHost == "": # old style
            run_command += ' -u %s' % (jobSite.dq2url)
        else:
            if lfcHost != "":
                # get the correct guids list (with only the direct access files)
                if not isBuildJob(job.outFiles):
                    _guids = self.getGuidsFromJobPars(job.jobPars, job.inFiles, job.inFilesGuids)
                    # only add the lfcHost if --usePFCTurl is not specified
                    if usePFCTurl:
                        run_command += ' --inputGUIDs \"%s\"' % (str(_guids))
                    else:
                        if not "--lfcHost" in run_command:
                            run_command += ' --lfcHost %s' % (lfcHost)
                        run_command += ' --inputGUIDs \"%s\"' % (str(_guids))
                else:
                    if not usePFCTurl and not "--lfcHost" in run_command:
                        run_command += ' --lfcHost %s' % (lfcHost)

        # if both direct access and the accessmode loop added a directIn switch, remove the first one from the string
        if run_command.count("directIn") > 1:
            run_command = run_command.replace("--directIn", "", 1)

        # make sure that the site supports direct access / file stager
#        if ("--directIn" in run_command or "--useFileStager" in run_command) and readpar('allowdirectaccess').lower() != "true":
#            ec = self.__error.ERR_DAFSNOTALLOWED
#            pilotErrorDiag = "Site does not allow requested direct access / file stager (payload will fail)"
#            tolog("!!WARNING!!1234!! %s" % (pilotErrorDiag))

        return ec, pilotErrorDiag, run_command

    def getFileTransferInfo(self, transferType, buildJob):
        """ Get all relevant fields related to file transfer """

        copysetup = readpar('copysetupin')

        # create the direct access dictionary
        fileTransferInfo = getDirectAccessDic(copysetup)

        # if copysetupin did not contain direct access info, try the copysetup instead
        if not fileTransferInfo:
            copysetup = readpar('copysetup')
            fileTransferInfo = getDirectAccessDic(copysetup)

        # should the copytool be used?
        useCopyTool = False
        useFileStager = False
        useDirectAccess = False
        lfcHost = readpar('lfchost')
        oldPrefix = ""
        newPrefix = ""
        dInfo = None
        if fileTransferInfo:
            dInfo = True
            # no direct access / remote I/O, use standard copytool (copy-to-scratch)
            if fileTransferInfo['useCopyTool']:
                useCopyTool = True
            # do not set the LFC host for file stager
            if fileTransferInfo['useFileStager']:
                useFileStager = True
            if fileTransferInfo['directIn']:
                useDirectAccess = True

            oldPrefix = fileTransferInfo['oldPrefix']
            newPrefix = fileTransferInfo['newPrefix']

        # override settings for transferType direct
        if transferType == 'direct':
            useCopyTool = False
            useFileStager = False
            useDirectAccess = True
            if oldPrefix == "" and newPrefix == "":
                lfcHost = ""

        # should pilot create TURL based PFC? (not done here, but setup needs to be aware of it)
        # if dInfo and useDirectAccess and oldPrefix == "" and newPrefix == "":
        if (transferType == 'direct' or (useFileStager and useDirectAccess)) and (oldPrefix == "" and newPrefix == "") and not buildJob:
#        if (transferType == 'direct' or (not useFileStager and useDirectAccess)) and (oldPrefix == "" and newPrefix == ""):
            usePFCTurl = True
        else:
            usePFCTurl = False

        # force usePFCTurl for all jobs
        if not buildJob and useDirectAccess:
            tolog("Forced usePFCTurl (reset old/newPrefix)")
            usePFCTurl = True
            oldPrefix = ""
            newPrefix = ""

        return dInfo, useCopyTool, useDirectAccess, useFileStager, oldPrefix, newPrefix, copysetup, usePFCTurl, lfcHost

    def getSetupFromCopysetup(self, copysetup):
        """ Extract the setup file from copysetup """

        # Remove the direct access info
        if "^" in copysetup:
            _copysetup = copysetup[:copysetup.find("^")]
        else:
            _copysetup = copysetup

        # Does copysetup exist?
        if _copysetup != "":
            if os.path.exists(_copysetup):
                tolog("Setup file exists: %s" % (_copysetup))
            else:
                tolog("!!WARNING!!2999!! Setup file does not exist: %s (reset to empty string)" % (_copysetup))
        else:
            tolog("No setup file in copysetup")

        return _copysetup

    def getGuidsFromJobPars(self, jobPars, inputFiles, inFilesGuids):
        """ Extract the correct guid from the input file list """

        # the guids list is used for direct reading in an LFC environment
        # 1. extract input file list for direct reading from jobPars
        # 2. for each input file in this list, find the corresponding guid from the input file guid list
        # since jobPars is entered by a human, the order of the input files might not be the same

        guidList = []

        jobPars = jobPars.replace("'","")
        jobPars = jobPars.replace(", ",",")

        pattern = re.compile(r'\-i \"\[([A-Za-z0-9.,_-]+)\]\"')
        directReadingInputFiles = re.findall(pattern, jobPars)
        inFiles = []
        if directReadingInputFiles != []:
            inFiles = directReadingInputFiles[0].split(",")
        else:
            match = re.search("-i ([A-Za-z0-9.\[\],_-]+) ", jobPars)
            if match != None:
                compactInFiles = match.group(1)
                match = re.search('(.*)\[(.+)\](.*)\[(.+)\]', compactInFiles)
                if match != None:
                    inputFiles = []
                    head = match.group(1)
                    tail = match.group(3)
                    body = match.group(2).split(',')
                    attr = match.group(4).split(',')
                    for idx in range(len(body)):
                        lfn = '%s%s%s%s' % (head, body[idx], tail, attr[idx])
                        inputFiles.append(lfn)
                else:
                    inputFiles = [compactInFiles]

        if inFiles != []:
            for inFile in inFiles:
                # get the corresponding index from the inputFiles list, which has the same order as inFilesGuids
                try:
                    index = inputFiles.index(inFile)
                except Exception, e:
                    tolog("!!WARNING!!2999!! Exception caught: %s (direct reading will fail)" % str(e))
                else:
                    # add the corresponding guid to the list
                    guidList.append(inFilesGuids[index])

        return guidList

    def getMetadataForRegistration(self, guid):
        """ Return metadata for [LFC] file registration """

        # This method can insert special metadata into the metadata.xml file
        # E.g. it can add preliminary XML tags for info that will only be known
        # at a later time, such as "<metadata att_name="surl" att_value="%s-surltobeset"/>\n' % (guid)"
        # The <guid>-surltobeset will be replaced by the pilot by the appropriate value once it is known
        # Inputs:
        #   guid = file guid
        # Returns:
        #   metadata string
        # See e.g. the CMSExperiment implementation

        # The method is called from pUtil::PFCxml() during metadata file creation

        return ""

    def getAttrForRegistration(self):
        """ Return the attribute of the metadata XML to be updated with surl value """

        # Used in combination with Experiment::getMetadataForRegistration()
        # The attribute (default 'surl') will be copied into the metadata string used for pattern matching
        # E.g. re.compile('\<metadata att\_name\=\"%s\" att\_value\=\"([a-zA-Z0-9-]+)\-surltobeset\"\/\>' % (attribute))

        return 'surl'

    def getExpSpecificMetadata(self, job, workdir):
        """ Return experiment specific metadata """

        # Inputs:
        #   job = PanDA pilot job object (see Job class)
        #   workdir = relevant work directory where the metadata is located
        # Returns:
        #   metadata xml string
        # See e.g. implementation in CMSExperiment

        return ""

    def getFileCatalogHosts(self):
        """ Return a list of file catalog hosts """

        # The method is used in combination with federated xrootd (FAX).
        # In case FAX is allowed on a given site, the pilot might need to lookup
        # replica information in more than one LFC catalog. Normally a site has only
        # one LFC (as set in schedconfig.lfchost). Providing a list of hosts will increase
        # the probability that FAX will succeed
        # See e.g. ATLASExperiment implementation

        return []

    def verifySwbase(self, appdir):
        """ Confirm existence of appdir/swbase """

        # appdir/swbase is a queuedata parameter specifying the base location of physics analysis / release software
        # This method will simply verify that the corresponding directory exists
        #
        # Input:
        #   appdir = application/software/release directory (e.g. /cvmfs/atlas.cern.ch/repo/sw)
        # Return:
        #   error code (0 for success)
        
        return 0

    def interpretPayloadStdout(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode):
        """ Payload error interpretation and handling """

        # NOTE: TODO, hide argument complexity with kwargs**

        # This method can be used to interpret special errors that only occur in actual payload stdout, e.g. memory errors that have
        # caused the payload to crash
        #
        # Inputs:
        #   job = PanDA pilot job object (see Job class)
        #   res =
        #   getstatusoutput_was_interrupted = True in case the payload execution command was aborted (e.g. keyboard CTRL-C)
        #   current_job_number = current job number, in case of multi-trf (ATLAS)
        #   runCommandList = list of payload execution commands (e.g. used by ATLAS to get to a setup file)
        #   failureCode = signal error code
        # Returns:
        #   Updated PanDA pilot job objectwith proper payload error information, if needed
        #
        # The following Job attributes can be updated here
        #   result = tuple of size 3 that contain the standard error info: result[0] = current job status (e.g. failed, finished, holding),
        #            result[1] = payload error code, result[2] = PanDA pilot error code
        #   pilotErrorDiag = error diagnostics (string of up to 256 characters that will appear on the PanDA monitor job web page for a failed job)
        #   exeError

        return job
