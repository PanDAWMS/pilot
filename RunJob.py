# Class definition:
#   RunJob
#   This is the main RunJob class; RunJobEvent etc will inherit from this class
#   Note: at the moment, this class is essentially the old runJob module turned object oriented.
#         The class will later become RunJobNormal, ie responible for running normal PanDA jobs.
#         At that point a new RunJob top class will be created containing methods that have been
#         identified as common between the various sub classes.
#   Instances are generated with RunJobFactory
#   Subclasses should implement all needed methods prototyped in this class
#   Note: not compatible with Singleton Design Pattern due to the subclassing

# Standard python modules
import os, sys, commands, getopt, time
import traceback
import atexit, signal
import stat
from optparse import OptionParser

# Pilot modules
import Site, pUtil, Job, Node, RunJobUtilities
import Mover as mover
from pUtil import debugInfo, tolog, isAnalysisJob, readpar, createLockFile, getDatasetDict, getChecksumCommand,\
     tailPilotErrorDiag, getFileAccessInfo, processDBRelease, getCmtconfig, getExtension, getExperiment, getGUID
from JobRecovery import JobRecovery
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from ProxyGuard import ProxyGuard
from shutil import copy2


# remove logguid, dq2url, debuglevel - not needed
# rename lfcRegistration to catalogRegistration
# relabelled -h, queuename to -b (debuglevel not used)


class RunJob(object):

    # private data members
    __runjob = "RunJob"                  # String defining the RunJob class
    __instance = None                    # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()              # PilotErrors object

#    __appdir = "/usatlas/projects/OSG"   # Default software installation directory
#    __debugLevel = 0                     # 0: debug info off, 1: display function name when called, 2: full debug info
#    __dq2url = "" # REMOVE
    __failureCode = None                 # set by signal handler when user/batch system kills the job
    __globalPilotErrorDiag = ""          # global pilotErrorDiag used with signal handler (only)
    __globalErrorCode = 0                # global error code used with signal handler (only)
    __inputDir = ""                      # location of input files (source for mv site mover)
    __fileCatalogRegistration = True     # should the pilot perform file catalog registration?
    __logguid = None                     # guid for the log file
    __outputDir = ""                     # location of output files (destination for mv site mover)
    __pilot_initdir = ""                 # location of where the pilot is untarred and started
    __pilotlogfilename = "pilotlog.txt"  # default pilotlog filename 
    __pilotserver = "localhost"          # default server
    __pilotport = 88888                  # default port
    __proxycheckFlag = True              # True (default): perform proxy validity checks, False: no check
    __pworkdir = "/tmp"                  # site work dir used by the parent
#    __queuename = ""                     # PanDA queue  NOT NEEDED
#    __sitename = "testsite"              # PanDA site  NOT NEEDED
    __stageinretry = 1                   # number of stage-in tries
    __stageoutretry = 1                  # number of stage-out tries
#    __testLevel = 0                      # test suite control variable (0: no test, 1: put error, 2: ...)  NOT USED
#    __workdir = "/tmp" # NOT USED
    __cache = ""                         # Cache URL, e.g. used by LSST

    # Getter and setter methods

    def getExperiment(self):
        """ Getter for __experiment """

        return self.__experiment

    def getFailureCode(self):
        """ Getter for __failureCode """

        return self.__failureCode

    def setFailureCode(self, code):
        """ Setter for __failureCode """

        self.__failureCode = code

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

    def setCache(self, cache):
        """ Setter for __cache """

        self.__cache = cache

    def getInputDir(self):
        """ Getter for __inputDir """

        return self.__inputDir

    def getFileCatalogRegistration(self):
        """ Getter for __fileCatalogRegistration """

        return self.__fileCatalogRegistration

    def getLogGUID(self):
        """ Getter for __logguid """

        return self.__logguid

    def getOutputDir(self):
        """ Getter for __outputDir """

        return self.__outputDir

    def getPilotInitDir(self):
        """ Getter for __pilot_initdir """

        return self.__pilot_initdir

    def getPilotLogFilename(self):
        """ Getter for __pilotlogfilename """

        return self.__pilotlogfilename

    def getPilotServer(self):
        """ Getter for __pilotserver """

        return self.__pilotserver

    def getPilotPort(self):
        """ Getter for __pilotport """

        return self.__pilotport

    def getProxyCheckFlag(self):
        """ Getter for __proxycheckFlag """

        return self.__proxycheckFlag

    def getParentWorkDir(self):
        """ Getter for __pworkdir """

        return self.__pworkdir

    def getStageInRetry(self):
        """ Getter for __stageinretry """

        return self.__stageinretry

    def getStageOutRetry(self):
        """ Getter for __stageoutretry """

        return self.__stageoutretry

    def getCache(self):
        """ Getter for __cache """

        return self.__cache

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def getRunJob(self):
        """ Return a string with the module name """

        return self.__runjob

    def argumentParser(self):
        """ Argument parser for the RunJob module """

        # Return variables
        appdir = None
        dq2url = None # REMOVE
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
        parser.add_option("-q", "--dq2url", dest="dq2url",
                          help="DQ2 URL TO BE RETIRED", metavar="DQ2URL")
        parser.add_option("-x", "--stageinretries", dest="stageinretry",
                          help="The number of stage-in retries", metavar="STAGEINRETRY")
        parser.add_option("-B", "--filecatalogregistration", dest="fileCatalogRegistration",
                          help="True (default): perform file catalog registration, False: no catalog registration", metavar="FILECATALOGREGISTRATION")
        parser.add_option("-E", "--stageoutretries", dest="stageoutretry",
                          help="The number of stage-out retries", metavar="STAGEOUTRETRY")
        parser.add_option("-F", "--experiment", dest="experiment",
                          help="Current experiment (default: ATLAS)", metavar="EXPERIMENT")
        parser.add_option("-H", "--cache", dest="cache",
                          help="Cache URL", metavar="CACHE")

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
            if options.dq2url:
#                self.__dq2url = options.dq2url
                dq2url = options.dq2url
            if options.experiment:
                self.__experiment = options.experiment
            if options.logguid:
                self.__logguid = options.logguid
            if options.inputDir:
                self.__inputDir = options.inputDir
            if options.fileCatalogRegistration:
                if options.fileCatalogRegistration.lower() == "false":            
                    self.__fileCatalogRegistration = False
                else:
                    self.__fileCatalogRegistration = True
            else:
                self.__fileCatalogRegistration = True
            if options.pilot_initdir:
                self.__pilot_initdir = options.pilot_initdir
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

        return sitename, appdir, workdir, dq2url, queuename

    def getRunJobFileName(self):
        """ Return the filename of the module """

        fullpath = sys.modules[self.__module__].__file__

        # Note: the filename above will contain both full path, and might end with .pyc, fix this
        filename = os.path.basename(fullpath)
        if filename.endswith(".pyc"):
            filename = filename[:-1] # remove the trailing 'c'

        return filename

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return True

    def cleanup(self, job, rf=None):
        """ Cleanup function """
        # 'rf' is a list that will contain the names of the files that could be transferred
        # In case of transfer problems, all remaining files will be found and moved
        # to the data directory for later recovery.

        tolog("********************************************************")
        tolog(" This job ended with (trf,pilot) exit code of (%d,%d)" % (job.result[1], job.result[2]))
        tolog("********************************************************")

        # clean up the pilot wrapper modules
        pUtil.removePyModules(job.workdir)

        if os.path.isdir(job.workdir):
            os.chdir(job.workdir)

            # remove input files from the job workdir
            remFiles = job.inFiles
            for inf in remFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (job.workdir, inf))
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
                tolog('job.workdir is %s pworkdir is %s ' % (job.workdir, self.__pworkdir)) # Eddie
                copy2("%s/metadata-%s.xml" % (job.workdir, job.jobId), "%s/metadata-%s.xml" % (self.__pworkdir, job.jobId))
            except Exception, e:
                tolog("Warning: Could not copy metadata-%s.xml to site work dir - ddm Adder problems will occure in case of job recovery" % (job.jobId))
                tolog('job.workdir is %s pworkdir is %s ' % (job.workdir, self.__pworkdir)) # Eddie
            if job.result[0] == 'holding' and job.result[1] == 0:
                try:
                    # create the data directory
                    os.makedirs(job.datadir)
                except OSError, e:
                    tolog("!!WARNING!!3000!! Could not create data directory: %s, %s" % (job.datadir, str(e)))
                else:
                    # find all remaining files in case 'rf' is not empty
                    remaining_files = []
                    moved_files_list = []
                    try:
                        if rf != None:
                            moved_files_list = RunJobUtilities.getFileNamesFromString(rf[1])
                            remaining_files = RunJobUtilities.getRemainingFiles(moved_files_list, job.outFiles) 
                    except Exception, e:
                        tolog("!!WARNING!!3000!! Illegal return value from Mover: %s, %s" % (str(rf), str(e)))
                        remaining_files = job.outFiles

                    # move all remaining output files to the data directory
                    nr_moved = 0
                    for _file in remaining_files:
                        try:
                            os.system("mv %s %s" % (_file, job.datadir))
                        except OSError, e:
                            tolog("!!WARNING!!3000!! Failed to move file %s (abort all)" % (_file))
                            break
                        else:
                            nr_moved += 1

                    tolog("Moved %d/%d output file(s) to: %s" % (nr_moved, len(remaining_files), job.datadir))

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
                        _fname = os.path.join(job.workdir, "PoolFileCatalog.xml")
                        tolog("Copying %s to %s" % (_fname, job.datadir))
                        try:
                            copy2(_fname, job.datadir)
                        except Exception, e:
                            tolog("!!WARNING!!3000!! Could not copy PoolFileCatalog.xml to data dir - expect ddm Adder problems during job recovery")

            # remove all remaining output files from the work directory
            # (a successfully copied file should already have been removed by the Mover)
            rem = False
            for inf in job.outFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (job.workdir, inf))
                    except Exception,e:
                        tolog("!!WARNING!!3000!! Ignore this Exception when deleting file %s: %s" % (inf, str(e)))
                        pass
                    else:
                        tolog("Lingering output file removed: %s" % (inf))
                        rem = True
            if not rem:
                tolog("All output files already removed from local dir")

        tolog("Payload cleanup has finished")

    def sysExit(self, job, rf=None):
        '''
        wrapper around sys.exit
        rs is the return string from Mover::put containing a list of files that were not transferred
        '''

        self.cleanup(job, rf=rf)
        sys.stderr.close()
        tolog("RunJob (payload wrapper) has finished")
        # change to sys.exit?
        os._exit(job.result[2]) # pilotExitCode, don't confuse this with the overall pilot exit code,
                                # which doesn't get reported back to panda server anyway

    def failJob(self, transExitCode, pilotExitCode, job, ins=None, pilotErrorDiag=None, docleanup=True):
        """ set the fail code and exit """

        if pilotExitCode and job.attemptNr < 4 and job.eventServiceMerge:
            pilotExitCode = PilotErrors.ERR_ESRECOVERABLE
        job.setState(["failed", transExitCode, pilotExitCode])
        if pilotErrorDiag:
            job.pilotErrorDiag = pilotErrorDiag
        tolog("Will now update local pilot TCP server")
        rt = RunJobUtilities.updatePilotServer(job, self.__pilotserver, self.__pilotport, final=True)
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)
        if docleanup:
            self.sysExit(job)

    def setup(self, job, jobSite, thisExperiment):
        """ prepare the setup and get the run command list """

        # start setup time counter
        t0 = time.time()
        ec = 0
        runCommandList = []

        # split up the job parameters to be able to loop over the tasks
        jobParameterList = job.jobPars.split("\n")
        jobHomePackageList = job.homePackage.split("\n")
        jobTrfList = job.trf.split("\n")
        job.release = thisExperiment.formatReleaseString(job.release)
        releaseList = thisExperiment.getRelease(job.release)

        tolog("Number of transformations to process: %s" % len(jobParameterList))
        if len(jobParameterList) > 1:
            multi_trf = True
        else:
            multi_trf = False

        # verify that the multi-trf job is setup properly
        ec, job.pilotErrorDiag, releaseList = RunJobUtilities.verifyMultiTrf(jobParameterList, jobHomePackageList, jobTrfList, releaseList)
        if ec > 0:
            return ec, runCommandList, job, multi_trf

        os.chdir(jobSite.workdir)
        tolog("Current job workdir is %s" % os.getcwd())

        # setup the trf(s)
        _i = 0
        _stdout = job.stdout
        _stderr = job.stderr
        _first = True
        for (_jobPars, _homepackage, _trf, _swRelease) in map(None, jobParameterList, jobHomePackageList, jobTrfList, releaseList):
            tolog("Preparing setup %d/%d" % (_i + 1, len(jobParameterList)))

            # reset variables
            job.jobPars = _jobPars
            job.homePackage = _homepackage
            job.trf = _trf
            job.release = _swRelease
            if multi_trf:
                job.stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))
                job.stderr = _stderr.replace(".txt", "_%d.txt" % (_i + 1))

            # post process copysetup variable in case of directIn/useFileStager
            _copysetup = readpar('copysetup')
            _copysetupin = readpar('copysetupin')
            if "--directIn" in job.jobPars or "--useFileStager" in job.jobPars or _copysetup.count('^') == 5 or _copysetupin.count('^') == 5:
                # only need to update the queuedata file once
                if _first:
                    RunJobUtilities.updateCopysetups(job.jobPars)
                    _first = False

            # setup the trf
            ec, job.pilotErrorDiag, cmd, job.spsetup, job.JEM, job.cmtconfig = thisExperiment.getJobExecutionCommand(job, jobSite, self.__pilot_initdir)
            if ec > 0:
                # setup failed
                break

            # add the setup command to the command list
            runCommandList.append(cmd)
            _i += 1

        job.stdout = _stdout
        job.stderr = _stderr
        job.timeSetup = int(time.time() - t0)
        tolog("Total setup time: %d s" % (job.timeSetup))

        return ec, runCommandList, job, multi_trf

    def stageIn(self, job, jobSite, analysisJob, pfc_name="PoolFileCatalog.xml"):
        """ Perform the stage-in """

        ec = 0
        statusPFCTurl = None
        usedFAXandDirectIO = False

        # Prepare the input files (remove non-valid names) if there are any
        ins, job.filesizeIn, job.checksumIn = RunJobUtilities.prepareInFiles(job.inFiles, job.filesizeIn, job.checksumIn)
        if ins:
            tolog("Preparing for get command")

            # Get the file access info (only useCT is needed here)
            useCT, oldPrefix, newPrefix, useFileStager, directIn = getFileAccessInfo()

            # Transfer input files
            tin_0 = os.times()
            ec, job.pilotErrorDiag, statusPFCTurl, FAX_dictionary = \
                mover.get_data(job, jobSite, ins, self.__stageinretry, analysisJob=analysisJob, usect=useCT,\
                               pinitdir=self.__pilot_initdir, proxycheck=False, inputDir=self.__inputDir, workDir=self.__pworkdir, pfc_name=pfc_name)
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

        return job, ins, statusPFCTurl, usedFAXandDirectIO

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

        if os.path.exists(filename):
            tolog("Found job report: %s" % (filename))

            # first backup the jobReport to the job workdir since it will be needed later
            # (the current location will disappear since it will be tarred up in the jobs' log file)
            d = os.path.join(workdir, '..')
            try:
                copy2(filename, os.path.join(d, _filename))
            except Exception, e:
                tolog("Warning: Could not backup %s to %s: %s" % (_filename, d, e))
            else:
                tolog("Backed up %s to %s" % (_filename, d))

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

        else:
            tolog("Job report not found: %s" % (filename))

        return exitCode, exitAcronym, exitMsg

    def extractDictionaryObject(self, obj, dictionary):
        """ Extract an object from a dictionary """

        _obj = None

        try:
            _obj = dictionary[obj]
        except Exception, e:
            tolog("Object %s not found in dictionary" % (obj))
        else:
            tolog('Extracted \"%s\"=%s from dictionary' % (obj, _obj))

        return _obj

    def executePayload(self, thisExperiment, runCommandList, job):
        """ execute the payload """

        # do not hide the proxy for PandaMover since it needs it or for sites that has sc.proxy = donothide
        #if 'DDM' not in jobSite.sitename and readpar('proxy') != 'donothide':
        #    # create the proxy guard object (must be created here before the sig2exc())
        #    proxyguard = ProxyGuard()
        #
        #    # hide the proxy
        #    hP_ret = proxyguard.hideProxy()
        #    if not hP_ret:
        #        tolog("Warning: Proxy exposed to payload")

        # run the payload process, which could take days to finish
        t0 = os.times()
        tolog("t0 = %s" % str(t0))
        res_tuple = (0, 'Undefined')

        # loop over all run commands (only >1 for multi-trfs)
        current_job_number = 0
        getstatusoutput_was_interrupted = False
        number_of_jobs = len(runCommandList)
        for cmd in runCommandList:
            current_job_number += 1
            try:
                # add the full job command to the job_setup.sh file
                to_script = cmd.replace(";", ";\n")
                thisExperiment.updateJobSetupScript(job.workdir, to_script=to_script)

                tolog("Executing job command %d/%d: %s" % (current_job_number, number_of_jobs, cmd))
                # Eddie: Commented out the part where we execute ONLY the payload under GLExec as now everything is under a glexec'ed environment
                #if readpar('glexec').lower() in ['true', 'uid']: 
                #    # execute trf under glexec
                #    res_tuple = executePayloadGLExec(cmd, job)
                #else:
                #    # execute trf normally
                res_tuple = commands.getstatusoutput(cmd)

            except Exception, e:
                tolog("!!FAILED!!3000!! Failed to run command %s" % str(e))
                getstatusoutput_was_interrupted = True
                if self.__failureCode:
                    job.result[2] = self.__failureCode
                    tolog("!!FAILED!!3000!! Failure code: %d" % (self.__failureCode))
                    break
            else:
                if res_tuple[0] == 0:
                    tolog("Job command %d/%d finished" % (current_job_number, number_of_jobs))
                else:
                    tolog("Job command %d/%d failed: res = %s" % (current_job_number, number_of_jobs, str(res_tuple)))
                    break

        t1 = os.times()
        tolog("t1 = %s" % str(t1))
        t = map(lambda x, y:x-y, t1, t0) # get the time consumed
        job.cpuConsumptionUnit, job.cpuConsumptionTime, job.cpuConversionFactor = pUtil.setTimeConsumed(t)
        tolog("Job CPU usage: %s %s" % (job.cpuConsumptionTime, job.cpuConsumptionUnit))
        tolog("Job CPU conversion factor: %1.10f" % (job.cpuConversionFactor))
        job.timeExe = int(round(t1[4] - t0[4]))

        tolog("Original exit code: %d" % (res_tuple[0]))
        tolog("Exit code: %d (returned from OS)" % (res_tuple[0]%255))

        # check the job report for any exit code that should replace the res_tuple[0]
        res0, exitAcronym, exitMsg = self.getTrfExitInfo(res_tuple[0], job.workdir)
        res = (res0, res_tuple[1], exitMsg)

        # dump an extract of the payload output
        if number_of_jobs > 1:
            _stdout = job.stdout
            _stderr = job.stderr
            _stdout = _stdout.replace(".txt", "_N.txt")
            _stderr = _stderr.replace(".txt", "_N.txt")
            tolog("NOTE: For %s output, see files %s, %s (N = [1, %d])" % (job.payload, _stdout, _stderr, number_of_jobs))
        else:
            tolog("NOTE: For %s output, see files %s, %s" % (job.payload, job.stdout, job.stderr))

        # JEM job-end callback
        try:
            from JEMstub import notifyJobEnd2JEM
            notifyJobEnd2JEM(job, tolog)
        except:
            pass # don't care (fire and forget)

        # restore the proxy
        #if hP_ret:
        #    rP_ret = proxyguard.restoreProxy()
        #    if not rP_ret:
        #        tolog("Warning: Problems with storage can occur since proxy could not be restored")
        #    else:
        #        hP_ret = False
        #        tolog("ProxyGuard has finished successfully")

        return res, job, getstatusoutput_was_interrupted, current_job_number

    def moveTrfMetadata(self, workdir, jobId):
        """ rename and copy the trf metadata """

        oldMDName = "%s/metadata.xml" % (workdir)
        _filename = "metadata-%s.xml.PAYLOAD" % (jobId)
        newMDName = "%s/%s" % (workdir, _filename)
        try:
            os.rename(oldMDName, newMDName)
        except:
            tolog("Warning: Could not open the original %s file, but harmless, pass it" % (oldMDName))
            pass
        else:
            tolog("Renamed %s to %s" % (oldMDName, newMDName))
            # now move it to the pilot work dir
            try:
                copy2(newMDName, "%s/%s" % (self.__pworkdir, _filename))
            except Exception, e:
                tolog("Warning: Could not copy %s to site work dir: %s" % (_filename, str(e)))
            else:
                tolog("Metadata was transferred to site work dir: %s/%s" % (self.__pworkdir, _filename))

    def createFileMetadata(self, outFiles, job, outsDict, dsname, datasetDict, sitename, analysisJob=False):
        """ create the metadata for the output + log files """

        ec = 0

        # get/assign guids to the output files
        if outFiles:
            if not pUtil.isBuildJob(outFiles):
                ec, job.pilotErrorDiag, job.outFilesGuids = RunJobUtilities.getOutFilesGuids(job.outFiles, job.workdir)
                if ec:
                    # missing PoolFileCatalog (only error code from getOutFilesGuids)
                    return ec, job, None
            else:
                tolog("Build job - do not use PoolFileCatalog to get guid (generated)")
        else:
            tolog("This job has no output files")

        # get the file sizes and checksums for the local output files
        # WARNING: any errors are lost if occur in getOutputFileInfo()
        ec, pilotErrorDiag, fsize, checksum = pUtil.getOutputFileInfo(list(outFiles), getChecksumCommand(), skiplog=True, logFile=job.logFile)
        if ec != 0:
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            self.failJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)

        if self.__logguid:
            guid = self.__logguid
        else:
            guid = job.tarFileGuid

        # create preliminary metadata (no metadata yet about log file - added later in pilot.py)
        _fname = "%s/metadata-%s.xml" % (job.workdir, job.jobId)
        try:
            _status = pUtil.PFCxml(job.experiment, _fname, list(job.outFiles), fguids=job.outFilesGuids, fntag="lfn", alog=job.logFile, alogguid=guid,\
                                   fsize=fsize, checksum=checksum, analJob=analysisJob)
        except Exception, e:
            pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
            tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag)) 
            self.failJob(job.result[1], error.ERR_MISSINGGUID, job, pilotErrorDiag=pilotErrorDiag)
        else:
            if not _status:
                pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                self.failJob(job.result[1], error.ERR_MISSINGGUID, job, pilotErrorDiag=pilotErrorDiag)

        tolog("..............................................................................................................")
        tolog("Created %s with:" % (_fname))
        tolog(".. log            : %s (to be transferred)" % (job.logFile))
        tolog(".. log guid       : %s" % (guid))
        tolog(".. out files      : %s" % str(job.outFiles))
        tolog(".. out file guids : %s" % str(job.outFilesGuids))
        tolog(".. fsize          : %s" % str(fsize))
        tolog(".. checksum       : %s" % str(checksum))
        tolog("..............................................................................................................")

        # convert the preliminary metadata-<jobId>.xml file to OutputFiles-<jobId>.xml for NG and for CERNVM
        # note: for CERNVM this is only really needed when CoPilot is used
        region = readpar("region")
        if region == 'Nordugrid' or sitename == 'CERNVM':
            if RunJobUtilities.convertMetadata4NG(os.path.join(job.workdir, job.outputFilesXML), _fname, outsDict, dsname, datasetDict):
                tolog("Metadata has been converted to NG/CERNVM format")
            else:
                job.pilotErrorDiag = "Could not convert metadata to NG/CERNVM format"
                tolog("!!WARNING!!1999!! %s" % (job.pilotErrorDiag))

        # try to build a file size and checksum dictionary for the output files
        # outputFileInfo: {'a.dat': (fsize, checksum), ...}
        # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
        # checksum for file a.dat: outputFileInfo['a.dat'][1]
        try:
            # remove the log entries
            _fsize = fsize[1:]
            _checksum = checksum[1:]
            outputFileInfo = dict(zip(job.outFiles, zip(_fsize, _checksum)))
        except Exception, e:
            tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
            outputFileInfo = {}
        else:
            tolog("Output file info dictionary created: %s" % str(outputFileInfo))

        return ec, job, outputFileInfo

    def getDatasets(self, job):
        """ get the datasets for the output files """

        # get the default dataset
        if job.destinationDblock and job.destinationDblock[0] != 'NULL' and job.destinationDblock[0] != ' ':
            dsname = job.destinationDblock[0]
        else:
            dsname = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name

        # create the dataset dictionary
        # (if None, the dsname above will be used for all output files)
        datasetDict = getDatasetDict(job.outFiles, job.destinationDblock, job.logFile, job.logDblock)
        if datasetDict:
            tolog("Dataset dictionary has been verified")
        else:
            tolog("Dataset dictionary could not be verified, output files will go to: %s" % (dsname))

        return dsname, datasetDict

    def stageOut(self, job, jobSite, outs, analysisJob, dsname, datasetDict, outputFileInfo):
        """ perform the stage-out """

        error = PilotErrors()
        pilotErrorDiag = ""
        rc = 0
        latereg = False
        rf = None

        # generate the xml for the output files and the site mover
        pfnFile = "OutPutFileCatalog.xml"
        try:
            _status = pUtil.PFCxml(job.experiment, pfnFile, outs, fguids=job.outFilesGuids, fntag="pfn")
        except Exception, e:
            job.pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
            tolog("!!WARNING!!1113!! %s" % (job.pilotErrorDiag)) 
            return error.ERR_MISSINGGUID, job, rf, latereg
        else:
            if not _status:
                job.pilotErrorDiag = "Metadata contains missing guid(s) for output file(s)"
                tolog("!!WARNING!!2999!! %s" % (job.pilotErrorDiag))
                return error.ERR_MISSINGGUID, job, rf, latereg

        tolog("Using the newly-generated %s/%s for put operation" % (job.workdir, pfnFile))

        # the cmtconfig is needed by at least the xrdcp site mover
        cmtconfig = getCmtconfig(job.cmtconfig)

        rs = "" # return string from put_data with filename in case of transfer error
        tin_0 = os.times()
        try:
            rc, job.pilotErrorDiag, rf, rs, job.filesNormalStageOut, job.filesAltStageOut = mover.mover_put_data("xmlcatalog_file:%s" % (pfnFile), dsname, jobSite.sitename,\
                                             ub=jobSite.dq2url, analysisJob=analysisJob, pinitdir=self.__pilot_initdir, scopeOut=job.scopeOut,\
                                             proxycheck=self.__proxycheckFlag, spsetup=job.spsetup, token=job.destinationDBlockToken,\
                                             userid=job.prodUserID, datasetDict=datasetDict, prodSourceLabel=job.prodSourceLabel,\
                                             outputDir=self.__outputDir, jobId=job.jobId, jobWorkDir=job.workdir, DN=job.prodUserID,\
                                             dispatchDBlockTokenForOut=job.dispatchDBlockTokenForOut, outputFileInfo=outputFileInfo,\
                                             lfcreg=self.__fileCatalogRegistration, jobDefId=job.jobDefinitionID, jobCloud=job.cloud, logFile=job.logFile,\
                                             stageoutTries=self.__stageoutretry, cmtconfig=cmtconfig, experiment=self.__experiment, fileDestinationSE=job.fileDestinationSE)
            tin_1 = os.times()
            job.timeStageOut = int(round(tin_1[4] - tin_0[4]))
        except Exception, e:
            tin_1 = os.times()
            job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

            if 'format_exc' in traceback.__all__:
                trace = traceback.format_exc()
                pilotErrorDiag = "Put function can not be called for staging out: %s, %s" % (str(e), trace)
            else:
                tolog("traceback.format_exc() not available in this python version")
                pilotErrorDiag = "Put function can not be called for staging out: %s" % (str(e))
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))

            rc = error.ERR_PUTFUNCNOCALL
            job.setState(["holding", job.result[1], rc])
        else:
            if job.pilotErrorDiag != "":
                if job.pilotErrorDiag.startswith("Put error:"):
                    pre = ""
                else:
                    pre = "Put error: "
                job.pilotErrorDiag = pre + tailPilotErrorDiag(job.pilotErrorDiag, size=256-len("pilot: Put error: "))

            tolog("Put function returned code: %d" % (rc))
            if rc != 0:
                # remove any trailing "\r" or "\n" (there can be two of them)
                if rs != None:
                    rs = rs.rstrip()
                    tolog("Error string: %s" % (rs))

                # is the job recoverable?
                if error.isRecoverableErrorCode(rc):
                    _state = "holding"
                    _msg = "WARNING"
                else:
                    _state = "failed"
                    _msg = "FAILED"

                # look for special error in the error string
                if rs == "Error: string Limit exceeded 250":
                    tolog("!!%s!!3000!! Put error: file name string limit exceeded 250" % (_msg))
                    job.setState([_state, job.result[1], error.ERR_LRCREGSTRSIZE])
                else:
                    job.setState([_state, job.result[1], rc])

                tolog("!!%s!!1212!! %s" % (_msg, error.getErrorStr(rc)))
            else:
                # set preliminary finished (may be overwritten below in the LRC registration)
                job.setState(["finished", 0, 0])

                # create a weak lockfile meaning that file transfer worked
                # (useful for job recovery if activated) in the job workdir
                createLockFile(True, jobSite.workdir, lockfile="ALLFILESTRANSFERRED")
                # create another lockfile in the site workdir since a transfer failure can still occur during the log transfer
                # and a later recovery attempt will fail (job workdir will not exist at that time)
                createLockFile(True, self.__pworkdir, lockfile="ALLFILESTRANSFERRED")

            if job.result[0] == "holding" and '(unrecoverable)' in job.pilotErrorDiag:
                job.result[0] = "failed"
                tolog("!!WARNING!!2999!! HOLDING state changed to FAILED since error is unrecoverable")

        return rc, job, rf, latereg

    def copyInputForFiles(self, workdir):
        """ """

        try:
            cmd = "cp %s/inputFor_* %s" % (self.__pilot_initdir, workdir)
            tolog("Executing command: %s" % (cmd))
            out = commands.getoutput(cmd)
        except IOError, e:
            pass
        tolog(out)

    # Methods used by event service RunJob* modules ..............................................................

    def stripSetupCommand(self, cmd, trfName):
        """ Remove the trf part of the setup command """

        location = cmd.find(trfName)
        return cmd[:location]

    def executeMakeRunEventCollectionScript(self, cmd, eventcollection_filename):
        """ Define and execute the event collection script """

        cmd += "get_files -jo %s" % (eventcollection_filename)
        tolog("Execute command: %s" % (cmd))

        # WARNING: PUT A TIMER AROUND THIS COMMAND
        rc, rs = commands.getstatusoutput(cmd)

        return rc, rs

    def prependMakeRunEventCollectionScript(self, input_file, output_file, eventcollection_filename):
        """ Prepend the event collection script """

        status = False
        eventcollection_filename_mod = ""

        with open(eventcollection_filename) as f1:
            eventcollection_filename_mod = eventcollection_filename.replace(".py",".2.py")
            with open(eventcollection_filename_mod, "w") as f2:
                f2.write("EvtMax = -1\n")
                f2.write("In = [ \'%s\' ]\n" % (input_file))
                f2.write("Out = \'%s\'\n" % (output_file))
                for line in f1:
                    f2.write(line)
                f2.close()
                f1.close()
                status = True

        return status, eventcollection_filename_mod

    def executeTAGFileCommand(self, cmd, eventcollection_filename_mod):
        """ Execute the TAG file creation script using athena """

        cmd += "athena.py %s >MakeRunEventCollection-stdout.txt" % (eventcollection_filename_mod)
        tolog("Executing command: %s" % (cmd))

        # WARNING: PUT A TIMER AROUND THIS COMMAND
        rc, rs = commands.getstatusoutput(cmd)

        return rc, rs

    def swapAthenaProcNumber(self, swap_value):
        """ Swap the current ATHENA_PROC_NUMBER so that it does not upset the job """
        # Note: only needed during TAG file creation

        athena_proc_number = 0
        try:
            athena_proc_number = int(os.environ['ATHENA_PROC_NUMBER'])
        except Exception, e:
            tolog("ATHENA_PROC_NUMBER not defined, setting it to: %s" % (swap_value))
            os.environ['ATHENA_PROC_NUMBER'] = str(swap_value)
        else:
            if swap_value == 0:
                del os.environ['ATHENA_PROC_NUMBER']
                tolog("Unset ATHENA_PROC_NUMBER")
            else:
                os.environ['ATHENA_PROC_NUMBER'] = str(swap_value)
                tolog("ATHENA_PROC_NUMBER swapped from \'%d\' to \'%d\'" % (athena_proc_number, swap_value))

        return athena_proc_number

    def createTAGFile(self, jobExecutionCommand, trfName, inFiles, eventcollection_filename):
        """ Create a TAG file """

        tag_file = ""
        tag_file_guid = getGUID()

        # We cannot have ATHENA_PROC_NUMBER set to a value larger than 1, since that will
        # activate AthenaMP. Reset it for now, and swap it back at the end of this method
        athena_proc_number = self.swapAthenaProcNumber(0)
        
        # Remove everything after the trf command from the job execution command
        cmd = self.stripSetupCommand(jobExecutionCommand, trfName)
        tolog("Stripped command: %s" % (cmd))

        # Define and execute the event collection script
        if cmd != "":
            rc, rs = self.executeMakeRunEventCollectionScript(cmd, eventcollection_filename)
            # Prepend the event collection script
            if rc == 0:
                input_file = inFiles[0]
                tag_file = input_file + ".TAG"
                status, eventcollection_filename_mod = self.prependMakeRunEventCollectionScript(input_file, tag_file, eventcollection_filename)

                # Finally create the TAG file
                if status:
                    rc, rs = self.executeTAGFileCommand(cmd, eventcollection_filename_mod)
                    if rc != 0:
                        tolog("!!WARNING!!3337!! Failed to create TAG file: rc=%d, rs=%s" % (rc, rs))
                        tag_file = ""
            else:
                tolog("!!WARNING!!3339!! Failed to download %s: rc=%d, rs=%s " % (eventcollection_filename, rc, rs))
        else:
            tolog("!!WARNING!!3330!! Failed to strip the job execution command, cannot create TAG file")

        # Now swap the ATHENA_PROC_NUMBER since it is needed for activating AthenaMP
        dummy = self.swapAthenaProcNumber(athena_proc_number)

        return tag_file, tag_file_guid

    # (end event service methods) ................................................................................


# main process starts here
if __name__ == "__main__":

    # Get error handler
    error = PilotErrors()

    # Get runJob object
    runJob = RunJob()

    # Define a new parent group
    os.setpgrp()

    # Protect the runJob code with exception handling
    hP_ret = False
    try:
        # always use this filename as the new jobDef module name
        import newJobDef

        jobSite = Site.Site()

        return_tuple = runJob.argumentParser()
        tolog("argumentParser returned: %s" % str(return_tuple))
        jobSite.setSiteInfo(return_tuple)

#            jobSite.setSiteInfo(argParser(sys.argv[1:]))

        # reassign workdir for this job
        jobSite.workdir = jobSite.wntmpdir

        if runJob.getPilotLogFilename() != "":
            pUtil.setPilotlogFilename(runJob.getPilotLogFilename())

        # set node info
        node = Node.Node()
        node.setNodeName(os.uname()[1])
        node.collectWNInfo(jobSite.workdir)

        # redirect stder
        sys.stderr = open("%s/runjob.stderr" % (jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)
        # get the experiment object
        thisExperiment = getExperiment(runJob.getExperiment())
        tolog("RunJob will serve experiment: %s" % (thisExperiment.getExperiment()))

        # set the cache (used e.g. by LSST)
        if runJob.getCache():
            thisExperiment.setCache(runJob.getCache())

        region = readpar('region')
        JR = JobRecovery()
        try:
            job = Job.Job()
            job.workdir = jobSite.workdir
            job.setJobDef(newJobDef.job)
            job.workdir = jobSite.workdir
            job.experiment = runJob.getExperiment()
            # figure out and set payload file names
            job.setPayloadName(thisExperiment.getPayloadName(job))
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            runJob.failJob(0, error.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag)

        # prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        job.datadir = runJob.getParentWorkDir() + "/PandaJob_%s_data" % (job.jobId)

        # register cleanup function
        atexit.register(runJob.cleanup, job)

        # to trigger an exception so that the SIGTERM signal can trigger cleanup function to run
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
            # print to stderr
            print >> sys.stderr, runJob.getGlobalPilotErrorDiag()
            raise SystemError(sig)

        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGUSR1, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)

        # see if it's an analysis job or not
        analysisJob = isAnalysisJob(job.trf.split(",")[0])
        if analysisJob:
            tolog("User analysis job")
        else:
            tolog("Production job")
        tolog("runJob received a job with prodSourceLabel=%s" % (job.prodSourceLabel))

        # setup starts here ................................................................................

        # update the job state file
        job.jobState = "setup"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # send [especially] the process group back to the pilot
        job.setState([job.jobState, 0, 0])
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = runJob.setup(job, jobSite, thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)
        tolog("Setup has finished successfully")

        # job has been updated, display it again
        job.displayJob()

        # (setup ends here) ................................................................................

        tolog("Setting stage-in state until all input files have been copied")
        job.setState(["stagein", 0, 0])
        # send the special setup string back to the pilot (needed for the log transfer on xrdcp systems)
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # stage-in .........................................................................................

        # update the job state file
        job.jobState = "stagein"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # update copysetup[in] for production jobs if brokerage has decided that remote I/O should be used
        if job.transferType == 'direct':
            tolog('Brokerage has set transfer type to \"%s\" (remote I/O will be attempted for input files, any special access mode will be ignored)' %\
                  (job.transferType))
            RunJobUtilities.updateCopysetups('', transferType=job.transferType)

        # stage-in all input files (if necessary)
        job, ins, statusPFCTurl, usedFAXandDirectIO = runJob.stageIn(job, jobSite, analysisJob)
        if job.result[2] != 0:
            tolog("Failing job with ec: %d" % (ec))
            runJob.failJob(0, job.result[2], job, ins=ins, pilotErrorDiag=job.pilotErrorDiag)

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
        if job.inFiles != ['']:
            runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, runJob.getParentWorkDir(), job.jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO)

        # copy any present @inputFor_* files from the pilot init dir to the rundirectory (used for ES merge jobs)
        #runJob.copyInputForFiles(job.workdir)

        # (stage-in ends here) .............................................................................

        # change to running state since all input files have been staged
        tolog("Changing to running state since all input files have been staged")
        job.setState(["running", 0, 0])
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # update the job state file
        job.jobState = "running"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # run the job(s) ...................................................................................

        # Set ATLAS_CONDDB if necessary, and other env vars
        RunJobUtilities.setEnvVars(jobSite.sitename)

        # execute the payload
        res, job, getstatusoutput_was_interrupted, current_job_number = runJob.executePayload(thisExperiment, runCommandList, job)

        # if payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        # payload error handling
        ed = ErrorDiagnosis()
        job = ed.interpretPayload(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, runJob.getFailureCode())
        if job.result[1] != 0 or job.result[2] != 0:
            runJob.failJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag)

        # stage-out ........................................................................................

        # update the job state file
        job.jobState = "stageout"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # verify and prepare and the output files for transfer
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, job.workdir)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            runJob.failJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # update the current file states
        updateFileStates(outs, runJob.getParentWorkDir(), job.jobId, mode="file_state", state="created")
        dumpFileStates(runJob.getParentWorkDir(), job.jobId)

        # create xml string to pass to dispatcher for atlas jobs
        outputFileInfo = {}
        if outs or (job.logFile and job.logFile != ''):
            # get the datasets for the output files
            dsname, datasetDict = runJob.getDatasets(job)

            # re-create the metadata.xml file, putting guids of ALL output files into it.
            # output files that miss guids from the job itself will get guids in PFCxml function

            # first rename and copy the trf metadata file for non-build jobs
            if not pUtil.isBuildJob(outs):
                runJob.moveTrfMetadata(job.workdir, job.jobId)

            # create the metadata for the output + log files
            ec, job, outputFileInfo = runJob.createFileMetadata(list(outs), job, outsDict, dsname, datasetDict, jobSite.sitename, analysisJob=analysisJob)
            if ec:
                runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)

        # move output files from workdir to local DDM area
        finalUpdateDone = False
        if outs:
            tolog("Setting stage-out state until all output files have been copied")
            job.setState(["stageout", 0, 0])
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

            # stage-out output files
            ec, job, rf, latereg = runJob.stageOut(job, jobSite, outs, analysisJob, dsname, datasetDict, outputFileInfo)
            # error handling
            if job.result[0] == "finished" or ec == error.ERR_PUTFUNCNOCALL:
                rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)
            else:
                rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True, latereg=latereg)
            if ec == error.ERR_NOSTORAGE:
                # update the current file states for all files since nothing could be transferred
                updateFileStates(outs, runJob.getParentWorkDir(), job.jobId, mode="file_state", state="not_transferred")
                dumpFileStates(runJob.getParentWorkDir(), job.jobId)

            finalUpdateDone = True
            if ec != 0:
                runJob.sysExit(job, rf)
            # (stage-out ends here) .......................................................................

        job.setState(["finished", 0, 0])
        if not finalUpdateDone:
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)
        runJob.sysExit(job)

    except Exception, errorMsg:

        error = PilotErrors()

        if runJob.getGlobalPilotErrorDiag() != "":
            pilotErrorDiag = "Exception caught in runJob: %s" % (runJob.getGlobalPilotErrorDiag())
        else:
            pilotErrorDiag = "Exception caught in runJob: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()    

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (e, pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in runJob: %s" % (e)
                tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))

#        # restore the proxy if necessary
#        if hP_ret:
#            rP_ret = proxyguard.restoreProxy()
#            if not rP_ret:
#                tolog("Warning: Problems with storage can occur since proxy could not be restored")
#            else:
#                hP_ret = False
#                tolog("ProxyGuard has finished successfully")

        tolog("sys.path=%s" % str(sys.path))
        cmd = "pwd;ls -lF %s;ls -lF;ls -lF .." % (runJob.getPilotInitDir())
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog("%s" % (out))

        job = Job.Job()
        job.setJobDef(newJobDef.job)
        job.pilotErrorDiag = pilotErrorDiag
        job.result[0] = "failed"
        if runJob.getGlobalErrorCode() != 0:
            job.result[2] = runJob.getGlobalErrorCode()
        else:
            job.result[2] = error.ERR_RUNJOBEXC
        tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag, docleanup=False)

    # end of runJob

    
    
