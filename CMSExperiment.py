# Class definition:
#   CMSExperiment
#   This class is the prototype of an experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from Experiment import Experiment          # Main experiment class
from PilotErrors import PilotErrors        # Error codes
from pUtil import tolog                    # Logging method that sends text to the pilot log
from pUtil import readpar                  # Used to read values from the schedconfig DB (queuedata)
#from pUtil import isAnalysisJob            # Is the current job a user analysis job or a production job?
from pUtil import getCmtconfig             # Get the cmtconfig from the job def or queuedata
from pUtil import verifyReleaseString      # To verify the release string (move to Experiment later)
#from pUtil import setPilotPythonVersion    # Which python version is used by the pilot
from pUtil import getSiteInformation       # Get the SiteInformation object corresponding to the given experiment
from pUtil import isBuildJob               # Is the current job a build job?
from pUtil import remove
from RunJobUtilities import getStdoutFilename   #

# Standard python modules
import os
import commands
import shlex
import getopt
from glob import glob

from optparse import (OptionParser,BadOptionError)

class PassThroughOptionParser(OptionParser):
    """
    An unknown option pass-through implementation of OptionParser.

    When unknown arguments are encountered, bundle with largs and try again,
    until rargs is depleted.  

    sys.exit(status) will still be called if a known argument is passed
    incorrectly (e.g. missing arguments or bad argument types, etc.)        
    """
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                OptionParser._process_args(self,largs,rargs,values)
            except (BadOptionError, Exception), e:
                #largs.append(e.opt_str)
                continue


class CMSExperiment(Experiment):

    # private data members
    __experiment = "CMS"                   # String defining the experiment
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(CMSExperiment, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def setParameters(self, *args, **kwargs):
        """ Set any internally needed variables """

        # set initial values
        self.__job = kwargs.get('job', None)
        if self.__job:
            self.__analysisJob = isAnalysisJob(self.__job.trf)

    def setPython(self):
        """ set the python executable """

        ec = 0
        pilotErrorDiag = ""
        pybin = ""

        python_list = ['python', 'python32', 'python2']
        pybin = python_list[0]
        for _python in python_list:
            _pybin = commands.getoutput('which %s' % (_python))
            if _pybin.startswith('/'):
                # found python executable
                pybin = _pybin
                break

        tolog("Using %s" % (pybin))
        return ec, pilotErrorDiag, pybin

    def extractJobPar(self, job, par, ptype='string'):

        strpars = job.jobPars
        cmdopt = shlex.split(strpars)
        parser = PassThroughOptionParser()
        parser.add_option(par,\
                          dest='par',\
                          type=ptype)
        (options,args) = parser.parse_args(cmdopt)
        return options.par 


    def getCMSRunCommand(self, job, jobSite, trfName):

        from RunJobUtilities import updateCopysetups

        ec = 0
        pilotErrorDiag = ""
        run_command = ""

        # get relevant file transfer info
        dInfo, useCopyTool, useDirectAccess, useFileStager, oldPrefix, newPrefix, copysetup, usePFCTurl, lfcHost =\
               self.getFileTransferInfo(job.transferType, isBuildJob(job.outFiles))

        # extract the setup file from copysetup (and verify that it exists)
        _copysetup = self.getSetupFromCopysetup(copysetup)
        tolog("copysetup = %s" % _copysetup)
        if _copysetup != "" and os.path.exists(_copysetup):
            run_command = 'source %s; ' % (_copysetup)

        # add the user proxy
        if os.environ.has_key('X509_USER_PROXY'):
            run_command += 'export X509_USER_PROXY=%s; ' % os.environ['X509_USER_PROXY']
        else:
            tolog("Could not add user proxy to the run command (proxy does not exist)")

        """
        strpars = job.jobPars
        cmdopt = shlex.split(strpars)
        parser = PassThroughOptionParser()
        parser.add_option('-a',\
                          dest='a',\
                          type='string')
        parser.add_option('-o',\
                          dest='o',\
                          type='string')
        parser.add_option('--inputFile',\
                          dest='inputFile',\
                          type='string')
        parser.add_option('--sourceURL',\
                          dest='sourceURL',\
                          type='string')
        parser.add_option('--jobNumber',\
                          dest='jobNumber',\
                          type='string')
        parser.add_option('--cmsswVersion',\
                          dest='cmsswVersion',\
                          type='string')
        parser.add_option('--scramArch',\
                          dest='scramArch',\
                          type='string')
        parser.add_option('--runAndLumis',\
                          dest='runAndLumis',\
                          type='string')
        (options,args) = parser.parse_args(cmdopt)

        paramsstring  = '-a %s '                % options.a
        paramsstring += '--sourceURL %s '       % options.sourceURL
        paramsstring += '--jobNumber=%s '       % options.jobNumber
        paramsstring += '--cmsswVersion=%s '    % options.cmsswVersion
        paramsstring += '--scramArch=%s '       % options.scramArch
        paramsstring += "--inputFile='%s' "     % options.inputFile
        paramsstring += "--runAndLumis='%s' "   % options.runAndLumis
        paramsstring += '-o "%s" '              % options.o

        tolog("paramsstring = %s" % paramsstring)
        """
        run_command += './%s %s' % (trfName, job.jobPars)


        return ec, pilotErrorDiag, run_command


    def isAnalysisJob(self, trf):
        """ Always true for the moment"""
        return True

    def isCMSRunJob(self, trf):
        if "CMSRunAnaly" in trf or "CMSRunMCProd" in trf:
            return True
        else:
            return False


    def getJobExecutionCommand(self, job, jobSite, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload """

        # Input tuple: (method is called from RunJob*)
        #   job: Job object
        #   jobSite: Site object
        #   pilot_initdir: launch directory of pilot.py
        #
        # Return tuple:
        #   pilot_error_code, pilot_error_diagnostics, job_execution_command, special_setup_command, JEM, cmtconfig
        # where
        #   pilot_error_code       : self.__error.<PILOT ERROR CODE as defined in PilotErrors class> (value should be 0 for successful setup)
        #   pilot_error_diagnostics: any output from problematic command or explanatory error diagnostics
        #   job_execution_command  : command to execute payload, e.g. cmd = "source <path>/setup.sh; <path>/python trf.py [options]"
        #   special_setup_command  : any special setup command that can be insterted into job_execution_command and is sent to stage-in/out methods
        #   JEM                    : Job Execution Monitor activation state (default value "NO", meaning JEM is not to be used. See JEMstub.py)
        #   cmtconfig              : cmtconfig symbol from the job def or schedconfig, e.g. "x86_64-slc5-gcc43-opt"

        pilotErrorDiag = ""
        cmd = ""
        JEM = "NO"

        # Is it's an analysis job or not?
        isCMSRunJob = self.isCMSRunJob(job.trf)
        tolog("isCMSRunJob = %s " % isCMSRunJob)

        # Command used to download trf
        wgetCommand = 'wget'

        # Get the cmtconfig value
        cmtconfig = getCmtconfig(job.cmtconfig)
        if cmtconfig != "":
            tolog("cmtconfig: %s" % (cmtconfig))

        # Set python executable
        ec, pilotErrorDiag, pybin = self.setPython()
        if ec == self.__error.ERR_MISSINGINSTALLATION:
            return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

        # Define the job execution command
        if isCMSRunJob:
            # Try to download the analysis trf
            status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
            if status != 0:
                return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            scramArchSetup = self.getScramArchSetupCommand(job)
            ec, pilotErrorDiag, cmdtrf = self.getCMSRunCommand(job, jobSite, trfName)
            cmd = "%s %s" % (scramArchSetup, cmdtrf)

        # Set special_setup_cmd if necessary
        special_setup_cmd = self.getSpecialSetupCommand()
        if special_setup_cmd != "":
            tolog("Special setup command: %s" % (special_setup_cmd))

        # Pipe stdout/err for payload to files
        cmd += " 1>%s 2>%s" % (job.stdout, job.stderr)
        tolog("\nCommand to run the job is: \n%s" % (cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig

    def getScramArchSetupCommand(self, job):
        """ Looks for the scramArch option in the job.jobPars attribute and build 
            the command to export the SCRAMARCH env variable with the correct value """

        scramArch = self.extractJobPar(job, '--scramArch')
        if scramArch != None:
            return "export SCRAM_ARCH=%s;" % scramArch
        return ""


    def getSpecialSetupCommand(self):
        """ Set special_setup_cmd if necessary """

        # Note: this special setup command is hardly used and could probably be removed
        # in case any special setup should be added to the setup string before the trf is executed, the command defined in this method
        # could be added to the run command by using method addSPSetupToCmd().
        # the special command is also forwarded to the get and put functions (currently not used)

        special_setup_cmd = ""

        # add envsetup to the special command setup on tier-3 sites
        # (unknown if this is still needed)

        si = getSiteInformation(self.__experiment)
        if si.isTier3():
            _envsetup = readpar('envsetup')
            if _envsetup != "":
                special_setup_cmd += _envsetup
                if not special_setup_cmd.endswith(';'):
                    special_setup_cmd += ";"

        return special_setup_cmd

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return False

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    def isOutOfMemory(self, **kwargs):
        """ Try to identify out of memory errors in the stderr/out """

        return False

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """

        return 0

    def specialChecks(self):
        """ Implement special checks here """
        # Return False if fatal failure, otherwise return True
        # The pilot will abort if this method returns a False

        status = False

        #tolog("No special checks for \'%s\'" % (self.__experiment))
        # set the python version used by the pilot
        # Is this really necessary for CMS? Function below moved to ATLASExperiment.
        #setPilotPythonVersion()

        tolog("SetPilotPython version:  special checks for \'%s\'" % (self.__experiment))


        return True # obviously change this to 'status' once implemented

    def getPayloadName(self, job):
        """ figure out a suitable name for the payload  """
        payloadname = "cmssw"
        return payloadname

    def verifySwbase(self, appdir):
        """ Called by pilot.py, check needed for handleQueuedata method """
        tolog("CMSExperiment - verifySwbase - nothing to do")

        return 0

    def checkSpecialEnvVars(self, sitename):
        """ Called by pilot.py """
        tolog("CMSExperiment - checkSpecialEnvVars - nothing to do")

        return 0

    def extractAppdir(self):
        """ Called by pilot.py, runMain method """
        tolog("CMSExperiment - extractAppdir - nothing to do")

        return 0, ""
   

    def getMetadataForRegistration(self, guid):
        # Return metadata (not known yet) for server LFC registration
        # use the GUID as identifier (the string "<GUID>-surltobeset" will later be replaced with the SURL)        
        xmlstring = ''
        xmlstring += '    <metadata att_name="surl" att_value="%s-surltobeset"/>\n' % (guid) 
        xmlstring += '    <metadata att_name="full_lfn" att_value="%s-surltobeset"/>\n' % (guid)

        return xmlstring

    def getAttrForRegistration(self):
        # Return the attribute of the PFCxml to be updated with surl value
        
        attr = 'full_lfn'

        return attr        


    def getExpSpecificMetadata(self, job, workdir):
        """ Return metadata extracted from jobReport.json"""

        fwjrMetadata = ''
        fwjrFile = os.path.join(workdir, "jobReport.json")
        tolog("Looking for jobReport file")
        if os.path.exists(fwjrFile):
            tolog("Found jobReport: %s" % fwjrFile)
            try:
                f = open(fwjrFile, 'r')
                for line in f.readlines():
                    fwjrMetadata += line
            except Exception, e:
                tolog("Failed to open jobReport file: %s" % str(e))
        else:
            tolog("jobReport not found in %s " % fwjrFile)

        return fwjrMetadata

    def handleTrfExitcode(self, job, res, error, filename):
        transExitCode = res[0]
        #Mancinelli: TODO map CMS transformation error codes with error messages
        if transExitCode:
            # Handle PandaMover errors
            # Mancinelli: do we need this?
            if transExitCode == 176:
                job.pilotErrorDiag = "PandaMover staging error: File is not cached"
                job.result[2] = error.ERR_PANDAMOVERFILENOTCACHED
            elif transExitCode == 86:
                job.pilotErrorDiag = "PandaMover transfer failure"
                job.result[2] = error.ERR_PANDAMOVERTRANSFER
            else:
                # check for specific errors in stdout
                # Mancinelli: do we have to check stdout?
                if os.path.exists(filename):
                    """
                    e1 = "prepare 5 database is locked"
                    e2 = "Error SQLiteStatement"
                    _out = commands.getoutput('grep "%s" %s | grep "%s"' % (e1, filename, e2))
                    if 'sqlite' in _out:
                        job.pilotErrorDiag = "NFS/SQLite locking problems: %s" % (_out)
                        job.result[2] = error.ERR_NFSSQLITE
                    else:
                        job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d" % (transExitCode)
                        # (do not set a pilot error code)
                    """
                    job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d" % (transExitCode)
                    # (do not set a pilot error code)
                else:
                    job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d (%s does not exist)" % (transExitCode, filename)
                    # (do not set a pilot error code)
        # set the trf diag error
        if res[2] != "":
            tolog("TRF diagnostics: %s" % (res[2]))
            job.exeErrorDiag = res[2]

        job.result[1] = transExitCode
        return job


    def removeRedundantFiles(self, workdir):
        """ Remove redundant files and directories """

        dir_list = [#"AtlasProduction*",
                    "*.py",
                    "*.pyc",
                    # Mancinelli
                    "pandaJobData.out",
                    "Pilot_VmPeak.txt",
                    "pandatracerlog.txt",
                    "pandawnutil",
                    "pilotlog.out",
                    "pilot.stderr",
                    "CMSRunAnaly.sh",
                    "*.tgz",
                    "PSetTweaks",
                    "WMCore.zip",
                    "lib",
                    "CMSSW_*",
                    "WMTaskSpace",
                    "process.id",
                    "cmsRun-main.sh",
                    #"PSet.pkl",
                    "jobState-*-test.pickle",
                    "ALLFILESTRANSFERRED",
                    "OutPutFileCatalog.xml",
                    "jobReportExtract.pickle",
                    "metadata-*.xml",
                    "PoolFileCatalog.xml"]

        for _dir in dir_list:
            files = glob(os.path.join(workdir, _dir))
            tolog("Mancinellidebug: removing files = %s" % files)
            rc = remove(files)
            if not rc:
                tolog("IGNORE: Failed to remove redundant file(s): %s" % (files))

        tolog("Mancinellidebug: content of workdir = %s" % os.listdir(workdir))

    def interpretPayloadStdout(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode):
        """ payload error handling """

        # NOTE: Move away ATLAS specific info in this method, e.g. vmPeak stuff

        error = PilotErrors()
        #Mancinelli: moved it in experiment class method handleTrfExitcode
        #transExitCode = res[0]%255
        tolog("Mancinellidebug: res = %s res[0] = %s" % (res, res[0]))

        # Get the proper stdout filename
        number_of_jobs = len(runCommandList)
        filename = getStdoutFilename(job.workdir, job.stdout, current_job_number, number_of_jobs)

        # Try to identify out of memory errors in the stderr
        out_of_memory = self.isOutOfMemory(job=job, number_of_jobs=number_of_jobs)
        failed = out_of_memory # failed boolean used below

        # A killed job can have empty output but still transExitCode == 0
        no_payload_output = False
        installation_error = False
        if getstatusoutput_was_interrupted:
            if os.path.exists(filename):
                if os.path.getsize(filename) > 0:
                    tolog("Payload produced stdout but was interrupted (getstatusoutput threw an exception)")
                else:
                    no_payload_output = True
                failed = True
            else:
                failed = True
                no_payload_output = True
        elif len(res[1]) < 20: # protect the following comparison against massive outputs
            if res[1] == 'Undefined':
                failed = True
                no_payload_output = True
        elif failureCode:
            failed = True
        else:
            # check for installation error
            res_tmp = res[1][:1024]
            if res_tmp[0:3] == "sh:" and 'setup.sh' in res_tmp and 'No such file or directory' in res_tmp:
                failed = True
                installation_error = True

        if res[0] or failed:
            #Mancinelli: all this common part with CMS?
            if failureCode:
                job.pilotErrorDiag = "Payload failed: Interrupt failure code: %d" % (failureCode)
                # (do not set pilot error code)
            elif getstatusoutput_was_interrupted:
                raise Exception, "Job execution was interrupted (see stderr)"
            elif out_of_memory:
                job.pilotErrorDiag = "Payload ran out of memory"
                job.result[2] = error.ERR_ATHENAOUTOFMEMORY
            elif no_payload_output:
                job.pilotErrorDiag = "Payload failed: No output"
                job.result[2] = error.ERR_NOPAYLOADOUTPUT
            elif installation_error:
                job.pilotErrorDiag = "Payload failed: Missing installation"
                job.result[2] = error.ERR_MISSINGINSTALLATION
            elif res[0]:
                #Mancinelli: calling for experiment class method to manage transformation exit code
                job = self.handleTrfExitcode(job, res, error, filename)
            else:
                job.pilotErrorDiag = "Payload failed due to unknown reason (check payload stdout)"
                job.result[2] = error.ERR_UNKNOWN
            tolog("!!FAILED!!3000!! %s" % (job.pilotErrorDiag))

        # handle non-zero failed job return code but do not set pilot error codes to all payload errors
        """
        if transExitCode or failed:
            if failureCode:
                job.pilotErrorDiag = "Payload failed: Interrupt failure code: %d" % (failureCode)
                # (do not set pilot error code)
            elif getstatusoutput_was_interrupted:
                raise Exception, "Job execution was interrupted (see stderr)"
            elif out_of_memory:
                job.pilotErrorDiag = "Payload ran out of memory"
                job.result[2] = error.ERR_ATHENAOUTOFMEMORY
            elif no_payload_output:
                job.pilotErrorDiag = "Payload failed: No output"
                job.result[2] = error.ERR_NOPAYLOADOUTPUT
            elif installation_error:
                job.pilotErrorDiag = "Payload failed: Missing installation"
                job.result[2] = error.ERR_MISSINGINSTALLATION
            elif transExitCode:
                # Handle PandaMover errors
                if transExitCode == 176:
                    job.pilotErrorDiag = "PandaMover staging error: File is not cached"
                    job.result[2] = error.ERR_PANDAMOVERFILENOTCACHED
                elif transExitCode == 86:
                    job.pilotErrorDiag = "PandaMover transfer failure"
                    job.result[2] = error.ERR_PANDAMOVERTRANSFER
                else:
                    # check for specific errors in athena stdout
                    if os.path.exists(filename):
                        e1 = "prepare 5 database is locked"
                        e2 = "Error SQLiteStatement"
                        _out = commands.getoutput('grep "%s" %s | grep "%s"' % (e1, filename, e2))
                        if 'sqlite' in _out:
                            job.pilotErrorDiag = "NFS/SQLite locking problems: %s" % (_out)
                            job.result[2] = error.ERR_NFSSQLITE
                        else:
                            job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d" % (transExitCode)
                            # (do not set a pilot error code)
                    else:
                        job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d (%s does not exist)" % (transExitCode, filename)
                        # (do not set a pilot error code)
            else:
                job.pilotErrorDiag = "Payload failed due to unknown reason (check payload stdout)"
                job.result[2] = error.ERR_UNKNOWN
            tolog("!!FAILED!!3000!! %s" % (job.pilotErrorDiag))

        # set the trf diag error
        if res[2] != "":
            tolog("TRF diagnostics: %s" % (res[2]))
            job.exeErrorDiag = res[2]

        job.result[1] = transExitCode
        """
        return job

    # Optional
    # Optional
    def useTracingService(self):
        """ Use the DQ2 Tracing Service """
        # A service provided by the DQ2 system that allows for file transfer tracking; all file transfers
        # are reported by the pilot to the DQ2 Tracing Service if this method returns True

        return False


if __name__ == "__main__":

    print "Implement test cases here"
    
