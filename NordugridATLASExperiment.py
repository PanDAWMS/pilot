# Class definition:
#   NordugridATLASExperiment
#   This class is the ATLAS experiment class for Nordugrid inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()

# import relevant python/pilot modules
from Experiment import Experiment      # Main experiment class
from pUtil import tolog                # Logging method that sends text to the pilot log
from pUtil import readpar              # Used to read values from the schedconfig DB (queuedata)
from pUtil import isAnalysisJob        # Is the current job a user analysis job or a production job?
from pUtil import verifyReleaseString  # To verify the release string (move to Experiment later)
from PilotErrors import PilotErrors    # Error codes
from ATLASExperiment import ATLASExperiment

# Standard python modules
import os
import re
import commands
from glob import glob

class NordugridATLASExperiment(ATLASExperiment):

    # private data members
    __experiment = "Nordugrid-ATLAS"
    __instance = None
    __warning = ""
    __analysisJob = False
    __job = None

    # Required methods

    def __init__(self):
        """ Default initialization """
# not needed?
        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ATLASExperiment, cls).__new__(cls, *args, **kwargs)

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
        else:
            self.__warning = "setParameters found no job object"

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
        #   cmtconfig              : cmtconfig symbol from the job def or schedconfig, e.g. "x86_64-slc5-gcc43-opt" [NOT USED IN THIS CLASS]

        pilotErrorDiag = ""
        cmd = ""
        special_setup_cmd = ""
        pysiteroot = ""
        siteroot = ""
        JEM = "NO"
        cmtconfig = ""

        # Is it's an analysis job or not?
        analysisJob = isAnalysisJob(job.trf)

        # Set the INDS env variable (used by runAthena)
        if analysisJob:
            self.setINDS(job.realDatasetsIn)

        # Command used to download runAthena or runGen
        wgetCommand = 'wget'

        # special setup for NG
        status, pilotErrorDiag, cmd = self.setupNordugridTrf(job, analysisJob, wgetCommand, pilot_initdir)
        if status != 0:
            return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

        # add FRONTIER debugging and RUCIO env variables
        cmd = self.addEnvVars2Cmd(cmd, job.jobId, job.processingType, jobSite.sitename, analysisJob)

        if readpar('cloud') == "DE":
            # Should JEM be used?
            metaOut = {}
            try:
                import sys
                from JEMstub import updateRunCommand4JEM
                # If JEM should be used, the command will get updated by the JEMstub automatically.
                cmd = updateRunCommand4JEM(cmd, job, jobSite, tolog, metaOut=metaOut)
            except:
                # On failure, cmd stays the same
                tolog("Failed to update run command for JEM - will run unmonitored.")

            # Is JEM to be used?
            if metaOut.has_key("JEMactive"):
                JEM = metaOut["JEMactive"]

            tolog("Use JEM: %s (dictionary = %s)" % (JEM, str(metaOut)))

        elif '--enable-jem' in cmd:
            tolog("!!WARNING!!1111!! JEM can currently only be used on certain sites in DE")

        # Pipe stdout/err for payload to files
        cmd += " 1>%s 2>%s" % (job.stdout, job.stderr)
        tolog("\nCommand to run the job is: \n%s" % (cmd))

        tolog("ATLAS_PYTHON_PILOT = %s" % (os.environ['ATLAS_PYTHON_PILOT']))

        if special_setup_cmd != "":
            tolog("Special setup command: %s" % (special_setup_cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return False

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    # Additional optional methods

    def setupNordugridTrf(self, job, analysisJob, wgetCommand, pilot_initdir):
        """ perform the Nordugrid trf setup """

        error = PilotErrors()
        pilotErrorDiag = ""
        cmd = ""

        # assume that the runtime script has already been created
        if not os.environ.has_key('RUNTIME_CONFIG_DIR'):
            pilotErrorDiag = "Environment variable not set: RUNTIME_CONFIG_DIR"
            tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
            return error.ERR_SETUPFAILURE, pilotErrorDiag, ""

        runtime_script = "%s/APPS/HEP/ATLAS-%s" % (os.environ['RUNTIME_CONFIG_DIR'], job.release)
        if os.path.exists(runtime_script):
            cmd = ". %s 1" % (runtime_script)
            if analysisJob:
                # try to download the analysis trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, ""
                trfName = "./" + trfName
            else:
                trfName = job.trf
                cmd += '; export ATLAS_RELEASE=%s;export AtlasVersion=%s;export AtlasPatchVersion=%s' % (job.homePackage.split('/')[-1],job.homePackage.split('/')[-1],job.homePackage.split('/')[-1])
            cmd += "; %s %s" % (trfName, job.jobPars)
        elif verifyReleaseString(job.release) == "NULL":
            if analysisJob:
                # try to download the analysis trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, ""
                trfName = "./" + trfName
            else:
                trfName = job.trf
            cmd = "%s %s" % (trfName, job.jobPars)        
        else:
            pilotErrorDiag = "Could not locate runtime script: %s" % (runtime_script)
            tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
            return error.ERR_SETUPFAILURE, pilotErrorDiag, ""

        return 0, pilotErrorDiag, cmd

    def getWarning(self):
        """ Return any warning message passed to __warning """

        return self.__warning

    def testImportLFCModule(self):
	return True

    def getRelease(self, release):
        """ Return a list of the software release id's """
        # Assuming 'release' is a string that separates release id's with '\n'
        # Used in the case of payload using multiple steps with different release versions
        # E.g. release = "19.0.0\n19.1.0" -> ['19.0.0', '19.1.0']

        if readpar('region') == 'Nordugrid':
            return os.environ['ATLAS_RELEASE'].split(",")
        else:
            return release.split("\n")

if __name__ == "__main__":

    print "Implement test cases here"
    
