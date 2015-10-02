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
from pUtil import timedCommand         # Standard time-out function
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

        if os.environ.has_key('Nordugrid_pilot') and os.environ.has_key('ATLAS_RELEASE'):
            return os.environ['ATLAS_RELEASE'].split(",")
        else:
            return release.split("\n")

    def checkSpecialEnvVars(self, sitename):
        """ Check special environment variables """

        # Set a special env variable that will be used to identify Nordugrid in other pilot classes
        os.environ['Nordugrid_pilot'] = ""

        # Call the method from the parent class
        ec = super(NordugridATLASExperiment, self).checkSpecialEnvVars(sitename)

        return ec

    # Optional
    def shouldExecuteUtility(self):
        """ Determine where a memory utility monitor should be executed """ 

        # The RunJob class has the possibility to execute a memory utility monitor that can track the memory usage
        # of the payload. The monitor is executed if this method returns True. The monitor is expected to produce
        # a summary JSON file whose name is defined by the getMemoryMonitorJSONFilename() method. The contents of
        # this file (ie. the full JSON dictionary) will be added to the jobMetrics at the end of the job (see
        # PandaServerClient class).

        return True

    # Optional
    def getUtilityJSONFilename(self):
        """ Return the filename of the memory monitor JSON file """

        # For explanation, see shouldExecuteUtility()
        return "memory_monitor_summary.json"

    def getSetupPath(self, job_command, trf):
        """ Get the setup path from the job execution command """

        setup = ""

        # Trim the trf if necessary (i.e. remove any paths which are present in buildJob jobs)
        trf = self.trimTrfName(trf)

        # Take care of special cases, e.g. trf="buildJob-.." but job_command="..; ./buildJob-.."
        special_case = "./%s" % (trf)
        if special_case in job_command:
            trf = special_case

        # Strip the setup command at the location of the trf name
        l = job_command.find(trf)
        if l > 0:
            setup = job_command[:l]

        # Make sure to remove any unwanted white spaces as well
        return setup.strip()

    def trimTrfName(self, trfName):
        """ Remove any unwanted strings from the trfName """

        if "/" in trfName:
            trfName = os.path.basename(trfName)

        return trfName

    def updateSetupPathWithReleaseAndCmtconfig(self, setup_path, release, alt_release, patched_release, alt_patched_release, cmtconfig, alt_cmtconfig):
        """ Update the setup path with an alternative release, pathched release and cmtconfig """

        # This method can be used to modify a setup path with an alternative release, patched release and cmtconfig
        # E.g. this can be used by a tool that might want to fall back to a preferred setup

        # Correct the release info
        if "-" in release: # the cmtconfig is appended, e.g. release='17.2.7-X86_64-SLC5-GCC43-OPT'
            cmtconfig = release[release.find('-')+1:]
            release = release[:release.find('-')]

        # Update the patched release with a tmp string
        if patched_release != "" and patched_release in setup_path:
            setup_path = setup_path.replace(patched_release, '__PATCHED_RELEASE__')

        # Update the release
        if release in setup_path:
            setup_path = setup_path.replace(release, alt_release)

        # Update the patched release
        if '__PATCHED_RELEASE__' in setup_path:
            setup_path = setup_path.replace('__PATCHED_RELEASE__', alt_patched_release)

        # Update the cmtconfig
        if cmtconfig != "" and cmtconfig in setup_path:
            setup_path = setup_path.replace(cmtconfig, alt_cmtconfig.upper())

        return setup_path

    # Optional
    def NOTUSEDgetUtilityCommand(self, **argdict):
        """ Prepare a utility command string """

        # This method can be used to prepare a setup string for an optional utility tool, e.g. a memory monitor,
        # that will be executed by the pilot in parallel with the payload.
        # The pilot will look for an output JSON file (summary.json) and will extract pre-determined fields
        # from it and report them with the job updates. Currently the pilot expects to find fields related
        # to memory information.

        pid = argdict.get('pid', 0)
        release = argdict.get('release', '')
        homePackage = argdict.get('homePackage', '')
        cmtconfig = argdict.get('cmtconfig', '')
        summary = self.getUtilityJSONFilename()
        job_command = argdict.get('job_command', '')
        trf = argdict.get('trf', 0)
        interval = 60

        # Get the setup path for the job command (everything up until the trf name)
        setup_path = self.getSetupPath(job_command, trf)

        default_release = "20.1.5"
        default_patch_release = "20.1.5.2" #"20.1.4.1"
        default_cmtconfig = "x86_64-slc6-gcc48-opt"
        default_swbase = "%s/atlas.cern.ch/repo/sw/software" % (self.getCVMFSPath())

        cacheVer = homePackage.split('/')[-1]

        # could anything be extracted?
        if homePackage == cacheVer or cmtconfig == "": # (no)
            # This means there is no patched release available, ie. we need to use the fallback
            useDefault = True
            cacheVer = ""
        else:
            useDefault = False

        tolog("setup_path=%s"%setup_path)
        tolog("release=%s"%release)
        tolog("default_release=%s"%default_release)
        tolog("cacheVer=%s"%cacheVer)
        tolog("default_patch_release=%s"%default_patch_release)
        tolog("cmtconfig=%s"%cmtconfig)
        tolog("default_cmtconfig=%s"%default_cmtconfig)
        default_setup = self.updateSetupPathWithReleaseAndCmtconfig(setup_path, release, default_release, cacheVer, default_patch_release, cmtconfig, default_cmtconfig)
        tolog("default_setup=%s"%default_setup)
        # Construct the name of the output file using the summary variable
        if summary.endswith('.json'):
            output = summary.replace('.json', '.txt')
        else:
            output = summary + '.txt'

        if useDefault:
            tolog("Will use default (fallback) setup for MemoryMonitor since patched release number is needed for the setup, and none is available")
            cmd = default_setup
        else:
            # Get the standard setup
            standard_setup = setup_path
            _cmd = standard_setup + " which MemoryMonitor"

            # Can the MemoryMonitor be found?
            try:
                ec, output = timedCommand(_cmd, timeout=60)
            except Exception, e:
                tolog("!!WARNING!!3434!! Failed to locate MemoryMonitor: will use default (for patch release %s): %s" % (default_patch_release, e))
                cmd = default_setup
            else:
                if "which: no MemoryMonitor in" in output:
                    tolog("Failed to locate MemoryMonitor: will use default (for patch release %s)" % (default_patch_release))
                    cmd = default_setup
                else:
                    # Standard setup passed the test
                    cmd = standard_setup

        # Now add the MemoryMonitor command
        cmd += "MemoryMonitor --pid %d --filename %s --json-summary %s --interval %d" % (pid, "memory_monitor_output.txt", summary, interval)

        return cmd

if __name__ == "__main__":

    print "Implement test cases here"
    
