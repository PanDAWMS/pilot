# Class definition:
#   LSSTExperiment
#   This class is the prototype of an experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
from Experiment import Experiment  # Main experiment class
from pUtil import tolog            # Logging method that sends text to the pilot log
from pUtil import readpar          # Used to read values from the schedconfig DB (queuedata)
from pUtil import isAnalysisJob  # Is the current job a user analysis job or a production job?
from pUtil import getCmtconfig  # Get the cmtconfig from the job def or queuedata
from pUtil import getCmtconfigAlternatives  # Get a list of locally available cmtconfigs
from pUtil import getSwbase  # To build path for software directory, e.g. using schedconfig.appdir (move to subclass)
from pUtil import getSiteInformation  # Get the SiteInformation object corresponding to the given experiment
from pUtil import verifyReleaseString  # To verify the release string (move to Experiment later)
from pUtil import verifySetupCommand  # Verify that a setup file exists

class LSSTExperiment(Experiment):

    # private data members
    __experiment = "LSST"
    __instance = None
    __experiment_sw_dir_var = "VO_%s_SW_DIR" % (__experiment)
    __experiment_python_pilot_var = "%s_PYTHON_PILOT" % (__experiment)

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(LSSTExperiment, cls).__new__(cls, *args, **kwargs)

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

#    def getJobExecutionCommand(self):
#        """ Define and test the command(s) that will be used to execute the payload """
#        # E.g. cmd = "source <path>/setup.sh; <path>/python "
#
#        cmd = ""
#
#        return cmd

    def getJobExecutionCommand(self, job, jobSite, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload """

        # Method is called from runJob

        # Input tuple: (method is called from runJob)
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
        special_setup_cmd = ""
        pysiteroot = ""
        siteroot = ""
        JEM = "NO"

        # Is it's an analysis job or not?
        analysisJob = isAnalysisJob(job.trf)

        # Set the INDS env variable (used by runAthena)
        if analysisJob:
            self.setINDS(job.realDatasetsIn)

        # Command used to download runAthena or runGen
        wgetCommand = 'wget'

        # Get the cmtconfig value
        cmtconfig = getCmtconfig(job.cmtconfig)

        # Get the local path for the software
        swbase = getSwbase(jobSite.appdir, job.atlasRelease, job.homePackage, job.processingType, cmtconfig)
        tolog("Local software path: swbase = %s" % (swbase))

        # Get cmtconfig alternatives
        cmtconfig_alternatives = getCmtconfigAlternatives(cmtconfig, swbase)
        tolog("Found alternatives to cmtconfig: %s (the first item is the default cmtconfig value)" % str(cmtconfig_alternatives))


#        LSST job is always non-ATLAS job, let's do the generic job part directly.
        if True:
            tolog("Generic job")

            # Set python executable (after SITEROOT has been set)
            if siteroot == "":
                try:
                    siteroot = os.environ['SITEROOT']
                except:
                    tolog("Warning: $SITEROOT unknown at this stage (3)")

            tolog("Will use $SITEROOT: %s (3)" % (siteroot))
            ec, pilotErrorDiag, pybin = self.setPython(siteroot, job.atlasRelease, job.homePackage, cmtconfig, jobSite.sitename)
            if ec == self.__error.ERR_MISSINGINSTALLATION:
                return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            if analysisJob:
                # Try to download the analysis trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Set up the run command
                if job.prodSourceLabel == 'ddm' or job.prodSourceLabel == 'software':
                    cmd = '%s %s %s' % (pybin, trfName, job.jobPars)
                else:
                    ec, pilotErrorDiag, cmd = self.getAnalysisRunCommand(job, jobSite, trfName)
                    if ec != 0:
                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            elif verifyReleaseString(job.homePackage) != 'NULL' and job.homePackage != ' ':
                cmd = "%s %s/%s %s" % (pybin, job.homePackage, job.trf, job.jobPars)
            else:
                cmd = "%s %s %s" % (pybin, job.trf, job.jobPars)

            # Set special_setup_cmd if necessary
            special_setup_cmd = self.getSpecialSetupCommand()

#        # add FRONTIER debugging and RUCIO env variables
#        cmd = self.addEnvVars2Cmd(cmd, job.jobId, job.processingType)

        # Pipe stdout/err for payload to files
        cmd += " 1>%s 2>%s" % (job.stdout, job.stderr)
        tolog("\nCommand to run the job is: \n%s" % (cmd))

        tolog("%s = %s" % (self.__experiment_python_pilot_var, os.environ[self.__experiment_python_pilot_var]))

        if special_setup_cmd != "":
            tolog("Special setup command: %s" % (special_setup_cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig


    def setPython(self, site_root, atlasRelease, homePackage, cmtconfig, sitename):
        """ set the python executable """

        ec = 0
        pilotErrorDiag = ""
        pybin = ""

        if os.environ.has_key(self.__experiment_sw_dir_var) and verifyReleaseString(atlasRelease) != "NULL":
            pybin = "python"

        if pybin == "":
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


    def getSpecialSetupCommand(self):
        """ Set special_setup_cmd if necessary """

        # Note: this special setup command is hardly used and could probably be removed
        # in case any special setup should be added to the setup string before the trf is executed, the command defined in this method
        # could be added to the run command by using method addSPSetupToCmd().
        # the special command is also forwarded to the get and put functions (currently not used)

        special_setup_cmd = ""

        # add envsetup to the special command setup on tier-3 sites
        # (unknown if this is still needed)

        # get the site information object
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

    def specialChecks(self, **kwargs):
        """ Implement special checks here """
        # Return False if fatal failure, otherwise return True
        # The pilot will abort if this method returns a False

        status = False

        tolog("No special checks for \'%s\'" % (self.__experiment))

        return True # obviously change this to 'status' once implemented


    def getPayloadName(self, job):
        """ Set a suitable name for the payload stdout """

        # The payload <name> gets translated into <name>_stdout.txt
        # which is the name of the stdout file produced by the payload execution
        # (essentially commands.getoutput("<setup>; <payload executable> [options] > <name>_stdout.txt"))

        # The job object can be used to create more precise stdout names (see e.g. the ATLASExperiment implementation)

        return "transformation"


if __name__ == "__main__":

    print "Implement test cases here"
    
#    a=ATLASExperiment()
#    a.specialChecks()
#    appdir='/cvmfs/atlas.cern.ch/repo/sw'
