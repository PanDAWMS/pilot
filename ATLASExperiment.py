# Class definition:
#   ATLASExperiment
#   This class is the ATLAS experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from Experiment import Experiment               # Main experiment class
from pUtil import tolog                         # Logging method that sends text to the pilot log
from pUtil import readpar                       # Used to read values from the schedconfig DB (queuedata)
from pUtil import isAnalysisJob                 # Is the current job a user analysis job or a production job?
from pUtil import grep                          # Grep function - reimplement using cli command
from pUtil import getCmtconfig                  # Get the cmtconfig from the job def or queuedata
from pUtil import getCmtconfigAlternatives      # Get a list of locally available cmtconfigs
from pUtil import verifyReleaseString           # To verify the release string (move to Experiment later)
from pUtil import getProperTimeout              #
from pUtil import timedCommand                  # Protect cmd with timed_command
from pUtil import getSiteInformation            # Get the SiteInformation object corresponding to the given experiment
from pUtil import isBuildJob                    # Is the current job a build job?
from pUtil import remove                        # Used to remove redundant file before log file creation
from pUtil import extractFilePaths              # Used by verifySetupCommand
from pUtil import getInitialDirs                # Used by getModernASetup()
from pUtil import isAGreaterOrEqualToB          #
from pUtil import convert_unicode_string        # Needed to avoid unicode strings in the memory output text file
from PilotErrors import PilotErrors             # Error codes
from FileHandling import readFile, writeFile    # File handling methods
from FileHandling import updatePilotErrorReport # Used to set the priority of an error
from FileHandling import getJSONDictionary      # Used by getUtilityInfo()
from RunJobUtilities import dumpOutput          # ASCII dump
from RunJobUtilities import getStdoutFilename   #
from RunJobUtilities import findVmPeaks         #
from RunJobUtilities import getSourceSetup      #

# Standard python modules
import re
import os
import time
import commands
from glob import glob

class ATLASExperiment(Experiment):

    # private data members
    __experiment = "ATLAS"
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __warning = ""
    __analysisJob = False
    __job = None                           # Current Job object
    __error = PilotErrors()                # PilotErrors object
    __doFileLookups = False                # True for LFC based file lookups
    __atlasEnv = False                     # True for releases beginning with "Atlas-"

    # Required methods

#    def __init__(self, *args, **kwargs):
    def __init__(self):
        """ Default initialization """

        pass
#        super(ATLASExperiment, self).__init__(self, *args, **kwargs)

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ATLASExperiment, cls).__new__(cls, *args, **kwargs)
#            cls.__instance = super(ATLASExperiment, cls).__new__(cls)

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

    def updateCmd1WithProject(self, cmd1, atlasProject):
        """ Add the known project to the setup command """

        if atlasProject != "" and atlasProject not in cmd1:
            cmd1 = cmd1.replace("notest","%s,notest" % (atlasProject))

        tolog("cmd1 = %s" % (cmd1))

        return cmd1

    def addMAKEFLAGS(self, jobCoreCount, cmd2):
        """ Correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make) """

        # ATHENA_PROC_NUMBER is set in Node.py using the schedconfig value
        try:
            coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
        except:
            coreCount = -1
        if coreCount == -1:
            try:
                coreCount = int(jobCoreCount)
            except:
                pass
            else:
                if coreCount >= 1:
                    cmd2 += 'export MAKEFLAGS="j%d QUICK=1 -l1";' % (coreCount)
                    tolog("Added multi-core support to cmd2: %s" % (cmd2))
        # make sure that MAKEFLAGS is always set
        if not "MAKEFLAGS=" in cmd2:
            cmd2 += 'export MAKEFLAGS="j1 QUICK=1 -l1";'

        return cmd2

    def getJobExecutionCommand(self, job, jobSite, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload """

        pilotErrorDiag = ""
        cmd = ""
        special_setup_cmd = ""
        pysiteroot = ""
        siteroot = ""
        JEM = "NO"

        # homePackage variants:
        #   user jobs
        #     1. AnalysisTransforms (using asetup)
        #        Setup example buildJob: (the local/setup comes from copysetup; ignore the cmtsite in the path, this will go away with the new setup procedure)
        #          old setup:
        #            cmd = export PANDA_RESOURCE="ANALY_PIC_SL6";export ROOT_TTREECACHE_SIZE=1;export FRONTIER_ID="[2674166005]";export CMSSW_VERSION=$FRONTIER_ID;export RUCIO_APPID="panda-client-0.5.56-jedi-athena";export RUCIO_ACCOUNT="pilot";export ROOTCORE_NCPUS=1;source /cvmfs/atlas.cern.ch/repo/sw/software/x86_64-slc6-gcc47-opt/19.2.0/cmtsite/asetup.sh 19.2.0,notest --cmtconfig x86_64-slc6-gcc47-opt ;export MAKEFLAGS="j1 QUICK=1 -l1";source /cvmfs/atlas.cern.ch/repo/sw/local/setup.sh;export X509_USER_PROXY=<path>;./buildJob-00-00-03 ..
        #          new setup:
        #            cmd =
        #        Setup example runAthena:
        #          old setup:
        #            cmd = export PANDA_RESOURCE="ANALY_BNL_LONG";export ROOT_TTREECACHE_SIZE=1;export FRONTIER_ID="[2674156736]";export CMSSW_VERSION=$FRONTIER_ID;export RUCIO_APPID="panda-client-0.5.56-jedi-athena";export RUCIO_ACCOUNT="pilot";export ROOTCORE_NCPUS=1;source /cvmfs/atlas.cern.ch/repo/sw/software/x86_64-slc6-gcc47-opt/19.2.0/cmtsite/asetup.sh 19.2.0,notest --cmtconfig x86_64-slc6-gcc47-opt ;export MAKEFLAGS="j1 QUICK=1 -l1";export X509_USER_PROXY=<path>;./runAthena-00-00-12 .. 
        #          new setup:
        #            cmd =
        #        Setup example buildGen:
        #          old setup:
        #            cmd = export PANDA_RESOURCE=\"ANALY_DESY-HH\";export ROOT_TTREECACHE_SIZE=1;export FRONTIER_ID=\"[2674275586]\";export CMSSW_VERSION=$FRONTIER_ID;export RUCIO_APPID=\"panda-client-0.5.56-jedi-run\";export RUCIO_ACCOUNT=\"pilot\";export ROOTCORE_NCPUS=1;source /cvmfs/atlas.cern.ch/repo/sw/software/i686-slc5-gcc43-opt/17.3.11/cmtsite/asetup.sh 17.3.11,notest --cmtconfig i686-slc5-gcc43-opt ;export MAKEFLAGS=\"j1 QUICK=1 -l1\";export X509_USER_PROXY=<path>;./buildGen-00-00-01 ..
        #          new setup:
        #            cmd =
        #     2. AnalysisTransforms-<project>_<cache>, e.g. project=AthAnalysisBase,AtlasDerivation,AtlasProduction,MCProd,TrigMC; cache=20.1.6.2,..
        #        Setup example
        #          old setup:
        #            cmd = 
        #     3. AnalysisTransforms-<project>_rel_<N>, e.g. project=AtlasOffline; N=0,1,2,..
        #        Setup example
        #          old setup:
        #            cmd = 
        #     4. [homaPackage not set]
        #        Setup example runGen:
        #          old setup:
        #            cmd = export PANDA_RESOURCE="ANALY_LPC";export ROOT_TTREECACHE_SIZE=1;export FRONTIER_ID="[2673995173]";export CMSSW_VERSION=$FRONTIER_ID;export RUCIO_APPID="gangarobot-rctest";export RUCIO_ACCOUNT="pilot";export ROOTCORE_NCPUS=1;export X509_USER_PROXY=<path>;./runGen-00-00-02 .. 
        #          new setup:
        #            cmd =
        #        Setup example buildGen:
        #          old setup:
        #            cmd = export PANDA_RESOURCE=\"ANALY_TRIUMF\";export ROOT_TTREECACHE_SIZE=1;export FRONTIER_ID=\"[2674133638]\";export CMSSW_VERSION=$FRONTIER_ID;export RUCIO_APPID=\"panda-client-0.5.56-jedi-run\";export RUCIO_ACCOUNT=\"pilot\";export ROOTCORE_NCPUS=1;source /cvmfs/atlas.cern.ch/repo/sw/local/setup.sh;export X509_USER_PROXY=<path>;./buildGen-00-00-01 
        #          new setup:
        #            cmd =
        #
        #   production jobs
        #     1. <project>/<cache>, e.g. AtlasDerivation/20.1.6.2, AtlasProd1/20.1.5.10.1
        #        Setup example
        #          old setup:
        #            cmd = export PANDA_RESOURCE=\"FZK-LCG2\";export FRONTIER_ID=\"[2674134772]\";export CMSSW_VERSION=$FRONTIER_ID;export RUCIO_APPID=\"merge\";export RUCIO_ACCOUNT=\"pilot\";source /cvmfs/atlas.cern.ch/repo/sw/software/i686-slc5-gcc43-opt/17.2.1/cmtsite/asetup.sh 17.2.1,notest --cmtconfig i686-slc5-gcc43-opt ;unset CMTPATH;cd /cvmfs/atlas.cern.ch/repo/sw/software/i686-slc5-gcc43-opt/17.2.1/TrigMC/17.2.1.4.3/TrigMCRunTime/cmt;source ./setup.sh;cd -;export AtlasVersion=17.2.1.4.3;export AtlasPatchVersion=17.2.1.4.3;Merging_trf.py
        #          new setup:
        #            cmd =
        #     2. <project>,rel_<N>[,devval], e.g. AtlasProduction,rel_4,devval
        #
        # Tested
        #   AtlasG4_tf.py:
        #     PandaID=2675460595
        #     Release=17.7.3
        #     homePackage=MCProd/17.7.3.9.6
        #   Sim_tf.py:
        #     PandaID=2675460792
        #     Release=1.0.3
        #     homePackage=AthSimulationBase/1.0.3
        #   Sim_tf.py, event service:
        #     PandaID=2676267339
        #     Release=Atlas-20.3.3
        #     homePackage=AtlasProduction/20.3.3.2
        #   Reco_tf.py:
        #     PandaID=2675925382
        #     Release=20.1.5
        #     homePackage=AtlasProduction/20.1.5.10


        # Is it a user job or not?
        analysisJob = isAnalysisJob(job.trf)

        # Get the cmtconfig value
        cmtconfig = getCmtconfig(job.cmtconfig)

        # Is it a standard ATLAS job? (i.e. with swRelease = 'Atlas-...') USE OLD FUNCTION FOR USER JOBS FOR NOW
        if not self.__atlasEnv or analysisJob:
            return self.getJobExecutionCommandOld(job, jobSite, pilot_initdir)

            # Set the INDS env variable (used by runAthena)
#            self.setINDS(job.realDatasetsIn)

            # Try to download the trf
#            status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
#            if status != 0:
#                return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

#        # Command used to download runAthena or runGen
#        wgetCommand = 'wget'

        else:

            # Normal setup (production and user jobs)

            # Extract the project (cacheDir) and cache version, if any
            m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', job.homePackage) # User jobs
            if m_cacheDirVer != None:
                # user jobs
                cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, job.release)
            elif "," in job.homePackage or "rel_" in job.homePackage:
                # nightlies; e.g. homePackage = "AtlasProduction,rel_0"
                cacheDir = job.homePackage
                cacheVer = None
            else:
                # normal production jobs; e.g. homePackage = "AtlasProduction/20.1.5"
                cacheDir, cacheVer = self.getSplitHomePackage(job.homePackage)

            # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
            asetup_path = self.getModernASetup()

            # Add the appropriate options (release/patch/project/cache)
            asetup_options = " "
            if cacheDir:
                asetup_options += cacheDir
            if cacheVer:
                if asetup_options == " ":
                    asetup_options += cacheVer
                else:
                    asetup_options += "," + cacheVer
            asetup_options += " --cmtconfig " + cmtconfig

            cmd = asetup_path + asetup_options

        # Add the transform and the job parameters
        cmd += ";%s %s" % (job.trf, job.jobPars)

        # Add FRONTIER debugging and RUCIO env variables
        if 'HPC_' in readpar("catchall") and 'HPC_HPC' not in readpar("catchall"):
            cmd['environment'] = self.getEnvVars2Cmd(job.jobId, job.processingType, jobSite.sitename, analysisJob)
        else:
            cmd = self.addEnvVars2Cmd(cmd, job.jobId, job.processingType, jobSite.sitename, analysisJob)
        if 'HPC_HPC' in readpar("catchall"):
            cmd = 'export JOB_RELEASE=%s;export JOB_HOMEPACKAGE=%s;JOB_CACHEVERSION=%s;JOB_CMTCONFIG=%s;%s' % (job.release, job.homePackage, cacheVer, cmtconfig, cmd)

        # Is JEM allowed to be used?
        if self.isJEMAllowed():
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

        tolog("\nCommand to run the job is: \n%s" % (cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig

    def getJobExecutionCommandOld(self, job, jobSite, pilot_initdir):
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
        swbase = self.getSwbase(jobSite.appdir, job.release, job.homePackage, job.processingType, cmtconfig)
        tolog("Local software path: swbase = %s" % (swbase))

        # Get cmtconfig alternatives
        cmtconfig_alternatives = getCmtconfigAlternatives(cmtconfig, swbase)
        tolog("Found alternatives to cmtconfig: %s (the first item is the default cmtconfig value)" % str(cmtconfig_alternatives))

        # Update the job parameters --input option for Merge trf's (to protect against potentially too long file lists)
#        if "--input=" in job.jobPars and "Merge_tf" in job.trf:
#            tolog("Will update input file list in job parameters and create input file list for merge job")
#            job.jobPars = self.updateJobParameters4Input(job.jobPars)

        # Is it a standard ATLAS job? (i.e. with swRelease = 'Atlas-...')
        if self.__atlasEnv :
            # Define the job runtime environment
            if not analysisJob and job.trf.endswith('.py'): # for production python trf

                tolog("Production python trf")

                if os.environ.has_key('VO_ATLAS_SW_DIR'):
                    scappdir = readpar('appdir')
                    # is this release present in the tags file?
                    if scappdir == "":
                        rel_in_tags = self.verifyReleaseInTagsFile(os.environ['VO_ATLAS_SW_DIR'], job.release)
                        if not rel_in_tags:
                            tolog("WARNING: release was not found in tags file: %s" % (job.release))
#                            tolog("!!FAILED!!3000!! ...")
#                            failJob(0, self.__error.ERR_MISSINGINSTALLATION, job, pilotserver, pilotport, ins=ins)
#                        swbase = os.environ['VO_ATLAS_SW_DIR'] + '/software'

                    # Get the proper siteroot and cmtconfig
                    ec, pilotErrorDiag, status, siteroot, cmtconfig = self.getProperSiterootAndCmtconfig(swbase, job.release, job.homePackage, cmtconfig)
                    if not status:
                        tolog("!!WARNING!!3000!! Since setup encountered problems, any attempt of trf installation will fail (bailing out)")
                        tolog("ec=%d" % (ec))
                        tolog("pilotErrorDiag=%s" % (pilotErrorDiag))

                        # Set the error priority
                        updatePilotErrorReport(ec, pilotErrorDiag, "1",  job.jobId, pilot_initdir)

                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                    else:
                        tolog("Will use SITEROOT=%s" % (siteroot))
                        pysiteroot = siteroot
                else:
                    if verifyReleaseString(job.release) != "NULL":
                        _s = os.path.join(os.path.join(swbase, cmtconfig), job.release)
                        if os.path.exists(_s):
                            siteroot = _s
                        else:
                            siteroot = os.path.join(swbase, job.release)
                    else:
                        siteroot = swbase
                    siteroot = siteroot.replace('//','/')

                # Get the install dir and update siteroot if necessary (dynamically install patch if not available)
                ec, pilotErrorDiag, siteroot, installDir = self.getInstallDir(job, jobSite.workdir, siteroot, swbase, cmtconfig)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Get the cmtsite setup command
                ec, pilotErrorDiag, cmd1 = self.getCmtsiteCmd(swbase, job.release, job.homePackage, cmtconfig, siteroot=pysiteroot)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Make sure the CMTCONFIG is available and valid
                ec, pilotErrorDiag, dummy, dummy, atlasProject = self.checkCMTCONFIG(cmd1, cmtconfig, job.release, siteroot=pysiteroot)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Add the project to the setup (only for HLT jobs for now, both on cvmfs and afs)
                if "AtlasP1HLT" in job.homePackage or "AtlasHLT" in job.homePackage:
                    cmd1 = self.updateCmd1WithProject(cmd1, atlasProject)

                # Get cmd2 for production jobs for set installDirs (not the case for unset homepackage strings)
                if installDir != "" and not "AthSimulation" in job.homePackage:
                    cmd2, pilotErrorDiag = self.getProdCmd2(installDir, job.homePackage)
                    if pilotErrorDiag != "":
                        return self.__error.ERR_SETUPFAILURE, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                else:
                    cmd2 = ""
                if 'HPC_HPC' in readpar("catchall"):
                    cmd2 = "export G4ATLAS_SKIPFILEPEEK=1"

                # Set special_setup_cmd if necessary
                special_setup_cmd = self.getSpecialSetupCommand()

            else: # for analysis python trf

                tolog("Preparing analysis job setup command")

                # try alternatives to cmtconfig if necessary
                first = True
                first_ec = 0
                first_pilotErrorDiag = ""
                for cmtconfig in cmtconfig_alternatives:
                    ec = 0
                    pilotErrorDiag = ""
                    tolog("Testing cmtconfig=%s" % (cmtconfig))

                    # Get the cmd2 setup command before cmd1 is defined since cacheDir/Ver can be used in cmd1
                    cmd2, cacheDir, cacheVer = self.getAnalyCmd2(swbase, cmtconfig, job.homePackage, job.release)

                    # Add sub path in case of AnalysisTransforms homePackage
                    if verifyReleaseString(job.homePackage) != "NULL":
                        reSubDir = re.search('AnalysisTransforms[^/]*/(.+)', job.homePackage)
                        subDir = ""
                        if reSubDir != None:
                            subDir = reSubDir.group(1)
                        tolog("subDir = %s" % (subDir))
                    else:
                        subDir = ""
                    path = os.path.join(swbase, subDir)

                    # Define cmd0 and cmd1
                    if verifyReleaseString(job.release) != "NULL":
                        if job.release < "16.1.0":
                            cmd0 = "source %s/%s/setup.sh;" % (path, job.release)
                            tolog("cmd0 = %s" % (cmd0))
                        else:
                            cmd0 = ""
                            tolog("cmd0 will not be used for release %s" % (job.release))
                    else:
                        cmd0 = ""

                    # Get the cmtsite setup command
                    ec, pilotErrorDiag, cmd1 = \
                        self.getCmtsiteCmd(swbase, job.release, job.homePackage, cmtconfig, analysisJob=True, siteroot=siteroot, cacheDir=cacheDir, cacheVer=cacheVer)
                    if ec != 0:
                        # Store the first error code
                        if first:
                            first = False
                            first_ec = ec
                            first_pilotErrorDiag = pilotErrorDiag

                        # Function failed, try the next cmtconfig value or exit
                        continue

                    tolog("cmd1 = %s" % (cmd1))

                    # Make sure the CMTCONFIG is available and valid
                    ec, pilotErrorDiag, siteroot, atlasVersion, atlasProject = \
                        self.checkCMTCONFIG(cmd1, cmtconfig, job.release, siteroot=siteroot, cacheDir=cacheDir, cacheVer=cacheVer)
                    if ec != 0 and ec != self.__error.ERR_COMMANDTIMEOUT:
                        # Store the first error code
                        if first:
                            first = False
                            first_ec = ec
                            first_pilotErrorDiag = pilotErrorDiag

                        # Function failed, try the next cmtconfig value or exit
                        continue
                    else:
                        tolog("Aborting alternative cmtconfig loop (will use cmtconfig=%s)" % (cmtconfig))
                        break

                # Exit if the tests above failed
                if ec != 0:
                    # Use the first error code if set
                    if first_ec != 0:
                        tolog("Will report the first encountered problem: ec=%d, pilotErrorDiag=%s" % (first_ec, first_pilotErrorDiag))
                        ec = first_ec
                        pilotErrorDiag = first_pilotErrorDiag

                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Cannot update cmd2/siteroot for unset release/homepackage strings
                if verifyReleaseString(job.release) == "NULL" or verifyReleaseString(job.homePackage) == "NULL":
                    tolog("Will not update cmd2/siteroot since release/homepackage string is NULL")
                else:
                    # Update cmd2 with AtlasVersion and AtlasProject from setup (and siteroot if not set)
                    _useAsetup = self.useAtlasSetup(swbase, job.release, job.homePackage, cmtconfig)
                    cmd2 = self.updateAnalyCmd2(cmd2, atlasVersion, atlasProject, _useAsetup)

                tolog("cmd2 = %s" % (cmd2))
                tolog("siteroot = %s" % (siteroot))

                # Set special_setup_cmd if necessary
                special_setup_cmd = self.getSpecialSetupCommand()

                # correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
                cmd2 = self.addMAKEFLAGS(job.coreCount, cmd2)

                # Prepend cmd0 to cmd1 if set and if release < 16.1.0
                if cmd0 != "" and job.release < "16.1.0":
                    cmd1 = cmd0 + cmd1

            # construct the command of execution
            if analysisJob:
                # Try to download the trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Set up runAthena
                ec, pilotErrorDiag, cmd3 = self.getAnalysisRunCommand(job, jobSite, trfName)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # NOTE: if TURL based PFC creation fails, getAnalysisRunCommand() needs to be rerun
                # Might not be possible, so if a user analysis job fails during TURL based PFC creation, fail the job
                # Or can remote I/O features just be turned off and cmd3 corrected accordingly?

            elif job.trf.endswith('.py'): # for python prod trf
                if os.environ.has_key('VO_ATLAS_SW_DIR'):
                    # set python executable (after SITEROOT has been set)
                    if siteroot == "":
                        try:
                            siteroot = os.environ['SITEROOT']
                        except:
                            tolog("Warning: $SITEROOT unknown at this stage (2)")
                    if pysiteroot == "":
                        tolog("Will use SITEROOT: %s (2)" % (siteroot))
                        ec, pilotErrorDiag, pybin = self.setPython(siteroot, job.release, job.homePackage, cmtconfig, jobSite.sitename)
                    else:
                        tolog("Will use pysiteroot: %s (2)" % (pysiteroot))
                        ec, pilotErrorDiag, pybin = self.setPython(pysiteroot, job.release, job.homePackage, cmtconfig, jobSite.sitename)

                    if ec == self.__error.ERR_MISSINGINSTALLATION:
                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                    # Prepare the cmd3 command with the python from the release and the full path to the trf
                    _cmd = cmd1
                    if cmd2 != "": # could be unset (in the case of unset homepackage strings)
                        _cmd += ";" + cmd2
                    cmd3 = self.getProdCmd3(_cmd, pybin, job.trf, job.jobPars)
                else:
                    cmd3 = "%s %s" % (job.trf, job.jobPars)

            elif verifyReleaseString(job.homePackage) != 'NULL':
                cmd3 = "%s/kitval/KitValidation/JobTransforms/%s/%s %s" %\
                       (swbase, job.homePackage, job.trf, job.jobPars)
            else:
                cmd3 = "%s/kitval/KitValidation/JobTransforms/%s %s" %\
                       (swbase, job.trf, job.jobPars)

            tolog("cmd3 = %s" % (cmd3))

            # Create the final command string
            cmd = cmd1
            if cmd2 != "":
                cmd += ";" + cmd2
            if cmd3 != "":
                cmd += ";" + cmd3
            # cmd2 and MAKEFLAGS can add an extra ;-sign, remove it
            cmd = cmd.replace(';;',';')

        else: # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

            tolog("Generic job")

            # Set python executable (after SITEROOT has been set)
            if siteroot == "":
                try:
                    siteroot = os.environ['SITEROOT']
                except:
                    tolog("Warning: $SITEROOT unknown at this stage (3)")

            tolog("Will use $SITEROOT: %s (3)" % (siteroot))
            ec, pilotErrorDiag, pybin = self.setPython(siteroot, job.release, job.homePackage, cmtconfig, jobSite.sitename)
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

                # correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
                cmd2 = self.addMAKEFLAGS(job.coreCount, "")
                tolog("cmd2 = %s" % (cmd2))
                cmd = cmd2 + cmd

                # should asetup be used? If so, sqeeze it into the run command (rather than moving the entire getAnalysisRunCommand() into this class)
                m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', job.homePackage)
                if m_cacheDirVer != None:
                    # homePackage="AnalysisTransforms-AthAnalysisBase_2.0.14"
                    # -> cacheDir = AthAnalysisBase, cacheVer = 2.0.14
                    cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, "dummy_atlasRelease")
                    tolog("cacheDir = %s" % (cacheDir))
                    tolog("cacheVer = %s" % (cacheVer))
                    if cacheDir != "" and cacheVer != "":

                        #asetup = "export AtlasSetup=%s/%s/%s/%s/AtlasSetup; " % (swbase, cacheDir, cmtconfig, cacheVer)
                        #asetup += "source $AtlasSetup/scripts/asetup.sh %s,%s --cmtconfig=%s;" % (cacheDir, cacheVer, cmtconfig)

                        asetup = self.getModernASetup()
                        asetup += " %s,%s --cmtconfig=%s;" % (cacheDir, cacheVer, cmtconfig)

                        # now squeeze it back in
                        cmd = cmd.replace('./' + trfName, asetup + './' + trfName)
                        tolog("Updated run command for special homePackage: %s" % (cmd))
                    else:
                        tolog("asetup not needed (mo special home package: %s)" % (homePackage))
                else:
                    tolog("asetup not needed (no special homePackage)")

            elif verifyReleaseString(job.homePackage) != 'NULL' and job.homePackage != ' ':

                if 'HPC_' in readpar("catchall"):
                    cmd = {"interpreter": pybin,
                           "payload": ("%s/%s" % (job.homePackage, job.trf)),
                           "parameters": job.jobPars }
                else:
                    cmd = "%s %s/%s %s" % (pybin, job.homePackage, job.trf, job.jobPars)
            else:
                if 'HPC_' in readpar("catchall"):
                    cmd = {"interpreter": pybin,
                           "payload": job.trf,
                           "parameters": job.jobPars }
                else:
                    cmd = "%s %s %s" % (pybin, job.trf, job.jobPars)

            # Set special_setup_cmd if necessary
            special_setup_cmd = self.getSpecialSetupCommand()

        # add FRONTIER debugging and RUCIO env variables
        if 'HPC_' in readpar("catchall"):
            cmd['environment'] = self.getEnvVars2Cmd(job.jobId, job.processingType, jobSite.sitename, analysisJob)
        else:
            cmd = self.addEnvVars2Cmd(cmd, job.jobId, job.processingType, jobSite.sitename, analysisJob)

        # Is JEM allowed to be used?
        if self.isJEMAllowed():
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

        tolog("\nCommand to run the job is: \n%s" % (cmd))

        if special_setup_cmd != "":
            tolog("Special setup command: %s" % (special_setup_cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig

    def getFileLookups(self):
        """ Return the file lookup boolean """

        return self.__doFileLookups

    def doFileLookups(self, doFileLookups):
        """ Update the file lookups boolean """

        self.__doFileLookups = doFileLookups

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return self.getFileLookups()

    def willDoAlternativeFileLookups(self):
        """ Should file lookups be done using alternative methods? """

        # E.g. in the migration period where LFC lookups are halted in favour of other methods in the Rucio API
        # (for ATLAS), this method could be useful. See the usage in Mover::getReplicaDictionary() which is called
        # after Experiment::willDoFileLookups() defined above. The motivation is that direct LFC calls are not to be
        # used any longer by the pilot, and in the migration period the actual LFC calls will be done in the Rucio
        # API. Eventually this API will switch to alternative file lookups.

        tolog("Using alternative file catalog lookups")

        return True

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        status = False

        # should the LFC file registration be done by the pilot or by the server?
        if readpar('lfcregister') != "server":
            status = True

        # make sure that the lcgcpSiteMover (and thus lcg-cr) is not used
        if readpar('copytool') == "lcgcp" or readpar('copytool') == "lcg-cp":
            status = False

        return status

    # Additional optional methods

    def removeRedundantFiles(self, workdir):
        """ Remove redundant files and directories """

        tolog("Removing redundant files prior to log creation")

        dir_list = ["AtlasProduction*",
                    "AtlasPoint1",
                    "AtlasTier0",
                    "buildJob*",
                    "CDRelease*",
                    "csc*.log",
                    "DBRelease*",
                    "EvgenJobOptions",
                    "external",
                    "fort.*",
                    "geant4",
                    "geomDB",
                    "geomDB_sqlite",
                    "home",
                    "o..pacman..o",
                    "pacman-*",
                    "python",
                    "runAthena*",
                    "share",
                    "sources.*",
                    "sqlite*",
                    "sw",
                    "tcf_*",
                    "triggerDB",
                    "trusted.caches",
                    "workdir",
                    "*.data*",
                    "*.events",
                    "*.py",
                    "*.pyc",
                    "*.root*",
                    "JEM",
                    "tmp*",
                    "*.tmp",
                    "*.TMP",
                    "MC11JobOptions",
                    "scratch",
                    "jobState-*-test.pickle",
                    "*.writing",
                    "pwg*",
                    "pwhg*",
                    "*PROC*",
                    "HPC",
                    "saga",
                    "radical"]

        # remove core and pool.root files from AthenaMP sub directories
        try:
            self.cleanupAthenaMP(workdir)
        except Exception, e:
            tolog("!!WARNING!!2341!! Failed to execure cleanupAthenaMP(): %s" % (e))

        # explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command (--dereference option)
        matches = []
        import fnmatch
        for root, dirnames, filenames in os.walk(workdir):
            for filename in fnmatch.filter(filenames, '*.a'):
                matches.append(os.path.join(root, filename))
        if matches != []:
            tolog("!!WARNING!!4990!! Encountered %d archive files - will be purged" % len(matches))
            rc = remove(matches)
            if not rc:
                tolog("WARNING: Failed to remove redundant files")
        else:
            tolog("Found no archive files")

        # note: these should be partitial file/dir names, not containing any wildcards
        exceptions_list = ["runargs", "runwrapper", "jobReport", "log."]

        for _dir in dir_list:
            files = glob(os.path.join(workdir, _dir))
            exclude = []

            # remove any dirs/files from the exceptions list
            if files:
                for exc in exceptions_list:
                    for f in files:
                        if exc in f:
                            exclude.append(f)

                if exclude != []:
                    tolog('To be excluded from removal: %s' % (exclude))

                _files = []
                for f in files:
                    if not f in exclude:
                        _files.append(f)
                files = _files

                tolog("To be removed: %s" % (files))
                rc = remove(files)
                if not rc:
                    tolog("IGNORE: Failed to remove redundant file(s): %s" % (files))

        # run a second pass to clean up any broken links
        broken = []
        for root, dirs, files in os.walk(workdir):
            for filename in files:
                path = os.path.join(root,filename)
                if os.path.islink(path):
                    target_path = os.readlink(path)
                    # Resolve relative symlinks
                    if not os.path.isabs(target_path):
                        target_path = os.path.join(os.path.dirname(path),target_path)             
                    if not os.path.exists(target_path):
                        broken.append(path)
                else:
                    # If it's not a symlink we're not interested.
                    continue

        if broken != []:
            tolog("!!WARNING!!4991!! Encountered %d broken soft links - will be purged" % len(broken))
            rc = remove(broken)
            if not rc:
                tolog("WARNING: Failed to remove broken soft links")
        else:
            tolog("Found no broken links")

    def getWarning(self):
        """ Return any warning message passed to __warning """

        return self.__warning

    def displayChangeLog(self):
        """ Display the cvmfs ChangeLog is possible """
        # 'head' the ChangeLog on cvmfs (/cvmfs/atlas.cern.ch/repo/sw/ChangeLog)

        # get the site information object
        si = getSiteInformation(self.__experiment)

        appdir = readpar('appdir')
        if appdir == "":
            if os.environ.has_key('VO_ATLAS_SW_DIR'):
                appdir = os.environ['VO_ATLAS_SW_DIR']
            else:
                appdir = ""

        if appdir != "":
            # there might be more than one appdir, try them all
            appdirs = si.getAppdirs(appdir)
            tolog("appdirs = %s" % str(appdirs))
            for appdir in appdirs:
                path = os.path.join(appdir, 'ChangeLog')
                if os.path.exists(path):
                    try:
                        rs = commands.getoutput("head %s" % (path))
                    except Exception, e:
                        tolog("!!WARNING!!1232!! Failed to read the ChangeLog: %s" % (e))
                    else:
                        rs = "\n"+"-"*80 + "\n" + rs
                        rs += "\n"+"-"*80
                        tolog("head of %s: %s" % (path, rs))
                else:
                    tolog("No such path: %s (ignore)" % (path))
        else:
            tolog("Can not display ChangeLog: Found no appdir")

    def getCVMFSPath(self):
        """ Return the proper cvmfs path """

        # get the site information object
        si = getSiteInformation(self.__experiment)
        return si.getFileSystemRootPath()

    def testCVMFS(self):
        """ Run the CVMFS diagnostics tool """

        status = False

        timeout = 5*60
        cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/atlas.cern.ch/repo/ATLASLocalRootBase;$ATLAS_LOCAL_ROOT_BASE/utilities/checkValidity.sh" % \
            (self.getCVMFSPath())
        tolog("Executing command: %s (time-out: %d)" % (cmd, timeout))
        exitcode, output = timedCommand(cmd, timeout=timeout)
        if exitcode != 0:
            if "No such file or directory" in output:
                tolog("!!WARNING!!1235!! Command checkValidity.sh was not found (can not run CVMFS validity test)")
                status = True
            elif "timed out" in output:
                tolog("!!WARNING!!1236!! Command checkValidity.sh timed out: %s (ignore)" % (output))
                status = True
            else:
                tolog("!!WARNING!!1234!! CVMFS diagnostics tool failed: %d, %s" % (exitcode, output))
        else:
            tolog("Diagnostics tool has verified CVMFS")
            status = True

        return status

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """
        # ..and a string of the form N|N|..|N with the number of jobs in the trf(s)

        job = kwargs.get('job', None)
        number_of_jobs = kwargs.get('number_of_jobs', 1)

        if not job:
            tolog("!!WARNING!!2332!! getNumberOfEvents did not receive a job object")
            return 0, 0, ""

        tolog("Looking for number of processed events (pass -1: jobReport.json)")

        from FileHandling import getNumberOfEvents
        nEventsRead = getNumberOfEvents(job.workdir)
        nEventsWritten = 0
        if nEventsRead > 0:
            return nEventsRead, nEventsWritten, str(nEventsRead)
        else:
            nEventsRead = 0

        tolog("Looking for number of processed events (pass 0: metadata.xml)")

        nEventsRead = self.processMetadata(job.workdir)
        nEventsWritten = 0
        if nEventsRead > 0:
            return nEventsRead, nEventsWritten, str(nEventsRead)
        else:
            nEventsRead = 0

        tolog("Looking for number of processed events (pass 1: Athena summary file(s))")
        nEventsRead, nEventsWritten = self.processAthenaSummary(job.workdir)
        if nEventsRead > 0:
            return nEventsRead, nEventsWritten, str(nEventsRead)

        tolog("Looking for number of processed events (pass 2: Resorting to brute force grepping of payload stdout)")
        nEvents_str = ""
        for i in range(number_of_jobs):
            _stdout = job.stdout
            if number_of_jobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stdout)
            N = 0
            if os.path.exists(filename):
                tolog("Processing stdout file: %s" % (filename))
                matched_lines = grep(["events processed so far"], filename)
                if len(matched_lines) > 0:
                    if "events read and" in matched_lines[-1]:
                        # event #415044, run #142189 2 events read and 0 events processed so far
                        N = int(re.match('.* run #\d+ \d+ events read and (\d+) events processed so far.*', matched_lines[-1]).group(1))
                    else:
                        # event #4, run #0 3 events processed so far
                        N = int(re.match('.* run #\d+ (\d+) events processed so far.*', matched_lines[-1]).group(1))

            if len(nEvents_str) == 0:
                nEvents_str = str(N)
            else:
                nEvents_str += "|%d" % (N)
            nEventsRead += N

        return nEventsRead, nEventsWritten, nEvents_str

    def processMetadata(self, workdir):
        """ Extract number of events from metadata.xml """

        N = 0

        filename = os.path.join(workdir, "metadata.xml")
        if os.path.exists(filename):
            # Get the metadata
            try:
                f = open(filename, "r")
            except IOError, e:
                tolog("!!WARNING!!1222!! Exception: %s" % (e))
            else:
                xmlIN = f.read()
                f.close()

                # Get the XML objects
                from xml.dom import minidom
                xmldoc = minidom.parseString(xmlIN)
                fileList = xmldoc.getElementsByTagName("File")

                # Loop over all files, assume that the number of events are the same in all files
                for _file in fileList:
                    lrc_metadata_dom = _file.getElementsByTagName("metadata")
                    for i in range(len(lrc_metadata_dom)):
                        _key = str(_file.getElementsByTagName("metadata")[i].getAttribute("att_name"))
                        _value = str(_file.getElementsByTagName("metadata")[i].getAttribute("att_value"))
                        if _key == "events" and _value:
                            try:
                                N = int(_value)
                            except Exception, e:
                                tolog("!!WARNING!!1222!! Number of events not an integer: %s" % (e))
                            else:
                                tolog("Number of events from metadata file: %d" % (N))
                            break
        else:
            tolog("%s does not exist" % (filename))

        return N

    def processAthenaSummary(self, workdir):
        """ extract number of events etc from athena summary file(s) """

        N1 = 0
        N2 = 0
        file_pattern_list = ['AthSummary*', 'AthenaSummary*']

        file_list = []
        # loop over all patterns in the list to find all possible summary files
        for file_pattern in file_pattern_list:
            # get all the summary files for the current file pattern
            files = glob(os.path.join(workdir, file_pattern))
            # append all found files to the file list
            for summary_file in files:
                file_list.append(summary_file)

        if file_list == [] or file_list == ['']:
            tolog("Did not find any athena summary files")
        else:
            # find the most recent and the oldest files
            oldest_summary_file = ""
            recent_summary_file = ""
            oldest_time = 9999999999
            recent_time = 0
            if len(file_list) > 1:
                for summary_file in file_list:
                    # get the modification time
                    try:
                        st_mtime = os.path.getmtime(summary_file)
                    except Exception, e:
                        tolog("!!WARNING!!1800!! Could not read modification time of file %s: %s" % (summary_file, str(e)))
                    else:
                        if st_mtime > recent_time:
                            recent_time = st_mtime
                            recent_summary_file = summary_file
                        if st_mtime < oldest_time:
                            oldest_time = st_mtime
                            oldest_summary_file = summary_file
            else:
                oldest_summary_file = file_list[0]
                recent_summary_file = oldest_summary_file
                oldest_time = os.path.getmtime(oldest_summary_file)
                recent_time = oldest_time

            if oldest_summary_file == recent_summary_file:
                tolog("Summary file: %s: Will be processed for errors and number of events" %\
                      (os.path.basename(oldest_summary_file)))
            else:
                tolog("Most recent summary file: %s (updated at %d): Will be processed for errors" %\
                      (os.path.basename(recent_summary_file), recent_time))
                tolog("Oldest summary file: %s (updated at %d): Will be processed for number of events" %\
                      (os.path.basename(oldest_summary_file), oldest_time))

            # Get the number of events from the oldest summary file
            try:
                f = open(oldest_summary_file, "r")
            except Exception, e:
                tolog("!!WARNING!!1800!! Failed to get number of events from summary file. Could not open file: %s" % str(e))
            else:
                lines = f.readlines()
                f.close()

                if len(lines) > 0:
                    for line in lines:
                        if "Events Read:" in line:
                            N1 = int(re.match('Events Read\: *(\d+)', line).group(1))
                        if "Events Written:" in line:
                            N2 = int(re.match('Events Written\: *(\d+)', line).group(1))
                        if N1 > 0 and N2 > 0:
                            break
                else:
                    tolog("!!WARNING!!1800!! Failed to get number of events from summary file. Encountered an empty summary file.")

                tolog("Number of events: %d (read)" % (N1))
                tolog("Number of events: %d (written)" % (N2))

            # Get the errors from the most recent summary file
            # ...

        return N1, N2

    def isOutOfMemory(self, **kwargs):
        """ Try to identify out of memory errors in the stderr/out """
        # (Used by ErrorDiagnosis)

        # make this function shorter, basically same code twice

        out_of_memory = False

        job = kwargs.get('job', None)
        number_of_jobs = kwargs.get('number_of_jobs', 1)

        if not job:
            tolog("!!WARNING!!3222!! isOutOfMemory() did not receive a job object")
            return False

        tolog("Checking for memory errors in stderr")
        for i in range(number_of_jobs):
            _stderr = job.stderr
            if number_of_jobs > 1:
                _stderr = _stderr.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stderr)
            if os.path.exists(filename):
                tolog("Processing stderr file: %s" % (filename))
                if os.path.getsize(filename) > 0:
                    tolog("WARNING: %s produced stderr, will dump to log" % (job.payload))
                    stderr_output = dumpOutput(filename)
                    if stderr_output.find("MemoryRescueSvc") >= 0 and \
                           stderr_output.find("FATAL out of memory: taking the application down") > 0:
                        out_of_memory = True
            else:
                tolog("Warning: File %s does not exist" % (filename))

        # try to identify out of memory errors in the stdout
        tolog("Checking for memory errors in stdout..")
        for i in range(number_of_jobs):
            _stdout = job.stdout
            if number_of_jobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stdout)
            if os.path.exists(filename):
                tolog("Processing stdout file: %s" % (filename))
                matched_lines = grep(["St9bad_alloc", "std::bad_alloc"], filename)
                if len(matched_lines) > 0:
                    tolog("Identified an out of memory error in %s stdout:" % (job.payload))
                    for line in matched_lines:
                        tolog(line)
                    out_of_memory = True
            else:
                tolog("Warning: File %s does not exist" % (filename))

        return out_of_memory

    def verifyReleaseInTagsFile(self, vo_atlas_sw_dir, atlasRelease):
        """ verify that the release is in the tags file """

        status = False

        # make sure the release is actually set
        if verifyReleaseString(atlasRelease) == "NULL":
            return status

        tags = dumpOutput(vo_atlas_sw_dir + '/tags')
        if tags != "":
            # is the release among the tags?
            if tags.find(atlasRelease) >= 0:
                tolog("Release %s was found in tags file" % (atlasRelease))
                status = True
            else:
                tolog("!!WARNING!!3000!! Release %s was not found in tags file" % (atlasRelease))
                # error = PilotErrors()
                # failJob(0, self.__error.ERR_MISSINGINSTALLATION, job, pilotserver, pilotport, ins=ins)
        else:
            tolog("!!WARNING!!3000!! Next pilot release might fail at this stage since there was no tags file")

        return status


    def getInstallDir(self, job, workdir, siteroot, swbase, cmtconfig):
        """ Get the path to the release """

        ec = 0
        pilotErrorDiag = ""

        # do not proceed for unset homepackage strings (treat as release strings in the following function)
        if verifyReleaseString(job.homePackage) == "NULL":
            return ec, pilotErrorDiag, siteroot, ""

        # install the trf in the work dir if it is not installed on the site
        # special case for nightlies (rel_N already in siteroot path, so do not add it)

        if "rel_" in job.homePackage:
            installDir = siteroot
        else:
            installDir = os.path.join(siteroot, job.homePackage)
        installDir = installDir.replace('//','/')

        tolog("Atlas release: %s" % (job.release))
        tolog("Job home package: %s" % (job.homePackage))
        tolog("Trf installation dir: %s" % (installDir))

        return ec, pilotErrorDiag, siteroot, installDir

    def getInstallDir2(self, job, workdir, siteroot, swbase, cmtconfig):
        """ Get the path to the release, install patch if necessary """

        ec = 0
        pilotErrorDiag = ""

        # do not proceed for unset homepackage strings (treat as release strings in the following function)
        if verifyReleaseString(job.homePackage) == "NULL":
            return ec, pilotErrorDiag, siteroot, ""

        # install the trf in the work dir if it is not installed on the site
        # special case for nightlies (rel_N already in siteroot path, so do not add it)
        if "rel_" in job.homePackage:
            installDir = siteroot
        else:
            installDir = os.path.join(siteroot, job.homePackage)
        installDir = installDir.replace('//','/')

        tolog("Atlas release: %s" % (job.release))
        tolog("Job home package: %s" % (job.homePackage))
        tolog("Trf installation dir: %s" % (installDir))

        # special case for nightlies (no RunTime dir)
        if "rel_" in job.homePackage:
            sfile = os.path.join(installDir, "setup.sh")
        else:
            sfile = installDir + ('/%sRunTime/cmt/setup.sh' % job.homePackage.split('/')[0])
        sfile = sfile.replace('//','/')
        if not os.path.isfile(sfile):

#            pilotErrorDiag = "Patch not available (will not attempt dynamic patch installation)"
#            tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
#            ec = self.__error.ERR_DYNTRFINST

# uncomment this section (and remove the comments in the above three lines) for dynamic path installation

            tolog("!!WARNING!!3000!! Trf setup file does not exist at: %s" % (sfile))
            tolog("Will try to install trf in work dir...")

            # Install trf in the run dir
            try:
                ec, pilotErrorDiag = self.installPyJobTransforms(job.release, job.homePackage, swbase, cmtconfig)
            except Exception, e:
                pilotErrorDiag = "installPyJobTransforms failed: %s" % str(e)
                tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
                ec = self.__error.ERR_DYNTRFINST
            else:
                if ec == 0:
                    tolog("Successfully installed trf")
                    installDir = workdir + "/" + job.homePackage

                    # replace siteroot="$SITEROOT" with siteroot=rundir
                    os.environ['SITEROOT'] = workdir
                    siteroot = workdir

# comment until here

        else:
            tolog("Found trf setup file: %s" % (sfile))
            tolog("Using install dir = %s" % (installDir))

        return ec, pilotErrorDiag, siteroot, installDir

    def installPyJobTransforms(self, release, package, swbase, cmtconfig):
        """ Install new python based TRFS """

        status = False
        pilotErrorDiag = ""

        import string

        if package.find('_') > 0: # jobdef style (e.g. "AtlasProduction_12_0_7_2")
            ps = package.split('_')
            if len(ps) == 5:
                status = True
                # dotver = string.join(ps[1:], '.')
                # pth = 'AtlasProduction/%s' % dotver
            else:
                status = False
        else: # Panda style (e.g. "AtlasProduction/12.0.3.2")
            # Create pacman package = AtlasProduction_12_0_7_1
            ps = package.split('/')
            if len(ps) == 2:
                ps2 = ps[1].split('.')
                if len(ps2) == 4 or len(ps2) == 5:
                    dashver = string.join(ps2, '_')
                    pacpack = '%s_%s' % (ps[0], dashver)
                    tolog("Pacman package name: %s" % (pacpack))
                    status = True
                else:
                    status = False
            else:
                status = False

        if not status:
            pilotErrorDiag = "installPyJobTransforms: Prod cache has incorrect format: %s" % (package)
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            return self.__error.ERR_DYNTRFINST, pilotErrorDiag

        # Check if it exists already in rundir
        tolog("Checking for path: %s" % (package))

        # Current directory should be the job workdir at this point
        if os.path.exists(package):
            tolog("Found production cache, %s, in run directory" % (package))
            return 0, pilotErrorDiag

        # Install pacman
        status, pilotErrorDiag = self.installPacman()
        if status:
            tolog("Pacman installed correctly")
        else:
            return self.__error.ERR_DYNTRFINST, pilotErrorDiag

        # Prepare release setup command
        if self.useAtlasSetup(swbase, release, package, cmtconfig):
            setup_pbuild = self.getProperASetup(swbase, release, package, cmtconfig, source=False)
        else:
            setup_pbuild = '%s/%s/cmtsite/setup.sh -tag=%s,AtlasOffline,%s' % (swbase, release, release, cmtconfig)
        got_JT = False
        caches = [
            'http://classis01.roma1.infn.it/pacman/Production/cache',
            'http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/Analysis/cache'
            ]

        # shuffle list so same cache is not hit by all jobs
        from random import shuffle
        shuffle(caches)
        for cache in caches:
            # Need to setup some CMTROOT first
            # Pretend platfrom for non-slc3, e.g. centOS on westgrid
            # Parasitacally, while release is setup, get DBRELEASE version too
            cmd = 'source %s' % (setup_pbuild)
            cmd+= ';CMT_=`echo $CMTCONFIG | sed s/-/_/g`'
            cmd+= ';cd pacman-*;source ./setup.sh;cd ..;echo "y"|'
            cmd+= 'pacman -pretend-platform SLC -get %s:%s_$CMT_ -trust-all-caches'%\
                  (cache, pacpack)
            tolog('Pacman installing JT %s from %s' % (pacpack, cache))

            exitcode, output = timedCommand(cmd, timeout=60*60)
            if exitcode != 0:
                pilotErrorDiag = "installPyJobTransforms failed: %s" % str(output)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            else:
                tolog('Installed JobTransforms %s from %s' % (pacpack, cache))
                got_JT = True
                break

        if got_JT:
            ec = 0
        else:
            ec = self.__error.ERR_DYNTRFINST

        return ec, pilotErrorDiag

    def installPacman(self):
        """ Pacman installation """

        pilotErrorDiag = ""

        # Pacman version
        pacman = 'pacman-3.18.3.tar.gz'

        urlbases = [
            'http://physics.bu.edu/~youssef/pacman/sample_cache/tarballs',
            'http://atlas.web.cern.ch/Atlas/GROUPS/SOFTWARE/OO/Production'
            ]

        # shuffle list so same server is not hit by all jobs
        from random import shuffle
        shuffle(urlbases)
        got_tgz = False
        for urlbase in urlbases:
            url = urlbase + '/' + pacman
            tolog('Downloading: %s' % (url))
            try:
                # returns httpMessage
                from urllib import urlretrieve
                (filename, msg) = urlretrieve(url, pacman)
                if 'content-type' in msg.keys():
                    if msg['content-type'] == 'application/x-gzip':
                        got_tgz = True
                        tolog('Got %s' % (url))
                        break
                    else:
                        tolog('!!WARNING!!4000!! Failed to get %s' % (url))
            except Exception ,e:
                tolog('!!WARNING!!4000!! URL: %s throws: %s' % (url, e))

        if got_tgz:
            tolog('Success')
            cmd = 'tar -zxf %s' % (pacman)
            tolog("Executing command: %s" % (cmd))
            (exitcode, output) = commands.getstatusoutput(cmd)
            if exitcode != 0:
                # Got a tgz but can't unpack it. Could try another source but will fail instead.
                pilotErrorDiag = "%s failed: %d : %s" % (cmd, exitcode, output)
                tolog('!!FAILED!!4000!! %s' % (pilotErrorDiag))
                return False, pilotErrorDiag
            else:
                tolog('Pacman tarball install succeeded')
                return True, pilotErrorDiag
        else:
            pilotErrorDiag = "Failed to get %s from any source url" % (pacman)
            tolog('!!FAILED!!4000!! %s' % (pilotErrorDiag))
            return False, pilotErrorDiag

    def getCmtsiteCmd(self, swbase, atlasRelease, homePackage, cmtconfig, siteroot=None, analysisJob=False, cacheDir=None, cacheVer=None):
        """ Get the cmtsite setup command """

        ec = 0
        pilotErrorDiag = ""
        cmd = ""

        if verifyReleaseString(homePackage) == "NULL":
            homePackage = ""

        # Handle sites using builds area in a special way
        if swbase[-len('builds'):] == 'builds' or verifyReleaseString(atlasRelease) == "NULL":
            _path = swbase
        else:
            _path = os.path.join(swbase, atlasRelease)

        if self.useAtlasSetup(swbase, atlasRelease, homePackage, cmtconfig):
            # homePackage=AnalysisTransforms-AtlasTier0_15.5.1.6
            # cacheDir = AtlasTier0
            # cacheVer = 15.5.1.6
            m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', homePackage)
            if m_cacheDirVer != None:
                cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, atlasRelease)
            elif "," in homePackage or "rel_" in homePackage: # if nightlies are used, e.g. homePackage = "AtlasProduction,rel_0"
                cacheDir = homePackage
            cmd = self.getProperASetup(swbase, atlasRelease, homePackage, cmtconfig, cacheVer=cacheVer, cacheDir=cacheDir)
        else:
            # Get the tags
            tags = self.getTag(analysisJob, swbase, atlasRelease, cacheDir, cacheVer)

            ec, pilotErrorDiag, status = self.isForceConfigCompatible(swbase, atlasRelease, homePackage, cmtconfig, siteroot=siteroot)
            if ec == self.__error.ERR_MISSINGINSTALLATION:
                return ec, pilotErrorDiag, cmd
            else:
                if status:
                    if "slc5" in cmtconfig and os.path.exists("%s/gcc43_inst" % (_path)):
                        cmd = "source %s/gcc43_inst/setup.sh;export CMTCONFIG=%s;" % (_path, cmtconfig)
                    elif "slc5" in cmtconfig and "slc5" in swbase and os.path.exists(_path):
                        cmd = "source %s/setup.sh;export CMTCONFIG=%s;" % (_path, cmtconfig)
                    else:
                        cmd = "export CMTCONFIG=%s;" % (cmtconfig)
                    cmd += "source %s/cmtsite/setup.sh %s,forceConfig" % (_path, tags)
                else:
                    cmd = "source %s/cmtsite/setup.sh %s" % (_path, tags)

        return ec, pilotErrorDiag, cmd

    def getCacheInfo(self, m_cacheDirVer, atlasRelease):
        """ Get the cacheDir and cacheVer """

        cacheDirVer = m_cacheDirVer.group(1)
        if re.search('_', cacheDirVer) != None:
            cacheDir = cacheDirVer.split('_')[0]
            cacheVer = re.sub("^%s_" % cacheDir, '', cacheDirVer)
        else:
            cacheDir = 'AtlasProduction'
            if atlasRelease in ['13.0.25']:
                cacheDir = 'AtlasPoint1'
            cacheVer = cacheDirVer
        tolog("cacheDir = %s" % (cacheDir))
        tolog("cacheVer = %s" % (cacheVer))

        return cacheDir, cacheVer

    def getTag(self, analysisJob, path, release, cacheDir, cacheVer):
        """ Define the setup tags """

        _setup = False
        tag = "-tag="

        if analysisJob:
            if cacheDir and cacheDir != "" and cacheVer and cacheVer != "" and cacheVer.count('.') < 4:
                # E.g. -tag=AtlasTier0,15.5.1.6,32,setup
                tag += "%s" % (cacheDir)
                tag += ",%s" % (cacheVer)
                _setup = True
            else:
                # E.g. -tag=AtlasOffline,15.5.1
                tag += "AtlasOffline"
                if verifyReleaseString(release) != "NULL":
                    tag += ",%s" % (release)
            # only add the "32" part if CMTCONFIG has been out-commented in the requirements file
            if self.isCMTCONFIGOutcommented(path, release):
                tag += ",32"
            if _setup:
                tag += ",setup"
        else:
            # for production jobs
            tag = "-tag=AtlasOffline"
            if verifyReleaseString(release) != "NULL":
                tag += ",%s" % (release)

        # always add the runtime
        tag += ",runtime"

        return tag

    def isCMTCONFIGOutcommented(self, path, release):
        """ Is CMTCONFIG out-commented in requirements file? """

        status = False
        filename = "%s%s/cmtsite/requirements" % (path, release)
        if os.path.exists(filename):
            cmd = "grep CMTCONFIG %s" % (filename)
            ec, rs = commands.getstatusoutput(cmd)
            if ec == 0:
                if rs.startswith("#"):
                    status = True

        return status

    def verifyCmtsiteCmd(self, exitcode, _pilotErrorDiag):
        """ Verify the cmtsite command """

        pilotErrorDiag = "unknown"

        if "#CMT> Warning: template <src_dir> not expected in pattern install_scripts (from TDAQCPolicy)" in _pilotErrorDiag:
            tolog("Detected CMT warning (return code %d)" % (exitcode))
            tolog("Installation setup command passed test (with precaution)")
        elif "Error:" in _pilotErrorDiag or "Fatal exception:" in _pilotErrorDiag:
            pilotErrorDiag = "Detected severe CMT error: %d, %s" % (exitcode, _pilotErrorDiag)
            tolog("!!WARNING!!2992!! %s" % (pilotErrorDiag))
        elif exitcode != 0:
            if "Command time-out" in _pilotErrorDiag:
                pilotErrorDiag = "cmtsite command was timed out: %s, %s" % (str(exitcode), _pilotErrorDiag)
            else:
                from futil import is_timeout
                if is_timeout(exitcode):
                    pilotErrorDiag = "cmtsite command was timed out: %d, %s" % (exitcode, _pilotErrorDiag)
                else:
                    if "timed out" in _pilotErrorDiag:
                        pilotErrorDiag = "cmtsite command was timed out: %d, %s" % (exitcode, _pilotErrorDiag)
                    else:
                        pilotErrorDiag = "cmtsite command failed: %d, %s" % (exitcode, _pilotErrorDiag)

            tolog("!!WARNING!!2992!! %s" % (pilotErrorDiag))
        else:
            tolog("Release test command returned exit code %d" % (exitcode))
            pilotErrorDiag = ""

        return pilotErrorDiag

    def verifyCMTCONFIG(self, releaseCommand, cmtconfig_required, siteroot="", cacheDir="", cacheVer=""):
        """ Make sure that the required CMTCONFIG match that of the local system """
        # ...and extract CMTCONFIG, SITEROOT, ATLASVERSION, ATLASPROJECT

        status = True
        pilotErrorDiag = ""

        _cmd = "%s;echo CMTCONFIG=$CMTCONFIG;echo SITEROOT=$SITEROOT;echo ATLASVERSION=$AtlasVersion;echo ATLASPROJECT=$AtlasProject" % (releaseCommand)

        exitcode, output = timedCommand(_cmd, timeout=getProperTimeout(_cmd))
        tolog("Command output: %s" % (output))

        # Verify the cmtsite command
        pilotErrorDiag = self.verifyCmtsiteCmd(exitcode, output)
        if pilotErrorDiag != "":
            return False, pilotErrorDiag, siteroot, "", ""

        # Get cmtConfig
        re_cmtConfig = re.compile('CMTCONFIG=(.+)')
        _cmtConfig = re_cmtConfig.search(output)
        if _cmtConfig:
            cmtconfig_local = _cmtConfig.group(1)
        else:
            tolog("CMTCONFIG not found in command output: %s" % (output))
            cmtconfig_local = ""

        # Set siteroot if not already set
        if siteroot == "":
            re_sroot = re.compile('SITEROOT=(.+)')
            _sroot = re_sroot.search(output)
            if _sroot:
                siteroot = _sroot.group(1)
            else:
                tolog("SITEROOT not found in command output: %s" % (output))

        # Get atlasVersion
        re_atlasVersion = re.compile('ATLASVERSION=(.+)')
        _atlasVersion = re_atlasVersion.search(output)
        if _atlasVersion:
            atlasVersion = _atlasVersion.group(1)
        else:
            tolog("AtlasVersion not found in command output: %s" % (output))
            if cacheVer != "":
                tolog("AtlasVersion will be set to: %s" % (cacheVer))
            atlasVersion = cacheVer

        # Get atlasProject
        re_atlasProject = re.compile('ATLASPROJECT=(.+)')
        _atlasProject = re_atlasProject.search(output)
        if _atlasProject:
            atlasProject = _atlasProject.group(1)
        else:
            tolog("AtlasProject not found in command output: %s" % (output))
            if cacheDir != "":
                tolog("AtlasProject will be set to: %s" % (cacheDir))
            atlasProject = cacheDir

        # Verify cmtconfig
        if cmtconfig_local == "":
            cmtconfig_local = "local cmtconfig not set"
        elif cmtconfig_local == "NotSupported":
            pilotErrorDiag = "CMTCONFIG is not supported on the local system: %s (required of task: %s)" %\
                             (cmtconfig_local, cmtconfig_required)
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            status = False
        elif cmtconfig_local == "NotAvailable":
            if "non-existent" in output:
                output = output.replace("\n",",")
                pilotErrorDiag = "Installation problem: %s" % (output)
            else:
                pilotErrorDiag = "CMTCONFIG is not available on the local system: %s (required of task: %s)" %\
                                 (cmtconfig_local, cmtconfig_required)
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            status = False

        if cmtconfig_required == "" or cmtconfig_required == None:
            cmtconfig_required = "required cmtconfig not set"

        # Does the required CMTCONFIG match that of the local system?
        if status and cmtconfig_required != cmtconfig_local:
            pilotErrorDiag = "Required CMTCONFIG (%s) incompatible with that of local system (%s)" %\
                             (cmtconfig_required, cmtconfig_local)
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            status = False

        return status, pilotErrorDiag, siteroot, atlasVersion, atlasProject

    def checkCMTCONFIG(self, cmd1, cmtconfig, atlasRelease, siteroot="", cacheDir="", cacheVer=""):
        """ Make sure the CMTCONFIG is available and valid """

        ec = 0
        pilotErrorDiag = ""
        atlasProject = ""
        atlasVersion = ""

        # verify CMTCONFIG for set release strings only
        if verifyReleaseString(atlasRelease) != "NULL":
            status, pilotErrorDiag, siteroot, atlasVersion, atlasProject = self.verifyCMTCONFIG(cmd1, cmtconfig, siteroot=siteroot, cacheDir=cacheDir, cacheVer=cacheVer)
            if status:
                tolog("CMTCONFIG verified for release: %s" % (atlasRelease))
                if siteroot != "":
                    tolog("Got siteroot = %s from CMTCONFIG verification" % (siteroot))
                if atlasVersion != "":
                    tolog("Got atlasVersion = %s from CMTCONFIG verification" % (atlasVersion))
                if atlasProject != "":
                    tolog("Got atlasProject = %s from CMTCONFIG verification" % (atlasProject))
            else:
                if "Installation problem" in pilotErrorDiag:
                    errorText = "Installation problem discovered in release: %s" % (atlasRelease)
                    ec = self.__error.ERR_MISSINGINSTALLATION
                elif "timed out" in pilotErrorDiag:
                    errorText = "Command used for extracting CMTCONFIG, SITEROOT, etc, timed out"
                    ec = self.__error.ERR_COMMANDTIMEOUT
                else:
                    errorText = "CMTCONFIG verification failed for release: %s" % (atlasRelease)
                    ec = self.__error.ERR_CMTCONFIG
                tolog("!!WARNING!!1111!! %s" % (errorText))
        else:
            tolog("Skipping CMTCONFIG verification for unspecified release")

        return ec, pilotErrorDiag, siteroot, atlasVersion, atlasProject

    def getProdCmd2(self, installDir, homePackage):
        """ Get cmd2 for production jobs """

        pilotErrorDiag = ""

        # Define cmd2
        try:
            # Special case for nightlies
            if "rel_" in homePackage or "AtlasP1HLT" in homePackage or "AtlasHLT" in homePackage: # rel_N is already in installDir, do not add like below
                cmd2 = '' #unset CMTPATH;'
            else:
                cmd2 = 'unset CMTPATH;cd %s/%sRunTime/cmt;source ./setup.sh;cd -;' % (installDir, homePackage.split('/')[0])

                # Correct setup for athena post 14.5 (N.B. harmless for version < 14.5)
                cmd2 += 'export AtlasVersion=%s;export AtlasPatchVersion=%s' % (homePackage.split('/')[-1], homePackage.split('/')[-1])
        except Exception, e:
            pilotErrorDiag = "Bad homePackage format: %s, %s" % (homePackage, str(e))
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            cmd2 = ""

        return cmd2, pilotErrorDiag

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

    def getAnalyCmd2(self, swbase, cmtconfig, homePackage, atlasRelease):
        """ Return a proper cmd2 setup command """

        cacheDir = None
        cacheVer = None
        cmd2 = ""

        # cannot set cmd2 for unset release/homepackage strings
        if verifyReleaseString(atlasRelease) == "NULL" or verifyReleaseString(homePackage) == "NULL":
            return cmd2, cacheDir, cacheVer

        # homePackage=AnalysisTransforms-AtlasTier0_15.5.1.6
        # cacheDir = AtlasTier0
        # cacheVer = 15.5.1.6
        m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', homePackage)
        if m_cacheDirVer != None:
            cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, atlasRelease)
            if not self.useAtlasSetup(swbase, atlasRelease, homePackage, cmtconfig):
                cmd2 = "export CMTPATH=$SITEROOT/%s/%s" % (cacheDir, cacheVer)

        return cmd2, cacheDir, cacheVer

    def updateAnalyCmd2(self, cmd2, atlasVersion, atlasProject, useAsetup):
        """ Add AtlasVersion and AtlasProject to cmd2 """

        # Add everything to cmd2 unless AtlasSetup is used
        if not useAsetup:
            if atlasVersion != "" and atlasProject != "":
                if cmd2 == "" or cmd2.endswith(";"):
                    pass
                else:
                    cmd2 += ";"
                cmd2 += "export AtlasVersion=%s;export AtlasProject=%s" % (atlasVersion, atlasProject)

        return cmd2

    def setPython(self, site_root, atlasRelease, homePackage, cmtconfig, sitename):
        """ set the python executable """

        ec = 0
        pilotErrorDiag = ""
        pybin = ""

        if os.environ.has_key('VO_ATLAS_SW_DIR') and verifyReleaseString(atlasRelease) != "NULL":
            #ec, pilotErrorDiag, _pybin = self.findPythonInRelease(site_root, atlasRelease, homePackage, cmtconfig, sitename)
            #if ec == self.__error.ERR_MISSINGINSTALLATION:
            #    return ec, pilotErrorDiag, pybin

            #if _pybin != "":
            #    pybin = _pybin

            #pybin = "`which python`"
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

    def findPythonInRelease(self, siteroot, atlasRelease, homePackage, cmtconfig, sitename):
        """ Set the python executable in the release dir (LCG sites only) """

        ec = 0
        pilotErrorDiag = ""
        py = ""

        tolog("Trying to find a python executable for release: %s" % (atlasRelease))
        scappdir = readpar('appdir')

        # only use underscored cmtconfig paths on older cvmfs systems and only for now (remove at a later time)
        _cmtconfig = cmtconfig.replace("-", "_")

        if scappdir != "":
            _swbase = self.getLCGSwbase(scappdir)
            tolog("Using swbase: %s" % (_swbase))

            # get the site information object
            si = getSiteInformation(self.__experiment)

            if self.useAtlasSetup(_swbase, atlasRelease, homePackage, cmtconfig):
                cmd = self.getProperASetup(_swbase, atlasRelease, homePackage, cmtconfig, tailSemiColon=True)
                tolog("Using new AtlasSetup: %s" % (cmd))
            elif os.path.exists("%s/%s/%s/cmtsite/setup.sh" % (_swbase, _cmtconfig, atlasRelease)) and (si.isTier3() or "CERNVM" in sitename):
                # use cmtconfig sub dir on CERNVM or tier3 (actually for older cvmfs systems)
                cmd  = "source %s/%s/%s/cmtsite/setup.sh -tag=%s,32,runtime;" % (_swbase, _cmtconfig, atlasRelease, atlasRelease)
            else:
                ec, pilotErrorDiag, status = self.isForceConfigCompatible(_swbase, atlasRelease, homePackage, cmtconfig, siteroot=siteroot)
                if ec == self.__error.ERR_MISSINGINSTALLATION:
                    return ec, pilotErrorDiag, py
                else:
                    if status:
                        if "slc5" in cmtconfig and os.path.exists("%s/%s/gcc43_inst" % (_swbase, atlasRelease)):
                            cmd = "source %s/%s/gcc43_inst/setup.sh;export CMTCONFIG=%s;" % (_swbase, atlasRelease, cmtconfig)
                        elif "slc5" in cmtconfig and "slc5" in _swbase and os.path.exists("%s/%s" % (_swbase, atlasRelease)):
                            cmd = "source %s/%s/setup.sh;export CMTCONFIG=%s;" % (_swbase, atlasRelease, cmtconfig)
                        else:
                            cmd = "export CMTCONFIG=%s;" % (cmtconfig)
                        cmd += "source %s/%s/cmtsite/setup.sh -tag=AtlasOffline,%s,forceConfig,runtime;" % (_swbase, atlasRelease, atlasRelease)
                    else:
                        cmd  = "source %s/%s/cmtsite/setup.sh -tag=%s,32,runtime;" % (_swbase, atlasRelease, atlasRelease)
        else:
            vo_atlas_sw_dir = os.path.expandvars('$VO_ATLAS_SW_DIR')
            if "gcc43" in cmtconfig and vo_atlas_sw_dir != '' and os.path.exists('%s/software/slc5' % (vo_atlas_sw_dir)):
                cmd  = "source $VO_ATLAS_SW_DIR/software/slc5/%s/setup.sh;" % (atlasRelease)
                tolog("Found explicit slc5 dir in path: %s" % (cmd))
            else:
                # no known appdir, default to VO_ATLAS_SW_DIR
                _appdir = vo_atlas_sw_dir
                _swbase = self.getLCGSwbase(_appdir)
                tolog("Using swbase: %s" % (_swbase))
                if self.useAtlasSetup(_swbase, atlasRelease, homePackage, cmtconfig):
                    cmd = self.getProperASetup(_swbase, atlasRelease, homePackage, cmtconfig, tailSemiColon=True)
                    tolog("Using new AtlasSetup: %s" % (cmd))
                else:
                    _path = os.path.join(_appdir, "software/%s/cmtsite/setup.sh" % (atlasRelease))
                    if os.path.exists(_path):
                        cmd = "source " + _path + ";"
                    else:
                        cmd = ""
                        tolog("!!WARNING!!1888!! No known path for setup script (using default python version)")

        cmd += "which python"
        exitcode, output = timedCommand(cmd, timeout=getProperTimeout(cmd))

        if exitcode == 0:
            if output.startswith('/'):
                tolog("Found: %s" % (output))
                py = output
            else:
                if '\n' in output:
                    output = output.split('\n')[-1]

                if output.startswith('/'):
                    tolog("Found: %s" % (output))
                    py = output
                else:
                    tolog("!!WARNING!!4000!! No python executable found in release dir: %s" % (output))
                    tolog("!!WARNING!!4000!! Will use default python")
                    py = "python"
        else:
            tolog("!!WARNING!!4000!! Find command failed: %d, %s" % (exitcode, output))
            tolog("!!WARNING!!4000!! Will use default python")
            py = "python"

        return ec, pilotErrorDiag, py

    def getLCGSwbase(self, scappdir):
        """ Return the LCG swbase """

        if os.path.exists(os.path.join(scappdir, 'software/releases')):
            _swbase = os.path.join(scappdir, 'software/releases')
        elif os.path.exists(os.path.join(scappdir, 'software')):
            _swbase = os.path.join(scappdir, 'software')
        else:
            _swbase = scappdir

        return _swbase

    def getProdCmd3(self, cmd, pybin, jobtrf, jobPars):
        """ Prepare the cmd3 command with the python from the release and the full path to the trf """
        # When python is invoked using the full path, it also needs the full path to the script

        return "%s %s" % (jobtrf, jobPars)
        #return "%s `which %s` %s" % (pybin, jobtrf, jobPars)
        #return "%s %s %s" % (pybin, jobtrf, jobPars)

    def getProdCmd3Old(self, cmd, pybin, jobtrf, jobPars):
        """ Prepare the cmd3 command with the python from the release and the full path to the trf """
        # When python is invoked using the full path, it also needs the full path to the script

        # First try to figure out where the trf is inside the release
        if not cmd.endswith(";"):
            cmd += ";"
        _cmd = "%swhich %s" % (cmd, jobtrf)
        _timedout = False

        exitcode, _trf = timedCommand(_cmd, timeout=getProperTimeout(cmd))
        if exitcode != 0:
            _timedout = True
        tolog("Trf: %s" % (_trf))

        # split the output if necessary (the path should be the last entry)
        if "\n" in _trf:
            _trf = _trf.split("\n")[-1]
            tolog("Trf: %s (extracted)" % (_trf))

        # could the trf be found?
        if "which: no" in _trf or jobtrf not in _trf or _timedout:
            tolog("!!WARNING!!2999!! Will not use python from the release since the trf path could not be figured out")
            cmd3 = "%s %s" % (jobtrf, jobPars)
        else:
            tolog("Will use python from the release: %s" % (pybin))
            tolog("Path to trf: %s" % (_trf))
            cmd3 = "%s %s %s" % (pybin, _trf, jobPars)

        return cmd3

    def addEnvVars2Cmd(self, cmd, jobId, processingType, sitename, analysisJob):
        """ Add env variables """

        _sitename = 'export PANDA_RESOURCE=\"%s\";' % (sitename)
        _frontier1 = 'export FRONTIER_ID=\"[%s]\";' % (jobId)
        _frontier2 = 'export CMSSW_VERSION=$FRONTIER_ID;'
        _ttc = ''

        # Unset ATHENA_PROC_NUMBER if set for event service Merge jobs
        if "Merge_tf" in cmd and os.environ.has_key('ATHENA_PROC_NUMBER'):
            _unset = "unset ATHENA_PROC_NUMBER;"
        else:
            _unset = ""

        _coreCount = ""
        if analysisJob:
            _ttc = 'export ROOT_TTREECACHE_SIZE=1;'
            try:
                coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
            except:
                _coreCount = 'export ROOTCORE_NCPUS=1;'
            else:
                _coreCount = 'export ROOTCORE_NCPUS=%d;' % (coreCount)
        if processingType == "":
            _rucio = ''
            tolog("!!WARNING!!1887!! RUCIO_APPID needs job.processingType but it is not set!")
        else:
            _rucio = 'export RUCIO_APPID=\"%s\";' % (processingType)
        _rucio += 'export RUCIO_ACCOUNT=\"pilot\";'

        return _sitename + _ttc + _frontier1 + _frontier2 + _rucio + _coreCount + _unset + cmd

    def getEnvVars2Cmd(self, jobId, processingType, sitename, analysisJob):
        """ Return array with enviroment variables """

        variables = []
        variables.append('export PANDA_RESOURCE=\"%s\";' % (sitename))
        variables.append('export FRONTIER_ID=\"[%s]\";' % (jobId))
        variables.append('export CMSSW_VERSION=$FRONTIER_ID;')
        variables.append('export ROOT_TTREECACHE_SIZE=1;')

        _coreCount = ""
        if analysisJob:
            try:
                coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
            except:
                pass
            else:
                _coreCount = 'export ROOTCORE_NCPUS=%d;' % (coreCount)
                variables.append(_coreCount)

        if processingType == "":
            tolog("!!WARNING!!1887!! RUCIO_APPID needs job.processingType but it is not set!")
        else:
            variables.append('export RUCIO_APPID=\"%s\";' % (processingType))
        variables.append('export RUCIO_ACCOUNT=\"pilot\";')

        return variables

    def isForceConfigCompatible(self, _dir, release, homePackage, cmtconfig, siteroot=None):
        """ Test if the installed AtlasSettings and AtlasLogin versions are compatible with forceConfig """
        # The forceConfig cmt tag was introduced in AtlasSettings-03-02-07 and AtlasLogin-00-03-07

        status = True
        ec = 0
        pilotErrorDiag = ""

        # only perform the forceConfig test for set release strings
        if verifyReleaseString(release) == "NULL":
            return ec, pilotErrorDiag, False

        names = {"AtlasSettings":"AtlasSettings-03-02-07", "AtlasLogin":"AtlasLogin-00-03-07" }
        for name in names.keys():
            try:
                ec, pilotErrorDiag, v, siteroot = self.getHighestVersionDir(release, homePackage, name, _dir, cmtconfig, siteroot=siteroot)
            except Exception, e:
                tolog("!!WARNING!!2999!! Exception caught: %s" % str(e))
                v = None
            if v:
                if v >= names[name]:
                    tolog("%s version verified: %s" % (name, v))
                else:
                    tolog("%s version too old: %s (older than %s, not forceConfig compatible)" % (name, v, names[name]))
                    status = False
            else:
                tolog("%s version not verified (not forceConfig compatible)" % (name))
                status = False

        return ec, pilotErrorDiag, status

    def getHighestVersionDir(self, release, homePackage, name, swbase, cmtconfig, siteroot=None):
        """ Grab the directory (AtlasLogin, AtlasSettings) with the highest version number """
        # e.g. v = AtlasLogin-00-03-26

        highestVersion = None
        ec = 0
        pilotErrorDiag = ""

        # get the siteroot
        if not siteroot:
            ec, pilotErrorDiag, status, siteroot, cmtconfig = self.getProperSiterootAndCmtconfig(swbase, release, homePackage, cmtconfig)
        else:
            status = True

        if ec != 0:
            return ec, pilotErrorDiag, None, siteroot

        if status and siteroot != "" and os.path.exists(os.path.join(siteroot, name)):
            _dir = os.path.join(siteroot, name)
        else:
            if swbase[-len('builds'):] == 'builds':
                _dir = os.path.join(swbase, name)
            else:
                _dir = os.path.join(swbase, release, name)

        if not os.path.exists(_dir):
            tolog("Directory does not exist: %s" % (_dir))
            return ec, pilotErrorDiag, None, siteroot

        tolog("Probing directory: %s" % (_dir))
        if os.path.exists(_dir):
            dirs = os.listdir(_dir)
            _dirs = []
            if dirs != []:
                tolog("Found directories: %s" % str(dirs))
                for d in dirs:
                    if d.startswith(name):
                        _dirs.append(d)
                if _dirs != []:
                    # sort the directories
                    _dirs.sort()
                    # grab the directory with the highest version
                    highestVersion = _dirs[-1]
                    tolog("Directory with highest version: %s" % (highestVersion))
                else:
                    tolog("WARNING: Found no %s dirs in %s" % (name, _dir))
            else:
                tolog("WARNING: Directory is empty: %s" % (_dir))
        else:
            tolog("Directory does not exist: %s" % (_dir))

        return ec, pilotErrorDiag, highestVersion, siteroot

    def getProperSiterootAndCmtconfig(self, swbase, release, homePackage, _cmtconfig, cmtconfig_alternatives=None):
        """ return a proper $SITEROOT and cmtconfig """

        status = False
        siteroot = ""
        ec = 0 # only non-zero for fatal errors (missing installation)
        pilotErrorDiag = ""

        # make sure the cmtconfig_alternatives is not empty/not set
        if not cmtconfig_alternatives:
            cmtconfig_alternatives = [_cmtconfig]

        if swbase[-len('builds'):] == 'builds':
            status = True
            return ec, pilotErrorDiag, status, swbase, _cmtconfig

        # loop over all available cmtconfig's until a working one is found (the default cmtconfig value is the first to be tried)
        for cmtconfig in cmtconfig_alternatives:
            ec = 0
            pilotErrorDiag = ""
            tolog("Testing cmtconfig=%s" % (cmtconfig))

            if self.useAtlasSetup(swbase, release, homePackage, cmtconfig):
                cmd = self.getProperASetup(swbase, release, homePackage, cmtconfig, tailSemiColon=True)
            elif "slc5" in cmtconfig and "gcc43" in cmtconfig:
                cmd = "source %s/%s/cmtsite/setup.sh -tag=AtlasOffline,%s,%s,runtime" % (swbase, release, release, cmtconfig)
            else:
                cmd = "source %s/%s/cmtsite/setup.sh -tag=AtlasOffline,%s,runtime" % (swbase, release, release)

            # verify that the setup path actually exists before attempting the source command
            ec, pilotErrorDiag = self.verifySetupCommand(cmd)
            if ec != 0:
                pilotErrorDiag = "getProperSiterootAndCmtconfig: Missing installation: %s" % (pilotErrorDiag)
                tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                ec = self.__error.ERR_MISSINGINSTALLATION
                continue

            # do not test the source command, it is enough to verify its existence
            (exitcode, output) = timedCommand(cmd, timeout=getProperTimeout(cmd))

            if exitcode == 0:
                # a proper cmtconfig has been found, now set the SITEROOT
                # e.g. /cvmfs/atlas.cern.ch/repo/sw/software/i686-slc5-gcc43-opt/17.2.11

                # note: this format (using cmtconfig is only valid on CVMFS, not on AFS)
                # VO_ATLAS_RELEASE_DIR is only set on AFS (CERN)
                if ("AtlasP1HLT" in homePackage or "AtlasHLT" in homePackage) and os.environ.has_key('VO_ATLAS_RELEASE_DIR'):
                    tolog("Encountered HLT homepackage: %s (must use special siteroot)" % (homePackage))
                    siteroot = os.path.join(swbase, release)
                elif homePackage.startswith('AthSimulation'):
                    tolog("Encountered an Ath* release: %s" % (homePackage))
                    siteroot = self.getSiterootWithHomepackage(swbase, homePackage, cmtconfig, release)
                else:
                    # default SITEROOT on CVMFS
                    if "/cvmfs" in swbase:
                        siteroot = os.path.join(os.path.join(swbase, cmtconfig), release)
                    else:
                        siteroot = os.path.join(swbase, release)

                siteroot = siteroot.replace('//','/')

                # make sure that the path actually exists
                if os.path.exists(siteroot):
                    tolog("SITEROOT path has been defined and exists: %s" % (siteroot))
                    status = True
                    break
                else:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: cmtconfig %s has been confirmed but SITEROOT does not exist: %s" % (cmtconfig, siteroot)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_MISSINGINSTALLATION
                    break

            elif exitcode != 0 or "Error:" in output or "(ERROR):" in output:
                # if time out error, don't bother with trying another cmtconfig

                tolog("ATLAS setup for SITEROOT failed: ec=%s, output=%s" % (str(exitcode), output))

                if "No such file or directory" in output:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: Missing installation: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_MISSINGINSTALLATION
                    continue
                elif "Error:" in output:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: Caught CMT error: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_SETUPFAILURE
                    continue
                elif "AtlasSetup(ERROR):" in output:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: Caught AtlasSetup error: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_SETUPFAILURE
                    continue
                elif "timed out" in output:
                    # CVMFS problem, no point in continuing
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: CVMFS setup command timed out: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_COMMANDTIMEOUT
                    break

        # reset errors if siteroot was found
        if status:
            ec = 0
            pilotErrorDiag = ""
        return ec, pilotErrorDiag, status, siteroot, cmtconfig

    def getVerifiedAtlasSetupPath(self, swbase, release, homePackage, cmtconfig):
        """ Get a verified asetup path"""

        rel_N = None
        path = None
        skipVerification = False # verification not possible for more complicated setup (nightlies)
        if 'HPC_' in readpar("catchall"):
            skipVerification = True # verification not possible for more complicated setup (nightlies)

        # First try with the cmtconfig in the path. If that fails, try without it

        # Are we using nightlies?
        if "rel_" in homePackage:
            # extract the rel_N bit and use it in the path
            rel_N = self.extractRelN(homePackage)
            tolog("Extracted %s from homePackage=%s" % (rel_N, homePackage))
            if rel_N:
                # path = "%s/%s/%s/%s/cmtsite/asetup.sh" % (swbase, cmtconfig, release, rel_N)
                path = self.getModernASetup(swbase=swbase)
                tolog("1. path = %s" % (path))
                skipVerification = True
        if not path:
            path = "%s/%s/%s/cmtsite/asetup.sh" % (swbase, cmtconfig, release)

        if not skipVerification:
            status = os.path.exists(path)
            if status:
                tolog("Using AtlasSetup (%s exists with cmtconfig in the path)" % (path))
            else:
                tolog("%s does not exist (trying without cmtconfig in the path)" % (path))
                if rel_N:
                    path = "%s/%s/%s/cmtsite/asetup.sh" % (swbase, release, rel_N)
                else:
                    path = "%s/%s/cmtsite/asetup.sh" % (swbase, release)
                status = os.path.exists(path)
                if status:
                    tolog("Using AtlasSetup (%s exists)" % (path))
                else:
                    tolog("Cannot use AtlasSetup since %s does not exist either" % (path))
        else:
            tolog("Skipping verification of asetup path for nightlies")
            status = True

        return status, path

    def useAtlasSetup(self, swbase, release, homePackage, cmtconfig):
        """ Determine whether AtlasSetup is to be used """

        # Previously this method returned False for older releases than 16.1.0. Since pilot release 64.0, this method returns True [i.e. use asetup for all ATLAS releases]
        return True

    def getSplitHomePackage(self, homePackage):
        """ Split the homePackage if it has a project/release format """
        # E.g. homePackage = AthSimulationBase/1.0.3 -> AthSimulationBase, 1.0.3
        # homePackage = AnalysisTransforms-AtlasP1HLT_20.2.3.6 -> 'AtlasP1HLT', '20.2.3.6'

        if "/" in homePackage:
            s = homePackage.split('/')
            project = s[0]
            release = s[1]
        else:
            if "AnalysisTransforms" in homePackage and ("AtlasP1HLT" in homePackage or "AtlasHLT" in homePackage):
                homePackage = homePackage.replace("AnalysisTransforms-", "")
                s = homePackage.split('_')
                project = s[0]
                release = s[1]
            else:
                project = homePackage
                release = ""

        return project, release

    def getSiterootWithHomepackage(self, swbase, homePackage, cmtconfig, release):
        """ Use the homePackage to set the siteroot """

        _project, _release = self.getSplitHomePackage(homePackage)
        if _release != "": # i.e. "/" is present in homePackage:
            path = os.path.join(swbase, _project) # E.g. /cvmfs/atlas.cern.ch/repo/sw/software/AthSimulationBase
            path = os.path.join(path, cmtconfig)
            siteroot = os.path.join(path, _release) # E.g. /cvmfs/atlas.cern.ch/repo/sw/software/AthSimulationBase/1.0.3
        else:
            if release == "":
                tolog("!!WARNING!!4545!! Found no slash in homePackage and release is not set - have to guess siteroot")
                if os.path.exists(os.path.join(swbase, _project)):
                    siteroot = os.path.join(swbase, _project)
                    if os.path.exists(os.path.join(siteroot, cmtconfig)):
                        siteroot = os.path.join(siteroot, cmtconfig)
                    else:
                        siteroot = os.path.join(swbase, _project)
                else:
                    siteroot = os.path.join(swbase, cmtconfig)
            else:
                tolog("!!WARNING!!4545!! Found no slash in homePackage - have to guess siteroot")
                # E.g. /cvmfs/atlas.cern.ch/repo/sw/software/i686-slc5-gcc43-opt/17.2.11
                siteroot = os.path.join(swbase, cmtconfig)
                siteroot = os.path.join(siteroot, release)

        return siteroot

    def getProperASetup(self, swbase, atlasRelease, homePackage, cmtconfig, tailSemiColon=False, source=True, cacheVer=None, cacheDir=None):
        """ return a proper asetup.sh command """

        # handle sites using builds area in a special way
        if swbase[-len('builds'):] == 'builds' or verifyReleaseString(atlasRelease) == "NULL":
            path = swbase
        else:
            # Check for special release (such as AthSimulationBase/1.0.3)
            done = False
            if homePackage.startswith('AthSimulation'):
                path = self.getSiterootWithHomepackage(swbase, homePackage, cmtconfig, "") # do not send the release in this case
                if os.path.exists(path):
                    tolog("Verified siteroot path: %s" % (path))
                    done = True
                else:
                    tolog("!!WARNING!!4545!! Siteroot path does not exist: %s" % (path))

            # Normal setup
            if not done:
                if os.path.exists(os.path.join(swbase, cmtconfig)):
                    if os.path.exists(os.path.join(os.path.join(swbase, cmtconfig), atlasRelease)):
                        path = os.path.join(os.path.join(swbase, cmtconfig), atlasRelease)
                    else:
                        path = os.path.join(swbase, atlasRelease)
                else:
                    path = os.path.join(swbase, atlasRelease)

        # need to tell asetup where the compiler is in the US (location of special config file)
        _path = "%s/AtlasSite/AtlasSiteSetup" % (path)
        if readpar('cloud') == "US" and os.path.exists(_path):
            _input = "--input %s" % (_path)
        else:
            _input = ""

        # add a tailing semicolon if needed
        if tailSemiColon:
            tail = ";"
        else:
            tail = ""

        # define the setup options
        if not cacheVer:
            cacheVer = atlasRelease

        # add the fast option if possible (for the moment, check for locally defined env variable)
        if os.environ.has_key("ATLAS_FAST_ASETUP"):
            options = cacheVer + ",notest,fast"
        else:
            options = cacheVer + ",notest"
        if cacheDir and cacheDir != "":
            options += ",%s" % (cacheDir)

        # special case for Ath* releases
        if homePackage.startswith('AthSimulation'):
            _project, _release = self.getSplitHomePackage(homePackage)
            options = options.replace("notest", "%s,notest" % (_project))

        # nightlies setup?
        if "rel_" in homePackage:
            # extract the rel_N bit and use it in the path
            rel_N = self.extractRelN(homePackage) # e.g. rel_N = rel_0
            if rel_N:
                tolog("Extracted %s from homePackage=%s" % (rel_N, homePackage))
                # asetup_path = "%s/%s/cmtsite/asetup.sh" % (path, rel_N)
                asetup_path = self.getModernASetup(swbase=swbase)
                tolog("2. path=%s" % (asetup_path))
                # use special options for nightlies (not the release info set above)
                # NOTE: 'HERE' IS NEEDED FOR MODERN SETUP
                # Special case for AtlasDerivation. In this case cacheVer = rel_N, so we don't want to add both cacheVer and rel_N,
                # and we need to add cacheDir and the release itself
                special_cacheDirs = ['AtlasDerivation'] # Add more cases if necessary
                if cacheDir in special_cacheDirs:
                    # strip any special cacheDirs from the release string, if present
                    for special_cacheDir in special_cacheDirs:
                        if special_cacheDir in atlasRelease:
                            tolog("Found special cacheDir=%s in release string: %s (will be removed)" % (special_cacheDir, atlasRelease)) # 19.1.X.Y-VAL-AtlasDerivation
                            atlasRelease = atlasRelease.replace('-' + special_cacheDir, '')
                            tolog("Release string updated: %s" % (atlasRelease))
                    options = cacheDir + "," + atlasRelease + "," + rel_N + ",notest,here" # E.g. AtlasDerivation,19.1.X.Y-VAL,rel_3,notest,here
                else:
                    # correct an already set cacheVer if necessary
                    if cacheVer == rel_N:
                        tolog("Found a cacheVer set to %s: resetting to atlasRelease=%s" % (cacheVer, atlasRelease))
                        cacheVer = atlasRelease
                    options = cacheVer + "," + rel_N + ",notest,here"
                    tolog("Options: %s" % (options))
            else:
                tolog("!!WARNING!!1111!! Failed to extract rel_N from homePackage=%s (forced to use default cmtsite setup)" % (homePackage))
                asetup_path = "%s/cmtsite/asetup.sh" % (path)
        else:
            asetup_path = "%s/cmtsite/asetup.sh" % (path)

        # make sure that cmd doesn't start with 'source' if the asetup_path start with 'export', if so reset it (cmd and asetup_path will be added below)
        if asetup_path.startswith('export'):
            cmd = ""
        elif source:
            # add the source command (default), not wanted for installPyJobTransforms()
            cmd = "source"
        else:
            cmd = ""

        # HLT on AFS
        if "AtlasP1HLT" in homePackage or "AtlasHLT" in homePackage:
            try:
                project, patch = self.getSplitHomePackage(homePackage) # ('AtlasP1HLT', '18.1.0.1')
            except Exception, e:
                tolog("!!WARNING!!1234!! Could not extract project and patch from %s" % (homePackage))
            else:
                tolog("Extracted %s, %s from homePackage=%s" % (patch, project, homePackage))
                if os.environ.has_key('VO_ATLAS_RELEASE_DIR'):
                    cmd = "export AtlasSetup=%s/../dist/AtlasSetup; " % readpar('appdir')
                    options = "%s,%s,notest,afs" % (patch, project)
                else:
                    cmd = "export AtlasSetup=%s/AtlasSetup; " % (path)
                    options = "%s,%s,notest" % (patch, project)
                    #cmd = "source"
                    #asetup_path = os.path.join(path, 'AtlasSetup/scripts/asetup.sh')
                asetup_path = "source $AtlasSetup/scripts/asetup.sh"

        # for HPC
        if 'HPC_HPC' in readpar("catchall"):
            quick_setup = "%s/setup-quick.sh" % (path)
            tolog("quick setup path: %s" % quick_setup)
            if os.path.exists(quick_setup):
                cmd = "source %s" % (quick_setup)
                asetup_path = ""
                cmtconfig = cmtconfig + " --cmtextratags=ATLAS,useDBRelease "

        return "%s %s %s --cmtconfig %s %s%s" % (cmd, asetup_path, options, cmtconfig, _input, tail)

    def extractRelN(self, homePackage):
        """ Extract the rel_N bit from the homePackage string """

        # s = "AtlasProduction,rel_0"
        # -> rel_N = "rel_0"

        rel_N = None

        if "AnalysisTransforms" in homePackage and "_rel_" in homePackage:
            pattern = re.compile(r"AnalysisTransforms\-[A-Za-z0-9]+\_(rel\_\d+)")
            found = re.findall(pattern, homePackage)
            if len(found) > 0:
                rel_N = found[0]

        elif not "," in homePackage:
            rel_N = homePackage

        elif homePackage != "":
            pattern = re.compile(r"[A-Za-z0-9]+,(rel\_\d+)")
            found = re.findall(pattern, homePackage)
            if len(found) > 0:
                rel_N = found[0]

        return rel_N

    def dump(self, path, cmd="cat"):
        """ Dump the content of path to the log """

        if cmd != "cat":
            _cmd = "%s %s" % (cmd, path)
            tolog("%s:\n%s" % (_cmd, commands.getoutput(_cmd)))
        else:
            if os.path.exists(path):
                _cmd = "%s %s" % (cmd, path)
                tolog("%s:\n%s" % (_cmd, commands.getoutput(_cmd)))
            else:
                tolog("Path %s does not exist" % (path))

    def displayArchitecture(self):
        """ Display system architecture """

        tolog("Architecture information:")

        cmd = "lsb_release -a"
        tolog("Excuting command: %s" % (cmd))
        out = commands.getoutput(cmd)
        if "Command not found" in out:
            # Dump standard architecture info files if available
            self.dump("/etc/lsb-release")
            self.dump("/etc/SuSE-release")
            self.dump("/etc/redhat-release")
            self.dump("/etc/debian_version")
            self.dump("/etc/issue")
            self.dump("$MACHTYPE", cmd="echo")
        else:
            tolog("\n%s" % (out))

    def specialChecks(self, **kwargs):
        """ Implement special checks here """

        status = False
        # appdir = kwargs.get('appdir', '')

        # Display system architecture
        self.displayArchitecture()

        # Display the cvmfs ChangeLog is possible
        self.displayChangeLog()

        # Set the python version used by the pilot
        self.setPilotPythonVersion()

        if ('HPC_' in readpar("catchall")) or ('ORNL_Titan_install' in readpar("nickname")):
            status = True
        else:
            # Test CVMFS
            status = self.testCVMFS()
        return status

    def checkSpecialEnvVars(self, sitename):
        """ Check special environment variables """

        ec = 0

        # check if ATLAS_POOLCOND_PATH is set
        try:
            if os.environ.has_key('ATLAS_POOLCOND_PATH'):
                tolog("ATLAS_POOLCOND_PATH = %s" % (os.environ['ATLAS_POOLCOND_PATH']))
            else:
                tolog("ATLAS_POOLCOND_PATH not set by wrapper")
        except Exception, e:
            tolog("WARNING: os.environ.has_key failed: %s" % str(e))

        if os.environ.has_key("VO_ATLAS_SW_DIR") and not "CERNVM" in sitename and not os.environ.has_key('Nordugrid_pilot'):
            vo_atlas_sw_dir = os.environ["VO_ATLAS_SW_DIR"]
            if vo_atlas_sw_dir != "":
                # on cvmfs the following dirs are symbolic links, so all tests are needed
                paths = [vo_atlas_sw_dir, os.path.join(vo_atlas_sw_dir, "software")]
                for path in paths:
                    if os.path.exists(path):
                        tolog("%s confirmed" % (path))
                    else:
                        tolog("!!FAILED!!1777!! %s does not exist" % (path))
                        ec = self.__error.ERR_NOSUCHFILE
                        break

                # require that the "local" directory exists on cvmfs
                path = os.path.join(vo_atlas_sw_dir, "local")
                if "cvmfs" in path:
                    if os.path.exists(path):
                        tolog("%s confirmed" % (path))
                    else:
                        tolog("!!FAILED!!1777!! %s does not exist" % (path))
                        ec = self.__error.ERR_NOSUCHFILE
                else:
                    tolog("Skipping verification of %s on non-cvmfs" % (path))
            else:
                tolog("VO_ATLAS_SW_DIR set to empty string (ignore)")

        return ec

    def getPayloadName(self, job):
        """ Figure out a suitable name for the payload stdout """

        if job.processingType in ['prun']:
            name = job.processingType
        else:
            jobtrf = job.trf.split(",")[0]
            if jobtrf.find("panda") > 0 and jobtrf.find("mover") > 0:
                name = "pandamover"
            elif jobtrf.find("Athena") > 0 or jobtrf.find("trf") > 0 or jobtrf.find("_tf") > 0:
                name = "athena"
            else:
                if isBuildJob(job.outFiles):
                    name = "buildjob"
                else:
                    name = "payload"

        return name

    def getMetadataForRegistration(self, guid):
        """ Return metadata (not known yet) for LFC registration """

        # Use the GUID as identifier (the string "<GUID>-surltobeset" will later be replaced with the SURL)
        return '    <metadata att_name="surl" att_value="%s-surltobeset"/>\n' % (guid)

    def getAttrForRegistration(self):
        """ Return the attribute of the metadata XML to be updated with surl value """

        return 'surl'

    def getFileCatalog(self):
        """ Return the default file catalog to use (e.g. for replica lookups) """
        # See usage in Mover.py
        # Note: no longer needed since list_replicas() doesn't need to know the catalog
        # Return a dummy default to allow the existing host loop to remain in Mover

        fileCatalog = "<rucio default>"
        tolog("Using file catalog: %s" % (fileCatalog))

        return fileCatalog

    def getFileCatalogHosts(self):
        """ Return a list of file catalog hosts """

        file_catalog_hosts = []

        return file_catalog_hosts

    def verifySwbase(self, appdir):
        """ Confirm existence of appdir/swbase """

        # appdir/swbase is a queuedata parameter specifying the base location of physics analysis / release software
        # This method will simply verify that the corresponding directory exists
        #
        # Input:
        #   appdir = application/software/release directory (e.g. /cvmfs/atlas.cern.ch/repo/sw)
        # Return:
        #   error code (0 for success)

        ec = 0

        if not "|" in appdir and not "^" in appdir: # as can be the case at CERN
            swbase = self.getSwbase(appdir, "", "", "", "")
            if os.path.exists(swbase):
                tolog("Application dir confirmed: %s" % (swbase))
            else:
                tolog("!!FAILED!!1999!! Software directory does not exist: %s" % (swbase))
                ec = self.__error.ERR_NOSOFTWAREDIR
        else:
            # get the site information object
            si = getSiteInformation(self.__experiment)

            tolog("Encountered complex appdir. Will verify each path individually (primary path must exist, other paths are optional)")

            appdirs = si.getAppdirs(appdir)
            tolog("appdirs = %s" % str(appdirs))
            primary = True
            for appdir in appdirs:
                if os.path.exists(appdir):
                    if primary:
                        tolog("Primary application directory confirmed: %s" % (appdir))
                        primary = False
                    else:
                        tolog("Additional application directory confirmed: %s" % (appdir))
                else:
                    if primary: # must exist
                        tolog("!!FAILED!!1999!! Primary application directory does not exist: %s" % (appdir))
                        ec = self.__error.ERR_NOSOFTWAREDIR
                        break
                    else:
                        tolog("Additional application directory does not exist: %s (ignore)" % (appdir))
        return ec

    def interpretPayloadStdout(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode):
        """ Payload error interpretation and handling """

        error = PilotErrors()
        transExitCode = res[0]%255

        # Get the proper stdout filename
        number_of_jobs = len(runCommandList)
        filename = getStdoutFilename(job.workdir, job.stdout, current_job_number, number_of_jobs)

        # Try to identify out of memory errors in the stderr
        out_of_memory = self.isOutOfMemory(job=job, number_of_jobs=number_of_jobs)
        failed = out_of_memory # failed boolean used below

        # Always look for the max and average VmPeak?
        if not self.__analysisJob and not self.shouldExecuteUtility():
            setup = getSourceSetup(runCommandList[0])
            job.vmPeakMax, job.vmPeakMean, job.RSSMean = findVmPeaks(setup)

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

        # handle non-zero failed job return code but do not set pilot error codes to all payload errors
        if transExitCode or failed:
            if failureCode:
                job.pilotErrorDiag = "Payload failed: Interrupt failure code: %d" % (failureCode)
                # (do not set pilot error code)
            elif getstatusoutput_was_interrupted:
                raise Exception, "Job execution was interrupted (see stderr)"
            elif out_of_memory:
                job.pilotErrorDiag = "Payload ran out of memory"
                job.result[2] = error.ERR_PAYLOADOUTOFMEMORY
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
        return job

    def isJEMAllowed(self):
        """ Is it allowed to use JEM services? """

        allowjem = False
        _allowjem = readpar('allowjem')

        if _allowjem == "":
            # allowjem not added to queuedata yet, use old method
            if readpar('cloud') == "DE":
                allowjem = True
        else:
            if _allowjem.lower() == "true":
                allowjem = True

        if allowjem:
            tolog("JEM is allowed")
        else:
            tolog("JEM is not allowed")

        return allowjem

    # Optional
    def doSpecialLogFileTransfer(self, eventService=False):
        """ Should the log file be transfered to a special SE? """

        # The log file can at the end of the job be stored in a special SE - in addition to the normal stage-out of the log file
        # If this method returns True, the JobLog class will attempt to store the log file in a secondary SE after the transfer of
        # the log to the primary/normal SE. Additional information about the secondary SE is required and can be specified in
        # another optional method defined in the *Experiment classes

        transferLogToObjectstore = False

        if "log_to_objectstore" in readpar('catchall') or eventService:
            transferLogToObjectstore = True
        if 'HPC_HPC' in readpar('catchall'):
            transferLogToObjectstore = True
        if 'HPC_HPCARC' in readpar('catchall'):
            transferLogToObjectstore = False

        return transferLogToObjectstore

    def extractInputOption(self, jobParameters):
        """ Extract the entire input file list from the job parameters including the input option """
        # jobParameters = .. --input=file1,file2,file3 ..
        # -> --input=file1,file2,file3
        # The string is later replaced with a "@file.txt" argparser directive (necessary in case the file list
        # is too long which would lead to an "argument list too long"-error)

        inputOption = ""

        # define the regexp pattern for the input option extraction
        pattern = re.compile(r'(\-\-input\=[^\s]*)')
        _option = re.findall(pattern, jobParameters)
        if _option != []:
            inputOption = _option[0]

        return inputOption

    def updateJobParameters4Input(self, jobParameters):
        """ Replace '--input=..' with @file argparser instruction """

        inputOption = self.extractInputOption(jobParameters)
        if inputOption == "":
            tolog("!!WARNING!!1223!! Option --file=.. could not be extracted from: %s" % (jobParameters))
        else:
            tolog("Extracted input option = %s" % (inputOption))

            # Put the extracted info in a file (to be automatically read by the argparser when the trf is executed)
            filename = "input_file_list.txt"
            try:
                f = open(filename, "w")
            except IOError, e:
                tolog("!!WARNING!!1234!! Failed to open input file list: %s" % (e))
            else:
                f.write(inputOption)
                f.close()
                tolog("Wrote extracted input file list to file %s" % (filename))

                jobParameters = jobParameters.replace(inputOption, "@%s" % (filename))
                tolog("updated job parameters = %s" % (jobParameters))

        return jobParameters

    def cleanupAthenaMP(self, workdir):
        """ Cleanup AthenaMP sud directories prior to log file creation """

        for ampdir in glob('%s/athenaMP-workers-*' % (workdir)):
            tolog("Attempting to cleanup %s" % (ampdir))
            for (p, d, f) in os.walk(ampdir):
                for filename in f:
                    if 'core' in filename or 'pool.root' in filename or 'tmp.' in filename:
                        path = os.path.join(p, filename)
                        tolog("Cleaning up %s" % (path))
                        os.unlink(path)

    def getJobExecutionCommand4EventService(self, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload for the event service """
        # E.g. cmd = ["source <path>/setup.sh; <path>/python <script>"]
        # The command returned from this method is executed using subprocess.Popen() from the runEvent module

        # The actual command must be declared as a list since that is expected by Popen()
        cmd = ["python %s/client_test.py 1>AthenaMP_stdout.txt 2>AthenaMP_stderr.txt" % (pilot_initdir)]

        return cmd

    # Optional
    def postGetJobActions(self, job):
        """ Perform any special post-job definition download actions here """

        # This method is called after the getJob() method has successfully downloaded a new job (job definition) from
        # the server. If the job definition e.g. contains information that contradicts WN specifics, this method can
        # be used to fail the job

        ec = 0
        pilotErrorDiag = ""

        # Make sure that ATHENA_PROC_NUMBER has a proper value for the current job
        if job.prodSourceLabel != "install":
            ec, pilotErrorDiag = self.verifyNCoresSettings(job.coreCount)
            if ec != 0:
                tolog("!!WARNING!!3222!! %s" % (pilotErrorDiag))
                return ec, pilotErrorDiag

        return ec, pilotErrorDiag

    # Local method (not defined in Experiment)
    def verifyNCoresSettings(self, jobCoreCount):
        """ Verify that nCores settings are correct """

        ec = 0
        pilotErrorDiag = ""

        try:
            coreCount = int(jobCoreCount)
        except:
            coreCount = None

        try:
            athenaProcNumber = int(os.environ['ATHENA_PROC_NUMBER'])
        except:
            athenaProcNumber = None

        # Make sure that ATHENA_PROC_NUMBER has a proper value for the current job
        if (coreCount == 1 or coreCount == None) and (athenaProcNumber > 1):
            ec = self.__error.ERR_CORECOUNTMISMATCH
            pilotErrorDiag = "Encountered a mismatch between core count from schedconfig (%s) and job definition (%s)" % (str(athenaProcNumber), str(coreCount))
            tolog("!!WARNING!!3333!! %s" % (pilotErrorDiag))
        else:
            tolog("Using core count values: %s (job definition), %s (schedconfig)" % (str(coreCount), str(athenaProcNumber)))

        return ec, pilotErrorDiag

    def getSwbase(self, appdir, release, homePackage, processingType, cmtconfig):
        """ Return the swbase variable """
        # appdir comes from thisSite.appdir (might not be set)
        # release info is needed to figure out the correct path to use when schedconfig.appdir is set

        swbase = ""

        # Verify the validity of the release string in case it is not set (as can be the case for prun jobs)
        release = verifyReleaseString(release)

        if os.environ.has_key('Nordugrid_pilot'):
            if os.environ.has_key('RUNTIME_CONFIG_DIR'):
                _swbase = os.environ['RUNTIME_CONFIG_DIR']
                if os.path.exists(_swbase):
                    swbase = _swbase
        elif os.environ.has_key('VO_ATLAS_SW_DIR'):
            # use the appdir from queuedata if available
            scappdir = readpar('appdir')
            # protect against complex appdir form
            if "|" in scappdir and appdir != "":
                #from SiteInformation import SiteInformation
                #si = SiteInformation()
                si = getSiteInformation(self.__experiment)
                ec, _scappdir = si.extractAppdir(scappdir, processingType, homePackage)
                if ec != 0:
                    tolog("!!WARNING!!2222!! Failed to extract complex appdir: %d, %s, %s, %s" % (ec, scappdir, processingType, homePackage))
                else:
                    scappdir = _scappdir
                tolog("Using alternative appdir=%s" % (scappdir))

            elif scappdir != "":
                tolog("Got a plain appdir from queuedata: %s" % (scappdir))
            else:
                tolog("Appdir not set in queuedata")

            if scappdir != "":
                # CERN-RELEASE:
                # appdir=/afs/cern.ch/atlas/software/releases (full path to releases)
                # CERN-UNVALID:
                # appdir=/afs/cern.ch/atlas/software/unvalidated/caches (full path to releases)
                # CERN-BUILDS:
                # appdir=/afs/cern.ch/atlas/software/builds (already points to the latest release, do not add release)
                # CERN-PROD:
                # appdir=/afs/cern.ch/atlas/software/releases (full path to releases)
                # Release can be added to appdir for CERN-RELEASE, CERN-UNVALID, CERN-PROD, but not to CERN-BUILDS
                if os.path.exists(os.path.join(scappdir, release)):
                    swbase = scappdir
                else:
                    # the build queue is special
                    if scappdir[-len('builds'):] == 'builds':
                        swbase = scappdir
                    # backup, old cases
                    elif os.path.exists(os.path.join(scappdir, 'software/releases')):
                        swbase = os.path.join(scappdir, 'software/releases')
                    # backup, for remaining LCG sites, only 'software' needs to be added
                    else:
                        swbase = os.path.join(scappdir, 'software')
                        if not os.path.exists(swbase):
                            swbase = scappdir
            else:
                tolog("VO_ATLAS_SW_DIR=%s" % (os.environ['VO_ATLAS_SW_DIR']))

                # primary software base (search appdir for alternatives)
                swbase = os.environ['VO_ATLAS_SW_DIR'] + '/software'
        else:
            # for non-LCG sites
            if appdir.find('atlas_app/atlas_rel') < 0:
                _swbase = os.path.join(appdir, 'atlas_app/atlas_rel')
                if os.path.exists(_swbase):
                    swbase = _swbase
                else:
                    swbase = appdir
            else:
                swbase = appdir

        # add cmtconfig sub dir for CERNVM and for cvmfs systems
        _cmtconfig = cmtconfig.replace("-", "_")
        _swbase = os.path.join(swbase, _cmtconfig)
        if os.path.exists(_swbase) and release != "" and release.upper() != "NULL":
            swbase = _swbase

        # uncomment if testing interactively at lxplus
        # swbase = appdir
        return swbase.replace('//','/')

    def setPilotPythonVersion(self):
        """ Set an environmental variable to the python version used by the pilot """
        # Needed to disentangle which python version runAthena should fall back to in case of problems with LFC import

        which_python = commands.getoutput("which python")
        if which_python.startswith('/'):
            os.environ['ATLAS_PYTHON_PILOT'] = which_python
            tolog("ATLAS_PYTHON_PILOT set to %s" % (which_python))
        else:
            tolog("!!WARNING!!1111!! Could not set ATLAS_PYTHON_PILOT to %s" % (which_python))

    # Optional
    def updateJobSetupScript(self, workdir, create=False, to_script=None):
        """ Create or update the job setup script (used to recreate the job locally if needed) """

        # If create=True, this step will only create the file with the script header (bash info)

        if create:
            filename = os.path.basename(super(ATLASExperiment, self).getJobSetupScriptName(workdir))
            tolog("Creating job setup script with stage-in and payload execution commands: %s" % (filename))
            to_script = "#!/bin/bash\n# %s %s\n\n" % (filename, time.strftime("%d %b %Y %H:%M:%S", time.gmtime(time.time())))

            # setup for EGI sites
            if os.environ.has_key('VO_ATLAS_SW_DIR'):
                to_script += "export VO_ATLAS_SW_DIR=%s\n" % (os.path.expandvars('$VO_ATLAS_SW_DIR'))
                to_script += "if [ -f $VO_ATLAS_SW_DIR/local/setup.sh ]; then\n  source $VO_ATLAS_SW_DIR/local/setup.sh\nfi"

        # Add the string to the setup script
        if to_script:
            super(ATLASExperiment, self).addToJobSetupScript(to_script, workdir)

    def verifyProxy(self, envsetup="", limit=None):
        """ Check for a valid voms/grid proxy longer than N hours """
        # Use 'limit' to set required length

        error = PilotErrors()
        pilotErrorDiag = ""

        if limit == None:
            limit = 48

        tolog("envsetup=%s"%(envsetup))
        from SiteMover import SiteMover
        if envsetup == "":
            envsetup = SiteMover.getEnvsetup()
        tolog("envsetup=%s"%(envsetup))
        envsetup = envsetup.strip()

        # add setup for arcproxy if it exists
        arcproxy_setup = "%s/atlas.cern.ch/repo/sw/arc/client/latest/slc6/x86_64/setup.sh" % (self.getCVMFSPath())
        _envsetup = ""
        if os.path.exists(arcproxy_setup):
            if envsetup != "":
                if not envsetup.endswith(";"):
                    envsetup += ";"

                # but remove any existing setup file in this path since it will tamper with the arcproxy setup
                pattern = re.compile(r'(source .+\;)')
                s = re.findall(pattern, envsetup)
                if s != []:
                    _envsetup = envsetup.replace(s[0], "")
                else:
                    _envsetup = envsetup

            _envsetup += ". %s;" % (arcproxy_setup)

        tolog("envsetup=%s"%(envsetup))

        # first try to use arcproxy since voms-proxy-info is not working properly on SL6 (memory issues on queues with limited memory)
        # cmd = "%sarcproxy -I |grep 'AC:'|awk '{sum=$5*3600+$7*60+$9; print sum}'" % (envsetup)
        cmd = "%sarcproxy -i vomsACvalidityLeft" % (_envsetup)
        tolog("Executing command: %s" % (cmd))
        exitcode, output = commands.getstatusoutput(cmd)
        if "command not found" in output:
            tolog("!!WARNING!!1234!! arcproxy is not available on this queue, this can lead to memory issues with voms-proxy-info on SL6: %s" % (output))
        else:
            ec, pilotErrorDiag = self.interpretProxyInfo(exitcode, output, limit)
            if ec == 0:
                tolog("Voms proxy verified using arcproxy")
                return 0, pilotErrorDiag
            elif ec == error.ERR_NOVOMSPROXY:
                return ec, pilotErrorDiag
            else:
                tolog("Will try voms-proxy-info instead")

        # -valid HH:MM is broken
        if "; ;" in envsetup:
            envsetup = envsetup.replace('; ;', ';')
            tolog("Removed a double ; from envsetup")
        cmd = "%svoms-proxy-info -actimeleft --file $X509_USER_PROXY" % (envsetup)
        tolog("Executing command: %s" % (cmd))
        exitcode, output = commands.getstatusoutput(cmd)
        if "command not found" in output:
            tolog("Skipping voms proxy check since command is not available")
        else:
            ec, pilotErrorDiag = self.interpretProxyInfo(exitcode, output, limit)
            if ec == 0:
                tolog("Voms proxy verified using voms-proxy-info")
                return 0, pilotErrorDiag

        if limit:
            cmd = "%sgrid-proxy-info -exists -valid %s:00" % (envsetup, str(limit))
        else:
            cmd = "%sgrid-proxy-info -exists -valid 24:00" % (envsetup)
        tolog("Executing command: %s" % (cmd))
        exitcode, output = commands.getstatusoutput(cmd)
        if exitcode != 0:
            if output.find("command not found") > 0:
                tolog("Skipping grid proxy check since command is not available")
            else:
                # Analyze exit code / output
                from futil import check_syserr
                check_syserr(exitcode, output)
                pilotErrorDiag = "Grid proxy certificate does not exist or is too short: %d, %s" % (exitcode, output)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_NOPROXY, pilotErrorDiag
        else:
            tolog("Grid proxy verified")

        return 0, pilotErrorDiag

    def interpretProxyInfo(self, ec, output, limit):
        """ Interpret the output from arcproxy or voms-proxy-info """

        exitcode = 0
        pilotErrorDiag = ""
        error = PilotErrors()

        tolog("ec=%d output=%s" % (ec, output))

        if ec != 0:
            if "Unable to verify signature! Server certificate possibly not installed" in output:
                tolog("!!WARNING!!2999!! Skipping voms proxy check: %s" % (output))
            # test for command errors
            elif "arcproxy:" in output:
                pilotErrorDiag = "Arcproxy failed: %s" % (output)
                tolog("!!WARNING!!2998!! %s" % (pilotErrorDiag))
                exitcode = error.ERR_GENERALERROR
            else:
                # Analyze exit code / output
                from futil import check_syserr
                check_syserr(ec, output)
                pilotErrorDiag = "Voms proxy certificate check failure: %d, %s" % (ec, output)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                exitcode = error.ERR_NOVOMSPROXY
        else:
            # remove any additional print-outs if present, assume that the last line is the time left
            if "\n" in output:
                output = output.split('\n')[-1]

            # test for command errors
            if "arcproxy:" in output:
                pilotErrorDiag = "Arcproxy failed: %s" % (output)
                tolog("!!WARNING!!2998!! %s" % (pilotErrorDiag))
                exitcode = error.ERR_GENERALERROR
            else:
                # on EMI-3 the time output is different (HH:MM:SS as compared to SS on EMI-2)
                if ":" in output:
                    ftr = [3600, 60, 1]
                    output = sum([a*b for a,b in zip(ftr, map(int,output.split(':')))])
                try:
                    validity = int(output)
                    if validity >= limit * 3600:
                        tolog("Voms proxy verified (%ds)" % (validity))
                    else:
                        pilotErrorDiag = "Voms proxy certificate does not exist or is too short. Lifetime %ds" % (validity)
                        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                        exitcode = error.ERR_NOVOMSPROXY
                except ValueError:
                    pilotErrorDiag = "Failed to evalute command output: %s" % (output)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    exitcode = error.ERR_GENERALERROR

        return exitcode, pilotErrorDiag

    def getRelease(self, release):
        """ Return a list of the software release id's """
        # Assuming 'release' is a string that separates release id's with '\n'
        # Used in the case of payload using multiple steps with different release versions
        # E.g. release = "19.0.0\n19.1.0" -> ['19.0.0', '19.1.0']

        if os.environ.has_key('Nordugrid_pilot') and os.environ.has_key('ATLAS_RELEASE'):
            return os.environ['ATLAS_RELEASE'].split(",")
        else:
            return release.split("\n")

    # Optional
    def formatReleaseString(self, release):
        """ Return a special formatted release string """
        # E.g. release = "Atlas-19.0.0" -> "19.0.0"

        self.__atlasEnv = release.startswith('Atlas-')
        if self.__atlasEnv : # require atlas env. to be set up
            if release.find("\n") > 0:
                # E.g. multi-trf: swRelease = 'Atlas-14.1.0\nAtlas-14.1.0' (normally 'Atlas-14.1.0')
                # We only want to keep the release number, not the 'Atlas' string
                rm = release.split('-')[0] + '-' # 'Atlas-'
                release = release.replace(rm, '')
            else:
                # update needed to handle cases like "Atlas-19.0.X.Y-VAL"
                release = release[release.find('-')+1:]

        return release

    def getModernASetup(self, swbase=None):
        """ Return the full modern setup for asetup """

        # Handle nightlies correctly, since these releases will have different initial paths
        path = "%s/atlas.cern.ch/repo" % (self.getCVMFSPath())
        if os.path.exists(path):
            # Handle nightlies correctly, since these releases will have different initial paths
            path = "%s/atlas.cern.ch/repo" % (self.getCVMFSPath())
            #if swbase:
            #    path = getInitialDirs(swbase, 3) # path = "/cvmfs/atlas-nightlies.cern.ch/repo"
            #    # correct for a possible change of the root directory (/cvmfs)
            #    path = path.replace("/cvmfs", self.getCVMFSPath())
            #else:
            #    path = "%s/atlas.cern.ch/repo" % (self.getCVMFSPath())
            cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/ATLASLocalRootBase;" % (path)
            cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;"
            cmd += "source $AtlasSetup/scripts/asetup.sh"

            return cmd
        else:
            appdir = readpar('appdir')
            if appdir == "":
                if os.environ.has_key('VO_ATLAS_SW_DIR'):
                    appdir = os.environ['VO_ATLAS_SW_DIR']
            if appdir != "":
                cmd = "source %s/scripts/asetup.sh" % appdir
                return cmd
        return ''

    def verifySetupCommand(self, _setup_str):
        """ Make sure the setup command exists """

        ec = 0
        pilotErrorDiag = ""

        # remove any '-signs
        _setup_str = _setup_str.replace("'", "")
        tolog("Will verify: %s" % (_setup_str))

        if _setup_str != "" and "source " in _setup_str:
            # if a modern setup is used (i.e. a naked asetup instead of asetup.sh), then we need to verify that the entire setup string works
            # and not just check the existance of the path (i.e. the modern setup is more complicated to test)
            if self.getModernASetup() in _setup_str:
                tolog("Modern asetup detected, need to verify entire setup (not just existance of path)")
                tolog("Executing command: %s" % (_setup_str))
                exitcode, output = timedCommand(_setup_str)
                if exitcode != 0:
                    pilotErrorDiag = output
                    tolog('!!WARNING!!2991!! %s' % (pilotErrorDiag))
                    if "No such file or directory" in output:
                        ec = self.__error.ERR_NOSUCHFILE
                    elif "timed out" in output:
                        ec = self.__error.ERR_COMMANDTIMEOUT
                    else:
                        ec = self.__error.ERR_SETUPFAILURE
                else:
                    tolog("Setup has been verified")
            else:
                # first extract the file paths from the source command(s)
                setup_paths = extractFilePaths(_setup_str)

                # only run test if string begins with an "/"
                if setup_paths:
                    # verify that the file paths actually exists
                    for setup_path in setup_paths:
                        if os.path.exists(setup_path):
                            tolog("File %s has been verified" % (setup_path))
                        else:
                            pilotErrorDiag = "No such file or directory: %s" % (setup_path)
                            tolog('!!WARNING!!2991!! %s' % (pilotErrorDiag))
                            ec = self.__error.ERR_NOSUCHFILE
                            break
                else:
                    # nothing left to test
                    pass
        else:
            tolog("Nothing to verify in setup: %s (either empty string or no source command)" % (_setup_str))

        return ec, pilotErrorDiag

    # Optional
    def useTracingService(self):
        """ Use the Rucio Tracing Service """
        # A service provided by the Rucio system that allows for file transfer tracking; all file transfers
        # are reported by the pilot to the Rucio Tracing Service if this method returns True

        return True

    # Optional
    def updateJobDefinition(self, job, filename):
        """ Update the job definition file and object before using it in RunJob """

        # This method is called from Monitor, before RunJob is launched, which allows to make changes to the job object after it was downloaded from the job dispatcher
        # (used within Monitor) and the job definition file (which is used from RunJob to recreate the same job object as is used in Monitor).
        # 'job' is the job object, defined in Job.py, while 'filename' is the name of the file containing the job definition information.

        # Update the job definition in case ATHENA_PROC_NUMBER has been set
        if os.environ.has_key('ATHENA_PROC_NUMBER'):
            try:
                coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
            except Exception, e:
                tolog("!!WARNING!!2332!! ATHENA_PROC_NUMBER not an integer (can not update job definition): %s" % (e))
            else:
                tolog("ATHENA_PROC_NUMBER set to %d - will update job object and job definition file" % (coreCount))
                job.coreCount = coreCount

                # Get the contents of the job definition
                contents = readFile(filename)
                if contents != "":
                    # Parse the job definition and extract the coreCount string + value
                    pattern = re.compile(r"coreCount\=([A-Za-z0-9]+)?")
                    found = re.findall(pattern, contents)
                    if len(found) > 0: # ie found a least 'coreCount='
                        try:
                            coreCount_string = "coreCount=%s" % found[0] # might or might not add an int, or even NULL
                        except Exception, e:
                            tolog("!!WARNING!!2333!! Failed to extract coreCount from job definition: %s" % (e))
                        else:
                            tolog("Extracted \'%s\' from job definition" % (coreCount_string))

                            # Update the coreCount
                            new_coreCount = "coreCount=%d" % (coreCount)
                            updated_contents = contents.replace(coreCount_string, new_coreCount)
                            if writeFile(filename, updated_contents):
                                tolog("Updated job definition with: \'%s\'" % (new_coreCount))
                            else:
                                tolog("!!WARNING!!2336!! Failed to update coreCount in job definition")
                    else:
                        tolog("!!WARNING!!2334!! coreCount could not be extracted from job definition")
                else:
                    tolog("!!WARNING!!2335!! Empty job definition")
        else:
            tolog("ATHENA_PROC_NUMBER is not set, will not update coreCount in job definition")

        return job

    # Optional
    def shouldExecuteUtility(self):
        """ Determine where a memory utility monitor should be executed """

        # The RunJob class has the possibility to execute a memory utility monitor that can track the memory usage
        # of the payload. The monitor is executed if this method returns True. The monitor is expected to produce
        # a summary JSON file whose name is defined by the getMemoryMonitorJSONFilename() method. The contents of
        # this file (ie. the full JSON dictionary) will be added to the jobMetrics at the end of the job (see
        # PandaServerClient class).
        #
        # Example of summary JSON file:
        #   {"Max":{"maxVMEM":40058624,"maxPSS":10340177,"maxRSS":16342012,"maxSwap":16235568},
        #    "Avg":{"avgVMEM":19384236,"avgPSS":5023500,"avgRSS":6501489,"avgSwap":5964997}}
        #
        # While running, the MemoryMonitor also produces a regularly updated text file with the following format: (tab separated)
        #   Time          VMEM        PSS        RSS        Swap         (first line in file)
        #   1447960494    16099644    3971809    6578312    1978060

        return True

    # Optional
    def getUtilityOutputFilename(self):
        """ Return the filename of the memory monitor text output file """

        # For explanation, see shouldExecuteUtility()
        return "memory_monitor_output.txt"

    # Optional
    def getUtilityJSONFilename(self):
        """ Return the filename of the memory monitor JSON file """

        # For explanation, see shouldExecuteUtility()
        return "memory_monitor_summary.json"

    def getUtilityInfoPath(self, workdir, pilot_initdir, allowTxtFile=False):
        """ Find the proper path to the utility info file """
        # Priority order:
        #   1. JSON summary file from workdir
        #   2. JSON summary file from pilot initdir
        #   3. Text output file from workdir (if allowTxtFile is True)

        path = os.path.join(workdir, self.getUtilityJSONFilename())
        init_path = os.path.join(pilot_initdir, self.getUtilityJSONFilename())
        if not os.path.exists(path):
            tolog("File does not exist: %s" % (path))
            if os.path.exists(init_path):
                path = init_path
            else:
                tolog("File does not exist either: %s" % (init_path))
                path = ""

            if path == "" and allowTxtFile:
                path = os.path.join(workdir, self.getUtilityOutputFilename())
                if not os.path.exists(path):
                    tolog("File does not exist either: %s" % (path))

        return path

    # Optional
    def getUtilityInfo(self, workdir, pilot_initdir, allowTxtFile=False):
        """ Add the utility info to the node structure if available """

        node = {}

        # Get the values from the memory monitor file
        summary_dictionary = self.getMemoryValues(workdir, pilot_initdir)

        # Fill the node dictionary
        if summary_dictionary and summary_dictionary != {}:
            try:
                node['maxRSS'] = summary_dictionary['Max']['maxRSS']
                node['maxVMEM'] = summary_dictionary['Max']['maxVMEM']
                node['maxSWAP'] = summary_dictionary['Max']['maxSwap']
                node['maxPSS'] = summary_dictionary['Max']['maxPSS']
                node['avgRSS'] = summary_dictionary['Avg']['avgRSS']
                node['avgVMEM'] = summary_dictionary['Avg']['avgVMEM']
                node['avgSWAP'] = summary_dictionary['Avg']['avgSwap']
                node['avgPSS'] = summary_dictionary['Avg']['avgPSS']
            except Exception, e:
                tolog("!!WARNING!!54541! Exception caught while parsing memory monitor file: %s" % (e))
                tolog("!!WARNING!!5455!! Will add -1 values for the memory info")
                node['maxRSS'] = -1
                node['maxVMEM'] = -1
                node['maxSWAP'] = -1
                node['maxPSS'] = -1
                node['avgRSS'] = -1
                node['avgVMEM'] = -1
                node['avgSWAP'] = -1
                node['avgPSS'] = -1
            else:
                tolog("Extracted info from memory monitor")
        else:
            tolog("Memory summary dictionary not yet available")

        return node

    def getMaxUtilityValue(self, value, maxValue, totalValue):
        """ Return the max and total value (used by memory monitoring) """
        # Return an error code, 1, in case of value error

        ec = 0
        try:
            value_int = int(value)
        except Exception, e:
            tolog("!!WARNING!!4543!! Exception caught: %s" % (e))
            ec = 1
        else:
            totalValue += value_int
            if value_int > maxValue:
                maxValue = value_int

        return ec, maxValue, totalValue

    def getMemoryValues(self, workdir, pilot_initdir):
        """ Find the values in the utility output file """

        # In case the summary JSON file has not yet been produced, create a summary dictionary with the same format
        # using the output text file (produced by the memory monitor and which is updated once per minute)
        #
        # FORMAT:
        #   {"Max":{"maxVMEM":40058624,"maxPSS":10340177,"maxRSS":16342012,"maxSwap":16235568},
        #    "Avg":{"avgVMEM":19384236,"avgPSS":5023500,"avgRSS":6501489,"avgSwap":5964997}}

        maxVMEM = -1
        maxRSS = -1
        maxPSS = -1
        maxSwap = -1
        avgVMEM = 0
        avgRSS = 0
        avgPSS = 0
        avgSwap = 0
        totalVMEM = 0
        totalRSS = 0
        totalPSS = 0
        totalSwap = 0
        N = 0
        summary_dictionary = {}

        # Get the path to the proper memory info file (priority ordered)
        path = self.getUtilityInfoPath(workdir, pilot_initdir, allowTxtFile=True)
        if os.path.exists(path):

            tolog("Using path: %s" % (path))

            # Does a JSON summary file exist? If so, there's no need to calculate maximums and averages in the pilot
            if path.lower().endswith('json'):
                # Read the dictionary from the JSON file
                summary_dictionary = getJSONDictionary(path)
            else:
                # Loop over the output file, line by line, and look for the maximum PSS value
                first = True
                with open(path) as f:
                    for line in f:
                        # Skip the first line
                        if first:
                            first = False
                            continue
                        line = convert_unicode_string(line)
                        if line != "":
                            try:
                                Time, VMEM, PSS, RSS, Swap = line.split("\t")
                            except Exception, e:
                                tolog("!!WARNING!!4542!! Unexpected format of utility output: %s (expected format: Time, VMEM, PSS, RSS, Swap)" % (line))
                            else:
                                # Convert to int
                                ec1, maxVMEM, totalVMEM = self.getMaxUtilityValue(VMEM, maxVMEM, totalVMEM) 
                                ec2, maxPSS, totalPSS = self.getMaxUtilityValue(PSS, maxPSS, totalPSS) 
                                ec3, maxRSS, totalRSS = self.getMaxUtilityValue(RSS, maxRSS, totalRSS) 
                                ec4, maxSwap, totalSwap = self.getMaxUtilityValue(Swap, maxSwap, totalSwap) 
                                if ec1 or ec2 or ec3 or ec4:
                                    tolog("Will skip this row of numbers due to value exception: %s" % (line))
                                else:
                                    N += 1
                    # Calculate averages and store all values
                    summary_dictionary = { "Max": {}, "Avg": {} }
                    summary_dictionary["Max"] = { "maxVMEM":maxVMEM, "maxPSS":maxPSS, "maxRSS":maxRSS, "maxSwap":maxSwap }
                    if N > 0:
                        avgVMEM = int(float(totalVMEM)/float(N))
                        avgPSS = int(float(totalPSS)/float(N))
                        avgRSS = int(float(totalRSS)/float(N))
                        avgSwap = int(float(totalSwap)/float(N))
                    summary_dictionary["Avg"] = { "avgVMEM":avgVMEM, "avgPSS":avgPSS, "avgRSS":avgRSS, "avgSwap":avgSwap }
                    tolog("summary_dictionary=%s"%str(summary_dictionary))
                f.close()
        else:
            if path == "":
                tolog("!!WARNING!!4541!! Filename not set for utility output")
            else:
                # Normally this means that the memory output file has not been produced yet
                pass
                # tolog("File does not exist: %s" % (path))

        return summary_dictionary

    # Optional
    def getUtilityCommand(self, **argdict):
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
        workdir = argdict.get('workdir', '.')
        interval = 60

        default_release = "20.1.5"
        default_patch_release = "20.1.5.2" #"20.1.4.1"
        default_cmtconfig = "x86_64-slc6-gcc48-opt"
        default_swbase = "%s/atlas.cern.ch/repo/sw/software" % (self.getCVMFSPath())
        #default_path = "%s/%s/%s/AtlasProduction/%s/InstallArea/%s/bin/MemoryMonitor" % (default_swbase, default_cmtconfig, default_release, default_patch_release, default_cmtconfig)
        default_setup = "source %s/%s/%s/cmtsite/asetup.sh %s,notest --cmtconfig %s" % (default_swbase, default_cmtconfig, default_release, default_patch_release, default_cmtconfig)

        # Construct the name of the output file using the summary variable
        if summary.endswith('.json'):
            output = summary.replace('.json', '.txt')
        else:
            output = summary + '.txt'

        # Get the standard setup
        cacheVer = homePackage.split('/')[-1]

        # Could anything be extracted?
        #if homePackage == cacheVer: # (no)
        if isAGreaterOrEqualToB(default_release, release) or default_release == release: # or NG
            # This means there is no patched release available, ie. we need to use the fallback
            useDefault = True
            tolog("%s >= %s" % (default_release, release))
        else:
            useDefault = False
            tolog("%s <= %s" % (default_release, release))
        useDefault = True

        if useDefault:
            tolog("Will use default (fallback) setup for MemoryMonitor since patched release number is needed for the setup, and none is available")
            cmd = default_setup
        else:
            # get the standard setup
            standard_setup = self.getProperASetup(default_swbase, release, homePackage, cmtconfig, cacheVer=cacheVer)
            _cmd = standard_setup + "; which MemoryMonitor"
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
        cmd += "; MemoryMonitor --pid %d --filename %s --json-summary %s --interval %d" % (pid, self.getUtilityOutputFilename(), summary, interval)
        cmd = "cd " + workdir + ";" + cmd

        return cmd

    # Optional
    def getGUIDSourceFilename(self):
        """ Return the filename of the file containing the GUIDs for the output files """

        # In the case of ATLAS, Athena produces an XML file containing the GUIDs of the output files. The name of this
        # file is PoolFileCatalog.xml. If this method returns an empty string (ie the default), the GUID generation will
        # be done by the pilot in RunJobUtilities::getOutFilesGuids()

        return "PoolFileCatalog.xml"

if __name__ == "__main__":

    a=ATLASExperiment()
    #a.specialChecks()
    print a.formatReleaseString("Atlas-19.0.X.Y-VAL")

    #appdir='/cvmfs/atlas.cern.ch/repo/sw'
    #a.specialChecks(appdir=appdir)

    #def useTracingService(self):

    #    return True

    #def sendTracingReport(self, exitCode):

    #    ts = TracingService()
    #    ts.send
