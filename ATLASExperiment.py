# Class de                tolog("Unset ATHENA_PROC_NUMBER")finition:
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
from pUtil import verifyReleaseString           # To verify the release string (move to Experiment later)
from pUtil import timedCommand                  # Protect cmd with timed_command
from pUtil import getSiteInformation            # Get the SiteInformation object corresponding to the given experiment
from pUtil import isBuildJob                    # Is the current job a build job?
from pUtil import remove                        # Used to remove redundant file before log file creation
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
                    # Note: the original request (AF) was to use j%d and not -j%d, now using the latter
                    cmd2 += 'export MAKEFLAGS="-j%d QUICK=1 -l1";' % (coreCount)
                    tolog("Added multi-core support to cmd2: %s" % (cmd2))
        # make sure that MAKEFLAGS is always set
        if not "MAKEFLAGS=" in cmd2:
            cmd2 += 'export MAKEFLAGS="-j1 QUICK=1 -l1";'

        return cmd2

    def isNightliesRelease(self, homePackage):
        """ Is the homePackage for a nightlies release? """

        # The pilot will regard the release as a nightlies if the homePackage contains a time-stamp
        # (the time stamp is aactually a sub directory;
        # e.g. /cvmfs/atlas-nightlies.cern.ch/repo/sw/21.0.X/2016-12-01T2101/)

        status = False

        # If a timestamp can be extracted from the homePackage, it is a nightlies release
        if self.extractNightliesTimestamp(homePackage) != "":
            status = True

        return status

    # Optional
    def shouldPilotPrepareASetup(self, noExecStrCnv, jobPars):
        """ Should pilot be in charge of preparing asetup? """
        # If noExecStrCnv is set, then jobPars is expected to contain asetup.sh + options

        prepareASetup = True
        if noExecStrCnv:
            if "asetup.sh" in jobPars:
                tolog("asetup will be taken from jobPars")
                prepareASetup = False
            else:
                tolog("noExecStrCnv is set but asetup command was not found in jobPars (pilot will prepare asetup)")
                prepareASetup = True
        else:
            tolog("Pilot will prepare asetup")
            prepareASetup = True

        return prepareASetup

    def getJobExecutionCommand(self, job, jobSite, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload """

        pilotErrorDiag = ""
        cmd = ""
        special_setup_cmd = ""
        JEM = "NO"

        # homePackage variants:
        #   user jobs
        #     1. AnalysisTransforms (using asetup)
        #     2. AnalysisTransforms-<project>_<cache>, e.g. project=AthAnalysisBase,AtlasDerivation,AtlasProduction,MCProd,TrigMC; cache=20.1.6.2,..
        #     3. AnalysisTransforms-<project>_rel_<N>, e.g. project=AtlasOffline; N=0,1,2,..
        #     4. [homaPackage not set]
        #
        #   production jobs
        #     1. <project>/<cache>, e.g. AtlasDerivation/20.1.6.2, AtlasProd1/20.1.5.10.1
        #     2. <project>,rel_<N>[,devval], e.g. AtlasProduction,rel_4,devval
        #
        # Tested (USER ANALYSIS)
        #     homePackage = AnalysisTransforms
        #       setup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        #                 source $AtlasSetup/scripts/asetup.sh 17.2.7,notest --platform x86_64-slc5-gcc43-opt --makeflags=\"$MAKEFLAGS\";
        #                 export MAKEFLAGS=\"-j1 QUICK=1 -l1\";[proxy export];./runAthena-00-00-11 ..
        #     homePackage = AnalysisTransforms-AtlasDerivation_20.1.5.7
        #       setup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        #                 source $AtlasSetup/scripts/asetup.sh AtlasDerivation,20.1.5.7,notest --platform x86_64-slc6-gcc48-opt --makeflags=\"$MAKEFLAGS\";
        #                 export MAKEFLAGS=\"-j1 QUICK=1 -l1\";[proxy export];./runAthena-00-00-11 ..
        #     homePackage=AnalysisTransforms-AtlasDerivation_rel_1
        #        setup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        #                  source $AtlasSetup/scripts/asetup.sh AtlasDerivation,20.1.X.Y-VAL,rel_1,notest --platform x86_64-slc6-gcc48-opt --makeflags=\"$MAKEFLAGS\";
        #                  export MAKEFLAGS=\"-j1 QUICK=1 -l1\";source /cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh;[proxy export];./runAthena-00-00-11 ..
        #     homePackage not set, release not set
        #        setup = source /cvmfs/atlas.cern.ch/repo/sw/local/setup.sh; [proxy export];./runGen-00-00-02 ..
        #     homePackage = AnalysisTransforms-AthAnalysisBase_2.3.11 (release = AthAnalysisBase/x86_64-slc6-gcc48-opt/2.3.11
        #        setup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        #                  source $AtlasSetup/scripts/asetup.sh AthAnalysisBase,2.3.11,notest --platform x86_64-slc6-gcc48-opt --makeflags="$MAKEFLAGS";
        #                  export MAKEFLAGS="-j1 QUICK=1 -l1";source /cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh;[proxy export];./runAthena-00-00-11 ..
        # Tested (PRODUCTION)
        #     homePackage = AtlasProduction/17.7.3.12
        #       setup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        #                 source $AtlasSetup/scripts/asetup.sh AtlasProduction,17.7.3.12 --platform x86_64-slc6-gcc46-opt --makeflags=\"$MAKEFLAGS\";Sim_tf.py ..
        #     homePackage = AtlasDerivation/20.1.5.7
        #       setup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        #                 source $AtlasSetup/scripts/asetup.sh AtlasDerivation,20.1.5.7 --platform x86_64-slc6-gcc48-opt --makeflags=\"$MAKEFLAGS\";Reco_tf.py ..
        #  TRF's:
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


        # Should the pilot do the asetup or do the jobPars already contain the information?
        prepareASetup = self.shouldPilotPrepareASetup(job.noExecStrCnv, job.jobPars)

        # Is it a user job or not?
        analysisJob = isAnalysisJob(job.trf)

        # Get the cmtconfig value
        cmtconfig = getCmtconfig(job.cmtconfig)

        # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
        asetup_path = self.getModernASetup(asetup=prepareASetup)
        asetup_options = " "

        # Is it a standard ATLAS job? (i.e. with swRelease = 'Atlas-...')
        if self.__atlasEnv:

            # Normal setup (production and user jobs)
            tolog("Preparing normal production/analysis job setup command")

            cmd = asetup_path
            if prepareASetup:
                options = self.getASetupOptions(job.release, job.homePackage)
                asetup_options = " " + options + " --platform " + cmtconfig

                # Always set the --makeflags option (to prevent asetup from overwriting it)
                asetup_options += ' --makeflags=\"$MAKEFLAGS\"'

                cmd += asetup_options

                # Verify that the setup works
                exitcode, output = timedCommand(cmd, timeout=5*60)
                if exitcode != 0:
                    if "No release candidates found" in output:
                        pilotErrorDiag = "No release candidates found"
                        tolog("!!WARNING!!3434!! %s" % (pilotErrorDiag))
                        return self.__error.ERR_NORELEASEFOUND, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                else:
                    tolog("Verified setup command")

            if analysisJob:
                # Set the INDS env variable (used by runAthena)
                self.setINDS(job.realDatasetsIn)

                # Try to download the trf
                ec, pilotErrorDiag, trfName = self.getAnalysisTrf('wget', job.trf, pilot_initdir)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                if prepareASetup:
                    ec, pilotErrorDiag, _cmd = self.getAnalysisRunCommand(job, jobSite, trfName)
                    if ec != 0:
                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                else:
                    _cmd = job.jobPars

                tolog("_cmd = %s" % (_cmd))

                # Correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
                cmd += "; " + self.addMAKEFLAGS(job.coreCount, "") + _cmd
            else:
                # Add Database commands if they are set by the local site
                cmd += os.environ.get('PILOT_DB_LOCAL_SETUP_CMD','')
                # Add the transform and the job parameters (production jobs)
                if prepareASetup:
                    cmd += ";%s %s" % (job.trf, job.jobPars)
                else:
                    cmd += "; " + job.jobPars

            cmd = cmd.replace(';;', ';')

        else: # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

            tolog("Generic job")

            # Set python executable
            ec, pilotErrorDiag, pybin = self.setPython(job.release, job.homePackage, cmtconfig, jobSite.sitename)
            if ec == self.__error.ERR_MISSINGINSTALLATION:
                return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            if analysisJob:
                # Try to download the analysis trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf('wget', job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Set up the run command
                if (job.prodSourceLabel == 'ddm' or job.prodSourceLabel == 'software') and prepareASetup:
                    cmd = '%s %s %s' % (pybin, trfName, job.jobPars)
                else:
                    if prepareASetup:
                        ec, pilotErrorDiag, cmd = self.getAnalysisRunCommand(job, jobSite, trfName)
                        if ec != 0:
                            return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                    else:
                        cmd = job.jobPars

                # correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
                cmd2 = self.addMAKEFLAGS(job.coreCount, "")
                tolog("cmd2 = %s" % (cmd2))
                cmd = cmd2 + cmd

                # should asetup be used? If so, sqeeze it into the run command (rather than moving the entire getAnalysisRunCommand() into this class)
                if prepareASetup:
                    m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', job.homePackage)
                    if m_cacheDirVer != None:
                        # homePackage="AnalysisTransforms-AthAnalysisBase_2.0.14"
                        # -> cacheDir = AthAnalysisBase, cacheVer = 2.0.14
                        cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, "dummy_atlasRelease")
                        if cacheDir != "" and cacheVer != "":
                            asetup = self.getModernASetup()
                            asetup += " %s,%s --platform=%s;" % (cacheDir, cacheVer, cmtconfig)

                            # now squeeze it back in
                            cmd = cmd.replace('./' + trfName, asetup + './' + trfName)
                            tolog("Updated run command for special homePackage: %s" % (cmd))
                        else:
                            tolog("asetup not needed (mo special home package: %s)" % (job.homePackage))
                    else:
                        tolog("asetup not needed (no special homePackage)")

            elif verifyReleaseString(job.homePackage) != 'NULL' and job.homePackage != ' ':

                if 'HPC_' in readpar("catchall"):
                    cmd = {"interpreter": pybin,
                           "payload": ("%s/%s" % (job.homePackage, job.trf)),
                           "parameters": job.jobPars }
                else:
                    if prepareASetup:
                        cmd = "%s %s/%s %s" % (pybin, job.homePackage, job.trf, job.jobPars)
                    else:
                        cmd = job.jobPars
            else:
                if 'HPC_' in readpar("catchall"):
                    cmd = {"interpreter": pybin,
                           "payload": job.trf,
                           "parameters": job.jobPars }
                else:
                    if prepareASetup:
                        cmd = "%s %s %s" % (pybin, job.trf, job.jobPars)
                    else:
                        cmd = job.jobPars

        # Add FRONTIER debugging and RUCIO env variables
        if 'HPC_' in readpar("catchall") and 'HPC_HPC' not in readpar("catchall"):
            cmd['environment'] = self.getEnvVars2Cmd(job.jobId, job.taskID, job.processingType, jobSite.sitename, analysisJob)
        else:
            cmd = self.addEnvVars2Cmd(cmd, job.jobId, job.taskID, job.processingType, jobSite.sitename, analysisJob)
        if 'HPC_HPC' in readpar("catchall"):
            cmd = 'export JOB_RELEASE=%s;export JOB_HOMEPACKAGE=%s;JOB_CMTCONFIG=%s;%s' % (job.release, job.homePackage, cmtconfig, cmd)

        ver = os.environ.get('ALRB_asetupVersion', None)
        if ver is not None:
            cmd = 'export ALRB_asetupVersion=%s;%s' % (ver, cmd)

        # Explicitly add the ATHENA_PROC_NUMBER (or JOB value)
        cmd = self.addAthenaProcNumber(cmd)

        # cmd = "export XRD_LOGLEVEL=Debug;" + cmd

        # Wrap the job execution command with Singularity if necessary
        from Singularity import singularityWrapper
        cmd = singularityWrapper(cmd, cmtconfig, job.workdir)
        tolog("\nCommand to run the job is: \n%s" % (cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig

    def addAthenaProcNumber(self, cmd):
        """
        Add the ATHENA_PROC_NUMBER to the payload command if necessary
        :param cmd: payload execution command
        :return: updated payload execution command
        """

        if not "ATHENA_PROC_NUMBER" in cmd:
            if "ATHENA_PROC_NUMBER" in os.environ:
                cmd = 'export ATHENA_PROC_NUMBER=%s;' % os.environ['ATHENA_PROC_NUMBER'] + cmd
            elif "ATHENA_PROC_NUMBER_JOB" in os.environ:
                try:
                    value = int(os.environ['ATHENA_PROC_NUMBER_JOB'])
                except:
                    tolog("!!WARNING!!3433!! Failed to convert ATHENA_PROC_NUMBER_JOB=%s to int" % (os.environ['ATHENA_PROC_NUMBER_JOB']))
                else:
                    if value > 1:
                        cmd = 'export ATHENA_PROC_NUMBER=%d;' % value + cmd
                    else:
                        tolog("Will not add ATHENA_PROC_NUMBER to cmd since the value is %d" % value)
            else:
                tolog("!!WARNING!!3434!! Don't know how to set ATHENA_PROC_NUMBER (could not find it in os.environ)")
        else:
            tolog("ATHENA_PROC_NUMBER already in job command")

        return cmd

    def getAnalysisRunCommand(self, job, jobSite, trfName):
        """ Get the run command for analysis jobs """
        # The run command is used to setup up user job transform

        from RunJobUtilities import updateCopysetups

        ec = 0
        pilotErrorDiag = ""
        run_command = ""

        # get the queuedata info
        # (directAccess info is stored in the copysetup variable)

        # get relevant file transfer info
        dInfo, useCopyTool, useDirectAccess, dummy, oldPrefix, newPrefix, copysetup, usePFCTurl =\
               self.getFileTransferInfo(job.transferType, isBuildJob(job.outFiles))

        # add the user proxy
        if os.environ.has_key('X509_USER_PROXY'):
            run_command += 'export X509_USER_PROXY=%s;' % os.environ['X509_USER_PROXY']
        else:
            tolog("Could not add user proxy to the run command (proxy does not exist)")

        # set up analysis trf
        run_command += './%s %s' % (trfName, job.jobPars)

        # sort out direct access info for non-FAX cases
        if dInfo:
            # in case of forced usePFCTurl
            if usePFCTurl and not '--usePFCTurl' in run_command:
                oldPrefix = ""
                newPrefix = ""
                run_command += ' --usePFCTurl'
                tolog("reset old/newPrefix (forced TURL mode (1))")

            # sort out when directIn should be used
            if useDirectAccess and '--directIn' not in job.jobPars and '--directIn' not in run_command:
                run_command += ' --directIn'

            # old style copysetups will contain oldPrefix and newPrefix needed for the old style remote I/O
            if oldPrefix != "" and newPrefix != "":
                run_command += ' --oldPrefix "%s" --newPrefix %s' % (oldPrefix, newPrefix)
            else:
                # --directIn should be used in combination with --usePFCTurl, but not --old/newPrefix
                if usePFCTurl and not '--usePFCTurl' in run_command:
                    run_command += ' --usePFCTurl'

        if job.transferType == 'fax' and readpar('direct_access_wan').lower() == 'true' and '--directIn' not in run_command:
            run_command += ' --directIn'

        if job.transferType == 'direct':
            # update the copysetup
            # transferType is only needed if copysetup does not contain remote I/O info
            updateCopysetups(run_command, transferType=job.transferType, useCT=False, directIn=useDirectAccess)

        # add options for file stager if necessary (ignore if transferType = direct)
        if "accessmode" in job.jobPars and job.transferType != 'direct':
            accessmode_useCT = None
            accessmode_directIn = None
            _accessmode_dic = { "--accessmode=copy":["copy-to-scratch mode", ""],
                                "--accessmode=direct":["direct access mode", " --directIn"]}
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
                    elif _mode == "--accessmode=direct":
                        # make sure copy-to-scratch and file stager get turned off
                        usePFCTurl = True
                        accessmode_useCT = False
                        accessmode_directIn = True
                    else:
                        usePFCTurl = False
                        accessmode_useCT = False
                        accessmode_directIn = False

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
            if "--directIn" in run_command and not "export X509_USER_PROXY" in run_command:
                if os.environ.has_key('X509_USER_PROXY'):
                    run_command = run_command.replace("./%s" % (trfName), "export X509_USER_PROXY=%s;./%s" % (os.environ['X509_USER_PROXY'], trfName))
                else:
                    tolog("Did not add user proxy to the run command (proxy does not exist)")

            # update the copysetup
            updateCopysetups(run_command, transferType=None, useCT=accessmode_useCT, directIn=accessmode_directIn)

        # add guids when needed
        # get the correct guids list (with only the direct access files)
        if not isBuildJob(job.outFiles):
            _guids = self.getGuidsFromJobPars(job.jobPars, job.inFiles, job.inFilesGuids)
            run_command += ' --inputGUIDs \"%s\"' % (str(_guids))

        # if both direct access and the accessmode loop added a directIn switch, remove the first one from the string
        if run_command.count("directIn") > 1:
            run_command = run_command.replace("--directIn", "", 1)

        return ec, pilotErrorDiag, run_command

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
                    "madevent",
                    "HPC",
                    "objectstore*.json",
                    "saga",
                    "radical",
                    "movers",
                    "_joproxy15",
                    "ckpt*",
                    "HAHM_*",
                    "Process",
                    "merged_lhef._0.events-new"]

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
        for root, dirnames, filenames in os.walk(os.path.dirname(workdir)):
            for filename in fnmatch.filter(filenames, 'EventService_premerge_*.tar'):
                matches.append(os.path.join(root, filename))
        if matches != []:
            tolog("!!WARNING!!4990!! Encountered %d archive files - will be purged" % len(matches))
            tolog("To be removed: %s" % (matches))
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

    def getCacheInfo(self, m_cacheDirVer, atlasRelease):
        """ Get the cacheDir and cacheVer """

        cacheDirVer = m_cacheDirVer.group(1)
        if re.search('_', cacheDirVer) != None:
            cacheDir = cacheDirVer.split('_')[0]
            cacheVer = re.sub("^%s_" % cacheDir, '', cacheDirVer)

            # Special case for AtlasDerivation. In this case cacheVer = rel_N
            # and we need to add cacheDir and the release itself
            special_cacheDirs = ['AtlasDerivation','AtlasOffline','VAL', '.X', '-GIT', 'master', 'multithreading'] # Add more cases if necessary
            if cacheDir in special_cacheDirs and self.isNightliesRelease(cacheVer):
                # strip any special cacheDirs from the release string, if present
                for special_cacheDir in special_cacheDirs:
                    if special_cacheDir in atlasRelease:
                        # do not remove any VAL strings
                        if not "VAL" in special_cacheDir:
                            tolog("Found special cacheDir=%s in release string: %s" % (special_cacheDir, atlasRelease)) # 19.1.X.Y-VAL-AtlasDerivation
                            # Note: A standard nightlies release with .X will not be replaced:
                            atlasRelease = atlasRelease.replace('-' + special_cacheDir, '')
                        tolog("Release string updated: %s" % (atlasRelease))
                        cacheVer = atlasRelease + "," + cacheVer
                        break

        else:
            cacheDir = 'AtlasProduction'
            cacheVer = cacheDirVer

        tolog("cacheDir = %s" % (cacheDir))
        tolog("cacheVer = %s" % (cacheVer))

        return cacheDir, cacheVer

    def setPython(self, atlasRelease, homePackage, cmtconfig, sitename):
        """ set the python executable """

        ec = 0
        pilotErrorDiag = ""
        pybin = ""

        if os.environ.has_key('VO_ATLAS_SW_DIR') and verifyReleaseString(atlasRelease) != "NULL":
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

    def addEnvVars2Cmd(self, cmd, jobId, taskId, processingType, sitename, analysisJob):
        """ Add env variables """

        _sitename = 'export PANDA_RESOURCE=\"%s\";' % (sitename)
        _frontier1 = 'export FRONTIER_ID=\"[%s_%s]\";' % (taskId, jobId)
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
        _rucio += 'export RUCIO_ACCOUNT=\"' + os.environ.get('RUCIO_ACCOUNT','pilot') + '\";'

        return _sitename + _ttc + _frontier1 + _frontier2 + _rucio + _coreCount + _unset + cmd

    def getEnvVars2Cmd(self, jobId, taskId, processingType, sitename, analysisJob):
        """ Return array with enviroment variables """

        variables = []
        variables.append('export PANDA_RESOURCE=\"%s\";' % (sitename))
        variables.append('export FRONTIER_ID=\"[%s_%s]\";' % (taskId, jobId))
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

    def getASetupOptions(self, atlasRelease, homePackage):
        """ Determine the proper asetup options """

        asetup_opt = []
        release = re.sub('^Atlas-', '', atlasRelease)

        # is it a user analysis homePackage?
        if 'AnalysisTransforms' in homePackage:

            _homepackage = re.sub('^AnalysisTransforms-*', '', homePackage)
            if _homepackage == '' or re.search('^\d+\.\d+\.\d+$', release) is None:
                if release != "":
                    asetup_opt.append(release)
            if _homepackage != '':
                asetup_opt += _homepackage.split('_')

        else:

            asetup_opt += homePackage.split('/')
            if release not in homePackage:
                asetup_opt.append(release)

        # Add the notest,here for all setups (not necessary for late releases but harmless to add)
        asetup_opt.append('notest')
        # asetup_opt.append('here')

        # Add the fast option if possible (for the moment, check for locally defined env variable)
        if os.environ.has_key("ATLAS_FAST_ASETUP"):
            asetup_opt.append('fast')

        return ','.join(asetup_opt)

    def extractNightliesTimestamp(self, homePackage):
        """ Extract the nightlies timestamp from the homePackage """

        # s = "AtlasProduction,2016-11-29T2114"
        # -> timestamp = "2016-11-29T2114"

        timestamp = ""

        if "AnalysisTransforms" in homePackage:
            pattern = re.compile(r"AnalysisTransforms\-[A-Za-z0-9]+\_(\d+\-\d+\S+\d+)")
            found = re.findall(pattern, homePackage)
            if len(found) > 0:
                timestamp = found[0]

        else:
            pattern = re.compile(r".(\d+\-\d+\S+\d+)")
            found = re.findall(pattern, homePackage)
            if len(found) > 0:
                timestamp = found[0]

        return timestamp

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
        """ Return metadata (not known yet) for later file registration """

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
                fsize = os.path.getsize(filename)
                if fsize > 0:
                    tolog("!!WARNING!!3444!! Payload produced stdout but was interrupted (getstatusoutput threw an exception)")
                else:
                    tolog("!!WARNING!!3444!! Payload produced zero size output (%s)" % (filename))
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
                        if self.isSQLiteLockingProblem(filename, job=job):
                            tolog("NFS/SQLite locking problem detected")
                        elif self.isBuiltOnWrongArchitecture(transExitCode, filename, job=job):
                            tolog("Job was built on the wrong architecture")
                        else:
                            job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d" % (transExitCode)
                            # (do not set a pilot error code)
                    else:
                        job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d (%s does not exist)" % (transExitCode, filename)
                        # (do not set a pilot error code)
            else:
                job.pilotErrorDiag = "Payload failed due to unknown reason (check payload stdout)"
                job.result[2] = error.ERR_UNKNOWN

            # Any errors due to signals can be ignored if the job was killed because of out of memory
            if os.path.exists(os.path.join(job.workdir, "MEMORYEXCEEDED")):
                tolog("Ignoring any previously detected errors (like signals) since MEMORYEXCEEDED file was found")
                job.pilotErrorDiag = "Payload exceeded maximum allowed memory"
                job.result[2] = error.ERR_PAYLOADEXCEEDMAXMEM

            tolog("!!FAILED!!3000!! %s" % (job.pilotErrorDiag))

        # set the trf diag error
        if res[2] != "":
            tolog("TRF diagnostics: %s" % (res[2]))
            job.exeErrorDiag = res[2]

        job.result[1] = transExitCode
        return job

    def isSQLiteLockingProblem(self, filename, job=None):
        """
        Scan for NFS/SQLite locking problems.
        Note: the function updates the job object.

        :param filename: path to payload stdout (string).
        :param job: job object.
        :return: Boolean (True if locking problem has been identified, False otherwise).
        """

        failed = False
        error = PilotErrors()

        e1 = "prepare 5 database is locked"
        e2 = "Error SQLiteStatement"
        _out = commands.getoutput('grep "%s" %s | grep "%s"' % (e1, filename, e2))
        if 'sqlite' in _out and job:
            job.pilotErrorDiag = "NFS/SQLite locking problems: %s" % _out
            job.result[2] = error.ERR_NFSSQLITE
            failed = True

        return failed

    def isBuiltOnWrongArchitecture(self, transExitCode, filename, job=None):
        """
        Detect if the job was built on the wrong architecture.

        :param transExitCode: payload exit code (int).
        :param filename: path to payload stdout (string).
        :param job: job object.
        :return: Boolean (True if wrong architecture, False if no problem detected).
        """

        failed = False
        error = PilotErrors()

        if transExitCode == 221:
            tolog("Exit code 221 detected, will scan payload stdout for GLIBC errors")
            e1 = "cling::DynamicLibraryManager::loadLibrary():"
            e2 = "GLIBC"
            e3 = "not found"
            _out = commands.getoutput('grep "%s" %s | grep "%s" | grep "%s"' % (e1, filename, e2, e3))
            if _out and job:
                job.pilotErrorDiag = "Architecture problem detected: %s" % _out
                job.result[2] = error.ERR_WRONGARCHITECTURE
            else:
                job.pilotErrorDiag = "General runGen failure"
                job.result[2] = error.ERR_RUNGENFAILURE
            failed = True

        tolog("isBuiltOnWrongArchitecture=%s" % str(failed))

        return failed

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
    def doSpecialLogFileTransfer(self, **argdict):
        """ Should the log file be transfered to a special SE? """

        # The log file can at the end of the job be stored in a special SE - in addition to the normal stage-out of the log file
        # If this method returns True, the JobLog class will attempt to store the log file in a secondary SE after the transfer of
        # the log to the primary/normal SE. Additional information about the secondary SE is required and can be specified in
        # another optional method defined in the *Experiment classes

        transferLogToObjectstore = False

        eventService = argdict.get('eventService', False)
        putLogToOS = argdict.get('putLogToOS', False)
        if "log_to_objectstore" in readpar('catchall') or eventService or putLogToOS:
            transferLogToObjectstore = True
        if 'HPC_HPC' in readpar('catchall'):
            transferLogToObjectstore = True
        if 'HPC_HPCARC' in readpar('catchall'):
            transferLogToObjectstore = False
        if 'disable_log_to_objectstore' in readpar('catchall'):
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
    def verifyNCoresSettings(self, coreCount):
        """ Verify that nCores settings are correct """

        ec = 0
        pilotErrorDiag = ""

        try:
            del os.environ['ATHENA_PROC_NUMBER_JOB']
            tolog("Unset existing ATHENA_PROC_NUMBER_JOB")
        except:
            pass

        try:
            athenaProcNumber = int(os.environ['ATHENA_PROC_NUMBER'])
        except:
            athenaProcNumber = None

        # Note: if ATHENA_PROC_NUMBER is set (by the wrapper), then do not overwrite it
        # Otherwise, set it to the value of job.coreCount
        # (actually set ATHENA_PROC_NUMBER_JOB and use it if it exists, otherwise use ATHENA_PROC_NUMBER directly;
        # ATHENA_PROC_NUMBER_JOB will always be the value from the job definition)
        if athenaProcNumber:
            tolog("Encountered a set ATHENA_PROC_NUMBER (%d), will not overwrite it" % athenaProcNumber)
        else:
            os.environ['ATHENA_PROC_NUMBER_JOB'] = coreCount
            tolog("Set ATHENA_PROC_NUMBER_JOB to %s (ATHENA_PROC_NUMBER will not be overwritten/set - JOB var will be added to cmd)" % (coreCount))

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

        from SiteMover import SiteMover
        if envsetup == "":
            envsetup = SiteMover.getEnvsetup()
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
            # next clause had problems: grid-proxy-info -exists -valid 0.166666666667:00
            #cmd = "%sgrid-proxy-info -exists -valid %s:00" % (envsetup, str(limit))
            # more accurate calculation of HH:MM
            limit_hours=int(limit*60)/60
            limit_minutes=int(limit*60+.999)-limit_hours*60
            cmd = "%sgrid-proxy-info -exists -valid %d:%02d" % (envsetup, limit_hours, limit_minutes)
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

    def getModernASetup(self, swbase=None, asetup=True):
        """ Return the full modern setup for asetup """
        # Only include the actual asetup script if asetup==True. This is not needed if the jobPars contain the payload command
        # but the pilot still needs to added the exports and the atlasLocalSetup.

        path = "%s/atlas.cern.ch/repo" % (self.getCVMFSPath())
        cmd = ""
        if os.path.exists(path):
            cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/ATLASLocalRootBase;" % (path)
            cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;"
            if asetup:
                cmd += "source $AtlasSetup/scripts/asetup.sh"
        else:
            appdir = readpar('appdir')
            if appdir == "":
                if os.environ.has_key('VO_ATLAS_SW_DIR'):
                    appdir = os.environ['VO_ATLAS_SW_DIR']
            if appdir != "":
                if asetup:
                    cmd = "source %s/scripts/asetup.sh" % appdir

        return cmd

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
        if 'HPC_HPC' in readpar("catchall"):
            return False

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

        # Get the values from the memory monitor file (json if it exists, otherwise the preliminary txt file)
        # Note that only the final json file will contain the totRBYTES, etc
        summary_dictionary = self.getMemoryValues(workdir, pilot_initdir)

        tolog("summary_dictionary=%s"%str(summary_dictionary))

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
                #try:
                #    rchar = summary_dictionary['Other']['rchar']
                #except:
                #    rchar = -1
                #else:
                #    node['rchar'] = rchar
                #try:
                #    wchar = summary_dictionary['Other']['wchar']
                #except:
                #    wchar = -1
                #else:
                #    node['wchar'] = wchar
                #try:
                #    rbytes = summary_dictionary['Other']['rbytes']
                #except:
                #    rbytes = -1
                #else:
                #    node['rbytes'] = rbytes
                #try:
                #    wbytes = summary_dictionary['Other']['wbytes']
                #except:
                #    wbytes = -1
                #else:
                #    node['wbytes'] = wbytes
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
                #node['rchar'] = -1
                #node['wchar'] = -1
                #node['rbytes'] = -1
                #node['wbytes'] = -1
            else:
                tolog("Extracted standard info from memory monitor json")
            try:
                node['totRCHAR'] = summary_dictionary['Max']['totRCHAR']
                node['totWCHAR'] = summary_dictionary['Max']['totWCHAR']
                node['totRBYTES'] = summary_dictionary['Max']['totRBYTES']
                node['totWBYTES'] = summary_dictionary['Max']['totWBYTES']
                node['rateRCHAR'] = summary_dictionary['Avg']['rateRCHAR']
                node['rateWCHAR'] = summary_dictionary['Avg']['rateWCHAR']
                node['rateRBYTES'] = summary_dictionary['Avg']['rateRBYTES']
                node['rateWBYTES'] = summary_dictionary['Avg']['rateWBYTES']
            except Exception, e:
                tolog("totRCHAR,totWCHAR,totRBYTES,totWBYTES,rateRCHAR,rateWCHAR,rateRBYTES,rateWBYTES were not found in memory monitor json (or json doesn't exist yet) - ignoring")
            else:
                tolog("totRCHAR,totWCHAR,totRBYTES,totWBYTES,rateRCHAR,rateWCHAR,rateRBYTES,rateWBYTES were extracted from memory monitor json")
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
        #    "Avg":{"avgVMEM":19384236,"avgPSS":5023500,"avgRSS":6501489,"avgSwap":5964997},
        #    "Other":{"rchar":NN,"wchar":NN,"rbytes":NN,"wbytes":NN}}

        maxVMEM = -1
        maxRSS = -1
        maxPSS = -1
        maxSwap = -1
        totRCHAR = -1 # tot=max
        totWCHAR = -1
        totRBYTES = -1
        totWBYTES = -1
        avgVMEM = 0
        avgRSS = 0

        avgPSS = 0
        avgSwap = 0
        totalVMEM = 0
        totalRSS = 0
        totalPSS = 0
        totalSwap = 0
        totalRCHAR = 0
        totalWCHAR = 0
        totalRBYTES = 0
        totalWBYTES = 0
        N = 0
        summary_dictionary = {}

        rchar = None
        wchar = None
        rbytes = None
        wbytes = None

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
                                # Remove empty entries from list (caused by multiple \t)
                                l = filter(None, line.split('\t'))
                                Time = l[0]
                                VMEM = l[1]
                                PSS = l[2]
                                RSS = l[3]
                                Swap = l[4]
                                # note: the last rchar etc values will be reported
                                if len(l) == 9:
                                    rchar = int(l[5])
                                    wchar = int(l[6])
                                    rbytes = int(l[7])
                                    wbytes = int(l[8])
                                else:
                                    rchar = None
                                    wchar = None
                                    rbytes = None
                                    wbytes = None
                            except Exception, e:
                                tolog("!!WARNING!!4542!! Unexpected format of utility output: %s (expected format: Time, VMEM, PSS, RSS, Swap [, RCHAR, WCHAR, RBYTES, WBYTES])" % (line))
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
                    summary_dictionary = { "Max": {}, "Avg": {}, "Other": {} }
                    summary_dictionary["Max"] = { "maxVMEM":maxVMEM, "maxPSS":maxPSS, "maxRSS":maxRSS, "maxSwap":maxSwap }
                    if rchar:
                        summary_dictionary["Other"]["rchar"] = rchar
                    if wchar:
                        summary_dictionary["Other"]["wchar"] = wchar
                    if rbytes:
                        summary_dictionary["Other"]["rbytes"] = rbytes
                    if wbytes:
                        summary_dictionary["Other"]["wbytes"] = wbytes
                    if N > 0:
                        avgVMEM = int(float(totalVMEM)/float(N))
                        avgPSS = int(float(totalPSS)/float(N))
                        avgRSS = int(float(totalRSS)/float(N))
                        avgSwap = int(float(totalSwap)/float(N))
                    summary_dictionary["Avg"] = { "avgVMEM":avgVMEM, "avgPSS":avgPSS, "avgRSS":avgRSS, "avgSwap":avgSwap }

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

        default_release = "21.0.22" #"21.0.18" #"21.0.17" #"20.7.5" #"20.1.5"
        # default_patch_release = "20.7.5.8" #"20.1.5.2" #"20.1.4.1"
        default_cmtconfig = "x86_64-slc6-gcc62-opt"
        # default_swbase = "%s/atlas.cern.ch/repo/sw/software" % (self.getCVMFSPath())
        default_swbase = "%s/atlas.cern.ch/repo" % (self.getCVMFSPath())
        # default_setup = "source %s/ATLASLocalRootBase/user/atlasLocalSetup.sh --quiet; " \
        #                 "source %s/ATLASLocalRootBase/x86_64/AtlasSetup/current/AtlasSetup/scripts/asetup.sh AtlasOffline,%s,notest" %\
        #                 (default_swbase, default_swbase, default_release)
        # default_setup = "source %s/ATLASLocalRootBase/user/atlasLocalSetup.sh --quiet; " \
        #                 "source %s/ATLASLocalRootBase/x86_64/AtlasSetup/current/AtlasSetup/scripts/asetup.sh Athena,%s" %\
        #                 (default_swbase, default_swbase, default_release)
        default_setup = self.getModernASetup() + " Athena," + default_release + " --platform " + default_cmtconfig

        # Construct the name of the output file using the summary variable
        if summary.endswith('.json'):
            output = summary.replace('.json', '.txt')
        else:
            output = summary + '.txt'

        # Get the standard setup
        cacheVer = homePackage.split('/')[-1]

        # Could anything be extracted?
        #if homePackage == cacheVer: # (no)
        if isAGreaterOrEqualToB(default_release, release) or default_release == release or release == "NULL":  # or NG
            # This means there is no patched release available, ie. we need to use the fallback
            useDefault = True
            tolog("%s >= %s" % (default_release, release))
        else:
            useDefault = False
            tolog("%s < %s" % (default_release, release))

            tolog("!!WARNING!!3434!! Currently only default MemoryMonitor version (%s) is allowed" % default_release)
            useDefault = True

        if useDefault:
            tolog("Will use default (fallback) setup for MemoryMonitor since patched release number is needed for the setup, and none is available")
            cmd = default_setup
        else:
            # get the standard setup
            options = self.getASetupOptions(release, homePackage)
            asetup_options = " " + options + " --platform " + cmtconfig
            standard_setup = self.getModernASetup() + asetup_options

            _cmd = standard_setup + "; which MemoryMonitor"
            # Can the MemoryMonitor be found?
            try:
                ec, output = timedCommand(_cmd, timeout=60)
            except Exception, e:
                tolog("!!WARNING!!3434!! Failed to locate MemoryMonitor: will use default: %s" % (e))
                cmd = default_setup
            else:
                if "which: no MemoryMonitor in" in output:
                    tolog("Failed to locate MemoryMonitor: will use default (for release %s)" % (default_release))
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
