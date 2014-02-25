#!/usr/bin/python -u 

import cgi, sys, atexit, signal
import commands, getopt, os, time, re
from SocketServer import BaseRequestHandler 
from glob import glob
from shutil import copy, copy2

from PilotErrors import PilotErrors
from JobState import JobState
from ProxyGuard import ProxyGuard
from pUtil import *
from JobRecovery import JobRecovery
from FileStateClient import createFileStates, updateFileState, dumpFileStates, getFileState
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from JobLog import JobLog
import Mover as mover
import Site, Job, Node, pUtil

# 58a:
# Forcing usePFCTurl in getFileTransferInfo() (Experiment)
# Updated usage of usePFCTurl in getAnalysisRunCommand() (Experiment)
# Added fax_mode to getLFCFileList() (Mover)
# Now using fax_mode in getLFCFileList() (Mover)
# Added fax_mode logic to getPoolFileCatalog() (Mover)
# Updated shouldPFC4TURLsBeCreated() to always create TURL based PFC for direct access (and empty old/newPrefix) (Mover)
# Removing "?svcClass" etc from TURLs/PFNs in updatePFN() if present to prevent problem in runGen/runAthena (Mover)
# Added --file $X509_USER_PROXY to verifyProxy(), dumpExtendedProxy() (SiteMover)
# Converting from "HH:MM:SS" format to SS for EMI-3 systems in verifyProxy() (SiteMover)
# Removed the allowdirectaccess requirement in shouldPFC4TURLsBeCreated() (Mover)
# Forced usePFCTurl in getFileTransferInfo(), reset old/newPrefix (Experiment)
# Testing CVMFS in specialChecks(), requested by Asoka De Silva (ATLASExperiment)
# Created testCVMFS() (ATLASExperiment)
# Changed "debugon" to "debug" in updateJobs() (pilot)
# No setup string in getTURLs() for lcg-getturls (Mover)
# Correcting for potentially missing sepaths in dst_gpfn, in getFinalLCGPaths() (SiteMover)
# Now identifying _CONDOR_JOB_AD in getBatchSytemJobID(), requested by David Rebatto (pUtil)
# Removed --ciphers options to curl in core_get_data(), requested by Jhen-Wei Huang (curlSiteMover)
# Relaxed warning message in getMatchingDestinationPath() (SiteMover)
# Added exception to exeErrorDiag "OK" message in postJobTask() (JobLog)
# Added file exceptions list to removeRedundantFiles(), ie files that will not be removed during workdir cleanup. Requested by
# Wolfgang Ehrenfeld (ATLASExperiment)
# Moved stripDQ2FromLFN() from Mover to pUtil
# Now removing any __DQ2-parts from the LFN prior to TURL based PFC creation in createPoolFileCatalog() (pUtil)
# Created convertSURLtoTURL() (Mover)
# Calling convertSURLtoTURL() from getTURLs() (alternative to using lcg-getturls) (Mover)
# Added -b T srmv2 to lcg-getturls in getTURLs() (Mover)
# Created updatePandaLogger(), code from Ilija Vukotic (PandaServerClient)
# Calling updatePandaLogger() from updatePandaServer() (PandaServerClient)
# Moved getQueuedata() to ATLASSiteInformation (SiteInformation, ATLASSiteInformation)
# Changed processQueuedata() call in prepareAlternativeStageOut() to getQueuedata() (Mover)
# Changed processQueuedata() call in handleQueuedata() to getQueuedata() (pilot)
# Merged processQueuedata() with getQueuedata() (ATLASSiteInformation)
# Removed processQueuedata() since it is no longer needed (SiteInformation)
# Added geant4 to directory list in removeRedundantFiles() (ATLASExperiment)
# Updated to the latest versions of CMSExperiment and CMSSiteInformation
# Renamed rmRedundants() to removeRedundantFiles() (CMSExperiment)
# Now using getSiteInformation() in displayChangeLog() (ATLASExperiment)
# Using verifySwbase() from experiment class in handleQueuedata() (pilot)
# Calling interpretPayloadStdout() from interpretPayload() using experiment object (ErrorDiagnosis)
# Moved interpretPayloadStdout() from ErrorDiagnosis to *Experiment classes

# 58b:
# Checking for zero length TURL dictionary in getTURLs() (Mover)
# Not setting an error code for the case of empty TURL dictionary (and ec = 0) in createPFC4TURLs() (Mover)
# Allowing failure of dq2.info, used to get LFC host list from ToA, to prevent exception in case of missing dq2.info module
# (non-critical problem seen on ANALY_BNL_SHORT) (ATLASExperiment)
# Added missing __error variable in ATLASSiteInformation
# Added dsdict argument to PFC4TURLs(), createPFC4TURLs(), getTURLs() (Mover)
# Sending dsdict to PFC4TURLs() in mover_get_data() (Mover)
# Sending dsdict to createPFC4TURLs() in PFC4TURLs() (Mover)
# Sending dsdict to getTURLs() in createPFC4TURLs() (Mover)
# Using dsdict to get the dataset name before calling convertSURLtoTURL() in getTURLs() (Mover)
# Sending dataset name to convertSURLtoTURL() in getTURLs() (Mover)
# Added dataset argument to convertSURLtoTURL() (Mover)
# Created convertSURLtoTURLUsingDataset(), used by convertSURLtoTURL() (Mover)
# Changed kSI2kseconds to s in setTimeConsumed(), only shown in job report (pUtil)
#
# 58c:
# Using copyprefix and faxredirector to construct old/newPrefix in getPrefices() (Mover)
# Created getAllQueuedataFilename(), downloadAllQueuenames(), getAllQueuedata(), getTier1Queuename() (Mover)
# Added RU to cloud list and updated US T1 in setTier1Info() (ATLASSiteInformation)
# Created getTier1Name() and a new getTier1Queue() (ATLASSiteInformation)
# Renamed getWNMem() to getsetWNMem(), used in runMain() (pilot)
# Removed useless hasQueuedata boolean from getsetWNMem(), handling in runMain() (pilot)
# Changed float to int for -k memory option in argParser() (pilot)
# Rewrote getsetWNMem() (pilot)
# Removed unused function getUlimitVMEM() (pUtil)
# Added new test code for updating PandaLogger for FAX jobs from Ilija Vukotic (PandaServerClient)
# Now counting number of file transfers with FAX when FAX is used as a primary site mover in mover_get_data() (Mover)
# Created toPandaLogger(), written by Ilija Vukotic (pUtil)
# Importing toPandaLogger and using it in updatePandaServer() (PandaServerClient)
#
# 58d:
# Added a try-statement around resource.setrlimit() in getsetWNMem() (pilot)
#
# 58e:
# Removed legacy code related to md5 and hashlib (Mover)
# Renamed m to has_userid, hashf to hash_pilotid in getInitialTracingReport() (Mover)
# Now importing relevant md5 module/function in getInitialTracingReport() to avoid deprecation warning (Mover)
# Applied same trick as above for the getPathFromScope() which previously required hashlib, now works with older python as well (Mover)
# Created getFAXDictionary() used by mover_get_data() (Mover)
# Now returning FAX dictionary instead of complex FAX tuple from mover_get_data(), get_data() (Mover)
# Now receiving FAX dictionary from mover_get_data() in get_data() (Mover)
# Now receiving FAX dictionary from get_data() in stageIn(), added extraction of info from dictionary (runJob)
# Added bytesWithoutFAX and bytesWithFAX to Job class (Job)
# Updated Panda Logger message for FAX changes (above) in updatePandaServer() (PandaServerClient)
# Removed legacy code (updatePandaLogger) (PandaServerClient)
# Added file size counters for FAX transfers in mover_get_data(), reported to Panda Logger in FAX mode (Mover)
# Added file size counters for FAX transfers to jobMetrics in getJobMetrics() (PandaServerClient)
# Added protection against None return from PerfMonComps.PMonSD.parse() in the VmPeak script (VmPeak)
# Added bytesWith[out]FAX to updateJobInfo() (RunJobUtilities)    [added to 59a]
# Added bytesWith[out]FAX to updateHandler::handle() (pilot)    [added to 59a]
# Now touching a KILLED file in signal handler, to abort multi-job loop after kill signal, requested by John Hover (pilot)    [added to 59a]
# Checking for KILLED file in multi-job loop and aborting if necessary, in runMain() (pilot)    [added to 59a]
# Corrected exclusions in removeRedundantFiles(), requested by Wolfgang Ehrenfeld (ATLASExperiment)
# Added protections against additional print-outs from voms-proxy-info, in verifyProxy(). Requested by Rodney Walker (SiteMover)
# Registered signal.SIGUSR1 un runMain() (pilot)    [added to 59a]
# Improved log message for local access in getFileAccess() (Mover)
# Removed FAX info from jobMetrics, in getJobMetrics() (PandaServerClient)
#
# 58f:
# Added test jobState file to removeRedundantFiles() list (ATLASExperiment)     [added to 59a]
# Added ERR_NOSUCHFILE to file corruption report in mover_get_data(), requested by Cedric Serfon (Mover)    [added to 59a]
# Added support for updating space check frequency via env variable in setUpdateFrequencies(), requested by David Lesny (pilot)    [added to 59a]
# Removed call to setHTTPProxies (pilot)    [added to 59a]
# Removed function setHTTPProxies (pUtil)    [added to 59a]
# Removed unneeded code and comments about http[s]_proxy (runJob)    [added to 59a]
# Removed http[s]_proxy from env variable list (atlasProdPilot)    [added to 59a]
# Added timedCommand() to testCVMFS(), requested by Rodney Walker (ATLASExperiment)    [added to 59a]
# Created interpretProxyInfo() (SiteMover)    [added to 59a]
# Updated verifyProxy() to primarily use arcproxy instead of voms-proxy-info (SiteMover)    [added to 59a]
# Renamed all s11 to exitcode and o11 to output (SiteMover)    [added to 59a]
# Created getDN() (pilot)    [added to 59a]
# Trying to use arcproxy before voms-proxy-info in getDN() used in getDispatcherDictionary() (pilot)    [added to 59a]
# Added appdir to fields list in evaluateQueuedata() (SiteInformation)    [added to 59a]
# Now handling |-signs in appdir, in evaluateQueuedata() (SiteInformation)    [added to 59a]
# Added hardcoded setup path for arcproxy in verifyProxy() (move to wrapper) (SiteMover)    [added to 59a]
# Added new global variable getjobmaxtime (pilot)    [added to 59a]
# Added new pilot option -G <getJobMaxTime> in usage(), argParser() (pilot)    [added to 59a]
# Reading -G option in argParser() (pilot)    [added to 59a]
# Rewrote getJob() to download jobs for a maximum time instead of maximum number of trials (pilot)    [added to 59a]
# Importing JobLog early in pilot to prevent later problem on some sites (e.g. at EELA-UTFSM) (pilot)    [added to 59a]
# Removed getProperPaths() (SiteMover)    [added to 59a]
# Added experiment variable in put_data() (aria2cSiteMover)    [added to 59a]
# Added si object in put_data() (aria2cSiteMover)    [added to 59a]
# Now using si object instead of self when calling getProperPaths() (aria2cSiteMover)    [added to 59a]
# Added token, prodSourceLabel arguments to getFinalLCGPaths() call in getProperPaths() (ATLASSiteInformation)    [added to 59a]
# Added token, prodSourceLabel arguments to getFinalLCGPaths() (SiteMover)    [added to 59a]
# Now getting the proper sepath using getPreDestination() in getFinalLCGPaths() (SiteMover)    [added to 59a]
# Changed the return error code in case of value exception in interpretProxyInfo() (SiteMover)    [added to 59a]
# Improved exit code handling from interpretProxyInfo() in verifyProxy() (SiteMover)    [added to 59a]
# Adding missing /CN=proxy to DN string if necessary in getDN() (pilot)    [added to 59a]
# Added "runwrapper", "jobReport", "log." to exclusion list in removeRedundantFiles(), requested by Graeme Stewart (ATLASExperiment)   [added to 59a]
# Added extractAppdir() to ATLASSiteInformation    [added to 59a]
# Now using new environmental variable VO_ATLAS_NIGHTLIES_DIR for nightlies in extractAppdir() (ATLASSiteInformation)    [added to 59a]
#
# 58g:
# Created PilotFAXErrorCodes and isPilotFAXErrorCode() (PilotErrors)    [added to 59a]
# now uses error.isPilotFAXErrorCode instead of error.isPilotResubmissionErrorCode in mover_get_data() (1103 was missing) (Mover)    [added to 59a]
# Added surl argument to sitemover.getGlobalFilePaths() call in convertSURLtoTURLUsingDataset() (Mover)    [added to 59a]
# Added surl argument to getGlobalFilePaths() (FAXSiteMover)    [added to 59a]
# Added surl argument to self.getGlobalFilePaths() call in findGlobalFilePath() (FAXSiteMover)    [added to 59a]
# Sending gpfn instead to lfn to self.findGlobalFilePath() in get_data() (FAXSiteMover)    [added to 59a]
# Change filename to surl argument in findGlobalFilePath() (FAXSiteMover)    [added to 59a]
# Extracting filename from surl in findGlobalFilePath() (FAXSiteMover)    [added to 59a]
# Created extractPattern() (pUtil)    [added to 59a]
# Updated getGlobalFilePaths() to use extractPattern() for generating a global Rucio path using new convension (FAXSiteMover)    [added to 59a]
# Redefined SITEROOT in getProperSiterootAndCmtconfig(), requested by Alessandro de Salvo (ATLASExperiment)    [added to 59a]
# Created a new version of getProperSiterootAndCmtconfig(), still verifying cmtconfig but using redefined SITEROOT (ATLASExperiment)    [added to 59a]
# Replaced call to findPythonInRelease() with "python" ["which python"] in setPython(), requested by Rodney Walker (ATLASExperiment)    [added to 59a]
# Simplified getProdCmd3() using no path for the trf ["which <trf>"], requested by Rodney Walker (ATLASExperiment)    [added to 59a]
# Fixed a problem with core file removal in removeCoreDumps(), wrong type, requested by Wolfgang Ehrenfeld (JobLog)    [added to 59a]
# Displaying IP number of WN in collectMachineFeatures() (Node)    [added to 59a]
# Created isJEMAllowed(), used from getJobExecutionCommand() (ATLASExperiment)    [added to 59a]
# Created getSpecialAppdir(), used by extractAppdir() (ATLASSiteInformation)    [added to 59a]
# Nightlies appdir extraction now uses getSpecialAppdir() (ATLASSiteInformation)     [added to 59a]
# Getting appdir from VO_ATLAS_RELEASE_DIR for AtlasP1HLT and AtlasHLT jobs, using getSpecialAppdir in extractAppdir() (ATLASSiteInformation)    [added to 59a]
# Added '2> /dev/null' to all uuidgen usages, since command threw stderr on some NG analysis jobs on slc6, requested by Andrej
# Filipcic (Job, JobLog, Mover, pUtil, pilot)     [added to 59a]
# Corrected findGlobalFilePath() and convertSURLtoTURLUsingDataset() for new Rucio file convention containing ":" (FAXSiteMover, Mover)    [added to 59a]
# Created getVerifiedAtlasSetupPath(), used by useAtlasSetup() (ATLASExperiment)    [added to 59a]
# Updated getProperSiterootAndCmtconfig() for special SITEROOT on AFS (needed for HLT) (ATLASExperiment)    [added to 59a]
# Added HLT special case in getProdCmd2() (ATLASExperiment)    [added to 59a]
# Added special setup for HLT jobs in getProperASetup() (ATLASExperiment)    [added to 59a]
# Replaced 'dummy' with 'atlasProject' after checkCMTCONFIG() call in getJobExecutionCommand() (ATLASExperiment)    [added to 59a]
# Using updateCmd1WithProject() in getJobExecutionCommand() (ATLASExperiment)    [added to 59a]
# Created updateCmd1WithProject() (ATLASExperiment)    [added to 59a]
# Extended extractFilePaths() to handle unevaluated env variables (pUtil)    [added to 59a]
# Created new setup options for HLT jobs in getProperASetup() (ATLASExperiment)    [added to 59a]
# Corrected LFNs containing rucio :-separators in createPoolFileCatalog() (pUtil)    [added to 59a]
#
# 58h:
# Created getRucioReplicaDictionary() and getRucioFileDictionary() (Mover)    [added to 59a]
# Created getFileCatalog() (Experiment, ATLASExperiment)    [added to 59a]
# Renamed getReplicas() to getReplicasLFC() (Mover)    [added to 59a]
# Renamed getReplicasDictionary() to getReplicaDictionaryLFC() (Mover)    [added to 59a]
# Renamed old getReplicaDictionary() to getReplicaDictionaryFile() (Mover)    [added to 59a]
# Created getReplicaDictionary(), used in getLFCFileList() (Mover)    [added to 59a]
# Renamed getLFCFileList() to getCatalogFileList (Mover)    [added to 59a]
# Renamed getLFCHosts() to getFileCatalogHosts (Mover, Experiment, ATLASExperiment)    [added to 59a]
# Now using getFileCatalog() in getFileCatalogHosts() instead of readpar('lfchost') (Mover)    [added to 59a]
# Added lfchost option to getReplicasLFC() (Mover)    [added to 59a]
# Added host argument to getReplicaDictionary(), sending it to getReplicasLFC() (Mover)    [added to 59a]
# Sending scope_dict to getCatalogFileList() in getPoolFileCatalog() (Mover)    [added to 59a]
# Added scope_dict argument to getCatalogFileList() (Mover)    [added to 59a]
# Sending scope_dict to getReplicaDictionary() in getCatalogFileList() (Mover)    [added to 59a]
# Added scope_dict argument to getReplicaDictionary() (Mover)    [added to 59a]
# Sending scope_dict to getReplicaDictionaryRucio() in getReplicaDictionary() (Mover)    [added to 59a]
# Updated logger URL in toPandaLogger(). Requested by Ilija Vukotic and Valeri Fine (pUtil)    [added to 59a]
# Added new version on JEMstub. Requested by Frank Volkmer (JEMstub)    [added to 59a]
# Renamed registerFileLFC() to registerFileInCatalog() (Mover, SiteMover)    [added to 59a]
# Created bulkRegisterFiles() used by registerFileInCatalog() (SiteMover)    [added to 59a]
# Added scope to registerFileInCatalog() call in mover_put_data() (Mover)    [added to 59a]
# Added scope argument to registerFileInCatalog() (SiteMover)    [added to 59a]
# Now using bulkRegisterFiles() for registering files using Rucio methods (SiteMover)    [added to 59a]
# Now sending logFile to getScope() to prevent problem with merged log files which contain .log. (Mover)    [added to 59a]
# Using logFile in getScope() (Mover)    [added to 59a]
# Sending thisExperiment.getFileCatalog() to registerFileInCatalog() from mover_put_data() (Mover)    [added to 59a]
# Now getting full schedconfig dump, using ".all.json" instead of ".pilot.json" in getQueuedata() (SiteInformation)    [added to 59a]
# Minor cleanup in if statement for lfc file registration in put_data() (Mover)    [added to 59a]
# Sending thisExperiment to getCatalogFileList() from getPoolFileCatalog() (Mover)    [added to 59a]
# Added thisExperiment argument to getCatalogFileList(), getReplicaDictionary() (Mover)    [added to 59a]
# Sending thisExperiment to getReplicaDictionary() from getCatalogFileList() (Mover)    [added to 59a]
# Created willDoAlternativeFileLookups(), used by getReplicaDictionary() (Experiment, ATLASExperiment)    [added to 59a]
# Sending thisExperiment.getFileCatalog() instead of lfc host to getReplicaDictionary() (Mover)    [added to 59a ??? not explicitly]
# Removed Exception in exception handling in getRucioReplicaDictionary() (Mover)    [added to 59a]
# Removed Exception in exception handling in isTapeSite(), getDQ2SEType(), reportFileCorruption(), bulkRegisterFiles() (SiteMover)[added to 59a]
# Removed RUCIO_ACCOUNT warning from checkSpecialEnvVars() (pUtil)    [added to 59a]
# Added RUCIO_ACCOUNT to payload setup in addEnvVars2Cmd() (ATLASExperiment)    [added to 59a]
# Created cleanupAthenaMP(). Code from Graeme Stewart and Wolfgang Ehrenfeld (ATLASExperiment)    [added to 59a]
# Calling cleanupAthenaMP() from removeRedundantFiles() (ATLASExperiment)    [added to 59a]
#
# 58i
# Removed Exception in getFileCatalogHosts() (ATLASExperiment)    [added to 59a]
# Corrected undefined variable in bulkRegisterFiles() (SiteMover)    [added to 59a]
# Truncating string in encode_string() to prevent potential problems with sending long TCP messages (pUtil)    [added to 59a]
# Added error handling for empty replica dictionaries in getReplicaDictionaryRucio() (Mover)    [added to 59a]
# Added special case for /cvmfs in getProperSiterootAndCmtconfig() (ATLASExperiment)    [added to 59a]
# Cutting away the trailing /rucio bit from the destination path in getFullPath(), instead of a full replace of
# /rucio in the path to prevent a problem with ruciotest paths used by David Cameron (SiteMover)    [added to 59a]
# Increased buffer size from 2k to 4k in updateHandler::handle() (pilot)    [added to 59a]
# Added warning message for too long TCP messages in updateJobInfo() (RunJobUtilities)    [added to 59a]
# Now using full path including protocol for file catalog hosts in getFileCatalogHosts() (ATLASExperiment)    [added to 59a]
# Removed exclusion of non-LFC hosts in getFileCatalogHosts() (ATLASExperiment)    [added to 59a]
# Corrected host instead of thisExperiment.getFileCatalog() in call to getReplicaDictionaryRucio() in getReplicaDictionary() (Mover)    [added to 59a]
# Added trackback info to exception handling in getRucioReplicaDictionary() (Mover)    [added to 59a]
# Added trackback info to exception handling in getFileCatalogHosts() (ATLASExperiment)    [added to 59a]
# Fixed left exception e string in getFileCatalogHosts() (ATLASExperiment)    [added to 59a]
#
# 58j:
# put_data() now using getProperPaths() (Support for Rucio) (SiteMover, xrootdSiteMover)    [added to 59a]
# Renamed analJob to analyJob in put_data() (xrootdSiteMover)    [added to 59a]
# Created verifySURLGUIDDictionary() used from getReplicaDictionaryRucio() (Mover)    [added to 59a]
# Removed LFC from error messages ERR_FAILEDLFCGETREPS, ERR_FAILEDLFCGETREP, ERR_FAILEDLFCREG (PilotErrors) [Update DB]    [added to 59a]

# Todo:
# Move checkSpecialEnvVars() from pUtil to Experiment.
# Cleanup lfchost usage in Mover. Mainly unused, or not needed.
# Remove usage of failureCode in runJob, ErrorDiagnosis, *Experiment. Does not work anyway
# Hide argument complexity of interpretPayload* methods by using kwarg**
# Continue testing alt stage-out, is files[Alt|Normal]StageOut sent to PandaServerClient?
# Payload in build job fails if copysetup has file stager turned on
# Test: Outcommented the call to removePyModules() from cleanup() (runJob)
# update db for error meanings:
# 1187: Renamed "Athena metadata is not available" to "Payload metadata is not available" (PilotErrors)
# 1212: Renamed "Athena ran out of memory" to "Payload ran out of memory" (PilotErrors)
# Update put_data() functions to use getProperPaths()
# use checkForDirectAccess in get_data() in more site movers
# enable ERR_DAFSNOTALLOWED in next pilot version (getAnalyCmd3(), RunJobUtilities)
# Correct two occurances of dst_grpn -> dst_gpfn in put_data() (xrdcpSiteMover)
# statusPFCTurl not returned from PFC4TURLs() and not used in a correct way in Mover, see mover_get_data()
# remove:
# stat test in createSiteWorkDir, cleanup (pilot) for QMUL
# test code for importing errordiagnosis
# shouldPFC4TURLsBeCreated() updated for short form of copysetup, useFileStager not needed for TURL based PFC decision (Mover)?
# Updated the PanDA Logger URL in toPandaLogger(). Requested by Valeri Fine and Ilija Vukotic (pUtil)

version = getPilotVersion()
pilot_version_tag = 'PR' # 'RC'
force_devpilot = False

# get the python executable path, to be used by child job
pyexe = sys.executable

# default job dispatcher web server
pshttpurl = "pandaserver.cern.ch"

# global variables with some default (test) values
psport = 25443                      # panda server listening port
uflag = None                        # flag if this pilot is to only get a user analysis job or ordinary job
abortDQSpace = 10                   # unit is in GB
warnDQSpace = 100                   # unit is in GB
localsizelimit_stdout = 2*1024**2   # size limit of payload stdout size during running. unit is in kB
localspacelimit = 2*1024**2         # space limit of remaining local disk size during running. unit is in kB
localspacelimit_user = 7*1024**2    # maximum size of user work area. unit is in kB
localspacelimit0 = 5*1024**2        # initial space limit before asking for a job. unit is in kB
outputlimit = 500*1024**3           # maximum allowed size of an output file. unit is B
pilotId = 'xtestP001'               # pilot id
jobSchedulerId = 'xtestJS001'       # scheduler id
heartbeatPeriod = 30*60             # 30 minutes limit for checking job state file
maxjobrecDefault = 20               # default maximum number of job recoveries
maxjobrec = maxjobrecDefault        # maximum number of job recoveries
zombieJobList = []                  # zombie list
prodJobDone = False                 # will be set to true when production job has finished
rmwkdir = None                      # if set to true, workdir will always be deleted
jobrec = True                       # perform job recovery per default
jobRequestFlag = True               # ask server for initial job (read from file otherwise)
debugLevel = 0                      # 0: debug info off, 1: display function name when called, 2: full debug info
maxNumberOfRecoveryAttempts = 15    # as attempted by the job recovery
stagein = False                     # set to True during stagein phase
stageout = False                    # set to True during stageout phase
queuename = ""                      # name of queue used to download config info
loopingLimitDefaultProd = 12*3600   # if job does not write anything in N hours, it is considered a looping job (production jobs)
loopingLimitDefaultUser = 3*3600    # if job does not write anything in N hours, it is considered a looping job (user analysis jobs)
loopingLimitMinDefault = 2*3600     # minimum allow looping limit
stageinretry = 2                    # number of stage-in tries
stageoutretry = 2                   # number of stage-out tries
logFileDir = ""                     # log file directory
testLevel = "0"                     # test suite control variable (0: no test, 1: put error, 2: ...)
loggingMode = None                  # True puts the pilot in logging mode (will not run a job), False is a normal pilot. None means run as before
updateServerFlag = True             # switch on/off Panda server updates (for testing only), default is True
globalSite = None                   # site object
globalWorkNode = None               # work node object
globalJob = None                    # job object
memory = None                       # for memory restrictions when asking the dispatcher for a new job (MB)
proxycheckFlag = True               # True (default): perform proxy validity checks, False: no check
wrapperFlag = False                 # True for wrappers that expect an exit code via return, False (default) when exit() can be used
cleanupLimit = 2                    # cleanup time limit in hours, see Cleaner.py
logTransferred = False              # Boolean to keep track of whether the log has been transferred or not
errorLabel = "WARNING"              # set to FAILED when job recovery is not used
nSent = 0                           # throttle variable
proxyguard = None                   # global proxyguard object needed for the signal handler to be able to restore the proxy if necessary
jobRecoveryMode = None              # when true, the pilot only runs job recovery, no payload download
pilotToken = None                   # pilot authentication token
countryGroup = ""                   # Country group selector for getJob request
workingGroup = ""                   # Working group selector for getJob request
allowOtherCountry = False           #
inputDir = ""                       # Location of input files (source for mv site mover)
outputDir = ""                      # Location of output files (destination for mv site mover)
jobIds = []                         # Global job id list
stageoutStartTime = None            # Set when pilot receives "stageout" state message, used by looping job killer
lfcRegistration = True              # Should the pilot perform LFC registration?
timefloor_default = None            # Time limit for multi-jobs in minutes (turned off by default)
useCoPilot = False                  # CERNVM Co-Pilot framework (on: let Co-Pilot finish job, off: let pilot finish job (default))
update_freq_server = 30*60          # Server update frequency, 30 minutes
experiment = "ATLAS"                # Current experiment (can be set with pilot option -F <experiment>)
getjobmaxtime = 3*60                # The max time the pilot is allowed to attempt job downloads (can be set with -G <maxtime>)
pandaJobDataFileName = "pandaJobData.out" # job definition file name

# set the pilot initdir pathname
pilot_initdir = os.getcwd()

# data structure for job information in main loop
# jobDic = {'prod/analy':[pid,job,os.getpgrp]}
# (analysis label currently not used)
jobDic = {}

def usage():
    """
    usage: python pilot.py -s <sitename> -d <workdir> -a <appdir> -w <url> -p <port> -q <dq2url> -u <user> -m <outputdir> -g <inputdir> -r <rmwkdir> -j <jrflag> -n <jrmax> -c <jrmaxatt> -f <jreqflag> -e <logfiledir> -b <debuglevel> -h <queuename> -x <stageinretry> -y <loggingMode> -z <updateserver> -k <memory> -t <proxycheckflag> -l <wrapperflag> -i <pilotreleaseflag> -o <countrygroup> -v <workingGroup> -A <allowOtherCountry> -B <lfcRegistration> -C <timefloor> -D <useCoPilot> -E <stageoutretry> -F <experiment> -G <getJobMaxTime>
    where:
               <sitename> is the name of the site that this job is landed,like BNL_ATLAS_1
               <workdir> is the pathname to the work directory of this job on the site
               <appdir> is the pathname to the directory of the executables
               <url> is the URL of the http web server that the pilot job should connect to
               <dq2url> is the URL of the https web server for the local site's DQ2 siteservice
               <port> is the port on which the web server listens on
               <user> is a flag meaning this pilot is to get a user analysis job from dispatcher if set to user (test will return a test job)
               <mtflag> controls if this pilot runs in single or multi-task mode, for multi-task mode, set it to true, all other values is for single mode
               <outputDir> location of output files (destination for mv site mover)
               <inputdir> location of input files (source for mv site mover)
               <rmwkdir> controls if the workdir of this pilot should be removed in the end or not, true or false
               <jrflag> turns on/off job recovery, true or false
               <jrmax> maximum number of job recoveries
               <jrmaxatt> maximum number of recovery attempts per lost job
               <jreqflag> job request flag that controls whether the initial job is received from server (true/default) or file (false), jobrec for recovery mode
               <logfiledir> if specified, the log file will only be copied to a dir (None means log file will not be registered)
               <debuglevel> 0: debug info off, 1: display function name when called, 2: full debug info
               <queuename> name of queue to be used as an argument for downloading config info (e.g. UBC-lcgpbs)
               <stageinretry> number of tries for stage-ins (default is 2)
               <stageoutretry> number of tries for stage-outs (default is 2)
               <loggingMode> True: pilot only reports space, etc, but does not run a job. False: normal pilot (default)
               <updateserver> True (default) for normal running, False is used for interactive test runs
               <memory> memory passed on to the dispatcher when asking for a job (MB; overrides queuedata)
               <proxycheckflag> True (default): perform proxy validity checks, False: no check
               <wrapperflag> True for wrappers that expect an exit code via return, False (default) when exit() can be used
               <pilotreleaseflag> PR for production pilots, RC for release candidate pilots
               <countrygroup> Country group selector for getJob request
               <workinggroup> Working group selector for getJob request
               <allowOtherCountry> True/False
               <lfcRegistration> True[False]: pilot will [not] perform LFC registration (default: True)
               <timefloor> Time limit for multi-jobs in minutes
               <useCoPilot> Expect CERNVM pilot to be executed by Co-Pilot (True: on, False: pilot will finish job (default))
               <experiment> Current experiment (default: ATLAS)
               <getJobMaxTime> The maximum time the pilot will attempt single job downloads (in minutes, default is 3 minutes, min value is 1)
    """
    #  <testlevel> 0: no test, 1: simulate put error, 2: ...
    print usage.__doc__

def argParser(argv):
    """ parse command line arguments for the main script """
    global pshttpurl, psport, uflag, pilotId, jobSchedulerId, rmwkdir, jobrec,\
           maxjobrec, jobRequestFlag, extractsId, debugLevel, maxNumberOfRecoveryAttempts,\
           queuename, stageinretry, stageoutretry, logFileDir, testLevel, loggingMode, updateServerFlag, memory,\
           proxycheckFlag, wrapperFlag, jobRecoveryMode, pilot_version_tag, countryGroup, workingGroup,\
           inputDir, outputDir, allowOtherCountry, lfcRegistration, timefloor_default, useCoPilot, experiment, getjobmaxtime

    # some default values
    sitename = "testsite"
    workdir = "/tmp"
    appdir = ""
    dq2url = ""

    tolog("argParser arguments: %s" % str(argv))    

    # find the pilot ID and JSID if any from the environment variables
    try:
        jsid = os.environ["PANDA_JSID"]
    except:
        pass
    else:
        jobSchedulerId = jsid
        print "jobScheduler ID = %s" % jobSchedulerId
    try:
        gtag = os.environ["GTAG"]
    except:
        pass
    else:
        pilotId = gtag
        print "pilot ID = %s" % pilotId

    try:
        # warning: option o and k have diffierent meaning for pilot and runJob
        opts, args = getopt.getopt(argv, 'a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:E:F:G:')
    except getopt.GetoptError:
        print "Invalid arguments and options!"
        usage()
        os._exit(5)
        
    for o, a in opts:
        if o == "-a": appdir = a
        elif o == "-b": debugLevel = int(a)
        elif o == "-c":
            maxNumberOfRecoveryAttempts = int(a)
            if maxNumberOfRecoveryAttempts < 0:
                maxNumberOfRecoveryAttempts = -maxNumberOfRecoveryAttempts
        elif o == "-d":
            workdir = a.strip()
            if workdir.startswith("{"): # is an env. variable
                try:
                    workdir_env = re.match('\{([^}]*)\}',workdir). expand("\g<1>")
                    workdir = os.environ[workdir_env]
                    print "Getting the workdir from env. variable %s: %s" % (workdir_env, workdir)
                except Exception,e:
                    print "Exception when trying to get the workdir from env. variable %s: %s" % (workdir_env, str(e))
                    # OSCER test:
                    # workdir = '/hep/data/griddata'
                    raise KeyError
        elif o == "-e": logFileDir = a
        elif o == "-f":
            jreq = a
            if jreq.upper() == "JOBREC":
                jobrec = True
                jobRecoveryMode = True
            elif jreq.upper() == "TRUE":
                jobRequestFlag = True
            else:
                jobRequestFlag = False
        elif o == "-g": inputDir = a
        elif o == "-h": queuename = a
        elif o == "-i":
            if a == "PR" or a == "RC":
                pilot_version_tag = a
            else:
                print "Unknown pilot version tag: %s" % (a)
        elif o == "-j":
            jr = a
            if jr.upper() == "TRUE":
                jobrec = True
            else:
                jobrec = False
        elif o == "-k": memory = int(a)
        elif o == "-l":
            wrFlag = a
            if wrFlag.upper() == "TRUE":
                wrapperFlag = True
            else:
                wrapperFlag = False
        elif o == "-m": outputDir = a
        elif o == "-n":
            maxjobrec = int(a)
            if maxjobrec < 0:
                maxjobrec = maxjobrecDefault
        elif o == "-o": countryGroup = a
        elif o == "-p": psport = int(a)
        elif o == "-q": dq2url = a
        elif o == "-r":
            rmwd = a
            if rmwd.upper() == "TRUE":
                rmwkdir = True
            elif rmwd.upper() == "FALSE":
                rmwkdir = False
        elif o == "-s": sitename = a
        elif o == "-t":
            pcFlag = str(a)
            if pcFlag.upper() == "TRUE":
                proxycheckFlag = True
            else:
                proxycheckFlag = False
        elif o == "-u": uflag = a
        elif o == "-v": workingGroup = a
        elif o == "-w": pshttpurl = a #"https://voatlas220.cern.ch" # a
        elif o == "-x": stageinretry = int(a)
        elif o == "-y":
            yflag = a
            if yflag.upper() == "TRUE":
                loggingMode = True
            elif yflag.upper() == "FALSE":
                loggingMode = False
            else:
                loggingMode = None
        elif o == "-z":
            updateFlag = str(a)
            if updateFlag.upper() == "TRUE":
                updateServerFlag = True
            else:
                updateServerFlag = False
        elif o == "-A":
            if a.upper() == "TRUE":
                allowOtherCountry = True
            else:
                allowOtherCountry = False
        elif o == "-B":
            if a.upper() == "FALSE":
                lfcRegistration = False
            else:
                lfcRegistration = True
        elif o == "-C": timefloor_default = int(a)
        elif o == "-D":
            if a.upper() == "FALSE":
                useCoPilot = False
            else:
                useCoPilot = True
        elif o == "-E": stageoutretry = int(a)
        elif o == "-F": experiment = a
        elif o == "-G": 
            try:
                _getjobmaxtime = int(a)*60 # convert to seconds
            except ValueError:
                print "getjobmaxtime not an integer:", a
            else:
                if _getjobmaxtime > 1:
                    getjobmaxtime = _getjobmaxtime
        else:
            print "Unknown option: %s (ignoring)" % o
            usage()

    # use sitename as queuename if queuename == ""
    if queuename == "":
        queuename = sitename

    # force user jobs for ANALY sites
    if sitename.startswith('ANALY_'):
        if uflag != 'user' and uflag != 'self' and uflag != 'ptest':
            uflag = 'user'
            tolog("Pilot user flag has been reset for analysis site (to value: %s)" % (uflag))
        else:
            tolog("Pilot user flag: %s" % str(uflag))

    # do not bother with checking the proxy for ptest jobs
#    if uflag == 'ptest':
#        proxycheckFlag = False

    return sitename, appdir, workdir, dq2url, queuename

def dumpPilotInfo(tofile=True):
    """ pilot info """

    tolog("Panda Pilot, version %s" % (version), tofile=tofile)
    tolog("Version tag = %s" % (pilot_version_tag))
    tolog("PilotId = %s, jobSchedulerId = %s" % (str(pilotId), str(jobSchedulerId)), tofile=tofile)
    tolog("Current time: %s" % (pUtil.timeStamp()), tofile=tofile)
    tolog("Run by Python %s" % (sys.version), tofile=tofile)
    tolog("%s bit OS" % (pUtil.OSBitsCheck()), tofile=tofile)
    tolog("Pilot init dir: %s" % (pilot_initdir), tofile=tofile)
    if tofile:
        tolog("All output written to file: %s" % (pUtil.getPilotlogFilename()))
    pilotUSER = commands.getoutput("whoami")
    tolog("Pilot executed by: %s" % (pilotUSER), tofile=tofile)

def moveLostOutputFiles(job, thisSite, remaining_files):
    """
    Move all output files from lost job workdir to local DDM area
    """

    ec = 0
    error = PilotErrors()
    pilotErrorDiag = ""

    transExitCode = job.result[1]
    chdir(job.datadir)

    # create the dataset dictionary before outFiles is overwritten
    # (if None, the dsname above will be used for all output files)
    datasetDict = getDatasetDict(job.outFiles, job.destinationDblock, job.logFile, job.logDblock)
    if datasetDict:
        tolog("Dataset dictionary has been verified")
    else:
        tolog("Dataset dictionary could not be verified, output files will go to default dsname (below)")

    # reset the output file information
    job.outFiles = remaining_files
    job.outFilesGuids = []

    # see if it's an analysis job or not
    analJob = isAnalysisJob(job.trf.split(",")[0])

    # recreate the guids
    for i in range (0, len(job.outFiles)):
        job.outFilesGuids.append(None)

    # open and parse xml to find the guids
    from xml.dom import minidom
    _filename = "%s/metadata-%s.xml" % (thisSite.workdir, str(job.jobId))
    if os.path.isfile(_filename):
        try:
            xmldoc = minidom.parse(_filename)
            _fileList = xmldoc.getElementsByTagName("File")
            tolog("Processing %d output files" % (len(job.outFiles)))
            for thisfile in _fileList:
                gpfn = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
                guid = str(thisfile.getAttribute("ID"))
                for i in range(0, len(job.outFiles)):
                    if job.outFiles[i] == gpfn:
                        job.outFilesGuids[i] = guid
                        tolog("Guid %s belongs to file %s" % (guid, gpfn))
        except Exception, e:
            tolog("!!FAILED!!1105!! Could not parse the metadata - guids unknown")
            job.setState(["failed", transExitCode, error.ERR_LOSTJOBPFC])
            chdir(thisSite.workdir)
            return -1
        else:
            tolog("Successfully read %s" % (_filename))
    else:
        tolog("!!FAILED!!1105!! Could not find %s - guids unknown" % (_filename))
        job.setState(["failed", transExitCode, error.ERR_LOSTJOBPFC])
        chdir(thisSite.workdir)
        return -1

    tolog("Remaining files:")
    dumpOrderedItems(remaining_files)
    tolog("Guids for remaining files:")
    dumpOrderedItems(job.outFilesGuids)

    # for backwards compatibility (recovered job object created by pilot version < 57a will not have the experiment data member)
    try:
        experiment = job.experiment
    except:
        experiment = "unknown"

    # recreate the OutPutFileCatalog.xml
    file_name = "OutPutFileCatalog.xml"
    file_path = os.path.join(thisSite.workdir, file_name)

    try:
        guids_status = pUtil.PFCxml(experiment, file_path, remaining_files, fguids=job.outFilesGuids, fntag="pfn", analJob=analJob, jr=True)
    except Exception, e:
        tolog("!!FAILED!!1105!! Exception caught (Could not generate xml for the remaining output files): %s" % str(e))
        job.setState(["failed", transExitCode, error.ERR_LOSTJOBXML])
        chdir(thisSite.workdir)
        return -1
    else:
        if not guids_status:
            pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_MISSINGGUID
        else:
            tolog("Successfully read: %s" % (file_path))
    if job.destinationDblock and job.destinationDblock[0] != 'NULL' and job.destinationDblock[0] != ' ':
        dsname = job.destinationDblock[0]
    else:
        dsname = "%s-%s-%s" % (time.localtime()[0:3])

    if not datasetDict:
        tolog("Output files will go to default dataset: %s" % (dsname))

    # the cmtconfig is needed by at least the xrdcp site mover
    cmtconfig = getCmtconfig(job.cmtconfig)

    tin_0 = os.times()
    rf = None
    _state = ""
    _msg = ""
    try:
        # Note: alt stage-out numbers are not saved in recovery mode (job object not returned from this function)
        rc, pilotErrorDiag, rf, rs, job.filesNormalStageOut, job.filesAltStageOut = mover.mover_put_data("xmlcatalog_file:%s" % (file_path), dsname,\
                                                              thisSite.sitename, ub=thisSite.dq2url, analysisJob=analJob,\
                                                              proxycheck=proxycheckFlag, spsetup=job.spsetup, scopeOut=job.scopeOut, scopeLog=job.scopeLog,\
                                                              token=job.destinationDBlockToken, pinitdir=pilot_initdir,\
                                                              datasetDict=datasetDict, prodSourceLabel=job.prodSourceLabel,\
                                                              jobId=job.jobId, jobWorkDir=job.workdir, DN=job.prodUserID,\
                                                              dispatchDBlockTokenForOut=job.dispatchDBlockTokenForOut, lfcreg=lfcRegistration,\
                                                              jobCloud=job.cloud, logFile=job.logFile, stageoutTries=stageoutretry, experiment=experiment,\
                                                              cmtconfig=cmtconfig, recoveryWorkDir=thisSite.workdir, fileDestinationSE=job.fileDestinationSE)
    except Exception, e:
        pilotErrorDiag = "Put function can not be called for staging out: %s" % str(e)
        tolog("!!%s!!1105!! %s" % (errorLabel, pilotErrorDiag))
        ec = error.ERR_PUTFUNCNOCALL
        _state = "holding"
        _msg = errorLabel
    else:
        if pilotErrorDiag != "":
            pilotErrorDiag = "Put error: " + tailPilotErrorDiag(pilotErrorDiag, size=256-len("pilot: Put error: "))

        tolog("Put function returned code: %d" % (rc))
        if rc != 0:
            # remove any trailing "\r" or "\n" (there can be two of them)
            if rs != None:
                rs = rs.rstrip()
                tolog(" Error string: %s" % (rs))

            # is the job recoverable?
            if error.isRecoverableErrorCode(rc):
                _state = "holding"
                _msg = "WARNING"
            else:
                _state = "failed"
                _msg = errorLabel

            # look for special error in the error string
            if rs == "Error: string Limit exceeded 250":
                tolog("!!%s!!3000!! Put error: file name string limit exceeded 250" % (_msg))
                ec = error.ERR_LRCREGSTRSIZE
            else:
                ec = rc

            tolog("!!%s!! %s" % (_msg, error.getErrorStr(rc)))
        else:
            # create a weak lockfile meaning that file transfer worked, and all output files have now been transferred
            createLockFile(True, thisSite.workdir, lockfile="ALLFILESTRANSFERRED")

            # file transfer worked, now register the output files in the LRC
            ub = thisSite.dq2url
            lfchost = readpar('lfchost')
            islogfile = False
            _state = ""
            _msg = ""

            if ub != "None" and ub != None and ub != "" and lfchost == "": # ub is 'None' outside the US
                # Perform the LRC file registration
                from FileRegistration import FileRegistration
                filereg = FileRegistration()
                ec, pilotErrorDiag, _state, _msg, _latereg = filereg.registerFilesLRC(ub, rf, islogfile, jobrec, thisSite.workdir, errorLabel)
                if ec != 0:
                    job.result[0] = _state
                    job.currentState = job.result[0]
            else:
                if lfchost != "":
                    tolog("No LRC file registration since lfchost is set")
                else:
                    tolog("No LRC file registration since dq2url is not set")

    # finish the time measurement of the stage-out
    tin_1 = os.times()
    job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

    # set the error codes in case of failure
    job.pilotErrorDiag = pilotErrorDiag
    if ec != 0:
        tolog("!!%s!!2999!! %s" % (_msg, pilotErrorDiag))
        job.setState([_state, transExitCode, ec])

    chdir(thisSite.workdir)
    return ec

def FinishedJob(job):
    """
    Figure out if this job finished
    (used by job recovery)
    """

    state = False

    # older job definitions do not have the finalstate member
    try:
        if job.finalstate == "finished":
            state = True
    except:
        # finalstate is not defined (use alternative but less precise method)
        tolog("!!WARNING!!1000!! Final state not defined - job was run by older pilot version")
        
        # job has finished if pilotErrorCode is in the allowed list or recoverable jobs
        # get the pilot error diag
        error = PilotErrors()
        if job.result[1] == 0 and error.isRecoverableErrorCode(job.result[2]):
            state = True
    if state:
        tolog("Final job state: finished")
    else:
        tolog("Final job state: failed")

    return state

def verifyRecoveryDir(recoveryDir):
    """ make sure that the recovery directory actually exists """

    # does the recovery dir actually exists?
    if recoveryDir != "":
        if os.path.exists(recoveryDir):
            tolog("Recovery directory exists: %s" % (recoveryDir))
        else:
            tolog("!!WARNING!!1190!! Recovery directory does not exist: %s (will not be used)" % (recoveryDir))
            recoveryDir = ""
    return recoveryDir

def runJobRecovery(thisSite, workerNode, _psport, extradir):
    """ run the lost job recovery algorithm """

    tmpdir = os.getcwd()

    # check queuedata for external recovery directory
    recoveryDir = "" # an empty recoveryDir means that recovery should search local WN disk for lost jobs
    try:
        recoveryDir = readpar('recoverdir')
    except:
        pass
    else:
        # make sure the recovery directory actually exists (will not be added to dir list if empty)
        recoveryDir = verifyRecoveryDir(recoveryDir)

    # run job recovery
    dirs = [ "" ]
    if recoveryDir != "":
        dirs.append(recoveryDir)
        tolog("Job recovery will scan both local disk and external disk")
    if extradir != "":
        if extradir not in dirs:
            dirs.append(extradir)
            tolog("Job recovery will also scan extradir (%s)" % (extradir))

    dircounter = 0
    for _dir in dirs:
        dircounter += 1
        tolog("Scanning for lost jobs [pass %d/%d]" % (dircounter, len(dirs)))
        try:
            cmd = "ls -lF %s" % (_dir)
            tolog("Executing command: %s" % (cmd))
            a,b=commands.getstatusoutput(cmd)
            tolog("%s" % (b))
            found_lost_jobs = RecoverLostJobs(_dir, thisSite, workerNode, _psport)
        except Exception, e:
            tolog("!!WARNING!!1999!! Failed during search for lost jobs: %s" % str(e))
        else:
            tolog("Recovered/Updated %d lost job(s)" % (found_lost_jobs))
    chdir(tmpdir)

def testExternalDir(recoveryDir):
    """ try to write a temp file in the external recovery dir """

    status = True

    testFile = "%s/testFile-%s.tmp" % (recoveryDir, commands.getoutput('uuidgen 2> /dev/null'))
    ec, rv = commands.getstatusoutput("touch %s" % (testFile))
    if ec != 0:
        tolog("!!WARNING!!1190!! Could not write test file to recovery dir (%s): %d, %s" % (testFile, ec, rv))
        tolog("!!WARNING!!1190!! Aborting move to external disk. Holding job will remain on local disk")
        status = False
    else:
        tolog("Successfully created a test file on the external disk (will now proceed with transferring the holding job)")
        ec, rv = commands.getstatusoutput("ls -l %s; rm %s" % (testFile, testFile))
        if ec != 0:
            tolog("!!WARNING!!1190!! Could not remove test file (%s): %d, %s (ignore since write succeeded)" %\
                  (testFile, ec, rv))

    return status

def removeTestFiles(job_state_files, mode="default"):
    """ temporary code for removing test files or standard job state files """
    # for mode="default", normal jobState-<jobId>.pickle files will be returned
    # for mode="test", jobState-<jobId>-test.pickle files will be returned

    new_job_state_files = []
    if mode == "default":
        for f in job_state_files:
            if not "-test.pickle" in f:
                new_job_state_files.append(f)
    else:
        for f in job_state_files:
            if "-test.pickle" in f:
                new_job_state_files.append(f)

    return new_job_state_files

def moveToExternal(workdir, recoveryDir):
    """
    move job state file(s), and remaining log/work dir(s) to an external dir for later recovery
    also updates the job state file with the new info
    """

    status = True

    # make sure the recovery directory actually exists
    recoveryDir = verifyRecoveryDir(recoveryDir)
    if recoveryDir == "":
        tolog("!!WARNING!!1190!! verifyRecoveryDir failed")
        return False

    tolog("Using workdir: %s, recoveryDir: %s" % (workdir, recoveryDir))
    JS = JobState()

    # grab all job state files from the workdir
    job_state_files = glob("%s/jobState-*.pickle" % (workdir))

    # purge any test job state files (testing for new job rec algorithm)
    job_state_files = removeTestFiles(job_state_files, mode="default")

    _n = len(job_state_files)
    tolog("Number of found jobState files: %d" % (_n))
    if _n == 0:
        return False

    for job_state_file in job_state_files:
        # read back all job info n order to update it with the new recovery dir info
        if JS.get(job_state_file):
            # decode the job state info
            _job, _site, _node, _recoveryAttempt = JS.decode()
            _basenameSiteWorkdir = os.path.basename(_site.workdir)
            _basenameJobWorkdir = os.path.basename(_job.workdir)
            _basenameJobDatadir = os.path.basename(_job.datadir)
            siteworkdir = _site.workdir

            # create the site work dir on the external disk
            externalDir = "%s/%s" % (recoveryDir, _basenameSiteWorkdir)
            tolog("Using external dir: %s" % (externalDir))
            # does the external dir already exist? (e.g. when $HOME is used)
            if os.path.exists(externalDir):
                tolog("External dir already exists")
            else:
                # group rw permission added as requested by LYON
                # ec, rv = commands.getstatusoutput("mkdir -m g+rw %s" % (externalDir))
                # 770 at the request of QMUL/Alessandra Forti?
                ec, rv = commands.getstatusoutput("mkdir -m 0770 %s" % (externalDir))
                if ec != 0:
                    if rv.find("changing permissions") >= 0 and rv.find("Operation not permitted") >= 0:
                        tolog("!!WARNING!!1190!! Was not allowed to created recovery dir with g+rw")
                        if os.path.exists(externalDir):
                            tolog("!!WARNING!!1190!! Recovery dir was nevertheless created: %s (will continue)" % (externalDir))
                        else:
                            tolog("!!WARNING!!1190!! Could not create dir on external disk: %s" % (rv))
                            return False
                    else:
                        tolog("!!WARNING!!1190!! Could not create dir on external disk: %s" % (rv))
                        return False
                else:
                    tolog("Successfully created external dir with g+rw")

            logfile = os.path.join(_site.workdir, _job.logFile)
            logfile_copied = os.path.join(_site.workdir, "LOGFILECOPIED")
            logfile_registered = os.path.join(_site.workdir, "LOGFILEREGISTERED")
            metadatafile1 = "metadata-%d.xml" % _job.jobId
            metadatafile2 = "metadata-%d.xml.PAYLOAD" % _job.jobId
            surlDictionary = os.path.join(_site.workdir, "surlDictionary-%s.%s" % (_job.jobId, getExtension()))
            moveDic = {"workdir" : _job.workdir, "datadir" : _job.datadir, "logfile" : logfile, "logfile_copied" : logfile_copied,\
                       "logfile_registered" : logfile_registered, "metadata1" : metadatafile1,\
                       "metadata2" : metadatafile2, "surlDictionary" : surlDictionary }
            tolog("Using moveDic: %s" % str(moveDic))
            failures = 0
            successes = 0
            for item in moveDic.keys():
                # does the item actually exists?
                # (e.g. the workdir should be tarred into the log and should not exist at this point)
                if os.path.exists(moveDic[item]):

                    # move the work dir to the external dir
                    ec, rv = commands.getstatusoutput("mv %s %s" % (moveDic[item], externalDir))
                    if ec != 0:
                        tolog("!!WARNING!!1190!! Could not move item (%s) to external dir (%s): %s" % (moveDic[item], externalDir, rv))
                        failures += 1
                    else:
                        tolog("Moved holding job item (%s) to external dir (%s)" % (moveDic[item], externalDir))
                        successes += 1

                        # set a new path for the item
                        if item == "workdir":
                            # probably the work dir has already been tarred
                            _job.workdir = os.path.join(recoveryDir, _basenameJobWorkdir)
                            tolog("Updated job workdir: %s" % (_job.workdir))
                        elif item == "datadir":
                            _job.datadir = os.path.join(externalDir, _basenameJobDatadir)
                            tolog("Updated job datadir: %s" % (_job.datadir))
                        else:
                            tolog("(Nothing to update in job state file for %s)" % (item))
                else:
                    # if the log is present, there will not be a workdir
                    tolog("Item does not exist: %s" % (moveDic[item]))

                    # set a new path for the item
                    if item == "workdir":
                        # probably the work dir has already been tarred
                        _job.workdir = os.path.join(recoveryDir, _basenameJobWorkdir)
                        tolog("Updated job workdir: %s" % (_job.workdir))

            # update the job state file with the new state information if any move above was successful
            if successes > 0:
                _site.workdir = externalDir
                tolog("Updated site workdir: %s" % (_site.workdir))
                _retjs = updateJobState(_job, _site, _node)
                if not _retjs:
                    tolog("!!WARNING!!1190!! Could not create job state file in external dir: %s" % (externalDir))
                    tolog("!!WARNING!!1190!! updateJobState failed at critical stage")
                    failures += 1
                else:
                    tolog("Created a new job state file in external dir")

                    # remove the LOCKFILE since it can disturb any future recovery
                    if os.path.exists("%s/LOCKFILE" % (siteworkdir)):
                        ec, rv = commands.getstatusoutput("rm %s/LOCKFILE" % (siteworkdir))
                        if ec != 0:
                            tolog("!!WARNING!!1190!! Could not remove LOCKFILE - can disturb future recovery")
                        else:
                            tolog("Removed LOCKFILE from work dir: %s" % (siteworkdir))

            if failures > 0:
                tolog("!!WARNING!!1190!! Since at least one move to the external disk failed, the original work area")
                tolog("!!WARNING!!1190!! will not be removed and should be picked up by a later pilot doing the recovery")
                status = False
            else:
                tolog("All files were successfully transferred to the external recovery area")
        else:
            tolog("!!WARNING!!1190!! Could not open job state file: %s" % (job_state_file))
            status = False

    return status

def createAtomicLockFile(file_path):
    """ Create an atomic lockfile while probing this dir to avoid a possible race-condition """

    lockfile_name = os.path.join(os.path.dirname(file_path), "ATOMIC_LOCKFILE")
    try:
        # acquire the lock
        fd = os.open(lockfile_name, os.O_EXCL|os.O_CREAT)
    except OSError:
        # work dir is locked, so exit
        tolog("Found lock file: %s (skip this dir)" % (lockfile_name))
        fd = None
    else:
        tolog("Created lock file: %s" % (lockfile_name))
    return fd, lockfile_name

def releaseAtomicLockFile(fd, lockfile_name):
    """ Release the atomic lock file """

    try:
        os.close(fd)
        os.unlink(lockfile_name)
    except Exception, e:
        if "Bad file descriptor" in str(e):
            tolog("Lock file already released")
        else:
            tolog("WARNING: Could not release lock file: %s" % str(e))
    else:
        tolog("Released lock file: %s" % (lockfile_name))

def RecoverLostJobs(recoveryDir, thisSite, workerNode, _psport):
    """
    This function searches the given directory path for (potentially deserted) directories, i.e. panda
    directories that have not been modified within the time limit (in hours, default is 72), that
    contain job recovery files. If a previous job failed during Put it created a job recovery file
    that another pilot can pick up and try to re-register.
    """

    error = PilotErrors()

    number_of_recoveries = 0
    file_nr = 0
    
    if recoveryDir != "":
        dir_path = recoveryDir
        tolog("Recovery algorithm will search external dir for lost jobs: %s" % (dir_path))
    else:
        dir_path = thisSite.wntmpdir
        if dir_path == "":
            dir_path = "/tmp" # reset to default
        tolog("Recovery algorithm will search local WN disk for lost jobs: %s" % (dir_path))

    currentDir = os.getcwd()
    try:
        os.path.isdir(dir_path)
    except:
        tolog("!!WARNING!!1100!! No such dir path (%s)" % (dir_path))
    else:
        JS = JobState()
        # grab all job state files in all work directories
        job_state_files = glob(dir_path + "/Panda_Pilot_*/jobState-*.pickle")

        # purge any test job state files (testing for new job rec algorithm)
        job_state_files = removeTestFiles(job_state_files, mode="default")

        tolog("Number of found job state files: %d" % (len(job_state_files)))

        if job_state_files:
            # loop over all found job state files
            for file_path in job_state_files:
                # create an atomic lockfile while probing this dir to avoid a possible race-condition
                fd, lockfile_name = createAtomicLockFile(file_path)
                if not fd:
                    continue

                # only check for LOCKFILE on the local WN, not on an external dir
                if recoveryDir == "":
                    # make sure the LOCKFILE for the holding job is present (except when an external recoverydir is scanned)
                    dirname = os.path.dirname(file_path)
                    lockFileName = dirname + "/LOCKFILE" 
                    if not os.path.exists(lockFileName):
                        # release the atomic lockfile and go to the next directory
                        releaseAtomicLockFile(fd, lockfile_name)
                        continue
                    else:
                        try:
                            tolog("Found %s created at %d" % (lockFileName, os.path.getmtime(dirname + "/LOCKFILE")))
                        except Exception, e:
                            tolog("!!WARNING!!1100!! (could not read modification time of %s): %s" % (dirname + "/LOCKFILE", str(e)))

                file_nr += 1
                if file_nr > maxjobrec:
                    tolog("Maximum number of job recoveries exceeded for this pilot: %d" % (maxjobrec))
                    # release the atomic lockfile and go to the next directory
                    releaseAtomicLockFile(fd, lockfile_name)
                    break
                tolog("Processing job state file %d/%d: %s" % (file_nr, len(job_state_files), file_path))
                current_time = int(time.time())

                # when was file last modified?
                try:
                    file_modification_time = os.path.getmtime(file_path)
                except:
                    # skip this file since it was not possible to read the modification time
                    # release the atomic lockfile and go to the next directory
                    releaseAtomicLockFile(fd, lockfile_name)
                    pass
                else:
                    # was the job state file updated longer than 2 heart beats ago?
                    if (current_time - file_modification_time) > 2*heartbeatPeriod:
                        # found lost job recovery file
                        tolog("File was last modified %d seconds ago (limit=%d, t=%d, tmod=%d)" %\
                              (current_time - file_modification_time, 2*heartbeatPeriod, current_time, file_modification_time))
                        # open the job state file
                        if JS.get(file_path):
                            # decode the job state info
                            _job, _site, _node, _recoveryAttempt = JS.decode()

                            # add member if it doesn't exist (new Job version)
                            try:
                                _tmp = _job.prodSourceLabel
                            except:
                                _job.prodSourceLabel = ''

                            # only continue if current pilot is of same type as lost job (to prevent potential permission problems)
                            if _job:
                                if not isSameType(_job.trf.split(",")[0], uflag):
                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue

                                #PN
                                # uncomment this code for recovery of certain panda ids only
#                                allowedJobIds = [1435974131]
#                                if _job.jobId not in allowedJobIds:
#                                    tolog("Job id %d not in allowed id list: %s" % (_job.jobId, str(allowedJobIds)))
#                                    continue

                            if _job and _site and _node:
                                tolog("Stored job state: %s" % (_job.result[0]))
                                # query the job state file for job information
                                if _job.result[0] == 'holding' or _job.result[0] == 'lostheartbeat':
                                    tolog("(1)")

                                    tolog("Job %d is currently in state \'%s\' (according to job state file - recover)" %\
                                          (_job.jobId, _job.result[0]))
                                elif _job.result[0] == 'failed':
                                    tolog("(2)")
                                    tolog("Job %d is currently in state \'%s\' (according to job state file - skip)" %\
                                          (_job.jobId, _job.result[0]))

                                    tolog("Further recovery attempts will be prevented for this job (will leave work dir)")
                                    if not JS.rename(_site, _job):
                                        tolog("(Fate of job state file left for next pilot)")

                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue
                                else:
                                    tolog("(3) Not enough information in job state file, query server")
                                    
                                    # get job status from server
                                    jobStatus, jobAttemptNr, jobStatusCode = pUtil.getJobStatus(_job.jobId, pshttpurl, _psport, pilot_initdir)

                                    # recover this job?
                                    if jobStatusCode == 20:
                                        tolog("Received general error code from dispatcher call (leave job for later pilot)")
                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue
                                    elif not (jobStatus == 'holding' and jobStatusCode == 0):
                                        tolog("Job %d is currently in state \'%s\' with attemptNr = %d (according to server - will not be recovered)" %\
                                              (_job.jobId, jobStatus, jobAttemptNr))

                                        if _job.attemptNr != jobAttemptNr or jobStatus == "transferring" or jobStatus == "failed" or \
                                               jobStatus == "notfound" or jobStatus == "finished" or "tobekilled" in _job.action:
                                            tolog("Further recovery attempts will be prevented for this job")
                                            if not JS.rename(_site, _job):
                                                tolog("(Fate of job state file left for next pilot)")
                                            else:
                                                if not JS.cleanup():
                                                    tolog("!!WARNING!!1110!! Failed to cleanup")
                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue
                                    else:
                                        # is the attemptNr defined?
                                        try:
                                            attemptNr = _job.attemptNr
                                        except Exception, e:
                                            tolog("!!WARNING!!1100!! Attempt number not defined [ignore]: %s" % str(e))
                                        else:
                                            # check if the attemptNr (set during initial getJob command) is the same
                                            # as the current jobAttemptNr from the server (protection against failed lost
                                            # heartbeat jobs due to reassigned panda job id numbers)
                                            if attemptNr != jobAttemptNr:
                                                tolog("!!WARNING!!1100!! Attempt number mismatch for job %d (according to server - will not be recovered)" %\
                                                      (_job.jobId))
                                                tolog("....Initial attempt number: %d" % (attemptNr))
                                                tolog("....Current attempt number: %d" % (jobAttemptNr))
                                                tolog("....Job status (server)   : %s" % (jobStatus))
                                                tolog("....Job status (state)    : %s" % (_job.result[0]))
                                                tolog("Further recovery attempts will be prevented for this job")
                                                if not JS.rename(_site, _job):
                                                    tolog("(Fate of job state file left for next pilot)")
                                                else:
                                                    if not JS.cleanup():
                                                        tolog("!!WARNING!!1110!! Failed to cleanup")
                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue
                                            else:
                                                tolog("Attempt numbers from server and job state file agree: %d" % (attemptNr))
                                        # the job state as given by the dispatcher should only be different from that of
                                        # the job state file for 'lostheartbeat' jobs. This state is only set like this
                                        # in the job state file. The dispatcher will consider it as a 'holding' job.
                                        tolog("Job %d is currently in state \'%s\' (according to job state file: \'%s\') - recover" %\
                                              (_job.jobId, jobStatus, _job.result[0]))

                                # only attempt recovery if the lost job ran on the same site as the current pilot
                                # (to avoid problems on two sites with shared WNs)
                                if _site.sitename == thisSite.sitename:
                                    tolog("Verified that lost job ran on the same site as the current pilot")
                                else:
                                    tolog("Aborting job recovery since the lost job ran on site %s but the current pilot is running on %s" % (_site.sitename, thisSite.sitename))
                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue

                                chdir(_site.workdir)

                                # abort if max number of recovery attempts has been exceeded
                                if _recoveryAttempt > maxNumberOfRecoveryAttempts - 1:
                                    tolog("!!WARNING!!1100!! Max number of recovery attempts exceeded: %d" % (maxNumberOfRecoveryAttempts))
                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBMAXEDOUT])
                                    rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                    if rt == 0:
                                        number_of_recoveries += 1
                                        tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                        # did the server send back a command?
                                        if "tobekilled" in _job.action:
                                            tolog("!!WARNING!!1100!! Panda server returned a \'tobekilled\' command")

                                        tolog("NOTE: This job has been terminated. Will now remove workdir.")
                                        if not JS.cleanup():
                                            tolog("!!WARNING!!1110!! Failed to cleanup")

                                    else:
                                        tolog("Panda server returned a %d" % (rt))
                                        tolog("(Failed to update panda server - leave for next pilot)")

                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue
                                else:
                                    # increase recovery attempt counter
                                    _recoveryAttempt += 1
                                tolog("Recovery attempt: %d" % (_recoveryAttempt))

                                # update job state file at this point to prevent a parallel pilot from doing a simultaneous recovery
                                _retjs = updateJobState(_job, _site, _node, _recoveryAttempt)

                                # should any file be registered? (data dirs will not exist in the following checks
                                # since late registration requires that all files have already been transferred)
                                # (Note: only for LRC sites)
                                rc = checkForLateRegistration(thisSite.dq2url, _job, _site, _node, type="output")
                                if rc == False:
                                    tolog("Resume this rescue operation later due to the previous errors")
                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue
                                rc = checkForLateRegistration(thisSite.dq2url, _job, _site, _node, type="log")
                                if rc == False:
                                    tolog("Resume this rescue operation later due to the previous errors")
                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue

                                # does log exist?
                                logfile = "%s/%s" % (_site.workdir, _job.logFile)
                                if os.path.exists(logfile):

                                    logfileAlreadyCopied = isLogfileCopied(_site.workdir)
                                    if logfileAlreadyCopied:
                                        tolog("Found log file        : %s (already transferred)" % (logfile))
                                    else:
                                        tolog("Found log file        : %s" % (logfile))

                                    # does data dir exist?
                                    if os.path.isdir(_job.datadir):

                                        tolog("Found data dir        : %s" % (_job.datadir))
                                        chdir(_job.datadir)

                                        # do output files exist?
                                        remaining_files = pUtil.getRemainingOutputFiles(_job.outFiles)
                                        tolog("Number of data files  : %d" % (len(remaining_files)))
                                        if remaining_files:

                                            # get the metadata
                                            strXML = getMetadata(_site.workdir, _job.jobId)

                                            # extract the outFilesGuids from the xml in case of build jobs
                                            # (_job.outFilesGuids has not been set for those, the guid is kept in the xml)
                                            if pUtil.isBuildJob(remaining_files):
                                                # match the guid in the metadata with the single file in remaining_files
                                                _guids = pUtil.getGuidsFromXML(_site.workdir, id=_job.jobId, filename=remaining_files[0])
                                                if len(_guids) == 1:
                                                    _job.outFilesGuids = _guids
                                                else:
                                                    tolog("Warning: could not identify guid in metadata")
                                                    tolog("Remaining files: %s" % str(remaining_files))
                                                    tolog("_guids: %s" % str(_guids))
                                                    tolog("metadata: \n%s" % (strXML))

                                            # can output files be moved?
                                            tolog("Trying to move data files")
                                            ec = moveLostOutputFiles(_job, _site, remaining_files)

                                            skip_for_now = False
                                            if ec != 0:
                                                if ec == error.ERR_MISSINGGUID:
                                                    tolog("!!FAILED!!1110!! Could not move lost output files to local DDM due to missing guid")
                                                    _job.finalstate = "failed"
                                                elif _job.result[2] == error.ERR_LOSTJOBPFC:
                                                    tolog("!!WARNING!!1110!! Could not move lost output files to local DDM due to PoolFileCatalog read failure")
                                                    _job.finalstate = "failed"
                                                elif _job.result[2] == error.ERR_LOSTJOBXML:
                                                    tolog("!!WARNING!!1110!! Could not move lost output files to local DDM due to xml generation failure")
                                                    _job.finalstate = "failed"
                                                else:
                                                    tolog("!!WARNING!!1110!! Could not move lost output files to local DDM (leave for next pilot)")
                                                    skip_for_now = True
                                                pass
                                            else:
                                                tolog("Remaining data files moved to SE")

                                                chdir(_site.workdir)

                                                # remove data dir
                                                if pUtil.remove([_job.datadir]):
                                                    tolog("Removed data dir")
                                                else:
                                                    tolog("!!WARNING!!1110!! Failed to remove data dir")
    
                                            # can log be registered?
                                            ret, _job = transferLogFile(_job, _site, dq2url=thisSite.dq2url, jr=True)
                                            if not ret:
                                                tolog("!!WARNING!!1110!! Could not register lost job log file (state set to holding)")
                                                _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBLOGREG])
                                            else:
                                                # only set finished state if data files are registered
                                                # _job.result[1] must be 0 at this point
                                                # since there were data files in the data dir
                                                # this job can not have failed (must be finished)
                                                # also verify that all output files have indeed been transferred
                                                if not skip_for_now:
                                                    if FinishedJob(_job):
                                                        if pUtil.verifyTransfer(_site.workdir):
                                                            # set new exit code
                                                            _job.setState(['finished', 0, 0])
                                                        else:
                                                            tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                    else:
                                                        tolog("!!WARNING!!1110!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                              (_job.result[1], _job.result[2]))
                                                        if _job.result[2] != error.ERR_LOSTJOBPFC and _job.result[2] != error.ERR_LOSTJOBXML: # state already set for these codes
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                else:
                                                    # there were problems with the file transfer to the local DDM
                                                    _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBFILETRANSFER])

                                            # update the server
                                            rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                            if rt == 0:
                                                number_of_recoveries += 1
                                                tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                # did the server send back a command?
                                                if "tobekilled" in _job.action:
                                                    tolog("!!WARNING!!1110!! Panda server returned a \'tobekilled\' command")
                                                    _job.result[0] = "failed"

                                                # only cleanup work dir if no error code has been set
                                                if _job.result[0] == "finished" or _job.result[0] == "failed":
                                                    chdir(currentDir)
                                                    if not JS.cleanup():
                                                        tolog("!!WARNING!!1110!! Failed to cleanup")
                                                        # release the atomic lockfile and go to the next directory
                                                        releaseAtomicLockFile(fd, lockfile_name)
                                                        continue

                                            else:
                                                tolog("!!WARNING!!1110!! Panda server returned a %d" % (rt))

                                                # store the final state so that the next pilot will know
                                                # store the metadata xml
                                                retNode['xml'] = strXML

                                                # update the job state file with the new state information
                                                _retjs = updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        else: # output files do not exist

                                            chdir(_site.workdir)

                                            # can log be registered?
                                            ret, _job = transferLogFile(_job, _site, dq2url=thisSite.dq2url, jr=True)
                                            if not ret:
                                                tolog("!!WARNING!!1110!! Could not register lost job log file (state set to holding)")
                                                _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBLOGREG])
                                            else:
                                                # set new exit code
                                                if FinishedJob(_job):
                                                    if pUtil.verifyTransfer(_site.workdir):
                                                        _job.setState(['finished', 0, 0])
                                                    else:
                                                        tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                        _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                else:
                                                    tolog("!!WARNING!!1110!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                          (_job.result[1], _job.result[2]))
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                
                                            # get the metadata
                                            strXML = getMetadata(_site.workdir, _job.jobId)

                                            # update the server
                                            rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                            if rt == 0:
                                                number_of_recoveries += 1

                                                # did the server send back a command?
                                                if "tobekilled" in _job.action:
                                                    tolog("!!WARNING!!1110!! Panda server returned a \'tobekilled\' command")
                                                    _job.result[0] = "failed"

                                                tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                                if _job.result[0] == 'finished' or _job.result[0] == "failed":
                                                    if not JS.cleanup():
                                                        tolog("!!WARNING!!1110!! Failed to cleanup")
                                                        # release the atomic lockfile and go to the next directory
                                                        releaseAtomicLockFile(fd, lockfile_name)
                                                        continue

                                            else:
                                                tolog("!!WARNING!!1110!! Panda server returned a %d" % (rt))

                                                # store the final state so that the next pilot will know
                                                # store the metadata xml
                                                retNode['xml'] = strXML

                                                # update the job state file with the new state information
                                                _retjs = updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue

                                    else: # data dir does not exist

                                        tolog("Data dir already deleted for this lost job (or never existed)")

                                        # can log be registered?
                                        ret, _job = transferLogFile(_job, _site, dq2url=thisSite.dq2url, jr=True)
                                        if not ret:
                                            tolog("!!WARNING!!1120!! Could not register lost job log file (state set to holding)")
                                            _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBLOGREG])
                                        else:
                                            # set exit code if lost job exited correctly
                                            if FinishedJob(_job):
                                                if pUtil.verifyTransfer(_site.workdir):
                                                    _job.setState(['finished', 0, 0])
                                                else:
                                                    tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                            else:
                                                tolog("!!WARNING!!1120!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                      (_job.result[1], _job.result[2]))
                                                _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])

                                        # was xml saved?
                                        strXML = ''
                                        try:
                                            strXML = _node['xml']
                                        except:
                                            tolog("XML could not be found - try to read from file")
                                            strXML = getMetadata(_site.workdir, _job.jobId)

                                        # update the server
                                        rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                        if rt == 0:
                                            number_of_recoveries += 1
                                            tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                            # only cleanup work dir if no error code has been set
                                            if _job.result[0] == 'finished':
                                                if not JS.cleanup():
                                                    tolog("!!WARNING!!1120!! Failed to cleanup")

                                            # did the server send back a command?
                                            if "tobekilled" in _job.action:
                                                tolog("!!WARNING!!1120!! Panda server returned a \'tobekilled\' command")
                                                _job.result[0] = "failed"
                                                
                                            # further recovery attempt unnecessary, but keep the work dir for debugging
                                            if _job.result[0] == "failed":
                                                tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                if not JS.rename(_site, _job):
                                                    tolog("(Fate of job state file left for next pilot)")

                                            # release the atomic lockfile and go to the next directory
                                            releaseAtomicLockFile(fd, lockfile_name)
                                            continue

                                        else:
                                            tolog("!!WARNING!!1120!! Panda server returned a %d" % (rt))

                                            # store the final state so that the next pilot will know

                                            # store the metadata xml
                                            retNode['xml'] = strXML

                                            # update the job state file with the new state information
                                            _retjs = updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue

                                else: # log file does not exist
                                    tolog("Log file already deleted for this job")

                                    # does work dir exist?
                                    if os.path.isdir(_job.newDirNM):

                                        tolog("Found renamed work dir: %s" % (_job.newDirNM))
                                        # is exit code set?
                                        ecSet = False
                                        if not _job.result[2]:
                                            # can exit code be read from file?
                                            _ec = pUtil.getExitCode(_job.newDirNM, "pilotlog.txt")
                                            if _ec == -1:
                                                tolog("!!WARNING!!1130!! Could not read exit code from file: %s" % (_job.newDirNM + "pilotlog.txt"))
                                                _job.setState(['failed', 0, error.ERR_LOSTJOBNOTFINISHED]) # lost job never finished
                                            else:
                                                ecSet = True
                                        else:
                                            _ec = _job.result[2]
                                            ecSet = True

                                        # EC was not set and could not be read from file
                                        if not ecSet:
                                            tolog("Exit code not found")

                                            # get the metadata
                                            # this metadata does not contain the metadata for the log
                                            strXML = getMetadata(_site.workdir, _job.jobId)

                                            # update the server
                                            rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                            if rt == 0:
                                                number_of_recoveries += 1
                                                tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                # this job can never be recovered - delete work dir
                                                if not JS.cleanup():
                                                    tolog("!!WARNING!!1130!! Failed to cleanup")
                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                                # did the server send back a command?
                                                if "tobekilled" in _job.action:
                                                    tolog("!!WARNING!!1130!! Panda server returned a \'tobekilled\' command")
                                                    _job.result[0] = "failed"

                                                # further recovery attempt unnecessary, but keep the work dir for debugging
                                                if _job.result[0] == "failed":
                                                    tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                    if not JS.rename(_site, _job):
                                                        tolog("(Fate of job state file left for next pilot)")

                                            else:
                                                tolog("!!WARNING!!1130!! Panda server returned a %d" % (rt))

                                                # store the final state so that the next pilot will know
                                                # store the metadata xml
                                                retNode['xml'] = strXML

                                                # update the job state file with the new state information
                                                _retjs = updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        else: # EC was set or could be read from file
                                            tolog("Found exit code       : %d" % (_ec))
                                            if _ec == 0:
                                                # does data directory exist?
                                                if os.path.isdir(_job.datadir):
                                                    tolog("Found data dir        : %s" % (_job.datadir))
                                                    chdir(_job.datadir)

                                                    # do output files exist?
                                                    remaining_files = pUtil.getRemainingOutputFiles(_job.outFiles)
                                                    tolog("Number of data files  : %d" % (len(remaining_files)))
                                                    if remaining_files:
                                                        # can output files be moved?
                                                        tolog("Trying to move data files")
                                                        ec = moveLostOutputFiles(_job, _site, remaining_files)
                                                        chdir(_site.workdir)
                                                        if ec != 0:
                                                            if ec == error.ERR_MISSINGGUID:
                                                                tolog("!!FAILED!!1130!! Could not move lost output files to local DDM due to missing guid")
                                                                _job.finalstate = "failed"
                                                            elif _job.result[2] == error.ERR_LOSTJOBPFC: # missing PoolFileCatalog
                                                                tolog("!!WARNING!!1130!! Could not move lost output files to local DDM due to PoolFileCatalog read failure")
                                                                _job.finalstate = "failed"
                                                            elif _job.result[2] == error.ERR_LOSTJOBXML: # could not generate xml file
                                                                tolog("!!WARNING!!1130!! Could not move lost output files to local DDM due xml generation failure")
                                                                _job.finalstate = "failed"
                                                            else:
                                                                tolog("!!WARNING!!1130!! Could not move lost output files to local DDM (leave for next pilot)")
                                                                _job.setState(['holding', 0, error.ERR_LOSTJOBFILETRANSFER])

                                                            # do not delete work dir (leave it for the next pilot to try again)

                                                            # get the metadata
                                                            strXML = getMetadata(_site.workdir, _job.jobId)

                                                            # update the server
                                                            rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                                            if rt == 0:
                                                                tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                                # did the server send back a command?
                                                                if "tobekilled" in _job.action:
                                                                    tolog("!!WARNING!!1130!! Panda server returned a \'tobekilled\' command")
                                                                    _job.result[0] = "failed"

                                                                # further recovery attempt unnecessary, but keep the work dir for debugging
                                                                if _job.result[0] == "failed":
                                                                    tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                                    if not JS.rename(_site, _job):
                                                                        tolog("(Fate of job state file left for next pilot)")

                                                            else:
                                                                tolog("!!WARNING!!1130!! Failed to update Panda server for job %d (exit code %d)" %\
                                                                      (_job.jobId, _job.result[2]))
                                                            # release the atomic lockfile and go to the next directory
                                                            releaseAtomicLockFile(fd, lockfile_name)
                                                            continue

                                                        else: # output files could be moved

                                                            tolog("Remaining data files moved to SE")
                                                            chdir(_site.workdir)

                                                            # remove data dir
                                                            if pUtil.remove([_job.datadir]):
                                                                tolog("Removed data dir")
                                                            else:
                                                                tolog("!!WARNING!!1130!! Failed to remove data dir")

                                                            # create log file and update panda server
                                                            postJobTask(_job, _site, workerNode, jr=True, ra=_recoveryAttempt)
                                                            number_of_recoveries += 1
                                                            # release the atomic lockfile and go to the next directory
                                                            releaseAtomicLockFile(fd, lockfile_name)
                                                            continue

                                                    else: # output files do not exist

                                                        if FinishedJob(_job):
                                                            if pUtil.verifyTransfer(_site.workdir):
                                                                _job.setState(['finished', 0, 0])
                                                            else:
                                                                tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                                _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                        else:
                                                            tolog("!!WARNING!!1130!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                                  (_job.result[1], _job.result[2]))
                                                            # failed since output files do not exist
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBFILETRANSFER])

                                                        # create log file and update panda server
                                                        postJobTask(_job, _site, workerNode, jr=True, ra=_recoveryAttempt)
                                                        number_of_recoveries += 1
                                                        # release the atomic lockfile and go to the next directory
                                                        releaseAtomicLockFile(fd, lockfile_name)
                                                        continue

                                                else: # data dir does not exist
                                                    tolog("No data dir for this lost job")

                                                    # create log file and update panda server
                                                    postJobTask(_job, _site, workerNode, jr=True, ra=_recoveryAttempt)
                                                    number_of_recoveries += 1
                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                            else: # EC != 0 
                                                # create log file and update panda server
                                                postJobTask(_job, _site, workerNode, jr=True, ra=_recoveryAttempt)
                                                number_of_recoveries += 1
                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue

                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue

                                    else: # work dir does not exist

                                        tolog("Work dir does not exist (log probably already transferred)")

                                        # should an old log be registered? (log file will not exist since late registration requires
                                        # that the log has already been transferred)
#                                        rc = checkForLateRegistration(thisSite.dq2url, _job, _site, _node, type="log")
#                                        if rc == False:
#                                            tolog("Resume this rescue operation later since the LRC is not working (log could not be registered)")
#                                            continue

                                        # does data directory exist?
                                        if os.path.isdir(_job.datadir):

                                            tolog("Found data dir: %s" % (_job.datadir))
                                            chdir(_job.datadir)

                                            # do output files exist?
                                            remaining_files = pUtil.getRemainingOutputFiles(_job.outFiles)
                                            tolog("Number of data files: %d" % (len(remaining_files)))
                                            if remaining_files:
                                                # can output files be moved?
                                                tolog("Trying to move data files")
                                                ec = moveLostOutputFiles(_job, _site, remaining_files)
                                                chdir(_site.workdir)
                                                if ec != 0:
                                                    if ec == error.ERR_MISSINGGUID:
                                                        tolog("!!WARNING!!1140!! Could not move lost output files to local DDM due to missing guid")
                                                        _job.finalstate = "failed"
                                                    elif _job.result[2] == error.ERR_LOSTJOBPFC: # missing PoolFileCatalog
                                                        tolog("!!WARNING!!1140!! Could not move lost output files to local DDM due to PoolFileCatalog read failure")
                                                        _job.finalstate = "failed"
                                                    elif _job.result[2] == error.ERR_LOSTJOBXML: # could not generate xml file
                                                        tolog("!!WARNING!!1140!! Could not move lost output files to local DDM due to xml generation failure")
                                                        _job.finalstate = "failed"
                                                    else:
                                                        tolog("!!WARNING!!1140!! Could not move lost output files to local DDM (leave for next pilot)")
                                                        _job.setState(['holding', 0, error.ERR_LOSTJOBFILETRANSFER])

                                                    # do not delete data dir (leave it for the next pilot to try again) unless failed

                                                    # get the metadata
                                                    strXML = getMetadata(_site.workdir, _job.jobId)

                                                    # update the server
                                                    rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                                    if rt == 0:
                                                        tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                        # did the server send back a command?
                                                        if "tobekilled" in _job.action:
                                                            tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                            _job.result[0] = "failed"

                                                        # further recovery attempt unnecessary, but keep the work dir for debugging
                                                        if _job.result[0] == "failed":
                                                            tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                            if not JS.rename(_site, _job):
                                                                tolog("(Fate of job state file left for next pilot)")

                                                    else:
                                                        tolog("!!WARNING!!1140!! Failed to update Panda server for job %d (exit code %d)" % (_job.jobId, _job.result[2]))

                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                                else: # output files could be moved

                                                    tolog("Remaining data files moved to SE")
                                                    chdir(_site.workdir)

                                                    # remove data dir
                                                    if pUtil.remove([_job.datadir]):
                                                        tolog("Removed data dir")
                                                    else:
                                                        tolog("!!WARNING!!1140!! Failed to remove data dir")

                                                    if FinishedJob(_job):
                                                        if pUtil.verifyTransfer(_site.workdir):
                                                            # set new exit code
                                                            _job.setState(['finished', 0, 0])
                                                        else:
                                                            tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                    else:
                                                        tolog("!!WARNING!!1140!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                              (_job.result[1], _job.result[2]))

                                                    # get the metadata
                                                    strXML = getMetadata(_site.workdir, _job.jobId)

                                                    # update the server
                                                    rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                                    if rt == 0:
                                                        number_of_recoveries += 1
                                                        tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                        # did the server send back a command?
                                                        if "tobekilled" in _job.action:
                                                            tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                            _job.result[0] = "failed"

                                                        # further recovery attempt unnecessary, but keep the work dir for debugging
                                                        if _job.result[0] == "failed":
                                                            tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                            if not JS.rename(_site, _job):
                                                                tolog("(Fate of job state file left for next pilot)")
                                                                
                                                        # only cleanup work dir if no error code has been set
                                                        if _job.result[0] == 'finished':
                                                            if not JS.cleanup():
                                                                tolog("!!WARNING!!1140!! Failed to cleanup")

                                                    else:
                                                        tolog("!!WARNING!!1140!! Failed to update Panda server for job %d (exit code %d)" % (_job.jobId, _job.result[2]))

                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                            else: # output files do not exist

                                                if FinishedJob(_job):
                                                    if pUtil.verifyTransfer(_site.workdir):
                                                        _job.setState(['finished', 0, 0])
                                                    else:
                                                        tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                        _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                else:
                                                    tolog("!!WARNING!!1140!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                          (_job.result[1], _job.result[2]))
                                                    # failed since output files do not exist
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBFILETRANSFER])
                                                tolog("Data files do not exist")

                                                # was xml saved?
                                                strXML = ''
                                                try:
                                                    strXML = _node['xml']
                                                except:
                                                    tolog("XML could not be found - try to read from file")
                                                    strXML = getMetadata(_site.workdir, _job.jobId)

                                                # update the server
                                                rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                                if rt == 0:
                                                    tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                                    number_of_recoveries += 1

                                                    # did the server send back a command?
                                                    if "tobekilled" in _job.action:
                                                        tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                        _job.result[0] = "failed"

                                                    # further recovery attempt unnecessary, but keep the work dir for debugging
                                                    if _job.result[0] == "failed":
                                                        tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                        if not JS.rename(_site, _job):
                                                            tolog("(Fate of job state file left for next pilot)")

                                                else:
                                                    tolog("!!WARNING!!1140!! Failed to update Panda server for job %d (exit code %d)" % (_job.jobId, _job.result[2]))

                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue

                                        else: # data dir does not exist
                                            tolog("No data dir for this lost job")

                                        # store results in case there's another server hickup
                                        # (since they might be overwritten)
                                        _result1 = _job.result[1]
                                        _result2 = _job.result[2]

                                        # lost heartbeat job?
                                        # (this state is only set for finished job when there was a temporary
                                        # problem with the server during dispatcher update)
                                        if _job.result[0] == 'lostheartbeat':
                                            tolog("Recovering lost heartbeat job")
                                            if FinishedJob(_job):
                                                if pUtil.verifyTransfer(_site.workdir):
                                                    _job.setState(['finished', 0, 0])
                                                else:
                                                    tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                            else:
                                                tolog("!!WARNING!!1140!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                      (_job.result[1], _job.result[2]))
                                                # should never happen since 'lostheartbeat' jobs are finished..
                                                _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])

                                        # the job might be finished and the code has entered this point due to a
                                        # non responding server in a prior recovery attempt
                                        elif _job.result[0] == 'finished':
                                            tolog("Job was finished, log and output files already registered. Will remind dispatcher")
                                        else:
                                            tolog("!!WARNING!!1140!! Neither log, data nor work dir exist (setting EC 1156: Pilot could not recover job)")
                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])

                                        # was xml saved?
                                        strXML = ''
                                        try:
                                            strXML = _node['xml']
                                        except:
                                            tolog("XML could not be found - try to read from file")
                                            strXML = getMetadata(_site.workdir, _job.jobId)

                                        # update the server
                                        rt, retNode = updatePandaServer(_job, _site, workerNode, _psport, xmlstr=strXML, ra=_recoveryAttempt, schedulerID=jobSchedulerId, pilotID=pilotId)
                                        if rt == 0:
                                            number_of_recoveries += 1
                                            tolog("Lost job %d updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                            if not JS.cleanup():
                                                tolog("!!WARNING!!1140!! Failed to cleanup")
                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue    

                                            # did the server send back a command?
                                            if "tobekilled" in _job.action:
                                                tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                _job.result[0] = "failed"

                                            # further recovery attempt unnecessary, but keep the work dir for debugging
                                            if _job.result[0] == "failed":
                                                tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                if not JS.rename(_site, _job):
                                                    tolog("(Fate of job state file left for next pilot)")
                                        else:
                                            tolog("!!WARNING!!1140!! Panda server returned a %d" % (rt))

                                            # store the final state so that the next pilot will know
                                            # store the metadata xml
                                            retNode['xml'] = strXML

                                            # update the job state file with the new state information
                                            _job.setState(['lostheartbeat', _result1, _result2])
                                            _retjs = updateJobState(_job, _site, retNode, _recoveryAttempt)

                    # Job state file was recently modified
                    else:
                        tolog("(Job state file was recently modified - skip it)")
                        # atomic lockfile will be released below

                # (end main "for file_path in job_state_files"-loop)
                # release the atomic lockfile and go to the next directory
                releaseAtomicLockFile(fd, lockfile_name)

    chdir(currentDir)
    return number_of_recoveries

def getProdSourceLabel(_uflag, sitename):
    """ determine the job type """

    prodSourceLabel = None

    # not None value; can be user (user analysis job), ddm (panda mover job, sitename should contain DDM)
    # test will return a testEvgen/testReco job, ptest will return a job sent with prodSourceLabel ptest
    if _uflag:
        if _uflag == 'self' or _uflag == 'ptest':
            if _uflag == 'ptest':
                prodSourceLabel = _uflag
            elif _uflag == 'self':
                prodSourceLabel = 'user'
        else:
            prodSourceLabel = _uflag

    # for PandaMover jobs the label must be ddm
    if "DDM" in sitename or (_uflag == 'ddm' and sitename == 'BNL_ATLAS_test'):
        prodSourceLabel = 'ddm'
    elif "Install" in sitename: # old, now replaced with prodSourceLabel=install
        prodSourceLabel = 'software'
    if readpar('status').lower() == 'test' and _uflag != 'ptest' and _uflag != 'ddm':
        prodSourceLabel = 'test'

    # override for release candidate pilots
    if pilot_version_tag == "RC":
        prodSourceLabel = "rc_test"
    if pilot_version_tag == "DDM":
        prodSourceLabel = "ddm"

    return prodSourceLabel

def getStatusCode(data):
    """ get and write the dispatcher status code to file """

    tolog("Parsed response: %s" % str(data))

    try:
        StatusCode = data['StatusCode']
    except Exception, e:
        pilotErrorDiag = "Can not receive any job from jobDispatcher: %s" % str(e)
        tolog("!!WARNING!!1200!! %s" % (pilotErrorDiag))
        StatusCode = '45'

    # Put the StatusCode in a file (used by some pilot wrappers), erase if it already exists
    writeDispatcherEC(StatusCode)

    return StatusCode

def writeDispatcherEC(EC):
    """ write the dispatcher exit code to file """

    filename = os.path.join(pilot_initdir, "STATUSCODE")
    if os.path.exists(filename):
        try:
            os.remove(filename)
        except Exception, e:
            tolog("Warning: Could not remove file: %s" % str(e))
        else:
            tolog("Removed existing STATUSCODE file")
    pUtil.writeToFile(os.path.join(filename), str(EC))

def getJob(thisSite, workerNode, number_of_jobs, error, _uflag, si):
    """ Download a new job from the dispatcher """

    ec = 0
    job = None

    # loop over getNewJob to allow for multiple attempts
    trial = 1
    t0 = time.time()
    tolog("Pilot will attempt single job download for a maximum of %d seconds" % (getjobmaxtime))
    while int(time.time() - t0) < getjobmaxtime:
        job, pilotErrorDiag = getNewJob(thisSite, workerNode, _uflag, si)
        if not job:
            if getjobmaxtime - int(time.time() - t0) > 60:
                tolog("[Trial %d] Could not find a job! (will try again after 60 s)" % (trial))
                time.sleep(60)
                trial += 1
            else:
                tolog("(less than 60 s left of the allowed %d s for job downloads, so not a good time for a nap!)" % (getjobmaxtime))
                break
        else:
            number_of_jobs += 1
            tolog("Increased job counter to %d" % (number_of_jobs))
            os.environ["PanDA_TaskID"] = job.taskID
            tolog("Task ID set to: %s" % (job.taskID))
            break

    if not job:
        if "No job received from jobDispatcher" in pilotErrorDiag or "Dispatcher has no jobs" in pilotErrorDiag:
            errorText = "!!FINISHED!!0!!Dispatcher has no jobs"
        else:
            errorText = "!!FAILED!!1999!!%s" % (pilotErrorDiag)

        # only set an error code if it's the first job
        if number_of_jobs == 0:
            ec = error.ERR_GENERALERROR
        else:
            errorText += "\nNot setting any error code since %d job(s) were already executed" % (number_of_jobs)
            ec = -1 # temporary

        # send to stderr
        tolog(errorText)
        print >> sys.stderr, errorText

    return ec, job, number_of_jobs

def getProperNodeName(nodename):
    """ Get the proper node name (if possible, containing the _CONDOR_SLOT (SlotID)) """

    # if possible (on a condor system) add the SlotID to the nodename: _CONDOR_SLOT@nodename
    if os.environ.has_key("_CONDOR_SLOT"):
        nodename = "%s@%s" % (os.environ["_CONDOR_SLOT"], nodename)

    return nodename

def backupDispatcherResponse(response, tofile):
    """ Backup response (will be copied to workdir later) """

    try:
        fh = open(pandaJobDataFileName, "w")
        fh.write(response)
        fh.close()
    except Exception, e:
        tolog("!!WARNING!!1999!! Could not store job definition: %s" % str(e), tofile=tofile)
    else:
        tolog("Job definition stored (for later backup): %s" % str(response), tofile=tofile)

def getDispatcherDictionary(thisSite, workerNode, _diskSpace, _uflag, tofile):
    """ Construct a dictionary for passing to jobDispatcher """

    pilotErrorDiag = ""

    # glExec proxy key
    _getProxyKey = "False"
    if readpar('glexec').lower() in ['true', 'uid']:
        _getProxyKey = "True"

    nodename = workerNode.nodename
    tolog("Node name: %s" % (nodename))

    jNode = {'siteName':thisSite.sitename,
             'cpu':workerNode.cpu,
             'mem':workerNode.mem,
             'diskSpace':_diskSpace,
             'node':nodename,
             'computingElement':thisSite.computingElement,
             'getProxyKey':_getProxyKey,
             'workingGroup':workingGroup}

    if countryGroup == "":
        tolog("No country group selected")
    else:
        jNode['countryGroup'] = countryGroup
        tolog("Using country group: %s" % (countryGroup))

    if workingGroup == "":
        tolog("No working group selected")
    else:
        tolog("Using working group: %s" % (jNode['workingGroup']))

    if allowOtherCountry:
        tolog("allowOtherCountry is set to True (will be sent to dispatcher)")
        jNode['allowOtherCountry'] = allowOtherCountry

    # should the job be requested for a special DN?
    if _uflag == 'self':
        # get the pilot submittor DN, and only process this users jobs
        DN, pilotErrorDiag = getDN()
        if DN == "":
            return {}, "", pilotErrorDiag
        else:
            jNode['prodUserID'] = DN

        tolog("prodUserID: %s" % (jNode['prodUserID']))

    # determine the job type
    prodSourceLabel = getProdSourceLabel(_uflag, thisSite.sitename)
    if prodSourceLabel:
        jNode['prodSourceLabel'] = prodSourceLabel
        tolog("prodSourceLabel: %s" % (jNode['prodSourceLabel']), tofile=tofile)

    # send the pilot token
    # WARNING: do not print the jNode dictionary since that will expose the pilot token
    if pilotToken:
        jNode['token'] = pilotToken

    return jNode, prodSourceLabel, pilotErrorDiag

def getDN():
    """ Return the DN for the pilot submitter """

    DN = ""
    pilotErrorDiag = ""

    # Try to use arcproxy first since voms-proxy-info behaves poorly under SL6
    # cmd = "arcproxy -I |grep 'subject'| sed 's/.*: //'"
    cmd = "arcproxy -i subject"
    tolog("Executing command: %s" % (cmd))
    err, out = commands.getstatusoutput(cmd)
    if "command not found" in out:
        tolog("!!WARNING!!1234!! arcproxy is not available")
        tolog("!!WARNING!!1235!! Defaulting to voms-proxy-info (can lead to memory problems with the command in case of low schedconfig.memory setting)")

        # Default to voms-proxy-info
        cmd = "voms-proxy-info -subject"
        tolog("Executing command: %s" % (cmd))
        err, out = commands.getstatusoutput(cmd)

    if err == 0:
        DN = out
        tolog("Got DN = %s" % (DN))

        CN = "/CN=proxy"
        if not DN.endswith(CN):
            tolog("!!WARNING!!1234!! DN does not end with %s (will be added)" % (CN))
            DN += CN
    else:
        pilotErrorDiag = "User=self set but cannot get proxy: %d, %s" % (err, out)

    return DN, pilotErrorDiag

def getNewJob(thisSite, workerNode, _uflag, si, tofile=True):
    """ Get a new job definition from the jobdispatcher or from file """
    global jobRequestFlag

    pilotErrorDiag = ""
    StatusCode = ''

    # determine which disk space to send to dispatcher (only used by dispatcher so no need to send actual available space)
    _maxinputsize = getMaxInputSize(MB=True)
    _disk = workerNode.disk
    tolog("Available WN disk space: %d MB" % (_disk))
    _diskSpace = min(_disk, _maxinputsize)
    tolog("Sending disk space %d MB to dispatcher" % (_diskSpace))

    # construct a dictionary for passing to jobDispatcher and get the prodSourceLabel
    jNode, prodSourceLabel, pilotErrorDiag = getDispatcherDictionary(thisSite, workerNode, _diskSpace, _uflag, tofile)
    if jNode == {}:
        errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
        tolog(errorText, tofile=tofile)
        # send to stderr
        print >> sys.stderr, errorText
        return None, pilotErrorDiag

    # should we ask the server for a job or should we read it from a file (as in the case of the test pilot)
    if not jobRequestFlag:
        # read job from file
        tolog("Looking for a primary job (reading from file)", tofile=tofile)
        _pandaJobDataFileName = os.path.join(pilot_initdir, pandaJobDataFileName)
        if os.path.isfile(_pandaJobDataFileName):
            try:
                f = open(_pandaJobDataFileName)
            except Exception,e:
                pilotErrorDiag = "[pilot] Can not open the file %s: %s" % (_pandaJobDataFileName, str(e))
                errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
                tolog(errorText, tofile=tofile)
                # send to stderr
                print >> sys.stderr, errorText
                return None, pilotErrorDiag
            else:
                # get the job definition from the file
                response = f.read()

                if len(response) == 0:
                    pilotErrorDiag = "[pilot] No job definition found in file: %s" % (_pandaJobDataFileName)
                    errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
                    tolog(errorText, tofile=tofile)
                    # send to stderr
                    print >> sys.stderr, errorText
                    return None, pilotErrorDiag

                jobRequestFlag = True
                f.close()

                # parse response message
                dataList = cgi.parse_qsl(response, keep_blank_values=True)

                # convert to map
                data = {}
                for d in dataList:
                    data[d[0]] = d[1]

                # get and write the dispatcher status code to file
                StatusCode = getStatusCode(data)
        else:
            pilotErrorDiag = "[pilot] Job definition file (%s) does not exist! (will now exit)" % (_pandaJobDataFileName)
            errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
            tolog(errorText, tofile=tofile)
            # send to stderr
            print >> sys.stderr, errorText
            return None, pilotErrorDiag
    else:
        # get a random server
        url = '%s:%s/server/panda' % (pshttpurl, str(psport))
        tolog("Looking for a primary job (contacting server at %s)" % (url), tofile=tofile)

        # make http connection to jobdispatcher
        # format: status, parsed response (data), response
        ret = pUtil.httpConnect(jNode, url, mode="GETJOB", path=pilot_initdir) # connection mode is GETJOB

        # get and write the dispatcher status code to file
        StatusCode = str(ret[0])

        # the original response will be put in a file in this function
        data = ret[1] # dictionary
        response = ret[2] # text

        # write the dispatcher exit code to file
        writeDispatcherEC(StatusCode)

        if ret[0]: # non-zero return
            return None, getDispatcherErrorDiag(ret[0])
    #PN
    #data['jobPars'] += ' --overwriteQueuedata={allowfax=True,faxredirector=root://glrd.usatlas.org/}'

    if StatusCode != '0':
        pilotErrorDiag = "No job received from jobDispatcher, StatusCode: %s" % (StatusCode)
        tolog("%s" % (pilotErrorDiag), tofile=tofile)
        return None, pilotErrorDiag

    # test if he attempt number was sent
    try:
        attemptNr = int(data['attemptNr'])
    except Exception,e:
        tolog("!!WARNING!!1200!! Failed to get attempt number from server: %s" % str(e), tofile=tofile)
    else:
        tolog("Attempt number from server: %d" % attemptNr)

    # should further job recovery be switched off? (for gangarobot jobs)
    global jobrec
    if "gangarobot" in data['processingType'] and jobrec:
        tolog("Switching off further job recovery for gangarobot job")

        # get the site information object
        si = getSiteInformation(experiment)

        jobrec = False
        ec = si.replaceQueuedataField("retry", "False")
    else:
        if jobrec:
            tolog("Job recovery is still switched on after job download")
        else:
            tolog("Job recovery is still switched off after job download")

    # should there be a delay before setting running state?
    global nSent
    try:
        nSent = int(data['nSent'])
    except Exception,e:
        nSent = 0
    else:
        tolog("Received nSent: %d" % (nSent))

    # backup response (will be copied to workdir later)
    backupDispatcherResponse(response, tofile)

    if data.has_key('prodSourceLabel'):
        if data['prodSourceLabel'] == "":
            tolog("Setting prodSourceLabel in job def data: %s" % (prodSourceLabel))
            data['prodSourceLabel'] = prodSourceLabel
        else:
            tolog("prodSourceLabel already set in job def data: %s" % (data['prodSourceLabel']))

            # override ptest value if install job to allow testing using dev pilot
            if prodSourceLabel == "ptest" and "atlpan/install/sw-mgr" in data['transformation']:
                tolog("Dev pilot will run test install job (job.prodSourceLabel set to \'install\')")
                data['prodSourceLabel'] = "install"
    else:
        tolog("Adding prodSourceLabel to job def data: %s" % (prodSourceLabel))
        data['prodSourceLabel'] = prodSourceLabel

    # look for special commands in the job parameters (can be set by HammerCloud jobs; --overwriteQueuedata, --disableFAX)
    # if present, queuedata needs to be updated (as well as jobParameters - special commands need to be removed from the string)
    data['jobPars'] = si.updateQueuedataFromJobParameters(data['jobPars'])

    # convert the data into a file for child process to pick for running real job later
    try:
        f = open("Job_%s.py" % data['PandaID'], "w")
        print >>f, "job=", data
        f.close()
    except Exception,e:
        pilotErrorDiag = "[pilot] Exception caught: %s" % str(e)
        tolog("!!WARNING!!1200!! %s" % (pilotErrorDiag), tofile=tofile)
        return None, pilotErrorDiag

    # create the new job
    newJob = Job.Job()
    newJob.setJobDef(data)  # fill up the fields with correct values now
    newJob.datadir = thisSite.workdir + "/PandaJob_%d_data" % (newJob.jobId)
    newJob.experiment = experiment

    if data.has_key('logGUID'):
        logGUID = data['logGUID']
        if logGUID != "NULL" and logGUID != "":
            newJob.tarFileGuid = logGUID
            tolog("Got logGUID from server: %s" % (logGUID), tofile=tofile)
        else:
            tolog("!!WARNING!!1200!! Server returned NULL logGUID", tofile=tofile)
            tolog("Using generated logGUID: %s" % (newJob.tarFileGuid), tofile=tofile)
    else:
        tolog("!!WARNING!!1200!! Server did not return logGUID", tofile=tofile)
        tolog("Using generated logGUID: %s" % (newJob.tarFileGuid), tofile=tofile)

    if newJob.prodSourceLabel == "":
        tolog("Giving new job prodSourceLabel=%s" % (prodSourceLabel))
        newJob.prodSourceLabel = prodSourceLabel
    else:
        tolog("New job has prodSourceLabel=%s" % (newJob.prodSourceLabel))

    # should we use debug mode?
    global update_freq_server
    if data.has_key('debug'):
        if data['debug'].lower() == "true":
            update_freq_server = 5*30
            tolog("Debug mode requested: Updating server update frequency to %d s" % (update_freq_server))

    return newJob, ""

def checkForLateRegistration(dq2url, job, site, node, type="output"):
    """
    check whether an old log file or any old output files
    need to be registered
    the type variable can assume the values "output" and "log"
    """

    # return:
    #  True: registration succeeded
    #  False: registration failed (job will remain in holding state)
    status = False

    if dq2url == "None" or dq2url == None or dq2url == "": # dq2url is 'None' outside the US
        return True

    rc = lateRegistration(dq2url, job, type=type) 
    if rc:
        # update job state file to prevent any further registration attempts for these files
        try:
            if type == "output":
                job.output_latereg = "False"
                job.output_fields = None
                _retjs = updateJobState(job, site, node)
                status = True
            elif type == "log":
                job.log_latereg = "False"
                job.log_field = None
                _retjs = updateJobState(job, site, node)
                status = True
            else:
                tolog("!!WARNING!!1150!! Unknown registration type (must be either output or log): %s" % (type))
        except Exception, e:
            tolog("!!WARNING!!1150!! Exception caught (ignore): %s" % str(e))
            pass
        else:
            tolog("Late registration performed for non-registered %s file(s)" % (type))
            if status and type == "log":
                # create a weak lock file for the log registration
                createLockFile(jobrec, site.workdir, lockfile="LOGFILEREGISTERED")
    elif rc == False:
        tolog("Late registration failed, job will remain in holding state")
    else: # None
        tolog("Late registration had nothing to do")
        status = None

    return status

def updatePandaServer(job, site, workerNode, port, xmlstr=None, spaceReport=False, log=None, ra=0, jr=False, schedulerID=None, pilotID=None, updateServer=True, stdout_tail=""):
    """ Update the panda server with the latest job info """

    # create and instantiate the client object
    from PandaServerClient import PandaServerClient
    client = PandaServerClient(pilot_version=version, \
                               pilot_version_tag=pilot_version_tag, \
                               pilot_initdir=pilot_initdir, \
                               jobSchedulerId=schedulerID, \
                               pilotId=pilotID, \
                               updateServer=updateServer, \
                               jobrec=jobrec, \
                               pshttpurl=pshttpurl)

    # update the panda server
    return client.updatePandaServer(job, site, workerNode, port, xmlstr=xmlstr, spaceReport=spaceReport, log=log, ra=ra, jr=jr, useCoPilot=useCoPilot, stdout_tail=stdout_tail)

def transferLogFile(job, site, dq2url=None, dest=None, jr=False):
    """
    save log tarball into DDM and register it to catalog, or copy it to 'dest'.
    the job recovery will use the current site info known by the current
    pilot and will override the old jobs' site.dq2url in case the dq2url
    has been updated
    """

    # create and instantiate the job log object
    from JobLog import JobLog
    joblog = JobLog(pilot_version=version, \
                    pilot_version_tag=pilot_version_tag, \
                    pilot_initdir=pilot_initdir, \
                    jobSchedulerId=jobSchedulerId, \
                    pilotId=pilotId, \
                    updateServer=updateServerFlag, \
                    jobrec=jobrec, \
                    pshttpurl=pshttpurl, \
                    testLevel=testLevel, \
                    proxycheckFlag=proxycheckFlag, \
                    outputDir=outputDir, \
                    lfcreg=lfcRegistration, \
                    jobIds=list(jobIds), \
                    logFileDir=logFileDir, \
                    errorLabel=errorLabel, \
                    useCoPilot=useCoPilot, \
                    stageoutTries=stageoutretry)

    # transfer the log
    return joblog.transferLogFile(job, site, experiment, dq2url=dq2url, dest=dest, jr=jr)

def restoreProxy():
    """ restore the hidden proxy if necessary """

    global proxyguard
    if proxyguard.isRead():
        if proxyguard.isRestored():
            tolog("Pilot found the proxy to be already restored")
        else:
            if not proxyguard.restoreProxy():
                tolog("Pilot failed to restore the proxy, log will not be retrievable")
            else:
                tolog("Pilot has restored the proxy")

def postJobTask(job, thisSite, thisNode, jr=False, ra=0, stdout_tail=None):
    """
    update Panda server with output info (xml) and make/save the tarball of the job workdir,
    only for finished or failed jobs.
    jr = job recovery
    ra = recovery attempt
    """

    # create and instantiate the job log object
    from JobLog import JobLog
    joblog = JobLog(pilot_version=version, \
                    pilot_version_tag=pilot_version_tag, \
                    pilot_initdir=pilot_initdir, \
                    jobSchedulerId=jobSchedulerId, \
                    pilotId=pilotId, \
                    updateServer=updateServerFlag, \
                    jobrec=jobrec, \
                    pshttpurl=pshttpurl, \
                    testLevel=testLevel, \
                    proxycheckFlag=proxycheckFlag, \
                    outputDir=outputDir, \
                    lfcreg=lfcRegistration, \
                    jobIds=list(jobIds), \
                    logFileDir=logFileDir, \
                    errorLabel=errorLabel, \
                    useCoPilot=useCoPilot, \
                    stageoutTries=stageoutretry, \
                    stdout_tail=stdout_tail)

    # create the log
    joblog.postJobTask(job, thisSite, experiment, thisNode, jr=jr, ra=ra)    
    
class watchDog:
    def pollChildren(self):
        """ check children processes, collect zombie jobs, and update jobDic status """

        error = PilotErrors()
        global jobDic, prodJobDone
        # tolog("---pollChildren: %s" % str(jobDic))
        for k in jobDic.keys():
            try:
                _id, rc = os.waitpid(jobDic[k][0], os.WNOHANG)
            except OSError, e:
                tolog("Harmless exception when checking job %s, %s" % (jobDic[k][1].jobId, str(e)))
                if str(e).rstrip() == "[Errno 10] No child processes":
                    pilotErrorDiag = "Exception caught by pilot watchdog: %s" % str(e)
                    tolog("!!FAILED!!1000!! Pilot setting state to failed since there are no child processes")
                    tolog("!!FAILED!!1000!! %s" % (pilotErrorDiag))
                    jobDic[k][1].result[0] = "failed"
                    jobDic[k][1].currentState = jobDic[k][1].result[0]
                    if jobDic[k][1].result[2] == 0:
                        jobDic[k][1].result[2] = error.ERR_NOCHILDPROCESSES
                    if jobDic[k][1].pilotErrorDiag == "":
                        jobDic[k][1].pilotErrorDiag = pilotErrorDiag
                else:
                    pass
            else:
                if _id: # finished
                    rc = rc%255 # exit code
                    if k == "prod": # production job is done
                        prodJobDone = True
                        tolog("Production job is done")
                    if jobDic[k][1].result[0] != "finished" and jobDic[k][1].result[0] != "failed" and jobDic[k][1].result[0] != "holding":
                        if not rc: # rc=0, ok job
                            if not jobDic[k][1].result[1]:
                                jobDic[k][1].result[1] = rc # transExitCode (because pilotExitCode is reported back by child job)
                        else: # rc != 0, failed job
                            jobDic[k][1].result[1] = rc # transExitCode

    def collectZombieJob(self, tn=None):
        """ collect zombie child processes, tn is the max number of loops, plus 1,
        to avoid infinite looping even if some child proceses really get wedged;
        tn=None means it will keep going till all children zombies collected.        
        """
        global zombieJobList
        time.sleep(1)
        if zombieJobList and tn > 1:
            tolog("--- collectZombieJob: --- %d, %s" % (tn, str(zombieJobList)))
            tn -= 1
            for x in zombieJobList:
                try:
                    tolog("Zombie collector trying to kill pid %s" % str(x))
                    _id, rc = os.waitpid(x, os.WNOHANG)
                except OSError,e:
                    tolog("Harmless exception when collecting zombie jobs, %s" % str(e))
                    zombieJobList.remove(x)
                else:
                    if _id: # finished
                        zombieJobList.remove(x)
                self.collectZombieJob(tn=tn) # recursion

        if zombieJobList and not tn: # for the infinite waiting case, we have to
            # use blocked waiting, otherwise it throws
            # RuntimeError: maximum recursion depth exceeded
            for x in zombieJobList:
                try:
                    _id, rc = os.waitpid(x, 0)
                except OSError,e:
                    tolog("Harmless exception when collecting zombie jobs, %s" % str(e))
                    zombieJobList.remove(x)
                else:
                    if _id: # finished
                        zombieJobList.remove(x)
                self.collectZombieJob(tn=tn) # recursion

class updateHandler(BaseRequestHandler):
    """ update jobDic status with the messages sent from child via socket, do nothing else """

    def handle(self):
        global jobDic, prodJobDone, finalstate, stagein, stageout, stageoutStartTime

        tolog("Connected from %s" % str(self.client_address))
        data = self.request.recv(4096)
        jobmsg = data.split(";")
        tolog("--- TCPServer: Message received from child is : %s" % str(jobmsg))
        jobinfo = {}
        for i in jobmsg:
            if not i: continue # skip empty line
            try:
                jobinfo[i.split("=")[0]] = i.split("=")[1]
            except Exception, e:
                tolog("!!WARNING!!1999!! Exception caught: %s" % (e))

        # update jobDic
        for k in jobDic.keys():
            if jobDic[k][2] == int(jobinfo["pgrp"]) and jobDic[k][1].jobId == int(jobinfo["jobid"]): # job pid matches
                # protect with try statement in case the pilot server goes down (jobinfo will be corrupted)
                try:
                    jobDic[k][1].currentState = jobinfo["status"]
                    if jobinfo["status"] == "stagein":
                        stagein = True
                        stageout = False
                        jobDic[k][1].result[0] = "running"
                    elif jobinfo["status"] == "stageout":
                        stagein = False
                        stageout = True
                        jobDic[k][1].result[0] = "running"
                        stageoutStartTime = int(time.time())
                    else:
                        stagein = False
                        stageout = False
                        jobDic[k][1].result[0] = jobinfo["status"]
                    jobDic[k][1].result[1] = int(jobinfo["transecode"]) # transExitCode
                    jobDic[k][1].result[2] = int(jobinfo["pilotecode"]) # pilotExitCode
                    jobDic[k][1].timeStageIn = jobinfo["timeStageIn"]
                    jobDic[k][1].timeStageOut = jobinfo["timeStageOut"]
                    jobDic[k][1].timeSetup = jobinfo["timeSetup"]
                    jobDic[k][1].timeExe = jobinfo["timeExe"]
                    jobDic[k][1].cpuConsumptionTime = jobinfo["cpuTime"]
                    jobDic[k][1].cpuConsumptionUnit = jobinfo["cpuUnit"]
                    jobDic[k][1].cpuConversionFactor = jobinfo["cpuConversionFactor"]
                    jobDic[k][1].jobState = jobinfo["jobState"]
                    jobDic[k][1].vmPeakMax = int(jobinfo["vmPeakMax"])
                    jobDic[k][1].vmPeakMean = int(jobinfo["vmPeakMean"])
                    jobDic[k][1].RSSMean = int(jobinfo["RSSMean"])
                    jobDic[k][1].JEM = jobinfo["JEM"]
                    jobDic[k][1].cmtconfig = jobinfo["cmtconfig"]
                    tolog("Pilot is using job.cmtconfig=%s" % (jobDic[k][1].cmtconfig))

                    tmp = jobDic[k][1].result[0]
                    if (tmp == "failed" or tmp == "holding" or tmp == "finished") and jobinfo.has_key("logfile"):
                        jobDic[k][1].logMsgFiles.append(jobinfo["logfile"])

                    if jobinfo.has_key("pilotErrorDiag"):
                        jobDic[k][1].pilotErrorDiag = decode_string(jobinfo["pilotErrorDiag"])

                    if jobinfo.has_key("exeErrorDiag"):
                        tolog("exeErrorDiag: %s" % (jobinfo["exeErrorDiag"]))
                        jobDic[k][1].exeErrorDiag = decode_string(jobinfo["exeErrorDiag"])
                        tolog("jobDic[k][1].exeErrorDiag = %s" % (jobDic[k][1].exeErrorDiag))

                    if jobinfo.has_key("exeErrorCode"):
                        jobDic[k][1].exeErrorCode = int(jobinfo["exeErrorCode"])
                        tolog("exeErrorCode: %s" % (jobinfo["exeErrorCode"]))

                    if jobinfo.has_key("filesWithFAX"):
                        jobDic[k][1].filesWithFAX = int(jobinfo["filesWithFAX"])

                    if jobinfo.has_key("filesWithoutFAX"):
                        jobDic[k][1].filesWithoutFAX = int(jobinfo["filesWithoutFAX"])

                    if jobinfo.has_key("bytesWithFAX"):
                        jobDic[k][1].bytesWithFAX = int(jobinfo["bytesWithFAX"])

                    if jobinfo.has_key("bytesWithoutFAX"):
                        jobDic[k][1].bytesWithoutFAX = int(jobinfo["bytesWithoutFAX"])

                    if jobinfo.has_key("filesAltStageOut"):
                        jobDic[k][1].filesAltStageOut = int(jobinfo["filesAltStageOut"])
                        tolog("got filesAltStageOut=%d" % (jobDic[k][1].filesAltStageOut))

                    if jobinfo.has_key("filesNormalStageOut"):
                        jobDic[k][1].filesNormalStageOut = int(jobinfo["filesNormalStageOut"])
                        tolog("got filesNormalStageOut=%d" % (jobDic[k][1].filesNormalStageOut))

                    if jobinfo.has_key("nEvents"):
                        try:
                            jobDic[k][1].nEvents = int(jobinfo["nEvents"])
                        except Exception, e:
                            tolog("!!WARNING!!2999!! jobinfo did not return an int as expected: %s" % str(e))
                            jobDic[k][1].nEvents = 0

                    if jobinfo.has_key("nEventsW"):
                        try:
                            jobDic[k][1].nEventsW = int(jobinfo["nEventsW"])
                        except Exception, e:
                            tolog("!!WARNING!!2999!! jobinfo did not return an int as expected: %s" % str(e))
                            jobDic[k][1].nEventsW = 0

                    if jobinfo.has_key("finalstate"):
                        jobDic[k][1].finalstate = jobinfo["finalstate"]

                    if jobinfo.has_key("spsetup"):
                        jobDic[k][1].spsetup = jobinfo["spsetup"]
                        # restore the = and ;-signs
                        jobDic[k][1].spsetup = jobDic[k][1].spsetup.replace("^", ";").replace("!", "=")
                        tolog("Handler received special setup command: %s" % (jobDic[k][1].spsetup))

                    if jobinfo.has_key("output_fields"):
                        jobDic[k][1].output_latereg = jobinfo["output_latereg"]
                        jobDic[k][1].output_fields = pUtil.stringToFields(jobinfo["output_fields"])
                        tolog("Got output_fields=%s" % str(jobDic[k][1].output_fields))
                        tolog("Converted from output_fields=%s" % str(jobinfo["output_fields"]))
                except Exception, e:
                    tolog("!!WARNING!!1998!! Caught en exception. Pilot server down? %s" % str(e))
                    try:
                        tolog("Received jobinfo: %s" % str(jobinfo))
                    except:
                        pass

        # tolog("---updateHandler : jobDic is %s" % str(jobDic))
        self.request.send("OK")

def cleanup(wd, initdir, wrflag):
    """ cleanup function """

    tolog("Overall cleanup function is called")
    # collect any zombie processes
    wd.collectZombieJob(tn=10)
    tolog("Collected zombie processes")

    # get the current work dir
    wkdir = pUtil.readStringFromFile(os.path.join(initdir, "CURRENT_SITEWORKDIR"))

    # is there an exit code?
    ec = pUtil.readCodeFromFile(os.path.join(wkdir, "EXITCODE"))

    # is there a process id
    pid = pUtil.readCodeFromFile(os.path.join(wkdir, "PROCESSID"))
    if pid != 0:
        tolog("Found process id %d in PROCESSID file, will now attempt to kill all of its subprocesses" % (pid))
        killProcesses(pid)

    if rmwkdir == None or rmwkdir == False:

        # in case of multi-jobs, the workdir will already be deleted
        if os.path.exists(wkdir):

            # does the job work dir contain a lock file for this job?
            if os.path.exists("%s/LOCKFILE" % (wkdir)):
                tolog("Lock file found: will not delete %s!" % (wkdir))
                lockfile = True
                try:
                    os.system("chmod -R g+w %s" % (initdir))
                except Exception, e:
                    tolog("Failed to chmod pilot init dir: %s" % str(e))
                    pass
                else:
                    tolog("Successfully changed permission on pilot init dir (for later pilots that may be run by different users)")
            else:
                lockfile = False

            # remove the work dir only when there are no job state files
            if not lockfile and rmwkdir == None:
                tolog("Attempting to remove the pilot workdir %s now!" % (wkdir))
                try:
                    chdir(initdir)
                    os.system("rm -rf %s" % (wkdir))
                except Exception, e:
                    tolog("!!WARNING!!1000!! Failed to remove pilot workdir: %s" % str(e))
            else:
                if lockfile:
                    # check if the workdir+job state file should be moved to an external directory
                    # check queuedata for external recovery directory
                    recoveryDir = ""
                    try:
                        recoveryDir = readpar('recoverdir')
                    except:
                        pass
                    else:
                        if recoveryDir != "":
                            if not moveToExternal(wkdir, recoveryDir):
                                tolog("Will not cleanup work area since move to external area at least partially failed")
                            else:
                                # cleanup work dir unless user do not want to
                                if rmwkdir == None:
                                    tolog("Removing the pilot workdir %s now! " % (wkdir))
                                    try:
                                        chdir("/")
                                        os.system("rm -rf %s" % (wkdir))
                                    except Exception, e:
                                        tolog("!!WARNING!!1000!! Failed to remove pilot workdir: %s" % str(e))
                                try:
                                    os.system("chmod -R g+w %s" % (recoveryDir))
                                except Exception, e:
                                    tolog("Failed to chmod recovery dir: %s" % str(e))
                                    pass
                                else:
                                    tolog("Successfully changed permission on external recovery dir (for later pilots that may be run by different users)")

                if rmwkdir == False:
                    tolog("rmwkdir flag set to False - will not delete %s" % (wkdir))

        else:
            tolog("Work dir already deleted by multi-job loop: %s" % (wkdir))

    # always remove the workdir if the rmwkdir was set at the pilot launch
    elif rmwkdir:
        if os.path.exists(wkdir):
            tolog("Removing the pilot workdir %s now! " % (wkdir))
            try:
                chdir("/")
                os.system("rm -rf %s" % (wkdir))
            except Exception,e:
                tolog("!!WARNING!!1000!! Failed to remove pilot workdir: %s" % str(e))
        else:
            tolog("Work dir already deleted by multi-job loop: %s" % (wkdir))

    else:
        tolog("rmwkdir flag set to False - will not delete %s" % (wkdir))

    tolog("Pilot cleanup has finished")

    # wait for the stdout to catch up (otherwise the full log is cut off in the batch stdout dump)
    time.sleep(10)

    # return exit code to wrapper (or caller, runMain())
    if wrflag:
        tolog("Done, returning %d to wrapper" % (ec))
        # flush buffers
        sys.stdout.flush()
        sys.stderr.flush()
        return ec
    else:
        tolog("Done, using system exit to quit")
        # flush buffers
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0) # need to call this to clean up the socket, thread etc resources

def fastCleanup(workdir):
    """ cleanup the site workdir """

    print "fastCleanup() called"

    # return to the pilot init dir, otherwise wrapper will not find curl.config
    chdir(pilot_initdir)

    if rmwkdir or rmwkdir == None:
        if os.path.exists(workdir):
            try:
                rc, rs = commands.getstatusoutput("rm -rf %s" % (workdir))
            except Exception, e:
                print "!!WARNING!!1999!! Could not remove site workdir: %s, %s" % (workdir, str(e))
            else:
                if rc == 0:
                    print "Removed site workdir: %s" % (workdir)
                else:
                    print "!!WARNING!!1999!! Could not remove site workdir: %s, %d, %s" % (workdir, rc, rs)
                    s = 3*60
                    max_attempts = 2
                    attempt = 0
                    while attempt < max_attempts:
                        print "Sleeping %d seconds before trying again (re-attempt %d/%d)" % (s, attempt+1, max_attempts)
                        time.sleep(s)
                        try:
                            rc, rs = commands.getstatusoutput("rm -rf %s" % (workdir))
                        except Exception, e:
                            print "!!WARNING!!1999!! Could not remove site workdir: %s, %s" % (workdir, str(e))
                        else:
                            if rc == 0:
                                print "Removed site workdir: %s" % (workdir)
                            else:
                                print "!!WARNING!!1999!! Could not remove site workdir: %s, %d, %s" % (workdir, rc, rs)
                                dir_list = os.listdir(workdir)
                                print str(dir_list)
                                for f in dir_list:
                                    if ".nfs" in f:
                                        fname = os.path.join(workdir, f)
                                        print "Found NFS lock file: %s" % (fname)
                                        cmd = "lsof %s" % (fname)
                                        print "Executing command: %s" % (cmd)
                                        out = commands.getoutput(cmd)
                                        print out

                                        pid = None
                                        pattern = re.compile('sh\s+([0-9]+)')
                                        lines = out.split('\n')
                                        for line in lines:
                                            _pid = pattern.search(line)
                                            if _pid:
                                                pid = _pid.group(1)
                                                break
                                        if pid:
                                            print "Attempting to kill pid=%s" % (pid)
                                            cmd = "kill -9 %s" % (pid)
                                            out = commands.getoutput(cmd)
                                            print out
                                cmd = 'ps -fwu %s' % (commands.getoutput("whoami"))
                                print "%s: %s" % (cmd + '\n', commands.getoutput(cmd))
                        attempt += 1
        else:
            print "Nothing to cleanup (site workdir does not exist: %s)" % (workdir)
    else:
        print "rmwkdir flag set to False - will not delete %s" % (workdir)

    # flush buffers
    sys.stdout.flush()
    sys.stderr.flush()

def killOrphans():
    """ Find and kill all orphan processes belonging to current pilot user """

    tolog("Searching for orphan processes")
    cmd = "ps -o pid,ppid,comm -u %s" % (commands.getoutput("whoami"))
    processes = commands.getoutput(cmd)
    pattern = re.compile('(\d+)\s+(\d+)\s+(\S+)')

    count = 0
    for line in processes.split('\n'):
        ids = pattern.search(line)
        if ids:
            pid = ids.group(1)
            ppid = ids.group(2)
            comm = ids.group(3)
            if ppid == '1':
                count += 1
                tolog("Found orphan process: pid=%s, ppid=%s" % (pid, ppid))
                cmd = 'kill -9 %s' % (pid)
                ec, rs = commands.getstatusoutput(cmd)
                if ec != 0:
                    tolog("!!WARNING!!2999!! %s" % (rs))
                else:
                    tolog("Killed orphaned process %s (%s)" % (pid, comm))

    if count == 0:
        tolog("Did not find any orphan processes")
    else:
        tolog("Found %d orphan process(es)" % (count))

def findProcessesInGroup(cpids, pid):
    """ recursively search for the children processes belonging to pid and return their pids
    here pid is the parent pid for all the children to be found
    cpids is a list that has to be initialized before calling this function and it contains
    the pids of the children AND the parent as well """

    cpids.append(pid)
    psout = commands.getoutput("ps -eo pid,ppid -m | grep %d" % pid)
    lines = psout.split("\n")
    if lines != ['']:
        for i in range(0, len(lines)):
            thispid = int(lines[i].split()[0])
            thisppid = int(lines[i].split()[1])
            if thisppid == pid:
                if debugLevel == 2:
                    debugInfo("one child of pid %d found: %d" % (thisppid, thispid))
                findProcessesInGroup(cpids, thispid)

def isZombie(pid):
    """ Return True if pid is a zombie process """

    zombie = False

    out = commands.getoutput("ps aux | grep %d" % (pid))
    if "<defunct>" in out:
        zombie = True

    return zombie

def dumpStackTrace(pid):
    """ run the stack trace command """

    # make sure that the process is not in a zombie state
    if not isZombie(pid):
        tolog("Running stack trace command on pid=%d:" % (pid))
        cmd = "pstack %d" % (pid)
        out = commands.getoutput(cmd)
        if out == "":
            tolog("(pstack returned empty string)")
        else:
            tolog(out)
    else:
        tolog("Skipping pstack dump for zombie process")

def killProcesses(pid):
    """ kill a job upon request """

    # firstly find all the children process IDs to be killed
    kids = []
    findProcessesInGroup(kids, pid)
    # reverse the process order so that the athena process is killed first (otherwise the stdout will be truncated)
    kids.reverse()
    tolog("Process IDs to be killed: %s (in reverse order)" % str(kids))

    # find which commands are still running
    try:
        cmds = getProcessCommands(os.geteuid(), kids)
    except Exception, e:
        tolog("getProcessCommands() threw an exception: %s" % str(e))
    else:
        if len(cmds) <= 1:
            tolog("Found no corresponding commands to process id(s)")
        else:
            tolog("Found commands still running:")
            for cmd in cmds:
                tolog(cmd)

            # loop over all child processes
            first = True
            for i in kids:
                # dump the stack trace before killing it
                dumpStackTrace(i)

                # kill the process gracefully
                try:
                    os.kill(i, signal.SIGTERM)
                except Exception,e:
                    tolog("WARNING: Exception thrown when killing the child process %d under SIGTERM, wait for kill -9 later: %s" % (i, str(e)))
                    pass
                else:
                    tolog("Killed pid: %d (SIGTERM)" % (i))

                if first:
                    _t = 60
                    first = False
                else:
                    _t = 10
                tolog("Sleeping %d s to allow process to exit" % (_t))
                time.sleep(_t)
    
                # now do a hardkill just in case some processes haven't gone away
                try:
                    os.kill(i, signal.SIGKILL)
                except Exception,e:
                    tolog("WARNING: Exception thrown when killing the child process %d under SIGKILL, ignore this if it is already killed by previous SIGTERM: %s" % (i, str(e)))
                    pass
                else:
                    tolog("Killed pid: %d (SIGKILL)" % (i))

def getProcessCommands(euid, pids):
    """ return a list of process commands corresponding to a pid list for user euid """

    _cmd = 'ps u -u %d' % (euid)
    processCommands = []
    ec, rs = commands.getstatusoutput(_cmd)
    if ec != 0:
        tolog("Command failed: %s" % (rs))
    else:
        # extract the relevant processes
        pCommands = rs.split('\n') 
        first = True
        for pCmd in pCommands:
            if first:
                # get the header info line
                processCommands.append(pCmd)
                first = False
            else:
                # remove extra spaces
                _pCmd = pCmd
                while "  " in _pCmd:
                    _pCmd = _pCmd.replace("  ", " ")
                items = _pCmd.split(" ")
                for pid in pids:
                    # items = username pid ...
                    if items[1] == str(pid):
                        processCommands.append(pCmd)
                        break

    return processCommands

def createSoftLink(thisSite, create_softlink=True):
    """ create a soft link to the athena stdout in the site work dir """
    # create_softlink is mutable

    # will only point to the first stdout file currently
    for k in jobDic.keys():
        # is this a multi-trf job?
        nJobs = jobDic[k][1].jobPars.count("\n") + 1
        for _i in range(nJobs):
            _stdout = jobDic[k][1].stdout
            if nJobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))
            filename = os.path.join(jobDic[k][1].workdir, _stdout)
            # create the soft link only if the stdout has been created
            if os.path.exists(filename):
                lnfilename = os.path.join(thisSite.workdir, _stdout)
                # only create the soft link once..
                if not os.path.exists(lnfilename):
                    # ..and only if the size of stdout is > 0
                    if os.path.getsize(filename) > 0:
                        ec, rs = commands.getstatusoutput("ln -s %s %s" % (filename, lnfilename))
                        if ec == 0:
                            tolog("Created soft link to %s in sitedir: %s" % (_stdout, lnfilename))
                        else:
                            tolog("!!WARNING!!1999!! Could not create soft link: %d, %s" % (ec, rs))
                        create_softlink = False
                else:
                    create_softlink = False
            else:
                tolog("(%s has not been created yet)" % (_stdout))

def dumpVars(thisSite):
    """ dump argParser variables """

    tolog("Pilot options:................................................")
    tolog("appdir: %s" % (thisSite.appdir))
    tolog("debugLevel: %s" % str(debugLevel))
    tolog("dq2url: %s" % str(thisSite.dq2url))
    tolog("jobrec: %s" % str(jobrec))
    tolog("jobRequestFlag: %s" % str(jobRequestFlag))
    tolog("jobSchedulerId: %s" % str(jobSchedulerId))
    tolog("maxjobrec: %s" % str(maxjobrec))
    tolog("maxNumberOfRecoveryAttempts: %s" % str(maxNumberOfRecoveryAttempts))
    tolog("pilotId: %s" % str(pilotId))
    tolog("pshttpurl: %s" % (pshttpurl))
    tolog("psport: %s" % (psport))
    tolog("queuename: %s" % (queuename))
    tolog("rmwkdir: %s" % str(rmwkdir))
    tolog("sitename: %s" % (thisSite.sitename))
    tolog("stageinretry: %s" % str(stageinretry))
    tolog("stageoutretry: %s" % str(stageoutretry))
    tolog("uflag: %s" % str(uflag))
    tolog("workdir: %s" % (thisSite.workdir))
    tolog("logFileDir: %s" % (logFileDir))
    tolog("..............................................................")

def getInOutDirs():
    """ return the input and output directories """

    global inputDir, outputDir
    if inputDir != "":
        tolog("Requested input file dir: %s" % (inputDir))
    else:
        # default inputDir only releveant for mv site mover
        inputDir = pilot_initdir
    if outputDir != "":
        tolog("Requested output file dir: %s" % (outputDir))
    else:
        # default outputDir only releveant for mv site mover
        outputDir = pilot_initdir
    return inputDir, outputDir

def diskCleanup(wntmpdir, _uflag):
    """ Perform disk cleanup """

    tolog("Preparing to execute Cleaner")
    from Cleaner import Cleaner

    dirs = [wntmpdir]
    _wntmpdir = readpar('wntmpdir')
    if wntmpdir != _wntmpdir and _wntmpdir != "":
        dirs.append(_wntmpdir)

    for _dir in dirs:
        tolog("Cleaning %s" % (_dir))
        cleaner = Cleaner(limit=cleanupLimit, path=_dir, uflag=_uflag)
        _ec = cleaner.cleanup()
        del cleaner

def checkLocalSE(sitename, error):
    """ Make sure that the local SE is responding """

    ec = 0

    if "ANALY" in sitename:
        analyJob = True
    else:
        analyJob = False

    if not mover.checkLocalSE(analyJob):
        _delay = 2*60
        tolog("!!WARNING!!1111!! Detected problem with the local SE")
        tolog("Taking a nap for %d s before trying again" % (_delay))
        time.sleep(_delay)
        if not mover.checkLocalSE(analyJob):
            tolog("!!WARNING!!1111!! Detected problem with the local SE (again) - giving up")
            ec = error.ERR_SEPROBLEM
    return ec

def verifyProxyValidity():
    """ Verify the proxy validity """

    ec = 0

    if proxycheckFlag:
        if 'pandadev' in pshttpurl:
            _limit = 2
        else:
            _limit = None
        ec, pilotErrorDiag = pUtil.verifyProxyValidity(_limit)
        if ec != 0:
            tolog("!!FAILED!!1999!! Did not find a valid proxy, will now abort")
    else:
        tolog("Skipping proxy verification since proxycheckFlag is False")

    return ec

def createSiteWorkDir(workdir, error):
    """ Create the pilot workdir and write the path to file """

    ec = 0

    tolog("Will attempt to create workdir: %s" % (workdir))
    try:
        # note: do not set permissions in makedirs since they will not come out correctly, 0770 -> 0750
        os.makedirs(workdir)
        os.chmod(workdir, 0770)
    except Exception, e:
        tolog("!!WARNING!!1999!! Exception caught: %s (will try os.mkdir instead)" % str(e))
        # a bug in makedirs can attempt to create existing basedirs, try to use mkdir instead
        try:
            # change to absolute permissions, requested by QMUL
            # note: do not set permissions in makedirs since they will not come out correctly, 0770 -> 0750
            os.mkdir(workdir)
            os.chmod(workdir, 0770)
        except Exception, e:
            errorText = "Exception caught: %s" % str(e)
            tolog("!!FAILED!!1999!! %s" % (errorText))
            ec = error.ERR_MKDIRWORKDIR
        else:
            ec = 0

            # verify permissions
            cmd = "stat %s" % (workdir)
            tolog("(1) Executing command: %s" % (cmd))
            rc, rs = commands.getstatusoutput(cmd)
            tolog("\n%s" % (rs))

    if ec == 0:
        path = os.path.join(pilot_initdir, "CURRENT_SITEWORKDIR")
        if os.path.exists(path):
            # remove the old file
            try:
                os.remove(path)
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not remove old file: %s, %s (attempt to overwrite)" % (path, str(e)))
            else:
                tolog("Removed old file: %s" % (path))
        tolog("Creating file: %s" % (path))
        pUtil.writeToFile(os.path.join(pilot_initdir, "CURRENT_SITEWORKDIR"), workdir)

    return ec

def getsetWNMem():
    """ Get the memory limit from queuedata or from the -k pilot option and set it """

    wn_mem = 0

    # Get the memory limit primarily from queuedata
    # Note: memory will soon be changed to maxmemory
    _maxmemory = readpar('maxmemory')
    if _maxmemory == "":
        _maxmemory = readpar('memory')

    if _maxmemory != "":
        try:
            maxmemory = int(_maxmemory) # Should already be an int
        except Exception, e:
            tolog("Could not convert maxmemory to an int: %s" % (e))
            maxmemory = -1
        else:
            tolog("Got max memory limit: %d MB (from queuedata)" % (maxmemory))
    else:
        maxmemory = -1

    # Get the max memory limit from the -k pilot option if specified
    if maxmemory == -1 and memory:
        try:
            maxmemory = int(memory)
        except Exception, e:
            tolog("Could not convert memory to an int: %s" % (e))
            maxmemory = -1
        else:
            tolog("Got max memory limit: %d MB (from pilot option -k)" % (maxmemory))

    # Set the memory limit
    if maxmemory > 0:
        
        # Convert MB to Bytes for the setrlimit function
        _maxmemory = maxmemory*1024**2
        try:
            import resource
            resource.setrlimit(resource.RLIMIT_AS, [_maxmemory, _maxmemory])
        except Exception, e:
            tolog("!!WARNING!!3333!! resource.setrlimit failed: %s" % (e))
        else:
            tolog("Max memory limit set to: %d B" % (_maxmemory))

        cmd = "ulimit -a"
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog("\n%s" % (out))
    else:
        tolog("Max memory will not be set")

    return maxmemory

def killLoopingJob(job, pid, error, setStageout=False):
    """ kill the looping job """

    # the child process is looping, kill it
    pilotErrorDiag = "Pilot has decided to kill looping job %d at %s" %\
                     (job.jobId, pUtil.timeStamp())
    tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))

    cmd = 'ps -fwu %s' % (commands.getoutput("whoami"))
    tolog("%s: %s" % (cmd + '\n', commands.getoutput(cmd)))
    cmd = 'ls -ltr %s' % (job.workdir)
    tolog("%s: %s" % (cmd + '\n', commands.getoutput(cmd)))
    cmd = 'ps -o pid,ppid,sid,pgid,tpgid,stat,comm -u %s' % (commands.getoutput("whoami"))
    tolog("%s: %s" % (cmd + '\n', commands.getoutput(cmd)))

    tolog("stageout=%s"%str(stageout))
    tolog("setStageout=%s"%str(setStageout))
    tolog("pass 1")
    killOrphans()
    killProcesses(pid)
    tolog("pass 2")
    killOrphans()
    if stagein:
        pilotErrorDiag += " (Job stuck in stage-in state)"
        tolog("!!FAILED!!1999!! Job stuck in stage-in state: file copy time-out")
        job.result[2] = error.ERR_GETTIMEOUT
    elif setStageout:
        pilotErrorDiag += " (Job stuck in stage-out state)"
        tolog("!!FAILED!!1999!! Job stuck in stage-out state: file copy time-out")
        job.result[2] = error.ERR_PUTTIMEOUT
    else:
        job.result[2] = error.ERR_LOOPINGJOB
        job.result[0] = "failed"
        job.currentState = job.result[0]
        job.pilotErrorDiag = pilotErrorDiag

    # remove any lingering input files from the work dir
    if len(job.inFiles) > 0:
        ec = pUtil.removeFiles(job.workdir, job.inFiles)

    return job

def loopingJobKiller(LastTimeFilesWereModified, loopingLimit):
    """ Look for looping job """

    global jobDic

    # get error handler
    error = PilotErrors()

    tolog("Checking for looping job")
    for k in jobDic.keys():

        # if current run state is "stageout", just make sure that the stage out copy command is not hanging
        if stageout:
            tolog("Making sure that the stage out copy command is not hanging")

            if not stageoutStartTime:
                tolog("!!WARNING!!1700!! Stage-out start time not set")
            else:
                # how much time has passed since stage-out began?
                time_passed = int(time.time()) - stageoutStartTime

                # find out the timeout limit for the relevant site mover
                from SiteMoverFarm import getSiteMover
                sitemover = getSiteMover(readpar('copytool'), "")
                timeout = sitemover.get_timeout()

                tolog("Stage-out with %s site mover began at %s (%d s ago, site mover time-out: %d s)" %\
                      (readpar('copytool'), time.strftime("%H:%M:%S", time.gmtime(stageoutStartTime)), time_passed, timeout))

                grace_time = 5*60
                if time_passed > timeout:
                    tolog("Adding a grace time of %d s in case copy command has been aborted but file removal is not complete" % (grace_time))
                if time_passed > timeout + grace_time:
                    jobDic[k][1] = killLoopingJob(jobDic[k][1], jobDic[k][0], error, setStageout=True)

        elif len(jobDic[k][1].outFiles) > 0: # only check for looping job for jobs with output files

            # loop over all job output files and find the one with the latest modification time
            # note that some files might not have been created at this point (e.g. RDO built from HITS)

            # locate all files that were modified the last N minutes
            cmd = "find %s -mmin -%d" % (jobDic[k][1].workdir, int(loopingLimit/60))
            tolog("Executing command: %s" % (cmd))
            out = commands.getoutput(cmd)
            if out != "":
                files = out.split("\n")
                if len(files) > 0:
                    # remove unwanted list items (*.py, *.pyc, workdir, ...)
                    _files = []
                    for _file in files:
                        if not (jobDic[k][1].workdir == _file or
                                ".lib.tgz" in _file or
                                ".py" in _file or
                                "PoolFileCatalog" in _file or
                                "setup.sh" in _file or
                                "jobState" in _file or
                                "pandaJob" in _file or
                                "runjob" in _file or
                                "matched_replicas" in _file or
                                "DBRelease-" in _file):
                            _files.append(_file)
#                        else:
#                            tolog("Ignored file: %s" % (_file))
                    if _files != []:
                        tolog("Found %d files that were recently updated (e.g. file %s)" % (len(_files), _files[0]))
#                        s = ""
#                        for _file in _files:
#                            s += _file + ", "
#                        tolog(s)
                        # get the current system time
                        LastTimeFilesWereModified[k] = int(time.time())
                    else:
                        tolog("WARNING: found no recently updated files!")
                else:
                    tolog("WARNING: found no recently updated files")
            else:
                tolog("WARNING: Found no recently updated files")

            # check if the last modification time happened long ago
            # (process is considered to be looping if it's files have not been modified within loopingLimit time)
            tolog("int(time.time())=%d"%int(time.time()))
            tolog("LastTimeFilesWereModified=%d"%LastTimeFilesWereModified[k])
            tolog("loopingLimit=%d"%loopingLimit)
            if (int(time.time()) - LastTimeFilesWereModified[k]) > loopingLimit:
                jobDic[k][1] = killLoopingJob(jobDic[k][1], jobDic[k][0], error)

    return LastTimeFilesWereModified

def verifyOutputFileSizes(error):
    """ Verify output file sizes """

    global jobDic
    pilotErrorDiag = ""
    job_index = 0
    rc = 0

    tolog("Verifying output file sizes")
    for k in jobDic.keys():
        if len(jobDic[k][1].outFiles) > 0:
            for file_name in jobDic[k][1].outFiles:
                findFlag = False
                # locate the file first
                out = commands.getoutput("find %s -name %s" % (jobDic[k][1].workdir, file_name))
                if out != "":
                    for line in out.split('\n'):
                        try:
                            file_size = os.path.getsize(line)
                            findFlag = True
                            if file_size > outputlimit:
                                pilotErrorDiag = 'File: \"%s\" is too large %d > %d B)' % (line, file_size, outputlimit)
                                tolog('!!WARNING!!2999!!%s' % (pilotErrorDiag))
                                job_index = k
                                rc = error.ERR_OUTPUTFILETOOLARGE
                            else:
                                tolog('File: \"%s\" currently has size %d < %d B)' % (line, file_size, outputlimit))
                        except:
                            pass
                if not findFlag and file_name != jobDic[k][1].logFile:
#                if not findFlag and not ".log." in file_name:
                    tolog("Could not access file %s: %s" % (file_name, out))

    return rc, pilotErrorDiag, job_index

def checkLocalDiskSpace(disk, error):
    """ Do we have enough local disk space left to run the job? """

    ec = 0

    # convert local space to B and compare with the space limit
    spaceleft = int(disk)*1024**2 # B (node.disk is in MB)
    _localspacelimit = localspacelimit0*1024 # B
    tolog("Local space limit: %d B" % (_localspacelimit))
    if spaceleft < _localspacelimit:
        tolog("!!FAILED!!1999!! Too little space left on local disk to run job: %d B (need > %d B)" %\
              (spaceleft, _localspacelimit))
        ec = error.ERR_NOLOCALSPACE
    else:
        tolog("Remaining local disk space: %d B" % (spaceleft))

    return ec

def getLoopingLimit(maxCpuCount, jobPars, sitename):
    """ Get the looping time limit for the current job (in seconds) """

    # start with the default looping time limit, use maxCpuCount if necessary
    if "ANALY_" in sitename:
        loopingLimit = loopingLimitDefaultUser
    else:
        loopingLimit = loopingLimitDefaultProd
    if maxCpuCount >= loopingLimitMinDefault:
        _loopingLimit = max(loopingLimitMinDefault, maxCpuCount)
    else:
        _loopingLimit = max(loopingLimit, maxCpuCount)
    if _loopingLimit != loopingLimit:
        tolog("Task request: Updated looping job limit from %d s to %d s using maxCpuCount" % \
              (loopingLimit, _loopingLimit))
        loopingLimit = _loopingLimit
    else:
        # increase the looping job limit for multi-trf jobs
        if jobPars.find("\n") >= 0:
            tolog("Multi-trf job encountered: will double the looping job limit")
            loopingLimit = 2*loopingLimit
        tolog("Using looping job limit: %d s" % (loopingLimit))
        tolog("maxCpuCount: %d s" % (maxCpuCount))

    return loopingLimit

def createJobWorkdir(job, thisSite, workerNode, stderr, error):
    """ Attempt to create the job workdir """

    ec, errorText = job.mkJobWorkdir(thisSite.workdir)
    if ec != 0:
        job.setState(["failed", 0, error.ERR_MKDIRWORKDIR])
        ret, retNode = updatePandaServer(job, thisSite, workerNode, psport, schedulerID=jobSchedulerId, pilotID=pilotId, updateServer=updateServerFlag)
        if ret == 0:
            tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
        else:
            tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
        # send to stderr
        print >> stderr, errorText
        # remove the site workdir before exiting
        pUtil.writeToFile(os.path.join(thisSite.workdir, "EXITCODE"), str(error.ERR_GENERALERROR))
    else:
        tolog("Created job workdir at %s" % (job.workdir))
        # copy the job def file into job workdir
        copy("./Job_%d.py" % job.jobId, "%s/newJobDef.py" % job.workdir)

    return ec, job

def throttleJob():
    """ short delay requested by the server """

    # dictionary for throttling job startup (setting running state)
    throttleDic = {'CA':6, 'CERN':1.33, 'DE':6, 'ES':6, 'FR':6, 'IT':6, 'ND':0, 'NDGF':0, 'NL':6, 'TW':6, 'UK':6, 'US':6}
    if nSent > 0:
        try:
            _M = throttleDic[readpar('cloud')]
        except Exception, e:
            tolog("Warning: %s (using default value 6 as multiplier for throttling)" % str(e))
            _M = 6
        _t = (nSent + 1)*_M
        # protect for potential unreasonably high sleeping times
        max_sleep = 60
        if _t > max_sleep:
            _t = max_sleep
            tolog("!!WARNING!!1111!! Throttle time out of bounds, reset to %d s (nSent = %d)" % (max_sleep, nSent))
        tolog("Throttle sleep: %d s" % (_t))
        time.sleep(_t)

def backupJobDef(number_of_jobs):
    """ Backup job definition """

    # note: the log messages here only appears in pilotlog.txt and not in the batch log since they are
    # written by the forked child process

    if os.path.exists(pandaJobDataFileName):
        tolog("Copying job definition (%s) to %s" % (pandaJobDataFileName, jobDic["prod"][1].workdir))
        try:
            copy2(pandaJobDataFileName, jobDic["prod"][1].workdir)
        except Exception, e:
            tolog("!!WARNING!!1999!! Could not backup job definition: %s" % str(e))
        else:
            pandaJobDataFileName_i = pandaJobDataFileName.replace(".out", "_%d.out" % (number_of_jobs))
            _path = os.path.join(pilot_initdir, pandaJobDataFileName_i)
            tolog("Copying job definition (%s) to %s" % (pandaJobDataFileName, _path))
            try:
                copy2(pandaJobDataFileName, _path)
            except Exception, e:
                tolog("!!WARNING!!1999!! Could not backup job definition: %s" % str(e))
    else:
        tolog("!!WARNING!!1999!! Could not backup job definition since file %s does not exist" % (pandaJobDataFileName))

def checkPayloadStdout(error):
    """ Check the size of the payload stdout """

    global jobDic
    skip = False

    # loop over all parallel jobs
    # (after multitasking was removed from the pilot, there is actually only one job)
    for k in jobDic.keys():
        # get list of log files
        fileList = glob("%s/log.*" % (jobDic[k][1].workdir))

        # is this a multi-trf job?
        nJobs = jobDic[k][1].jobPars.count("\n") + 1
        for _i in range(nJobs):
            # get name of payload stdout file created by the pilot
            _stdout = jobDic[k][1].stdout
            if nJobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))
            filename = "%s/%s" % (jobDic[k][1].workdir, _stdout)

            # add the primary stdout file to the fileList
            fileList.append(filename)

        # now loop over all files and check each individually (any large enough file will fail the job)
        for filename in fileList:

            if os.path.exists(filename):
                try:
                    # get file size in bytes
                    fsize = os.path.getsize(filename)
                except Exception, e:
                    tolog("!!WARNING!!1999!! Could not read file size of %s: %s" % (filename, str(e)))
                else:
                    # is the file too big?
                    if fsize > localsizelimit_stdout*1024: # convert to bytes
                        pilotErrorDiag = "Payload stdout file too big: %d B (larger than limit %d B)" % (fsize, localsizelimit_stdout*1024)
                        tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
                        # kill the job
                        killProcesses(jobDic[k][0])
                        jobDic[k][1].result[0] = "failed"
                        jobDic[k][1].currentState = jobDic[k][1].result[0]
                        jobDic[k][1].result[2] = error.ERR_STDOUTTOOBIG
                        jobDic[k][1].pilotErrorDiag = pilotErrorDiag
                        skip = True

                        # remove the payload stdout file after the log extracts have been created

                        # remove any lingering input files from the work dir
                        if len(jobDic[k][1].inFiles) > 0:
                            ec = pUtil.removeFiles(jobDic[k][1].workdir, jobDic[k][1].inFiles)
                    else:
                        tolog("Payload stdout (%s) within allowed size limit (%d B): %d B" % (_stdout, localsizelimit_stdout*1024, fsize))
            else:
                tolog("(Skipping file size check of payload stdout file (%s) since it has not been created yet)" % (_stdout))

    return skip

def getStdoutDictionary():
    """ Create a dictionary with the tails of all running payloads """

    global jobDic
    stdout_dictionary = {}
    number_of_lines = 20 # tail -20 filename

    # loop over all parallel jobs
    # (after multitasking was removed from the pilot, there is actually only one job)
    for k in jobDic.keys():

        jobId = jobDic[k][1].jobId

        # abort if not debug mode, but save an empty entry in the dictionary
        if jobDic[k][1].debug.lower() != "true":
            stdout_dictionary[jobId] = ""
            continue

        # is this a multi-trf job?
        nJobs = jobDic[k][1].jobPars.count("\n") + 1
        for _i in range(nJobs):
            _stdout = jobDic[k][1].stdout
            if nJobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))

            filename = "%s/%s" % (jobDic[k][1].workdir, _stdout)
            if os.path.exists(filename):
                try:
                    # get the tail
                    cmd = "tail -%d %s" % (number_of_lines, filename)
                    tolog("Executing command: %s" % (cmd))
                    stdout = commands.getoutput(cmd)
                except Exception, e:
                    tolog("!!WARNING!!1999!! Tail command threw an exception: %s" % (e))
                    stdout_dictionary[jobId] = "(no stdout, caught exception: %s) % (e)"
                else:
                    if stdout == "":
                        tolog("!!WARNING!!1999!! Tail revealed empty stdout for file %s" % (filename))
                        stdout_dictionary[jobId] = "(no stdout)"
                    else:
                        # add to tail dictionary
                        stdout_dictionary[jobId] = stdout

                # add the number of lines (later this should always be sent)
                pattern = re.compile(r"(\d+) [\S+]")
                cmd = "wc -l %s" % (filename)
                try:
                    _nlines = commands.getoutput(cmd)
                except Exception, e:
                    pilotErrorDiag = "wc command threw an exception: %s" % (e)
                    tolog("!!WARNING!!1999!! %s" % (pilotErrorDiag))
                    nlines = pilotErrorDiag
                else:
                    try:
                        nlines = re.findall(pattern, _nlines)[0]
                    except Exception, e:
                        pilotErrorDiag = "re.findall threw an exception: %s" % (e)
                        tolog("!!WARNING!!1999!! %s" % (pilotErrorDiag))
                        nlines = pilotErrorDiag
                stdout_dictionary[jobId] += "\n[%s]" % (nlines)
            else:
                tolog("(Skipping tail of payload stdout file (%s) since it has not been created yet)" % (_stdout))
                stdout_dictionary[jobId] = "(stdout not available yet)"

    tolog("Returning tail stdout dictionary with %d entries" % len(stdout_dictionary.keys()))
    return stdout_dictionary

def checkLocalSpace(disk, error, skip):
    """ check the remaining local disk space during running """

    global jobDic
    spaceleft = int(disk)*1024**2 # B (node.disk is in MB)
    _localspacelimit = localspacelimit*1024 # B

    # do we have enough local disk space to continue running the job?
    if spaceleft < _localspacelimit:
        pilotErrorDiag = "Too little space left on local disk to run job: %d B (need > %d B)" % (spaceleft, _localspacelimit)
        tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
        for k in jobDic.keys():
            # kill the job
            killProcesses(jobDic[k][0])
            jobDic[k][1].result[0] = "failed"
            jobDic[k][1].currentState = jobDic[k][1].result[0]
            jobDic[k][1].result[2] = error.ERR_NOLOCALSPACE
            jobDic[k][1].pilotErrorDiag = pilotErrorDiag
            skip = True

            # remove any lingering input files from the work dir
            if len(jobDic[k][1].inFiles) > 0:
                ec = pUtil.removeFiles(jobDic[k][1].workdir, jobDic[k][1].inFiles)
    else:
        tolog("Remaining local disk space: %d B" % (spaceleft))

    return skip

# localspacelimit_workdir += readpar('maxinputsize') or use readpar('maxwdir') if set

def getMaxWorkDirSize():
    """ Return the maximum allowed size of the work directory for user jobs """

    try:
        maxwdirsize = int(readpar('maxwdir'))*1024**2 # from MB to B, e.g. 16336 MB -> 17,129,537,536 B
    except:
        maxInputSize = getMaxInputSize()
        maxwdirsize = maxInputSize + localsizelimit_stdout*1024
        tolog("Work directory size check will use %d B as a max limit (maxinputsize [%d B] + local size limit for stdout [%d B])" %\
              (maxwdirsize, maxInputSize, localsizelimit_stdout*1024))
    else:
        tolog("Work directory size check will use %d B as a max limit (maxwdirsize)" % (maxwdirsize))

    return maxwdirsize

def checkWorkDir(error, skip):
    """ Check the size of the pilot work dir for use jobs """

    global jobDic

    # get the limit of the workdir
    maxwdirsize = getMaxWorkDirSize()

    # after multitasking was removed from the pilot, there is actually only one job
    for k in jobDic.keys():
        # get size of workDir
        workDir = "%s" % (jobDic[k][1].workdir)
        if os.path.exists(workDir):
            try:
                # get the size in kB
                size_str = commands.getoutput("du -sk %s" % (workDir))
            except Exception, e:
                tolog("Warning: failed to check remaining space: %s, %s" % (workDir, str(e)))
            else:
                # e.g., size_str = "900\t/scratch-local/nilsson/pilot3z"
                try:
                    # remove tab and path, and convert to int (and B)
                    size = int(size_str.split("\t")[0])*1024
                except Exception, e:
                    tolog("Warning: failed to convert to int: %s" % str(e))
                else:
                    # is user dir within allowed size limit?
                    if size > maxwdirsize:
                        pilotErrorDiag = "Work directory (%s) too large: %d B (must be < %d B)" %\
                                         (workDir, size, maxwdirsize)
                        tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))

                        # kill the job
                        killProcesses(jobDic[k][0])
                        jobDic[k][1].result[0] = "failed"
                        jobDic[k][1].currentState = jobDic[k][1].result[0]
                        jobDic[k][1].result[2] = error.ERR_USERDIRTOOLARGE
                        jobDic[k][1].pilotErrorDiag = pilotErrorDiag
                        skip = True

                        # remove any lingering input files from the work dir
                        if len(jobDic[k][1].inFiles) > 0:
                            ec = pUtil.removeFiles(jobDic[k][1].workdir, jobDic[k][1].inFiles)
                    else:
                        tolog("Checked size of user analysis work directory %s: %d B (within %d B limit)" %\
                              (workDir, size, maxwdirsize))
        else:
            tolog("(Skipping size check of workDir since it has not been created yet)")

    return skip

def checkOutputFileSizes(error, skip):
    """ check that the output file sizes are within the limit """
    # return True for too large files, to skip looping test and normal server update

    global jobDic

    # verify the file sizes
    rc, pilotErrorDiag, job_index = verifyOutputFileSizes(error)
    if rc == 0:
        tolog("Completed output file size verification")
    else:
        # found too large output file, stop the job
        jobDic[job_index][1].result[0] = "failed"
        jobDic[job_index][1].currentState = jobDic[job_index][1].result[0]
        jobDic[job_index][1].result[2] = rc
        jobDic[job_index][1].pilotErrorDiag = pilotErrorDiag
        skip = True

    return skip

def updateJobs(thisSite, workerNode, error):
    """ Make final server update for all ended jobs """

    global jobDic, jobrec, update_freq_server

    # get the stdout tails
    stdout_dictionary = getStdoutDictionary()

    # loop over all parallel jobs, update server, kill job if necessary
    # (after multitasking was removed from the pilot, there is actually only one job)
    for k in jobDic.keys():
        tmp = jobDic[k][1].result[0]
        if tmp != "finished" and tmp != "failed" and tmp != "holding":

            # get the tail if possible
            try:
                stdout_tail = stdout_dictionary[jobDic[k][1].jobId]
            except:
                stdout_tail = "(stdout tail not available)"

            # update the panda server
            ret, retNode = updatePandaServer(jobDic[k][1], thisSite, workerNode, psport, schedulerID=jobSchedulerId, pilotID=pilotId,
                                             updateServer=updateServerFlag, stdout_tail=stdout_tail)
            if ret == 0:
                tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
            else:
                tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))

            # kill a job if signaled from panda server
            if "tobekilled" in jobDic[k][1].action:
                pilotErrorDiag = "Pilot received a panda server signal to kill job %d at %s" %\
                                 (jobDic[k][1].jobId, pUtil.timeStamp())
                tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
                if jobrec:
                    jobrec = False
                    tolog("Switching off job recovery")
                # kill the real job process(es)
                killProcesses(jobDic[k][0])
                jobDic[k][1].result[0] = "failed"
                jobDic[k][1].currentState = jobDic[k][1].result[0]
                jobDic[k][1].result[2] = error.ERR_PANDAKILL
                jobDic[k][1].pilotErrorDiag = pilotErrorDiag

            # did we receive a command to turn on debug mode?
            if "debug" in jobDic[k][1].action.lower():
                tolog("Pilot received a command to turn on debug mode from the server")
                update_freq_server = 5*60
                tolog("Server update frequency lowered to %d s" % (update_freq_server))
                jobDic[k][1].debug = "True"

            # did we receive a command to turn off debug mode?
            if "debugoff" in jobDic[k][1].action.lower():
                tolog("Pilot received a command to turn off debug mode from the server")
                update_freq_server = 30*60
                tolog("Server update frequency increased to %d s" % (update_freq_server))
                jobDic[k][1].debug = "False"

def cleanUpEndedJobs(thisSite, workerNode, error):
    """ clean up the ended jobs (if there are any) """

    global prodJobDone, jobDic, zombieJobList

    # after multitasking was removed from the pilot, there is actually only one job
    first = True
    for k in jobDic.keys():
        perr = jobDic[k][1].result[2]
        terr = jobDic[k][1].result[1]
        tmp = jobDic[k][1].result[0]
        if tmp == "finished" or tmp == "failed" or tmp == "holding":
            tolog("Clean up the ended job: %s" % str(jobDic[k]))

            # do not put the getStdoutDictionary() call outside the loop since cleanUpEndedJobs() is called every minute
            # only call getStdoutDictionary() once
            if first:
                # get the stdout tails
                tolog("Refreshing tail stdout dictinary prior to finishing the job(s)")
                stdout_dictionary = getStdoutDictionary()
                first = False

            # refresh the stdout tail if necessary
            # get the tail if possible
            try:
                stdout_tail = stdout_dictionary[jobDic[k][1].jobId]
            except:
                stdout_tail = "(stdout tail not available)"

            # verify permissions
            cmd = "stat %s" % (thisSite.workdir)
            tolog("(2a) Executing command: %s" % (cmd))
            rc, rs = commands.getstatusoutput(cmd)
            tolog("\n%s" % (rs))
            # verify permissions
            cmd = "stat %s" % (pilot_initdir)
            tolog("(2b) Executing command: %s" % (cmd))
            rc, rs = commands.getstatusoutput(cmd)
            tolog("\n%s" % (rs))

            # cleanup the job workdir, save/send the job tarball to DDM, and update
            # panda server with the final job state
            postJobTask(jobDic[k][1], thisSite, workerNode, jr=False, stdout_tail=stdout_tail)
            if k == "prod":
                prodJobDone = True

            # for NG write the error code, if any
            if readpar('region') == "Nordugrid" and (perr != 0 or terr != 0):
                if perr != 0:
                    ec = perr
                else:
                    ec = terr
                pUtil.writeToFile(os.path.join(thisSite.workdir, "EXITCODE"), str(ec))

            # move this job from jobDic to zombieJobList for later collection
            zombieJobList.append(jobDic[k][0]) # only needs pid of this job for cleanup

            # athena processes can loop indefinately (e.g. pool utils), so kill all subprocesses just in case
            tolog("Killing remaining subprocesses (if any)")
            if jobDic[k][1].result[2] == error.ERR_OUTPUTFILETOOLARGE:
                tolog("pass 1")
                killOrphans()
            killProcesses(jobDic[k][0])
            if jobDic[k][1].result[2] == error.ERR_OUTPUTFILETOOLARGE:
                tolog("pass 2")
                killOrphans()
            # remove the process id file to prevent cleanup from trying to kill the remaining processes another time
            # (should only be necessary for jobs killed by the batch system)
            if os.path.exists(os.path.join(thisSite.workdir, "PROCESSID")):
                try:
                    os.remove(os.path.join(thisSite.workdir, "PROCESSID"))
                except Exception, e:
                    tolog("!!WARNING!!2999!! Could not remove process id file: %s" % str(e))
                else:
                    tolog("Process id file removed")

            # ready with this object, delete it
            del jobDic[k]

def handleQueuedata(_queuename, _pshttpurl, error, thisSite, _jobrec, _experiment, forceDownload=False):
    """ handle the queuedata download and post-processing """

    tolog("Processing queuedata")

    # get the site information object
    si = getSiteInformation(_experiment)

    # get the experiment object
    thisExperiment = getExperiment(_experiment) 

    # (re-)download the queuedata
    ec, hasQueuedata = si.getQueuedata(_queuename, forceDownload=forceDownload)
    if ec != 0:
        return ec, thisSite, _jobrec, hasQueuedata

    if hasQueuedata:
        # update queuedata and thisSite if necessary
        ec, _thisSite, _jobrec = si.postProcessQueuedata(_queuename, _pshttpurl, thisSite, _jobrec, force_devpilot)
        if ec != 0:
            return error.ERR_GENERALERROR, thisSite, _jobrec, hasQueuedata
        else:
            thisSite = _thisSite

        # should the server or the pilot do the LFC registration?
        global lfcRegistration
        if readpar("lfcregister") == "server":
            lfcRegistration = False
            tolog("File registration will be done by server")

            # special check for storm sites
            _copytool = readpar('copytool')
            _copytoolin = readpar('copytoolin')
            if _copytool == "storm" and _copytoolin == "":
                _copytool = "lcgcp2"
                _copytoolin = "storm"
                tolog("!!WARNING!!1112!! Found schedconfig misconfiguration: Forcing queuedata update for storm site: copytool=%s, copytoolin=%s" % (_copytool, _copytoolin))
                ec = si.replaceQueuedataField("copytool", _copytool)
                ec = si.replaceQueuedataField("copytoolin", _copytoolin)
        else:
            # since lfcregister is not set, make sure that copytool is not set to lcgcp2
            if readpar("copytool") == "lcgcp2" or readpar("copytool") == "lcg-cp2" and readpar('region') != 'US':
                tolog("!!FAILED!!1111!! Found schedconfig misconfiguration: Site cannot use copytool=lcgcp2 without lfcregister=server")
                return error.ERR_GENERALERROR, thisSite, _jobrec, hasQueuedata

            tolog("LFC registration will be done by pilot")

        # should the number of stage-in/out retries be updated?
        global stageinretry, stageoutretry
        stageinretry = getStagingRetry("stage-in")
        stageoutretry = getStagingRetry("stage-out")

    # does the application directory exist?
    ec = thisExperiment.verifySwbase(readpar('appdir'))
    if ec != 0:
        return ec, thisSite, _jobrec, hasQueuedata

    # update experiment for Nordugrid
    global experiment
    if readpar('region').lower() == "nordugrid":
        experiment = "Nordugrid-ATLAS"

    # reset site.appdir
    thisSite.appdir = readpar('appdir')

    return ec, thisSite, _jobrec, hasQueuedata

def getStagingRetry(staging):
    """ Return a proper stage-in/out retry option """

    if staging == "stage-in":
        _STAGINGRETRY = readpar("stageinretry")
        _stagingTries = stageinretry # default value (2)
    else:
        _STAGINGRETRY = readpar("stageoutretry")
        _stagingTries = stageoutretry # default value (2)

    if _STAGINGRETRY != "":
        try:
            _stagingTries = int(_STAGINGRETRY)
        except Exception, e:
            tolog("!!WARNING!!1113!! Problematic %s retry number: %s, %s" % (staging, _STAGINGRETRY, e))
        else:
            stagingTries = _stagingTries
            tolog("Updated %s retry number to %d" % (staging, stagingTries))
    else:
        stagingTries = _stagingTries
        tolog("Updated %s retry number to %d" % (staging, stagingTries))

    return stagingTries

def setUpdateFrequencies():
    """ Set the update frequency of user workdir etc checks """

    update_freq_proc = 5*60              # Update frequency, process checks [s], 5 minutes
    update_freq_space = 10*60            # Update frequency, space checks [s], 10 minutes

    if os.environ.has_key('NON_LOCAL_ATLAS_SCRATCH'):
        if os.environ['NON_LOCAL_ATLAS_SCRATCH'].lower() == "true":
            if os.environ.has_key('NON_LOCAL_ATLAS_SCRATCH_SPACE'):
                try:
                    space_n = int(os.environ['NON_LOCAL_ATLAS_SCRATCH_SPACE'])
                except Exception, e:
                    tolog("!!WARNING!!1234!! Exception caught: %s" % (e))
                else:
                    if space_n > 0 and space_n < 10:
                        update_freq_space = 10*60*space_n
                    else:
                        tolog("!!WARNING!!1234!! NON_LOCAL_ATLAS_SCRATCH_SPACE is out or range: %d (0 < n < 10)" % (space_n))
            else:
                update_freq_space = 30*60

    tolog("Update frequencies:")
    tolog("...Processes: %d s" % (update_freq_proc))
    tolog(".......Space: %d s" % (update_freq_space))
    tolog("......Server: %d s" % (update_freq_server))

    return update_freq_proc, update_freq_space

def checkProcesses(pid):
    """ Check the number of running processes """

    kids = []
    n = 0
    try:
        findProcessesInGroup(kids, pid)
    except Exception, e:
        tolog("!!WARNING!!2888!! Caught exception in findProcessesInGroup: %s" % (e))
    else:
        n = len(kids)
        tolog("Number of running processes: %d" % (n))
    return n

def getMaxtime():
    """ Get the maximum time this pilot is allowed to run """

    _maxtime = readpar('maxtime')
    if not _maxtime or _maxtime == "0":
        maxtime = 999999
    else:
        try:
            maxtime = int(_maxtime)
        except:
            maxtime = 999999

    return maxtime

def failMaxTimeJob(error):
    """ Reached maximum batch system time limit, fail the job """

    tolog("!!WARNING!!1999!! The pilot has decided to kill the job since there is less than 10 minutes of the allowed batch system running time")
    pilotErrorDiag = "Reached maximum batch system time limit"
    tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))

    global jobDic

    # after multitasking was removed from the pilot, there is actually only one job
    for k in jobDic.keys():
        # kill the job
        killProcesses(jobDic[k][0])
        jobDic[k][1].result[0] = "failed"
        jobDic[k][1].currentState = jobDic[k][1].result[0]
        jobDic[k][1].result[2] = error.ERR_REACHEDMAXTIME
        jobDic[k][1].pilotErrorDiag = pilotErrorDiag

# main process starts here
def runMain(runpars):

    # keep track of when the pilot was started
    pilot_startup = int(time.time())

    # get error handler
    error = PilotErrors()

    # protect the bulk of the pilot code with exception handling
    isJobDownloaded = False
    isServerUpdated = False
    try:
        # dump some pilot info, version id, etc to stdout
        dumpPilotInfo(tofile=False)

        global jobIds, stagein, stageout, stageoutStartTime

        # read the pilot token
        global pilotToken
        pilotToken = getPilotToken(tofile=False)

        # create the watch dog
        wdog = watchDog()

        # extend PYTHONPATH to include the local workdir path
        sys.path.insert(1, pilot_initdir)

        # add the current dir to the path to make sure pilot modules can be found
        sys.path.append(os.path.abspath(os.curdir))

        # parse the pilot argument list (e.g. queuename is updated)
        args = argParser(runpars)

        # fill in the site information by parsing the argument list
        thisSite = Site.Site()
        thisSite.setSiteInfo(args)

        # verify inputDir and outputDir
        _inputDir, _outputDir = getInOutDirs()

        global jobrec # define globality here to avoid warning message in stderr
        ec, thisSite, jobrec, hasQueuedata = handleQueuedata(queuename, pshttpurl, error, thisSite, jobrec, experiment, forceDownload=False)
        if ec != 0:
            return ec

        # the maximum time this pilot is allowed to run
        maxtime = getMaxtime()

        # get the experiment object
        thisExperiment = getExperiment(experiment)
        if thisExperiment:
            tolog("Pilot will serve experiment: %s" % (thisExperiment.getExperiment()))
        else:
            tolog("!!FAILED!!1234!! Did not get an experiment object from the factory")
            return error.ERR_GENERALERROR

        # perform special checks for given experiment
        if not thisExperiment.specialChecks():
            return error.ERR_GENERALERROR

        global errorLabel
        if not jobrec:
            errorLabel = "FAILED"

        # set node info
        workerNode = Node.Node()
        workerNode.setNodeName(getProperNodeName(os.uname()[1]))

        # collect WN info .........................................................................................
        # do not include the basename in the path since it has not been created yet
        # i.e. remove Panda_Pilot* from the workdir path
        # tolog("Collecting WN info from: %s" % (os.path.dirname(thisSite.workdir)))
        # workerNode.collectWNInfo(os.path.dirname(thisSite.workdir))

        # overwrite mem since this should come from either pilot argument or queuedata
        # workerNode.mem = getWNMem(hasQueuedata)

        # update the globals used in the exception handler
        globalSite = thisSite
        globalWorkNode = workerNode

        # create the initial pilot workdir
        ec = createSiteWorkDir(thisSite.workdir, error)
        if ec != 0:
            return ec

        # register cleanup function
        atexit.register(cleanup, wdog, pilot_initdir, wrapperFlag)

        # check special environment variables
        ec = checkSpecialEnvVars(thisSite.sitename)
        if ec != 0:
            return ec

        def sig2exc(sig, frm):
            """ signal handler """

            error = PilotErrors()
            global jobDic, logTransferred, proxyguard

            errorText = "!!FAILED!!1999!! [pilot] Signal %s is caught in pilot parent process!" % str(sig)
            tolog(errorText)
            ec = error.ERR_KILLSIGNAL
            # send to stderr
            print >> sys.stderr, errorText

            # touch a KILLED file which will be seen by the multi-job loop, to prevent further jobs from being started
            createLockFile(False, globalSite.workdir, "KILLED")

#            # restore the hidden proxy if necessary
#            try:
#                restoreProxy()
#            except Exception, e:
#                tolog("restoreProxy() failed: %s" % str(e))

            # here add the kill function to kill all the real jobs processes
            for k in jobDic.keys():
                tmp = jobDic[k][1].result[0]
                if tmp != "finished" and tmp != "failed" and tmp != "holding":
                    if sig == signal.SIGTERM:
                        ec = error.ERR_SIGTERM
                    elif sig == signal.SIGQUIT:
                        ec = error.ERR_SIGQUIT
                    elif sig == signal.SIGSEGV:
                        ec = error.ERR_SIGSEGV
                    elif sig == signal.SIGXCPU:
                        ec = error.ERR_SIGXCPU
                    elif sig == signal.SIGBUS:
                        ec = error.ERR_SIGBUS
                    elif sig == signal.SIGUSR1:
                        ec = error.ERR_SIGUSR1
                    else:
                        ec = error.ERR_KILLSIGNAL

                    jobDic[k][1].result[0] = "failed"
                    jobDic[k][1].currentState = jobDic[k][1].result[0]
                    # do not overwrite any previous error
                    if jobDic[k][1].result[2] == 0:
                        jobDic[k][1].result[2] = ec
                    if not logTransferred: 
                        jobDic[k][1].pilotErrorDiag = "Job killed by signal %s: Signal handler has set job result to FAILED, ec = %d" %\
                                                      (str(sig), ec)
                        logMsg = "!!FAILED!!1999!! %s\n%s" % (jobDic[k][1].pilotErrorDiag, version)
                        tolog(logMsg)
                        ret, retNode = updatePandaServer(jobDic[k][1], globalSite, globalWorkNode, psport, log=logMsg, schedulerID=jobSchedulerId, pilotID=pilotId, updateServer=updateServerFlag)
                        if ret == 0:
                            tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
                    else:
                        # log should have been transferred
                        jobDic[k][1].pilotErrorDiag = "Job killed by signal %s: Signal handler has set job result to FAILED, ec = %d" %\
                                                      (str(sig), ec)
                        logMsg = "!!FAILED!!1999!! %s\n%s" % (jobDic[k][1].pilotErrorDiag, version)
                        tolog(logMsg)
                        
                    killProcesses(jobDic[k][0])
                    # most of the time there is not enough time to build the log
                    # postJobTask(jobDic[k][1], globalSite, globalWorkNode, jr=False)
            pUtil.writeToFile(os.path.join(globalSite.workdir, "EXITCODE"), str(ec))
            raise SystemError(sig) # this one will trigger the cleanup function to be called
    
        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)
        signal.signal(signal.SIGUSR1, sig2exc)

        # perform job recovery ....................................................................................

        if jobrec:
            runJobRecovery(thisSite, workerNode, psport, readpar('wntmpdir'))
            if jobRecoveryMode:
                tolog("Pilot is in Job Recovery Mode, no payload will be downloaded, will now finish")
                return 0

        # perform disk cleanup ....................................................................................
        diskCleanup(thisSite.wntmpdir, uflag)

        # start the monitor and watchdog process
        from PilotTCPServer import PilotTCPServer
        monthread = PilotTCPServer(updateHandler)
        if not monthread.port:
            tolog("!!WARNING!!1234!! Failed to open TCP connection to localhost (worker node network problem), cannot continue")
            fastCleanup(thisSite.workdir)
            return error.ERR_NOTCPCONNECTION
        else:
            tolog("Pilot TCP server will use port: %d" % (monthread.port))
        monthread.start()

        # multi job loop will begin here...........................................................................

        # master job counter
        number_of_jobs = 0

        # multi-job variables
        from random import shuffle
        number_of_failed_jobs = 0
        maxFailedMultiJobs = 3
        multiJobTimeDelays = range(2, maxFailedMultiJobs+2) # [2,3,4]
        shuffle(multiJobTimeDelays)

        # set the update frequency for process monitoring and output file size and user workdir size checks
        update_freq_proc, update_freq_space = setUpdateFrequencies()

        # get the timefloor from the queuedata, the pilot is allowed to run multi-jobs within this limit
        # if set to zero, only one job will be executed
        timefloor = getTimeFloor(timefloor_default)

        # get the site information object
        si = getSiteInformation(experiment)
        if si:
            tolog("Using site information for experiment: %s" % (si.getExperiment()))
        else:
            tolog("!!FAILED!!1234!! Did not get an experiment object from the factory")
            return error.ERR_GENERALERROR

        # loop until pilot has run out of time (defined by timefloor)
        multijob_startup = int(time.time())
        hasMultiJob = False
        while True:
            # create the pilot workdir (if it was not created before, needed for the first job)
            if number_of_jobs > 0:
                # update the workdir (i.e. define a new workdir and create it)
                thisSite.workdir = thisSite.getWorkDir()
                ec = createSiteWorkDir(thisSite.workdir, error)
                if ec != 0:
                    return ec
                globalSite = thisSite

            # make sure we are in the current work dir
            chdir(thisSite.workdir)

# 59a is the same until this point

            # all output will be written to the pilot log as well as to stdout [with the tolog() function]
            pUtil.setPilotlogFilename("%s/pilotlog.txt" % (thisSite.workdir))

            # redirect stderr
            pUtil.setPilotstderrFilename("%s/pilot.stderr" % (thisSite.workdir))
            sys.stderr = open(pUtil.getPilotstderrFilename(), 'w')

            # dump some pilot info, version id, etc (to the log file this time)
            tolog("\n\nEntered multi-job loop. Current work dir: %s\n" % (os.getcwd()))
            dumpPilotInfo(tofile=True)
            dumpVars(thisSite)
            if timefloor != 0:
                tolog("Entering main pilot loop: multi job enabled (number of processed jobs: %d)" % (number_of_jobs))
                hasMultiJob = True
            else:
                tolog("Entering main pilot loop: multi job disabled")
                # do not reset hasMultiJob

            # local checks begin here..................................................................................

            # do we have a valid proxy?
            ec = verifyProxyValidity()
            if ec != 0:
                return ec

            # collect WN info again to avoid getting wrong disk info from gram dir which might differ from the payload workdir
            tolog("Collecting WN info from: %s (again)" % (thisSite.workdir))
            workerNode.collectWNInfo(thisSite.workdir)

            # overwrite mem since this should come from either pilot argument or queuedata
            # note: this function also sets the ulimit
            workerNode.mem = getsetWNMem()

            # update the globals used in the exception handler
            globalSite = thisSite
            globalWorkNode = workerNode

            # do we have enough local disk space to run the job?
            ec = checkLocalDiskSpace(workerNode.disk, error)
            if ec != 0:
                tolog("Pilot was executed on host: %s" % (workerNode.nodename))
                fastCleanup(thisSite.workdir)
                return ec

            # getJob begins here....................................................................................

            # make sure the pilot TCP server is still running
            tolog("Verifying that pilot TCP server is still alive...")
            if isPilotTCPServerAlive('localhost', monthread.port):
                tolog("...Pilot TCP server is still running")
            else:
                tolog("!!WARNING!!1231!! Pilot TCP server is down - aborting pilot")
                fastCleanup(thisSite.workdir)
                return error.ERR_NOPILOTTCPSERVER

            # prod job start time counter
            tp_0 = os.times()

            # reset stageout start time (used by looping job killer)
            stageoutStartTime = None
            stagein = False
            stageout = False

            # create the first job, usually a production job, but analysis job is ok as well
            # we just use the first job as a MARKER of the "walltime" of the pilot
            isJobDownloaded = False # (reset in case of multi-jobs)

            ec, job, number_of_jobs = getJob(thisSite, workerNode, number_of_jobs, error, uflag, si)
            if ec != 0:
                # remove the site workdir before exiting
                # pUtil.writeExitCode(thisSite.workdir, error.ERR_GENERALERROR)
                # raise SystemError(1111)
                fastCleanup(thisSite.workdir)
                if ec == -1: # reset temporary error code (see getJob)
                    ec = 0
                return ec
            else:
                isJobDownloaded = True
                tolog("Using job definition id: %s" % (job.jobDefinitionID))

            tp_1 = os.times()
            job.timeGetJob = int(round(tp_1[4] - tp_0[4]))

            # update the global used in the exception handler
            globalJob = job

            # update job id list
            jobIds.append(job.jobId)

            # does the application directory exist?
            ec, thisSite.appdir = si.extractAppdir(thisSite.appdir, job.processingType, job.homePackage)
            if ec != 0:
                job.result[0] = 'failed'
                job.currentState = job.result[0]
                job.result[2] = ec
                ret, retNode = updatePandaServer(job, thisSite, workerNode, psport, schedulerID=jobSchedulerId, pilotID=pilotId, updateServer=updateServerFlag)
                fastCleanup(thisSite.workdir)
                return ec

            # update the job state file
            JR = JobRecovery()
            job.jobState = "startup"
            _retjs = JR.updateJobStateTest(job, thisSite, workerNode, mode="test")
            if not _retjs:
                tolog("Could not update job state test file: %s" % str(_retjs))

            # getJob() ends here.....................................................................................

            # copy some supporting modules to the workdir for pilot job to run
            ec = pUtil.stageInPyModules(pilot_initdir, thisSite.workdir)
            if ec != 0:
                tolog("Pilot cannot continue since not all modules could be copied to work directory")
                fastCleanup(thisSite.workdir)
                return ec

            tolog("Current time :%s" % (pUtil.timeStamp()))
            tolog("The site this pilot runs on: %s" % (thisSite.sitename))
            tolog("Pilot executing on host: %s" % (workerNode.nodename))
            tolog("The workdir this pilot runs on:%s" % (thisSite.workdir))
            tolog("The dq2url for this site is: %s" % (thisSite.dq2url))
            tolog("The DQ2 SE has %d GB space left (NB: dCache is defaulted to 999999)" % (thisSite.dq2space))
            tolog("New job has prodSourceLabel: %s" % (job.prodSourceLabel))
            # # create the backup proxyguard object used to restore the hidden proxy in case of
            # # looping jobs or runJob failures
            # if 'DDM' not in thisSite.sitename and readpar('proxy') != 'donothide':
            #     global proxyguard
            #     proxyguard = ProxyGuard()
            #     if not proxyguard.setProxy():
            #         tolog("Pilot failed to backup the proxy")
            #     else:
            #         tolog("Pilot has backed up the proxy")
            # else:
            #     tolog("Pilot will not backup the proxy")

            # update the globals used in the exception handler
            globalSite = thisSite
            globalWorkNode = workerNode

            # does the looping job limit need to be updated?
            loopingLimit = getLoopingLimit(job.maxCpuCount, job.jobPars, thisSite.sitename)

            # figure out and set payload file names
            job.setPayloadName(thisExperiment.getPayloadName(job))

            # update the global used in the exception handler
            globalJob = job

            if job.prodUserID != "":
                tolog("Pilot executing job for user: %s" % (job.prodUserID))

            # update job status and id
            job.result[0] = "starting"
            job.currentState = job.result[0]
            os.environ['PandaID'] = str(job.jobId)

            # create job workdir
            ec, job = createJobWorkdir(job, thisSite, workerNode, sys.stderr, error)
            if ec != 0:
                globalSite = thisSite
                raise SystemError(1111)
            else:
                globalJob = job

            # create the job setup script (used to recreate the job locally if needed)
            createJobSetupScript(job.workdir)

            # create the initial file state dictionary
            createFileStates(thisSite.workdir, job.jobId, outFiles=job.outFiles, logFile=job.logFile, type="output")
            dumpFileStates(thisSite.workdir, job.jobId, type="output")
            if job.inFiles != ['']:
                createFileStates(thisSite.workdir, job.jobId, inFiles=job.inFiles, type="input")
                dumpFileStates(thisSite.workdir, job.jobId, type="input")

            # are the output files within the allowed limit?
            # (keep the LFN verification at this point since the wrkdir is now created, needed for creating the log in case of failure)
            ec, job.pilotErrorDiag = verifyLFNLength(job.outFiles)
            if ec != 0:
                tolog("Updating PanDA server for the failed job (error code %d)" % (ec))
                job.result[0] = 'failed'
                job.currentState = job.result[0]
                job.result[2] = ec
                postJobTask(job, thisSite, workerNode, jr=False)
                fastCleanup(thisSite.workdir)
                return ec
            else:
                tolog("LFN length(s) verified, within allowed limit")

            # print job info
            job.displayJob()
            jobDic["prod"] = [None, job, None] # pid, job, os.getpgrp()

            # send space report now, at the beginning of the job
            if loggingMode == None:
                ret, retNode = updatePandaServer(jobDic["prod"][1], thisSite, workerNode, psport, spaceReport=True, schedulerID=jobSchedulerId, pilotID=pilotId, updateServer=updateServerFlag)
            else:
                ret, retNode = updatePandaServer(jobDic["prod"][1], thisSite, workerNode, psport, spaceReport=False, schedulerID=jobSchedulerId, pilotID=pilotId, updateServer=updateServerFlag)
            if ret == 0:
                tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
                isServerUpdated = True
            else:
                tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
                isServerUpdated = False

            # short delay requested by the server
            throttleJob()

            # maximum number of found processes
            maxNProc = 0

            # fork into two processes, one for the pilot main control loop, and one for runJob
            pid_1 = os.fork()
            if pid_1: # parent process
                # store the process id in case cleanup need to kill lingering processes
                pUtil.writeToFile(os.path.join(thisSite.workdir, "PROCESSID"), str(pid_1))
                jobDic["prod"][0] = pid_1
                jobDic["prod"][2] = os.getpgrp()
                jobDic["prod"][1].result[0] = "running"
                # do not set jobDic["prod"][1].currentState = "running" here (the state is at this point only needed for the server)
                ret, retNode = updatePandaServer(jobDic["prod"][1], thisSite, workerNode, psport, schedulerID=jobSchedulerId, pilotID=pilotId, updateServer=updateServerFlag)
                if ret == 0:
                    tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
                else:
                    tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
            else: # child job
                tolog("Starting child process in dir: %s" % jobDic["prod"][1].workdir)
                # add site specific python path here??
                jobargs = [pyexe, "runJob.py", 
                           "-a", thisSite.appdir,
                           "-d", jobDic["prod"][1].workdir,
                           "-l", pilot_initdir,
                           "-q", thisSite.dq2url,
                           "-p", str(monthread.port),
                           "-s", thisSite.sitename,
                           "-o", thisSite.workdir,
                           "-h", queuename,
                           "-i", jobDic["prod"][1].tarFileGuid,
                           "-b", str(debugLevel),
                           "-t", str(proxycheckFlag),
                           "-k", pUtil.getPilotlogFilename(),
                           "-x", str(stageinretry),
                           "-v", str(testLevel),
                           "-g", _inputDir,
                           "-m", _outputDir,
                           "-B", str(lfcRegistration),
                           "-E", str(stageoutretry),
                           "-F", experiment]
                tolog("Launching runJob with options: %s" % str(jobargs))
                if debugLevel == 2:
                    debugInfo("Job arguments: %s" % str(jobargs))

                # copy all python files to workdir
                pUtil.stageInPyModules(thisSite.workdir, jobDic["prod"][1].workdir)

                # backup job definition
                backupJobDef(number_of_jobs)

                # start runJob process
                chdir(jobDic["prod"][1].workdir)
                sys.path.insert(1,".")
                os.execvpe(pyexe, jobargs, os.environ)

            # Control variables for looping jobs
            LastTimeFilesWereModified = {}
            for k in jobDic.keys(): # loop over production and possible analysis job
                LastTimeFilesWereModified[k] = int(time.time())

            # main monitoring loop
            iteration = 1
            curtime = int(time.time())
            curtime_sp = curtime
            curtime_of = curtime
            curtime_proc = curtime
            create_softlink = True
            while True:
                tolog("--- Main pilot monitoring loop (job id %s, state:%s, iteration %d)" % (job.jobId, job.currentState, iteration))
                # check frequency
                time.sleep(60)

                # create a soft link in the site workdir to the payload stdout
                if create_softlink:
                    try:
                        createSoftLink(thisSite, create_softlink=create_softlink)
                    except Exception, e:
                        tolog("!!WARNING!!1999!! Caught an exception during soft link creation: %s" % str(e))
                        create_softlink = False

                # every ten minutes, check the remaining disk space and size of user workDir (for analysis jobs)
                # and the size of the payload stdout file
                skip = False
                if (int(time.time()) - curtime_sp) > update_freq_space:
                    # check the size of the payload stdout
                    skip = checkPayloadStdout(error)

                    # update the worker node info (i.e. get the remaining disk space)
                    workerNode.collectWNInfo(thisSite.workdir)
                    skip = checkLocalSpace(workerNode.disk, error, skip)

                    # check the size of the workdir for user jobs
                    if uflag:
                        skip = checkWorkDir(error, skip)

                    # update the time for checking disk space
                    curtime_sp = int(time.time())

                # monitor the number of running processes and the pilot running time
                if (int(time.time()) - curtime_proc) > update_freq_proc:
                    # check the number of running processes
                    nProc = checkProcesses(jobDic["prod"][0])
                    if nProc > maxNProc:
                        maxNProc = nProc

                    # monitor the pilot running time (once every five minutes = update_freq_proc)
                    time_passed_since_pilot_startup = int(time.time()) - pilot_startup
                    tolog("Time passed since pilot startup = %d s (maximum allowed batch system time = %d s)" % (time_passed_since_pilot_startup, maxtime))
                    if (maxtime - time_passed_since_pilot_startup) < 10*60 and not stageout:
                        # reached maximum batch system time limit
                        failMaxTimeJob(error)
                        skip = True

                    # update the time for checking processes
                    curtime_proc = int(time.time())

                # verify output file sizes every ten minutes and check the number of running processes
                if (int(time.time()) - curtime_of) > update_freq_space:
                    # check the output file sizes
                    skip = checkOutputFileSizes(error, skip)

                    # update the time for checking output file sizes
                    curtime_of = int(time.time())

                # every 30 minutes, look for looping jobs
                if (int(time.time()) - curtime) > update_freq_server and not skip: # 30 minutes
                    # check when the workdir files were last updated or that the stageout command is not hanging
                    LastTimeFilesWereModified = loopingJobKiller(LastTimeFilesWereModified, loopingLimit)

                    # make final server update for all ended jobs
                    updateJobs(thisSite, workerNode, error)

                    # update the time for checking looping jobs
                    curtime = int(time.time())

                # check if any jobs are done by scanning the process list
                # some jobs might have sent updates to the monitor thread about their final states at other times
                wdog.pollChildren()

                # clean up the ended jobs (if there are any)
                cleanUpEndedJobs(thisSite, workerNode, error)

                # collect all the zombie processes
                wdog.collectZombieJob(tn=10)

                # is there still a job in the jobDic?
                if len(jobDic) == 0: # no child jobs in jobDic
                    tolog("The job has finished")
                    break
                else:
                    iteration += 1

            # do not bother with saving the log file if it has already been transferred and registered
            try:
                state = getFileState(job.logFile, thisSite.workdir, job.jobId, type="output")
                tolog("Current log file state: %s" % str(state))
                if os.path.exists(os.path.join(thisSite.workdir, job.logFile)) and state[0] == "transferred" and state[1] == "registered":
                    tolog("Safe to remove the log file")
                    ec = pUtil.removeFiles(thisSite.workdir, [job.logFile])
                else:
                    tolog("Will not remove log file at this point (possibly already removed)")
            except Exception, e:
                tolog("!!WARNING!!1111!! %s" % (e))

            tolog("--------------------------------------------------------------------------------")
            tolog("Number of processed jobs              : %d" % (number_of_jobs))
            tolog("Maximum number of monitored processes : %d" % (maxNProc))
            tolog("Pilot executed last job in directory  : %s" % (thisSite.workdir))
            tolog("Current time                          : %s" % (pUtil.timeStamp()))
            tolog("--------------------------------------------------------------------------------")

            # a bit more cleanup
            wdog.collectZombieJob()

            # call the cleanup function (only needed for multi-jobs)
            if hasMultiJob:
                cleanup(wdog, pilot_initdir, True)

            # is there still time to run another job?
            if os.path.exists(os.path.join(globalSite.workdir, "KILLED")):
                tolog("Aborting multi-job loop since a KILLED file was found")
                break
            elif timefloor == 0:
                tolog("No time floor set, no time to run another job")
                break
            else:
                time_since_multijob_startup = int(time.time()) - multijob_startup
                tolog("Time since multi-job startup: %d s" % (time_since_multijob_startup))
                if timefloor > time_since_multijob_startup:

                    # do not run too many failed multi-jobs, abort if necessary
                    if job.result[2] != 0:
                        number_of_failed_jobs += 1
                    if number_of_failed_jobs >= maxFailedMultiJobs:
                        tolog("Passed max number of failed multi-jobs (%d), aborting multi-job mode" % (maxFailedMultiJobs))
                        break

                    tolog("Since time floor is set to %d s, there is time to run another job" % (timefloor))

                    # need to re-download the queuedata since the previous job might have modified it
                    ec, thisSite, jobrec, hasQueuedata = handleQueuedata(queuename, pshttpurl, error, thisSite, jobrec, experiment, forceDownload=True)
                    if ec != 0:
                        break

                    # do not continue immediately if the previous job failed due to an SE problem
                    if job.result[2] != 0:
                        _delay = 60*multiJobTimeDelays[number_of_failed_jobs - 1]
                        if error.isGetErrorCode(job.result[2]):
                            tolog("Taking a nap for %d s since the previous job failed during stage-in" % (_delay))
                            time.sleep(_delay)
                        elif error.isPutErrorCode(job.result[2]):
                            tolog("Taking a nap for %d s since the previous job failed during stage-out" % (_delay))
                            time.sleep(_delay)
                        else:
                            tolog("Will not take a nap since previous job failed with a non stage-in/out error")

                    continue
                else:
                    tolog("Since time floor is set to %d s, there is no time to run another job" % (timefloor))
                    break

            # flush buffers
            sys.stdout.flush()
            sys.stderr.flush()

            # multi-job loop ends here ..............................................................................

        tolog("No more jobs to execute")

        # wait for the stdout to catch up (otherwise the full log is cut off in the batch stdout dump)
        time.sleep(10)
        tolog("End of the pilot")

        # flush buffers
        sys.stdout.flush()
        sys.stderr.flush()

    # catch any uncaught pilot exceptions
    except Exception, errorMsg:

        error = PilotErrors()
        # can globalJob be added here?
        global logTransferred

        if len(str(errorMsg)) == 0:
            errorMsg = "(empty error string)"
                                                                
        import traceback
        if 'format_exc' in traceback.__all__:
            pilotErrorDiag = "Exception caught in pilot: %s, %s" % (str(errorMsg), traceback.format_exc())
        else:
            tolog("traceback.format_exc() not available in this python version")
            pilotErrorDiag = "Exception caught in pilot: %s" % (str(errorMsg))
        tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))

        cmd = "ls -lF %s" % (pilot_initdir)
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog(out)

        if globalSite:
            if os.path.exists(globalSite.workdir):
                cmd = "ls -lF %s" % (globalSite.workdir)
                tolog("Executing command: %s" % (cmd))
                out = commands.getoutput(cmd)
                tolog(out)
            else:
                tolog("Directory %s does not exist" % (globalSite.workdir))

        if isJobDownloaded:
            if isServerUpdated:
                tolog("Do a full cleanup since job was downloaded and server updated")

                # was the process id added to jobDic?
                bPID = False
                try:
                    for k in jobDic.keys():
                        tolog("Found process id in jobDic: %d" % (jobDic[k][0]))
                except:
                    tolog("Process id not added to jobDic")
                else:
                    bPID = True

                if bPID:
                    tolog("Cleanup using jobDic")
                    for k in jobDic.keys():
                        if os.path.exists(jobDic[k][1].workdir):
                            cmd = "ls -lF %s" % (jobDic[k][1].workdir)
                            tolog("Executing command: %s" % (cmd))
                            out = commands.getoutput(cmd)
                            tolog(out)
                        else:
                            tolog("Directory %s does not exist" % (jobDic[k][1].workdir))

                        jobDic[k][1].result[0] = "failed"
                        jobDic[k][1].currentState = jobDic[k][1].result[0]
                        if jobDic[k][1].result[2] == 0:
                            jobDic[k][1].result[2] = error.ERR_PILOTEXC
                        if jobDic[k][1].pilotErrorDiag == "":
                            jobDic[k][1].pilotErrorDiag = pilotErrorDiag
                        if globalSite:
                            postJobTask(jobDic[k][1], globalSite, globalWorkNode, jr=False)
                            logTransferred = True
                        tolog("Killing process: %d" % (jobDic[k][0]))
                        killProcesses(jobDic[k][0])
                        # move this job from jobDic to zombieJobList for later collection
                        zombieJobList.append(jobDic[k][0]) # only needs pid of this job for cleanup
                        del jobDic[k]

                    # collect all the zombie processes
                    wdog.collectZombieJob(tn=10)
                else:
                    tolog("Cleanup using globalJob")
                    globalJob.result[0] = "failed"
                    globalJob.currentState = globalJob.result[0]
                    globalJob.result[2] = error.ERR_PILOTEXC
                    globalJob.pilotErrorDiag = pilotErrorDiag
                    if globalSite:
                        postJobTask(globalJob, globalSite, globalWorkNode, jr=False)
            else:
                if globalSite:
                    tolog("Do a fast cleanup since server was not updated after job was downloaded (no log)")
                    fastCleanup(globalSite.workdir)
        else:
            if globalSite:
                tolog("Do a fast cleanup since job was not downloaded (no log)")
                fastCleanup(globalSite.workdir)
        return error.ERR_PILOTEXC

    # end of the pilot
    else:
        return 0

# main
if __name__ == "__main__":
    runMain(sys.argv[1:])
