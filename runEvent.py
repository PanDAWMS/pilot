# runEvent module for receiving and processing events from the Event Server
#
# For stand-alone testing:
# queuedata.json must exist. Download instructions: (e.g.)
# > curl -sS "http://pandaserver.cern.ch:25085/cache/schedconfig/BNL_PROD_MCORE-condor.all.json" >queuedata.json
# where "BNL_PROD_MCORE-condor" is the PanDA queue name
#
# Execute runEvent module like this:
# > python runEvent.py ..

import os
import sys
import time
import stat
import getopt
import atexit
import signal
import commands
import traceback
from json import loads
from shutil import copy2
from subprocess import Popen
from xml.dom import minidom
    
import Job
import Node
import Site
import pUtil
import RunJobUtilities
import Mover as mover
from JobRecovery import JobRecovery
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from StoppableThread import StoppableThread
from pUtil import debugInfo, tolog, isAnalysisJob, readpar, createLockFile, getDatasetDict, getAtlasRelease, getChecksumCommand,\
     tailPilotErrorDiag, getFileAccessInfo, getCmtconfig, getExtension, getExperiment, getEventService, httpConnect,\
     getSiteInformation, getGUID


try:
    from PilotYamplServer import PilotYamplServer
except Exception, e:
    PilotYamplServer = None
    print "runJob caught exception:",e

# global variables
pilotserver = "localhost"          # default server
pilotport = 88888                  # default port
failureCode = None                 # set by signal handler when user/batch system kills the job
pworkdir = "/tmp"                  # site work dir used by the parent
logguid = None                     # guid for the log file
debugLevel = 0                     # 0: debug info off, 1: display function name when called, 2: full debug info
pilotlogfilename = "pilotlog.txt"  # default pilotlog filename 
stageinretry = None                # number of stage-in tries
stageoutretry = None               # number of stage-out tries
pilot_initdir = ""                 # location of where the pilot is untarred and started
proxycheckFlag = True              # True (default): perform proxy validity checks, False: no check
globalPilotErrorDiag = ""          # global pilotErrorDiag used with signal handler (only)
globalErrorCode = 0                # global error code used with signal handler (only)
inputDir = ""                      # location of input files (source for mv site mover)
outputDir = ""                     # location of output files (destination for mv site mover)
lfcRegistration = True             # should the pilot perform LFC registration?
experiment = "ATLAS"               # Current experiment (can be set with pilot option -F <experiment>)
event_loop_running = False         #
output_file_dictionary = {}        # OLD REMOVE
output_files = []                  # A list of all files that have been successfully staged-out, used by createFileMetadata()
guid_list = []                     # Keep track of downloaded GUIDs
lfn_list = []                      # Keep track of downloaded LFNs
eventRange_dictionary = {}         # eventRange_dictionary[event_range_id] = [path, cpu, wall]
eventRangeID_dictionary = {}       # eventRangeID_dictionary[event_range_id] = True (corr. output file has been transferred)
stageout_queue = []                #
_pfc_path = ""                     # The path to the pool file catalog
yampl_server = None                #
yampl_thread = None                #
athenamp_is_ready = False          #
asyncOutputStager_thread = None    #
analysisJob = False                # True for analysis job
jobId = ""                         # PanDA job id
globalJob = None                   # Global job object, needed by the stage-out thread
jobSite = None                     # Global site object
output_area = "" # REMOVE LATER

def usage():
    """
    usage: python runEvent.py -s <sitename> -d <workdir> -a <appdir> -l <pilotinitdir> -w <url> -p <port> -q <dq2url> -g <inputdir> -m <outputdir> -o <pworkdir> -i <guid> -b <debuglevel> -k <pilotlogfilename> -x <stageintries> -t <proxycheckflag> -B <lfcRegistration> -E <stageouttries -F <experiment> -v <reserved>
    where:
               <sitename> is the name of the site that this job is landed,like BNL_ATLAS_1
               <workdir> is the pathname to the work directory of this job on the site
               <appdir> is the pathname to the directory of the executables
               <pilotinitdir> is the path to where to pilot code is untarred and started
               <url> is the URL of the pilot TCP server that the job should send updates to
               <port> is the port on which the local pilot TCP server listens on
               <dq2url> is the URL of the https web server for the local site's DQ2 siteservice
               <pworkdir> is the pathname to the work directory of the parent
               <guid> guid for the log file
               <debuglevel> 0: debug info off, 1: display function name when called, 2: full debug info
               <pilotlogfilename> name of log file
               <stageintries> number of tries for stage-in (default is 2)
               <stageouttries> number of tries for stage-out (default is 2)
               <proxycheckflag> True (default): perform proxy validity checks, False: no check
               <outputDir> location of output files (destination for mv site mover)
               <inputdir> location of input files (source for mv site mover)
               <lfcRegistration> True[False]: pilot will [not] perform LFC registration (default: True)
               <experiment> Current experiment (default: ATLAS)
               <reserved> [Add new argument if necessary]
    """
    print usage.__doc__

def argParser(argv):
    """ parse command line arguments for the main script """
    global pilotserver, pilotport, uflag, pworkdir, logguid, debugLevel, pilotlogfilename, stageinretry,\
           stageoutretry, pilot_initdir, proxycheckFlag, inputDir, outputDir, lfcRegistration, experiment

    # some default values
    sitename = "testsite"
    queuename = ""
    workdir = "/tmp"
    pworkdir = "/tmp"
    appdir = "/usatlas/projects/OSG"
    dq2url = ""
    
    try:
        opts, args = getopt.getopt(argv, 'a:b:c:d:g:h:i:k:l:m:o:p:q:s:t:v:w:x:B:E:F:')
    except getopt.GetoptError:
        tolog("!!FAILED!!3000!! Invalid arguments and options!")
        usage()
        os._exit(5)

    for o, a in opts:
        if o == "-a": appdir = str(a)
        elif o == "-b": debugLevel = int(a)
        elif o == "-d": workdir = str(a)
        elif o == "-g": inputDir = str(a)
        elif o == "-h": queuename = a
        elif o == "-i": logguid = str(a)
        elif o == "-k": pilotlogfilename = str(a)
        elif o == "-l": pilot_initdir = a
        elif o == "-m": outputDir = str(a)
        elif o == "-o": pworkdir = str(a)
        elif o == "-p": pilotport = int(a)
        elif o == "-q": dq2url = str(a)
        elif o == "-s": sitename = str(a)
        elif o == "-t":
            pcFlag = str(a)
            if pcFlag.upper() == "TRUE":
                proxycheckFlag = True
            else:
                proxycheckFlag = False
        elif o == "-v": dummy = str(a)
        elif o == "-w": pilotserver = str(a)
        elif o == "-x": stageinretry = int(a)
        elif o == "-B":
            if a.upper() == "FALSE":
                lfcRegistration = False
            else:
                lfcRegistration = True
        elif o == "-E": stageoutretry = int(a)
        elif o == "-F": experiment = a
        else:
            tolog("!!FAILED!!3000!! Unknown option: %s (ignoring)" % (o))
            usage()

    # use sitename as queuename if queuename == ""
    if queuename == "":
        queuename = sitename

    if debugLevel > 0:
        debugInfo("Debug level set to %d in argParser()" % debugLevel)
    if debugLevel == 2:
        debugInfo("sitename: %s" % sitename)
        debugInfo("workdir: %s" % workdir)
        debugInfo("pworkdir: %s" % pworkdir)
        debugInfo("appdir: %s" % appdir)
        debugInfo("pilot_initdir: %s" % pilot_initdir)
        debugInfo("pilotserver: %s" % pilotserver)
        debugInfo("pilotport: %d" % pilotport)
        debugInfo("dq2url: %s" % dq2url)
        debugInfo("logguid: %s" % logguid)

    return sitename, appdir, workdir, dq2url, queuename

def cleanup(job, rf=None):
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
            copy2("%s/metadata-%d.xml" % (job.workdir, job.jobId), "%s/metadata-%d.xml" % (pworkdir, job.jobId))
        except Exception, e:
            tolog("Warning: Could not copy metadata-%d.xml to site work dir - ddm Adder problems will occure in case of job recovery" % \
                  (job.jobId))

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

def sysExit(job, rf=None):
    '''
    wrapper around sys.exit
    rs is the return string from Mover::put containing a list of files that were not transferred
    '''

    cleanup(job, rf=rf)
    sys.stderr.close()
    tolog("runEvent (payload wrapper) has finished")
    # change to sys.exit?
    os._exit(job.result[2]) # pilotExitCode, don't confuse this with the overall pilot exit code,
                            # which doesn't get reported back to panda server anyway

def failJob(transExitCode, pilotExitCode, job, pilotserver, pilotport, ins=None, pilotErrorDiag=None, docleanup=True):
    """ set the fail code and exit """

    job.setState(["failed", transExitCode, pilotExitCode])
    if pilotErrorDiag:
        job.pilotErrorDiag = pilotErrorDiag
    tolog("Will now update local pilot TCP server")
    rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport, final=True)
    if ins:
        ec = pUtil.removeFiles(job.workdir, ins)
    if docleanup:
        sysExit(job)

def setup(job, jobSite, thisExperiment):
    """ prepare the setup and get the run command list """

    # start setup time counter
    t0 = time.time()

    ec = 0
    runCommandList = []

    # split up the job parameters to be able to loop over the tasks
    jobParameterList = job.jobPars.split("\n")
    jobHomePackageList = job.homePackage.split("\n")
    jobTrfList = job.trf.split("\n")
    jobAtlasRelease = getAtlasRelease(job.release)

    tolog("Number of transformations to process: %s" % len(jobParameterList))
    if len(jobParameterList) > 1:
        multi_trf = True
    else:
        multi_trf = False

    # verify that the multi-trf job is setup properly
    ec, job.pilotErrorDiag, jobAtlasRelease = RunJobUtilities.verifyMultiTrf(jobParameterList, jobHomePackageList, jobTrfList, jobAtlasRelease)
    if ec > 0:
        return ec, runCommandList, job, multi_trf
            
    os.chdir(jobSite.workdir)
    tolog("Current job workdir is %s" % os.getcwd())
    
    # setup the trf(s)
    _i = 0
    _stdout = job.stdout
    _stderr = job.stderr
    _first = True
    for (_jobPars, _homepackage, _trf, _swRelease) in map(None, jobParameterList, jobHomePackageList, jobTrfList, jobAtlasRelease):
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
        ec, job.pilotErrorDiag, cmd, job.spsetup, job.JEM, job.cmtconfig = thisExperiment.getJobExecutionCommand(job, jobSite, pilot_initdir)
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

def stageIn(job, jobSite, analysisJob, pilot_initdir, pworkdir):
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
            mover.get_data(job, jobSite, ins, stageinretry, analysisJob=analysisJob, usect=useCT,\
                           pinitdir=pilot_initdir, proxycheck=False, inputDir=inputDir, workDir=pworkdir)
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

def getTrfExitInfo(exitCode, workdir):
    """ Get the trf exit code and info from job report if possible """

    exitAcronym = ""
    exitMsg = ""

    # does the job report exist?
    extension = getExtension(alternative='pickle')
    if extension.lower() == "json":
        filename = os.path.join(workdir, "jobReport.%s" % (extension))
    else:
        filename = os.path.join(workdir, "jobReportExtract.%s" % (extension))

    # It might take a short while longer until the job report is created (unknown why)
    count = 1
    max_count = 10
    nap = 5
    found = False
    while count <= max_count:
        if os.path.exists(filename):
            tolog("Found job report: %s" % (filename))
            found = True
            break
        else:
            tolog("Waiting %d s for job report to arrive (#%d/%d)" % (nap, count, max_count))
            time.sleep(nap)
            count += 1

    if found:
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
            _exitCode = extractDictionaryObject("exitCode", data)
            if _exitCode:
                if _exitCode == 0 and exitCode != 0:
                    tolog("!!WARNING!!1111!! Detected inconsistency in %s: exitcode listed as 0 but original trf exit code was %d (using original error code)" %\
                          (filename, exitCode))
                else:
                    exitCode = _exitCode
            _exitAcronym = extractDictionaryObject("exitAcronym", data)
            if _exitAcronym:
                exitAcronym = _exitAcronym
            _exitMsg = extractDictionaryObject("exitMsg", data)
            if _exitMsg:
                exitMsg = _exitMsg

            f.close()

            tolog("Trf exited with:")
            tolog("...exitCode=%d" % (exitCode))
            tolog("...exitAcronym=%s" % (exitAcronym))
            tolog("...exitMsg=%s" % (exitMsg))

            # Ignore special trf error for now
            if exitCode == 65 and exitAcronym == "TRF_EXEC_FAIL":
                exitCode = 0
                exitAcronym = ""
                exitMsg = ""
                tolog("!!WARNING!!3333!! Reset TRF error codes..")
    else:
        tolog("Job report not found: %s" % (filename))

    return exitCode, exitAcronym, exitMsg

def extractDictionaryObject(object, dictionary):
    """ Extract an object from a dictionary """

    _obj = None

    try:
        _obj = dictionary[object]
    except Exception, e:
        tolog("Object %s not found in dictionary" % (object))
    else:
        tolog('Extracted \"%s\"=%s from dictionary' % (object, _obj))

    return _obj

def moveTrfMetadata(pworkdir):
    """ rename and copy the trf metadata """

    oldMDName = "%s/metadata.xml" % (globalJob.workdir)
    _filename = "metadata-%s.xml.PAYLOAD" % (globalJob.jobId)
    newMDName = "%s/%s" % (globalJob.workdir, _filename)
    try:
        os.rename(oldMDName, newMDName)
    except:
        tolog("Warning: Could not open the original %s file, but harmless, pass it" % (oldMDName))
        pass
    else:
        tolog("Renamed %s to %s" % (oldMDName, newMDName))
        # now move it to the pilot work dir
        try:
            copy2(newMDName, "%s/%s" % (pworkdir, _filename))
        except Exception, e:
            tolog("Warning: Could not copy %s to site work dir: %s" % (_filename, str(e)))
        else:
            tolog("Metadata was transferred to site work dir: %s/%s" % (pworkdir, _filename))

def getMetadataFilename(event_range_id):
    """ Return the metadata file name """

    return os.path.join(globalJob.workdir, "metadata-%s.xml" % (event_range_id))

def createFileMetadata2(outFiles, outsDict, dsname, datasetDict, sitename, analJob=False):
    """ create the metadata for the output + log files """

    ec = 0

    # Get the guids to the output files from the SURL dictionary
#    from SiteMover import SiteMover
#    sitemover = SiteMover()
#    surlDictionary = sitemover.getSURLDictionary(pworkdir, globalJob.jobId)
#    tolog("Got SURL dictionary = %s" % (surlDictionary))
    globalJob.outFilesGuids = [] #surlDictionary.keys()
    outFiles = []

    # get the file sizes and checksums for the local output files
    # WARNING: any errors are lost if occur in getOutputFileInfo()
    ec, pilotErrorDiag, fsize, checksum = pUtil.getOutputFileInfo(list(outFiles), getChecksumCommand(), skiplog=True, logFile=globalJob.logFile)
    if ec != 0:
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        failJob(globalJob.result[1], ec, globalJob, pilotErrorDiag=pilotErrorDiag)

    # Get the correct log guid (a new one is generated for the Job() object, but we need to get it from the -i logguid parameter)
    if logguid:
        guid = logguid
    else:
        guid = globalJob.tarFileGuid

    # Convert the output file list to LFNs
    files = []
#    for f in outFiles:
#        fn = os.path.basename(f)
#        files.append(fn)

    # Create preliminary metadata (no metadata yet about log file - added later in pilot.py)
    _fname = "%s/metadata-%d.xml" % (globalJob.workdir, globalJob.jobId)
    try:
        _status = pUtil.PFCxml(globalJob.experiment, _fname, files, fguids=globalJob.outFilesGuids, fntag="lfn", alog=globalJob.logFile, alogguid=guid,\
                               fsize=fsize, checksum=checksum, analJob=analJob)
    except Exception, e:
        pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
        tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag)) 
        failJob(globalJob.result[1], error.ERR_MISSINGGUID, globalJob, pilotErrorDiag=pilotErrorDiag)
    else:
        if not _status:
            pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            failJob(globalJob.result[1], error.ERR_MISSINGGUID, globalJob, pilotErrorDiag=pilotErrorDiag)

    tolog("NOTE: Output file info will not be sent to the server as part of xml metadata")
    tolog("..............................................................................................................")
    tolog("Created %s with:" % (_fname))
    tolog(".. log            : %s (to be transferred)" % (globalJob.logFile))
    tolog(".. log guid       : %s" % (guid))
    tolog(".. out files      : %s" % str(globalJob.outFiles))
    tolog(".. out file guids : %s" % str(globalJob.outFilesGuids))
    tolog(".. fsize          : %s" % str(fsize))
    tolog(".. checksum       : %s" % str(checksum))
    tolog("..............................................................................................................")

    # convert the preliminary metadata-<jobId>.xml file to OutputFiles-<jobId>.xml for NG and for CERNVM
    # note: for CERNVM this is only really needed when CoPilot is used
    if region == 'Nordugrid' or sitename == 'CERNVM':
        if RunJobUtilities.convertMetadata4NG(os.path.join(globalJob.workdir, globalJob.outputFilesXML), _fname, outsDict, dsname, datasetDict):
            tolog("Metadata has been converted to NG/CERNVM format")
        else:
            globalJob.pilotErrorDiag = "Could not convert metadata to NG/CERNVM format"
            tolog("!!WARNING!!1999!! %s" % (globalJob.pilotErrorDiag))

    # try to build a file size and checksum dictionary for the output files
    # outputFileInfo: {'a.dat': (fsize, checksum), ...}
    # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
    # checksum for file a.dat: outputFileInfo['a.dat'][1]
    try:
        # remove the log entries
        _fsize = fsize[1:]
        _checksum = checksum[1:]
        outputFileInfo = dict(zip(globalJob.outFiles, zip(_fsize, _checksum)))
    except Exception, e:
        tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
        outputFileInfo = {}
    else:
        tolog("Output file info dictionary created: %s" % str(outputFileInfo))

    return ec, outputFileInfo

def createFileMetadata(outputFile, event_range_id):
    """ Create the metadata for an output file """

    # This function will create a metadata file called metadata-<event_range_id>.xml using file info
    # from PoolFileCatalog.xml
    # Return: ec, pilotErrorDiag, outputFileInfo, fname
    #         where outputFileInfo: {'<full path>/filename.ext': (fsize, checksum, guid), ...} 
    #         (dictionary is used for stage-out)
    #         fname is the name of the metadata/XML file containing the file info above

    error = PilotErrors()

    ec = 0
    pilotErrorDiag = ""
    outputFileInfo = {}

    # Get/assign a guid to the output file
    guid = getGUID()
    if guid == "":
        ec = error.ERR_UUIDGEN
        pilotErrorDiag = "uuidgen failed to produce a guid"
        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        return ec, pilotErrorDiag, None

    guid_list = [guid]
    tolog("Generated GUID %s for file %s" % (outputFile, guid_list[0]))

#    ec, pilotErrorDiag, guid_list = RunJobUtilities.getOutFilesGuids([outputFile], job_workdir, TURL=True)
#    if ec:
#        # Missing PoolFileCatalog
#        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
#        return ec, pilotErrorDiag, None
#    else:
#        tolog("guid_list = %s" % str(guid_list))

    # Get the file size and checksum for the local output file
    # WARNING: any errors are lost if occur in getOutputFileInfo()
    ec, pilotErrorDiag, fsize_list, checksum_list = pUtil.getOutputFileInfo([outputFile], getChecksumCommand(), skiplog=True)
    if ec != 0:
        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        return ec, pilotErrorDiag, None
    else:
        tolog("fsize = %s" % str(fsize_list))
        tolog("checksum = %s" % str(checksum_list))

    # Create the metadata
    try:
        fname = getMetadataFilename(event_range_id) 
        tolog("Metadata filename = %s" % (fname))
    except Exception,e:
        tolog("!!WARNING!!2222!! Caught exception: %s" % (e))

    _status = pUtil.PFCxml(experiment, fname, fnlist=[outputFile], fguids=guid_list, fntag="pfn", fsize=fsize_list,\
                               checksum=checksum_list, analJob=analysisJob)
    if not _status:
        pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        return ec, pilotErrorDiag, None

    tolog("..............................................................................................................")
    tolog("Created %s with:" % (fname))
    tolog(".. output file      : %s" % (outputFile))
    tolog(".. output file guid : %s" % str(guid_list))
    tolog(".. fsize            : %s" % str(fsize_list))
    tolog(".. checksum         : %s" % str(checksum_list))
    tolog("..............................................................................................................")

    # Build a file size and checksum dictionary for the output file
    # outputFileInfo: {'a.dat': (fsize, checksum, guid), ...}
    # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
    # checksum for file a.dat: outputFileInfo['a.dat'][1]
    try:
        outputFileInfo = dict(zip([outputFile], zip(fsize_list, checksum_list, guid_list)))
    except Exception, e:
        tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
    else:
        tolog("Output file info dictionary created: %s" % str(outputFileInfo))

    return ec, pilotErrorDiag, outputFileInfo, fname

def getDatasets():
    """ Get the datasets for the output files """

    # Get the default dataset
    if globalJob.destinationDblock and globalJob.destinationDblock[0] != 'NULL' and globalJob.destinationDblock[0] != ' ':
        dsname = globalJob.destinationDblock[0]
    else:
        dsname = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name

    # Create the dataset dictionary
    # (if None, the dsname above will be used for all output files)
    datasetDict = getDatasetDict(globalJob.outFiles, globalJob.destinationDblock, globalJob.logFile, globalJob.logDblock)
    if datasetDict:
        tolog("Dataset dictionary has been verified: %s" % str(datasetDict))
    else:
        tolog("Dataset dictionary could not be verified, output files will go to: %s" % (dsname))

    return dsname, datasetDict

def stageOut(file_list, pilot_initdir, dsname, datasetDict, outputFileInfo, metadata_fname):
    """ perform the stage-out """

    error = PilotErrors()
    ec = 0
    pilotErrorDiag = ""

    # generate the xml for the output files and the site mover
#    pfnFile = "OutPutFileCatalog.xml"
#    try:
#        _status = pUtil.PFCxml(globalJob.experiment, pfnFile, file_list, fguids=globalJob.outFilesGuids, fntag="pfn")
#    except Exception, e:
#        pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
#        tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag)) 
#        return error.ERR_MISSINGGUID, pilotErrorDiag
#    else:
#        if not _status:
#            pilotErrorDiag = "Metadata contains missing guid(s) for output file(s)"
#            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
#            return error.ERR_MISSINGGUID, pilotErrorDiag
#
#    tolog("Using the newly-generated %s/%s for put operation" % (globalJob.workdir, pfnFile))

    # the cmtconfig is needed by at least the xrdcp site mover
    cmtconfig = getCmtconfig(globalJob.cmtconfig)

    rs = "" # return string from put_data with filename in case of transfer error
    tin_0 = os.times()
    tolog("metdata_fname=%s"%metadata_fname)
    tolog("outputFileInfo=%s"%str(outputFileInfo))
    try:
        ec, pilotErrorDiag, rf, rs, globalJob.filesNormalStageOut, globalJob.filesAltStageOut = mover.mover_put_data("xmlcatalog_file:%s" %\
                                         (metadata_fname), dsname, jobSite.sitename,\
                                         ub=jobSite.dq2url, analysisJob=analysisJob, pinitdir=pilot_initdir, scopeOut=globalJob.scopeOut,\
                                         proxycheck=proxycheckFlag, spsetup=globalJob.spsetup, token=globalJob.destinationDBlockToken,\
                                         userid=globalJob.prodUserID, datasetDict=datasetDict, prodSourceLabel=globalJob.prodSourceLabel,\
                                         outputDir=outputDir, jobId=globalJob.jobId, jobWorkDir=globalJob.workdir, DN=globalJob.prodUserID,\
                                         dispatchDBlockTokenForOut=globalJob.dispatchDBlockTokenForOut, outputFileInfo=outputFileInfo,\
                                         lfcreg=lfcRegistration, jobDefId=globalJob.jobDefinitionID, jobCloud=globalJob.cloud,\
                                         logFile=globalJob.logFile, stageoutTries=stageoutretry, cmtconfig=cmtconfig, experiment=experiment,\
                                         fileDestinationSE=globalJob.fileDestinationSE, eventService=True)
        tin_1 = os.times()
        globalJob.timeStageOut = int(round(tin_1[4] - tin_0[4]))
    except Exception, e:
        tin_1 = os.times()
        globalJob.timeStageOut = int(round(tin_1[4] - tin_0[4]))

        if 'format_exc' in traceback.__all__:
            trace = traceback.format_exc()
            pilotErrorDiag = "Put function can not be called for staging out: %s, %s" % (str(e), trace)
        else:
            tolog("traceback.format_exc() not available in this python version")
            pilotErrorDiag = "Put function can not be called for staging out: %s" % (str(e))
        tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))

        ec = error.ERR_PUTFUNCNOCALL
        globalJob.setState(["holding", globalJob.result[1], ec])
    else:
        if globalJob.pilotErrorDiag != "":
            globalJob.pilotErrorDiag = "Put error: " + tailPilotErrorDiag(globalJob.pilotErrorDiag, size=256-len("pilot: Put error: "))

        tolog("Put function returned code: %d" % (ec))
        if ec != 0:
            # is the job recoverable?
            if error.isRecoverableErrorCode(ec):
                _state = "holding"
                _msg = "WARNING"
            else:
                _state = "failed"
                _msg = "FAILED"

            # globalJob.setState([_state, globalJob.result[1], ec])

            tolog("!!%s!!1212!! %s" % (_msg, error.getErrorStr(ec)))
        #else:
            # globalJob.setState(["finished", 0, 0])

        #if globalJob.result[0] == "holding" and '(unrecoverable)' in globalJob.pilotErrorDiag:
        #    globalJob.result[0] = "failed"
        #    tolog("!!WARNING!!2999!! HOLDING state changed to FAILED since error is unrecoverable")

    return ec, pilotErrorDiag

def setPoolFileCatalogPath(path):
    """ Set the path to the PoolFileCatalog """

    global _pfc_path
    _pfc_path = path

def getPoolFileCatalogPath():
    """ Return the path to the PoolFileCatalog """

    return _pfc_path

def asynchronousOutputStagerOld():

    from os import listdir
    from os.path import isfile, join

    # for now..
    output_dir = os.getcwd()

    # loop and wait for an output file to be produced. once a new output file is found, stage-out the file
    while event_loop_running:
        tolog("Waiting for output file to be produced")
        time.sleep(8)

        # has any new files been added to the output directory?
        # (list all files in the output directory)
        onlyfiles = [ f for f in listdir(output_dir) if isfile(join(output_dir,f)) ] # ie do not include directories, only files
        for f in onlyfiles:
            if not output_file_dictionary.has_key(f):
                # found a new output files
                output_file_dictionary[f] = False

        # output files will be marked True when stage-out has completed
        # in case of stage-out problems, files will remain marked as False
        # if event_loop_running is False and there are files marked with False at the end of the stage-out loop, move files to recovery dir and quit

        # begin stage-out if onlyfiles list has entries
        for f in onlyfiles:
            if output_file_dictionary[f] == False:
                # stage out file f
                # ..
                ec = -1 #..
                if ec == 0:
                    # remove file, mark as True, ie successful transfer
                    # os.remove(f)
                    output_file_dictionary[f] = True

    #
    tolog("No more events, are all files staged out?")

    # has any new files been added to the output directory?
    # (list all files in the output directory)
    onlyfiles = [ f for f in listdir(output_dir) if isfile(join(output_dir,f)) ] # ie do not include directories, only files
    for f in onlyfiles:
        if not output_file_dictionary.has_key(f):
            # found a new output files
            output_file_dictionary[f] = False

    for f in output_file_dictionary.keys():
        if output_file_dictionary[f] == False:
            # attempt stage-out

            # in case of failure
            tolog("File %s was not staged out, move to recovery directory")
            # ..

def getEventRangeID(filename):
    """ Return the event range id for the corresponding output file """

    event_range_id = ""
    for event_range in eventRange_dictionary.keys():
        if eventRange_dictionary[event_range][0] == filename:
            event_range_id = event_range
            break

    return event_range_id

def transferToObjectStore(outputFileInfo, metadata_fname):
    """ Transfer the output file to the object store """

    # FORMAT:  outputFileInfo = {'<full path>/filename.ext': (fsize, checksum, guid), ...} 
    # Normally, the dictionary will only contain info about a single file

    ec = 0
    pilotErrorDiag = ""

    # Get the site information object                                                                                                           
    si = getSiteInformation(experiment)

    # Extract all information from the dictionary
    for path in outputFileInfo.keys():

        fsize = outputFileInfo[path][0]
        checksum = outputFileInfo[path][1]
        guid = outputFileInfo[path][2]

        tolog("path=%s"%path)
        # Use the put_data() function from the FAX site mover

        # First backup some schedconfig fields that need to be modified for the secondary transfer                                                  
        copytool_org = readpar('copytool')

        # Temporarily modify the schedconfig fields with values
        tolog("Temporarily modifying queuedata for log file transfer to secondary SE")
        ec = si.replaceQueuedataField("copytool", "objectstore")

        # needs to know source, destination, fsize=0, fchecksum=0, **pdict, from pdict: lfn, guid, logPath
        # where source is local file path and destination is not used, set to empty string

        # Get the dataset name for the output file                                                                                                     
        dsname, datasetDict = getDatasets()

        # Transfer the file
        ec, pilotErrorDiag = stageOut([path], pilot_initdir, dsname, datasetDict, outputFileInfo, metadata_fname)

        # Finally restore the modified schedconfig fields                                                                                           
        tolog("Restoring queuedata fields")
        _ec = si.replaceQueuedataField("copytool", copytool_org)

    tolog("ec=%d" % (ec))
    tolog("pilotErrorDiag=%s"%(pilotErrorDiag))
    return ec, pilotErrorDiag

def asynchronousOutputStager():
    """ Transfer output files to stage-out area asynchronously """

    # Note: this is run as a thread

    tolog("Asynchronous output stager thread initiated")
    while not asyncOutputStager_thread.stopped():

        if len(stageout_queue) > 0:
            for f in stageout_queue:
                # Create the output file metadata (will be sent to server)
                tolog("Preparing to stage-out file %s" % (f))
                event_range_id = getEventRangeID(f)
                if event_range_id == "":
                    tolog("!!WARNING!!1111!! Did not find the event range for file %s in the event range dictionary" % (f))
                else:
                    tolog("Creating metadata for file %s and event range id %s" % (f, event_range_id))
                    ec, pilotErrorDiag, outputFileInfo, metadata_fname = createFileMetadata(f, event_range_id)
                    if ec == 0:
                        try:
                            ec, pilotErrorDiag = transferToObjectStore(outputFileInfo, metadata_fname)
                        except Exception, e:
                            tolog("!!WARNING!!2222!! Caught exception: %s" % (e))
                        else:
                            tolog("Removing %s from stage-out queue" % (f))
                            stageout_queue.remove(f)
                            tolog("Adding %s to output file list" % (f))
                            output_files.append(f)
                            tolog("output_files = %s" % (output_files))
                            if ec == 0:
                                status = 'finished'
                            else:
                                status = 'failed'
#                                tolog("Transfer failed, will move the output to the recovery area")
#                                cmd = "mv %s %s" % (f, output_area)
#                                try:
#                                    process = Popen(cmd, shell=True)
#                                    stdout, stderr = process.communicate()
#                                except Exception, e:
#                                    tolog("!!WARNING!!2344!! Caught exception: %s" % (e))

                                # Note: the rec pilot must update the server appropriately

                            # Time to update the server
                            tolog("Transfer %s" % (status))
                            try:
                                msg = updateEventRange(event_range_id, eventRange_dictionary[event_range_id], status=status)
                            except Exception, e:
                                tolog("!!WARNING!!2233!! updateEventRange threw an exception: %s" % (e))
                            else:
                                tolog("updateEventRange has returned")
                    else:
                        tolog("!!WARNING!!1112!! Failed to create file metadata: %d, %s" % (ec, pilotErrorDiag))

        time.sleep(1)

    tolog("Asynchronous output stager thread has been stopped")

def yamplListener():
    """ Listen for yampl messages """

    # Note: this is run as a thread

    global athenamp_is_ready

    # Listen for messages as long as the thread is not stopped
    while not yampl_thread.stopped():

        try:
            # Receive a message
            tolog("Waiting for a new Yampl message")
            size, buf = yampl_server.receive()
            while size == -1 and not yampl_thread.stopped():
                time.sleep(1)
                size, buf = yampl_server.receive()
            tolog("Received new Yampl message: %s" % buf)

            # Interpret the message and take the appropriate action
            if "Ready for events" in buf:
                buf = ""
                tolog("AthenaMP is ready for events")
                athenamp_is_ready = True

            elif buf.startswith('/'):
                tolog("Received file and process info from client: %s" % (buf))

                # Extract the information from the yampl message
                path, event_range_id, cpu, wall = interpretMessage(buf)
                if path not in stageout_queue and path != "":
                    # Correct the output file name if necessary
                    # path = correctFileName(path, event_range_id)

                    # Add the extracted info to the event range dictionary
                    eventRange_dictionary[event_range_id] = [path, cpu, wall]

                    # Add the file to the stage-out queue
                    stageout_queue.append(path)
                    tolog("File %s has been added to the stage-out queue (length = %d)" % (path, len(stageout_queue)))
            else:
                tolog("Pilot received message:%s" % buf)
        except Exception,e:
            tolog("Caught exception:%s" % e)
        time.sleep(1)

    tolog("yamplListener has finished")

def correctFileName(path, event_range_id):
    """ Correct the output file name if necessary """

    # Make sure the output file name follows the format OUTPUT_FILENAME_FROM_JOBDEF.EVENT_RANGE_ID

    outputFileName = globalJob.outFiles[0]
    if outputFileName != "":
        fname = os.path.basename(path)
        dirname = os.path.dirname(path)

        constructedFileName = outputFileName + "." + event_range_id
        if fname == constructedFileName:
            tolog("Output file name verified")
        else:
            tolog("Output file name does not follow convension: OUTPUT_FILENAME_FROM_JOBDEF.EVENT_RANGE_ID: %s" % (fname))
            fname = constructedFileName
            _path = os.path.join(dirname, fname)
            cmd = "mv %s %s" % (path, _path)
            out = commands.getoutput(cmd)
            path = _path
            tolog("Corrected output file name: %s" % (path))

    return path

def interpretMessage(msg):
    """ Interpret a yampl message containing file and processing info """

    # The message is assumed to have the following format
    # Format: "<file_path>,<event_range_id>,CPU:<number_in_sec>,WALL:<number_in_sec>"
    # Return: path, event_range_id, cpu time (s), wall time (s)

    path = ""
    event_range_id = ""
    cpu = ""
    wall = ""

    if "," in msg:
        message = msg.split(",")

        try:
            path = message[0]
        except:
            tolog("!!WARNING!!1100!! Failed to extract file path from yampl message: %s" % (msg))

        try:
            event_range_id = message[1]
        except:
            tolog("!!WARNING!!1101!! Failed to extract event range id from yampl message: %s" % (msg))

        try:
            # CPU:<number_in_sec>
            _cpu = message[2]
            cpu = _cpu.split(":")[1]
        except:
            tolog("!!WARNING!!1102!! Failed to extract CPU time from yampl message: %s" % (msg))

        try:
            # WALL:<number_in_sec>
            _wall = message[3]
            wall = _wall.split(":")[1]
        except:
            tolog("!!WARNING!!1103!! Failed to extract wall time from yampl message: %s" % (msg))

    else:
        tolog("!!WARNING!!1122!! Unknown yampl message format: missing commas: %s" % (msg))

    return path, event_range_id, cpu, wall

def getTokenExtractorProcess(thisExperiment, setup, input_tag_file, stdout=None, stderr=None):
    """ Execute the TokenExtractor """

    # Define the command
    cmd = "%s TokenExtractor -src PFN:%s RootCollection" % (setup, input_tag_file)

    # Execute and return the TokenExtractor subprocess object
    return thisExperiment.getSubprocess(cmd, stdout=stdout, stderr=stderr)

def getAthenaMPProcess(thisExperiment, runCommand, stdout=None, stderr=None):
    """ Execute AthenaMP """

    # Execute and return the AthenaMP subprocess object
    #return thisExperiment.getSubprocess(thisExperiment.getJobExecutionCommand4EventService(pilot_initdir)) # move method to EventService class
    return thisExperiment.getSubprocess(runCommand, stdout=stdout, stderr=stderr)

def createYamplServer():
    """ Create the yampl server socket object """

    global yampl_server
    status = False

    # Create the server socket
    if PilotYamplServer:
        yampl_server = PilotYamplServer(socketname='EventService_EventRanges', context='local')

        # is the server alive?
        if not yampl_server.alive():
            # destroy the object
            tolog("!!WARNING!!3333!! Yampl server is not alive")
            yampl_server = None
        else:
            status = True
    else:
        tolog("!!WARNING!!3333!! PilotYamplServer object is not available")

    return status

def getTAGFile(inFiles):
    """ Extract the TAG file from the input files list """

    # Note: assume that there is only one TAG file
    tag_file = ""
    for f in inFiles:
        if ".TAG." in f:
            tag_file = f
            break

    return tag_file

def sendMessage(message):
    """ Send a yampl message """

    tolog("message = %s, type = %s" % (str(message), type(message)))

    # Filter away unwanted fields
    if "scope" in message:
        # First replace an ' with " since loads() cannot handle ' signs properly
        # Then convert to a list and get the 0th element (there should be only one)
        try:
            #_msg = loads(message.replace("'",'"'))[0]
            _msg = loads(message.replace("'",'"').replace('u"','"'))[0]
        except Exception, e:
            tolog("!!WARNING!!2233!! Caught exception: %s" % (e))
        else:
            # _msg = {u'eventRangeID': u'79-2161071668-11456-1011-1', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1020, u'startEvent': 1011, u'scope': u'mc12_8TeV', u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}
            # Now remove the "scope" key/value
            scope = _msg.pop("scope")
            # Convert back to a string
            message = str([_msg])
            tolog("Removed scope key-value from message")

    yampl_server.send(message)
    tolog("Sent %s" % message)

# NOT NEEDED
def extractScope(name):
    """ Method to extract the scope from the dataset/file name """

    if name.lower().startswith('user') or name.lower().startswith('group'):
        return name.lower().split('.')[0] + '.' + name.lower().split('.')[1]

    return name.split('.')[0]

def getPoolFileCatalog(ub, guid_list, dsname, lfn_list, pilot_initdir, analysisJob, tokens, workdir, dbh, DBReleaseIsAvailable,\
                           scope_dict, filesizeIn, checksumIn, thisExperiment=None, pfc_name="PoolFileCatalog.xml"):
    """ Wrapper function for the actual getPoolFileCatalog function in Mover """

    # This function is a wrapper to the actual getPoolFileCatalog() in Mover, but also contains SURL to TURL conversion

    from SiteMover import SiteMover
    sitemover = SiteMover()
    ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic = mover.getPoolFileCatalog(ub, guid_list, lfn_list, pilot_initdir,\
                                                                                              analysisJob, tokens, workdir, dbh,\
                                                                                              DBReleaseIsAvailable, scope_dict, filesizeIn, checksumIn,\
                                                                                              sitemover, thisExperiment=thisExperiment, pfc_name=pfc_name)
    if ec != 0:
        tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))
    else:
        # Create the file dictionaries needed for the TURL conversion
        file_nr = 0
        fileInfoDic = {}
        dsdict = {}
        xmldoc = minidom.parseString(xml_from_PFC)
        fileList = xmldoc.getElementsByTagName("File")
        for thisfile in fileList: # note that there should only ever be one file
            surl = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
            guid = guid_list[file_nr]
            # Fill the file info dictionary (ignore the file size and checksum values since they are irrelevant for the TURL conversion - set to 0)
            fileInfoDic[file_nr] = (guid, surl, 0, 0)
            if not dsdict.has_key(dsname): dsdict[dsname] = []
            dsdict[dsname].append(os.path.basename(surl))
            file_nr += 1

        transferType = ""
        sitename = ""
        usect = False
        eventService = True

        # Create a TURL based PFC
        ec, pilotErrorDiag, createdPFCTURL, usect = mover.PFC4TURLs(analysisJob, transferType, fileInfoDic, pfc_name, sitemover, sitename, usect, dsdict, eventService)
        if ec != 0:
            tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

        # Finally return the TURL based PFC
        file_info_dictionary = {}
        if ec == 0:
            file_info_dictionary = mover.getFileInfoDictionaryFromXML(getPoolFileCatalogPath())

        return ec, pilotErrorDiag, file_info_dictionary

def createPoolFileCatalog(inFiles, scopeIn, inFilesGuids, tokens, filesizeIn, checksumIn, thisExperiment, workdir):
    """ Create the Pool File Catalog """

    # Create the scope dictionary
    scope_dict = {}
    n = 0
    for lfn in inFiles:
        scope_dict[lfn] = scopeIn[n]
        n += 1

    tolog("Using scope dictionary for initial PFC: %s" % str(scope_dict))

    dsname = 'dummy_dsname' # not used by getPoolFileCatalog()
    analysisJob = False
    ub = ''
    dbh = None
    DBReleaseIsAvailable = False

    setPoolFileCatalogPath(os.path.join(workdir, "PoolFileCatalog.xml"))
    tolog("Using PFC path: %s" % (getPoolFileCatalogPath()))


    # Get the TURL based PFC
    ec, pilotErrorDiag, file_info_dictionary = getPoolFileCatalog(ub, inFilesGuids, dsname, inFiles, pilot_initdir, analysisJob,\
                                                                  tokens, workdir, dbh, DBReleaseIsAvailable, scope_dict,\
                                                                  filesizeIn, checksumIn, thisExperiment=thisExperiment,
                                                                  pfc_name=getPoolFileCatalogPath())
    if ec != 0:
        tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

    return ec, pilotErrorDiag, file_info_dictionary

def createPoolFileCatalogFromMessage(message, thisExperiment):
    """ Prepare and create the PFC using file/guid info from the event range message """

    # This function is using createPoolFileCatalog() to create the actual PFC using
    # the info from the server message

    # Note: the PFC created by this function will only contain a single LFN
    # while the intial PFC can contain multiple LFNs

    # WARNING!!!!!!!!!!!!!!!!!!!!!!
    # Consider rewrite: this function should append an entry into the xml, not replace the entire xml file


    ec = 0
    pilotErrorDiag = ""
    file_info_dictionary = {}

    if not "No more events" in message:
        # Convert string to list
        msg = loads(message)

        # Get the LFN and GUID (there is only one LFN/GUID per event range)
        try:
            # must convert unicode strings to normal strings or the catalog lookups will fail
            lfn = str(msg[0]['LFN'])
            guid = str(msg[0]['GUID'])
            scope = str(msg[0]['scope'])
        except Exception, e:
            ec = -1
            pilotErrorDiag = "Failed to extract LFN from event range: %s" % (e)
            tolog("!!WARNING!!3434!! %s" % (pilotErrorDiag))
        else:
            # Has the file already been used? (If so, the PFC already exists)
            if guid in guid_list:
                tolog("PFC for GUID in downloaded event range has already been created")
            else:
                guid_list.append(guid)
                lfn_list.append(lfn)

                tolog("Updating PFC for lfn=%s, guid=%s, scope=%s" % (lfn, guid, scope))

                # Create the PFC (includes replica lookup over multiple catalogs)
                #from Mover import getPoolFileCatalog

                scope_dict = { lfn : scope }
                tokens = ['NULL']
                filesizeIn = ['']
                checksumIn = ['']
                dsname = 'dummy_dsname' # not used by getPoolFileCatalog()
                analysisJob = False
                workdir = os.getcwd()
                ub = ''
                dbh = None
                DBReleaseIsAvailable = False

#                ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic = getPoolFileCatalog(ub, guid_list, dsname, lfn_list, pilot_initdir,\
                ec, pilotErrorDiag, file_info_dictionary = getPoolFileCatalog(ub, guid_list, dsname, lfn_list, pilot_initdir,\
                                                                                  analysisJob, tokens, workdir, dbh, DBReleaseIsAvailable,\
                                                                                  scope_dict, filesizeIn, checksumIn, thisExperiment=thisExperiment)
                if ec != 0:
                    tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

    return ec, pilotErrorDiag, file_info_dictionary

def downloadEventRanges(jobId):
    """ Download event ranges from the Event Server """

    # Return the server response (instruction to AthenaMP)
    # Note: the returned message is a string (of a list of dictionaries). If it needs to be converted back to a list, use json.loads(message)

    tolog("Server: Downloading new event ranges..")

    # message = "[{u'lastEvent': 2, u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-1', u'startEvent': 2, u'GUID':u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'}]"

    message = ""
    url = "https://aipanda007.cern.ch:25443/server/panda"
#    url = "https://pandaserver.cern.ch:25443/server/panda"
    node = {}
    node['pandaID'] = jobId

    # open connection
    ret = httpConnect(node, url, path=os.getcwd(), mode="GETEVENTRANGES")
    response = ret[1]

    if ret[0]: # non-zero return code
        message = "Failed to download event range - error code = %d" % (ret[0])
    else:
        message = response['eventRanges']

    if message == "" or message == "[]":
        message = "No more events"

    return message

def updateEventRange(event_range_id, eventRangeList, status='finished'):
    """ Update an event range on the Event Server """

    # Return the server response (instruction to AthenaMP)
    # Note: the returned message is a string (of a list of dictionaries). If it needs to be converted back to a list, use json.loads(message)

    tolog("Server: Updating an event range..")

    # message = "[{u'lastEvent': 2, u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-1', u'startEvent': 2, u'GUID':u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'}]"

    message = ""
    url = "https://aipanda007.cern.ch:25443/server/panda"
#    url = "https://pandaserver.cern.ch:25443/server/panda"
    node = {}
    node['eventRangeID'] = event_range_id
#    node['output'] = eventRangeList[0]
#    _xml = ""
#    try:
#        f = open(getMetadataFilename(event_range_id), "r")
#        tolog("Reading file %s" % (getMetadataFilename(event_range_id)))
#        _xml = f.read()
#    except IOError, e:
#        tolog("!!WARNING!!3333!! Caught exception: %s" % (e))
#    else:
#        f.close()

#    tolog("xml = %s" % (_xml))
#    node['xml'] = _xml

#    node['cpu'] =  eventRangeList[1]
#    node['wall'] = eventRangeList[2]
    node['eventStatus'] = status
    tolog("node = %s" % str(node))

    # open connection
    ret = httpConnect(node, url, path=os.getcwd(), mode="UPDATEEVENTRANGE")
#    response = ret[1]

    if ret[0]: # non-zero return code
        message = "Failed to download event range - error code = %d" % (ret[0])
    else:
        message = ""

    return message

# REMOVE AFTER TESTING
def createSEDir():
    """ Create a dir to represent the SE """
    # The pilot will stage-out files to this directory

    d = os.path.join(os.getcwd(), "output_area")
    if not os.path.exists(d):
        os.makedirs(d)

    return d

# REMOVE AFTER TESTING
def setOutputArea():
    """ Set the global output area """

    global output_area
    output_area = createSEDir()
    tolog("Files will be transferred to %s" % (output_area))

def getStdoutStderrFileObjects(stdoutName="stdout.txt", stderrName="stderr.txt"):
    """ Create stdout/err file objects """

    try:
        stdout = open(os.path.join(os.getcwd(), stdoutName), "w")
        stderr = open(os.path.join(os.getcwd(), stderrName), "w")
    except Exception, e:
        tolog("!!WARNING!!3330!! Failed to open stdout/err files: %s" % (e))
        stdout = None
        stderr = None

    return stdout, stderr

def testES():

    tolog("Note: queuedata.json must be available")
    os.environ['PilotHomeDir'] = os.getcwd()
    thisExperiment = getExperiment("ATLAS")
    message = downloadEventRanges()
    #createPoolFileCatalogFromMessage(message, thisExperiment)

def extractEventRanges(message):
    """ Extract all event ranges from the server message """

    # This function will return a list of event range dictionaries

    event_ranges = []

    try:
        event_ranges = loads(message)
    except Exception, e:
        tolog("Could not extract any event ranges: %s" % (e))

    return event_ranges

def extractEventRangeIDs(event_ranges):
    """ Extract the eventRangeID's from the event ranges """

    eventRangeIDs = []
    for event_range in event_ranges:
        eventRangeIDs.append(event_range['eventRangeID'])

    return eventRangeIDs

def areAllOutputFilesTransferred():
    """ """

    status = True
    for eventRangeID in eventRangeID_dictionary.keys():
        if eventRangeID_dictionary[eventRangeID] == False:
            status = False
            break

    return status

def addEventRangeIDsToDictionary(currentEventRangeIDs):
    """ Add the latest eventRangeIDs list to the total event range id dictionary """

    # The eventRangeID_dictionary is used to keep track of which output files have been returned from AthenaMP
    # (eventRangeID_dictionary[eventRangeID] = False means that the corresponding output file has not been created/transferred yet)
    # This is necessary since otherwise the pilot will not know what has been processed completely when the "No more events"
    # message arrives from the server

    for eventRangeID in currentEventRangeIDs:
        if not eventRangeID_dictionary.has_key(eventRangeID):
            eventRangeID_dictionary[eventRangeID] = False

def getProperInputFileName(input_files):
    """ Return the first non TAG file name in the input file list """

    # AthenaMP needs to know the name of an input file to be able to start
    # Currently an Event Service job also has a TAG file in the input file list
    # but AthenaMP cannot start with that file, so identify the proper name and return it

    filename = ""
    for f in input_files:
        if ".TAG." in f:
            continue
        else:
            filename = f
            break

    return filename

# main process starts here
if __name__ == "__main__":

    if not os.environ.has_key('PilotHomeDir'):
        os.environ['PilotHomeDir'] = os.getcwd()

    # get error handler
    error = PilotErrors()

    # define a new parent group
    os.setpgrp()

    # protect the runEvent code with exception handling
    hP_ret = False
    try:
        # always use this filename as the new jobDef module name
        import newJobDef

        jobSite = Site.Site()
        jobSite.setSiteInfo(argParser(sys.argv[1:]))
        # reassign workdir for this job
        jobSite.workdir = jobSite.wntmpdir
    
        if pilotlogfilename != "":
            pUtil.setPilotlogFilename(pilotlogfilename)
    
        # set node info
        node = Node.Node()
        node.setNodeName(os.uname()[1])
        node.collectWNInfo(jobSite.workdir)
    
        # redirect stderr
        sys.stderr = open("%s/runevent.stderr" % (jobSite.workdir), "w")
    
        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)

        # get the experiment object
        thisExperiment = getExperiment(experiment)
        tolog("runEvent will serve experiment: %s" % (thisExperiment.getExperiment()))

        # get the event service object using the experiment name (since it can be experiment specific)
        thisEventService = getEventService(experiment)

        region = readpar('region')
        JR = JobRecovery()
        try:
            job = Job.Job()
            job.setJobDef(newJobDef.job)
            job.workdir = jobSite.workdir
            job.experiment = experiment
            # figure out and set payload file names
            job.setPayloadName(thisExperiment.getPayloadName(job))
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            failJob(0, error.ERR_UNKNOWN, job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)

        # Set the global variables used by the threads
        jobId = job.jobId
        globalJob = job

        # prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        globalJob.datadir = pworkdir + "/PandaJob_%d_data" % (job.jobId)
#        job.datadir = pworkdir + "/PandaJob_%d_data" % (job.jobId)

        # register cleanup function
        atexit.register(cleanup, job)
    
        # to trigger an exception so that the SIGTERM signal can trigger cleanup function to run
        # because by default signal terminates process without cleanup.
        def sig2exc(sig, frm):
            """ signal handler """

            error = PilotErrors()
            global failureCode, globalPilotErrorDiag, globalErrorCode
            globalPilotErrorDiag = "!!FAILED!!3000!! SIGTERM Signal %s is caught in child pid=%d!\n" % (sig, os.getpid())
            tolog(globalPilotErrorDiag)
            if sig == signal.SIGTERM:
                globalErrorCode = error.ERR_SIGTERM
            elif sig == signal.SIGQUIT:
                globalErrorCode = error.ERR_SIGQUIT
            elif sig == signal.SIGSEGV:
                globalErrorCode = error.ERR_SIGSEGV
            elif sig == signal.SIGXCPU:
                globalErrorCode = error.ERR_SIGXCPU
            elif sig == signal.SIGBUS:
                globalErrorCode = error.ERR_SIGBUS
            elif sig == signal.SIGUSR1:
                globalErrorCode = error.ERR_SIGUSR1
            else:
                globalErrorCode = error.ERR_KILLSIGNAL
            failureCode = globalErrorCode
            # print to stderr
            print >> sys.stderr, globalPilotErrorDiag
            raise SystemError(sig)

        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)

        # see if it's an analysis job or not
#        global analysisJob
        analysisJob = isAnalysisJob(globalJob.trf.split(",")[0])
#        analysisJob = isAnalysisJob(job.trf.split(",")[0])



        # REMOVE AFTER TESTING
        # Create the output area (files received from the client will be transferred here)
        setOutputArea()


        # Create a yampl server object (global yampl_server)
        if createYamplServer():
            tolog("The Yampl server is alive")
        else:
            pilotErrorDiag = "The Yampl server could not be created, cannot continue"
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            failJob(0, globalJob.result[2], globalJob, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)

        # Setup starts here ................................................................................

        # Update the job state file
        globalJob.jobState = "setup"
        _retjs = JR.updateJobStateTest(globalJob, jobSite, node, mode="test")

        # Send [especially] the process group back to the pilot
        globalJob.setState([job.jobState, 0, 0])
        rt = RunJobUtilities.updatePilotServer(globalJob, pilotserver, pilotport)

        # prepare the setup and get the run command list
        ec, runCommandList, globalJob, multi_trf = setup(globalJob, jobSite, thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (globalJob.pilotErrorDiag))
            failJob(0, ec, globalJob, pilotserver, pilotport, pilotErrorDiag=globalJob.pilotErrorDiag)
        tolog("Setup has finished successfully")

        # job has been updated, display it again
        globalJob.displayJob()

        # stage-in .........................................................................................

        # update the job state file
        globalJob.jobState = "stagein"
        _retjs = JR.updateJobStateTest(globalJob, jobSite, node, mode="test")

        # update copysetup[in] for production jobs if brokerage has decided that remote I/O should be used
        if globalJob.transferType == 'direct':
            tolog('Brokerage has set transfer type to \"%s\" (remote I/O will be attempted for input files, any special access mode will be ignored)' %\
                  (globalJob.transferType))
            RunJobUtilities.updateCopysetups('', transferType=globalJob.transferType)

        # stage-in all input files (if necessary)
        globalJob, ins, statusPFCTurl, usedFAXandDirectIO = stageIn(globalJob, jobSite, analysisJob, pilot_initdir, pworkdir)
        if globalJob.result[2] != 0:
            tolog("Failing job with ec: %d" % (ec))
            failJob(0, globalJob.result[2], globalJob, pilotserver, pilotport, ins=ins, pilotErrorDiag=globalJob.pilotErrorDiag)

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
        if globalJob.inFiles != ['']:
            runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, pworkdir, globalJob.jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO)

        # (stage-in ends here) .............................................................................

        # Prepare XML for input files to be read by the Event Server

        # runEvent determines the physical file replica(s) to be used as the source for input event data
        # It determines this from the input dataset/file info provided in the PanDA job spec

        # threading starts here ............................................................................

        event_loop_running = True
        payload_running = False

        # Create and start the stage-out thread which will run in an infinite loop until it is stopped
        asyncOutputStager_thread = StoppableThread(name='asynchronousOutputStager', target=asynchronousOutputStager)
        asyncOutputStager_thread.start()

        # Create and start the yampl listener thread
        yampl_thread = StoppableThread(name='yamplListener', target=yamplListener)
        yampl_thread.start()

        # Stdout/err file objects
        tokenextractor_stdout = None
        tokenextractor_stderr = None
        athenamp_stdout = None
        athenamp_stderr = None

        # Create and start the TokenExtractor
        input_tag_file = getTAGFile(globalJob.inFiles)
        if input_tag_file != "":
            tolog("Will run TokenExtractor on file %s" % (input_tag_file))

            # Extract the proper setup string from the run command
            setupString = thisEventService.extractSetup(runCommandList[0])
            tolog("The Token Extractor will be setup using: %s" % (setupString))

            # Create the file objects
            tokenextractor_stdout, tokenextractor_stderr = getStdoutStderrFileObjects(stdoutName="tokenextractor_stdout.txt", stderrName="tokenextractor_stderr.txt")

            # Get the Token Extractor command
            tokenExtractorProcess = getTokenExtractorProcess(thisExperiment, setupString, input_tag_file, stdout=tokenextractor_stdout, stderr=tokenextractor_stderr)

#            out, err = tokenExtractorProcess.communicate()
#            errcode = tokenExtractorProcess.returncode

#            tolog("out=%s" % (out))
#            tolog("err=%s" % (err))
#            tolog("errcode=%s" % str(errcode))
        else:
            pilotErrorDiag = "Required TAG file could not be identified in input file list"
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))

            # stop threads
            # ..

            failJob(0, globalJob.result[2], globalJob, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)



        # athenamp gets stuck here as soon as it is launched. why? it appears to be blocking
        # try to print stdout/err

        # Create the file objects
        athenamp_stdout, athenamp_stderr = getStdoutStderrFileObjects(stdoutName="athenamp_stdout.txt", stderrName="athenamp_stderr.txt")

        # Remove the 1>.. 2>.. bit from the command string (not needed since Popen will handle the streams)
        if " 1>" in runCommandList[0] and " 2>" in runCommandList[0]:
            runCommandList[0] = runCommandList[0][:runCommandList[0].find(' 1>')]

        # AthenaMP needs the PFC when it is launched (initial PFC using info from job definition)
        # The returned file info dictionary contains the TURL for the input file. AthenaMP needs to know the full path for the --inputEvgenFile option
        ec, pilotErrorDiag, file_info_dictionary = createPoolFileCatalog(globalJob.inFiles, globalJob.scopeIn, globalJob.inFilesGuids, globalJob.prodDBlockToken, globalJob.filesizeIn, globalJob.checksumIn, thisExperiment, pworkdir)
        if ec != 0:
            tolog("!!WARNING!!4440!! Failed to create initial PFC - cannot continue, will stop all threads")

            # stop threads
            # ..

            failJob(0, globalJob.result[2], globalJob, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)

        # AthenaMP needs to know where exactly is the PFC
        runCommandList[0] += " '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (getPoolFileCatalogPath())


        # ONLY IF STAGE-IN IS SKIPPED: (WHICH CURRENTLY DOESN'T WORK)

        # Now update the --inputEvgenFile option with the full path to the input file using the TURL
        #inputFile = getProperInputFileName(job.inFiles)
        #turl = file_info_dictionary[inputFile][0]
        #runCommandList[0] = runCommandList[0].replace(inputFile, turl)
        #tolog("Replaced '%s' with '%s' in the run command" % (inputFile, turl))

        # Create and start the AthenaMP process
        athenaMPProcess = getAthenaMPProcess(thisExperiment, runCommandList[0], stdout=athenamp_stdout, stderr=athenamp_stderr)

        # Main loop ........................................................................................

        # nonsense counter used to get different "event server" message using the downloadEventRanges() function
        tolog("Entering monitoring loop")

        nap = 5
        while True:
            # if the AthenaMP workers are ready for event processing, download some event ranges
            # the boolean will be set to true in the yamplListener after the "Ready for events" message is received from the client
            if athenamp_is_ready:

                # Pilot will download some event ranges from the Event Server
                message = downloadEventRanges(globalJob.jobId)

                # Create a list of event ranges from the downloaded message
                event_ranges = extractEventRanges(message)

                # Are there any event ranges?
                if event_ranges == []:
                    tolog("No more events")
                    sendMessage("No more events")
                    break

                # Get the current list of eventRangeIDs
                currentEventRangeIDs = extractEventRangeIDs(event_ranges)

                # Store the current event range id's in the total event range id dictionary
                addEventRangeIDsToDictionary(currentEventRangeIDs)

                # Create a new PFC for the current event ranges
                ec, pilotErrorDiag, file_info_dictionary = createPoolFileCatalogFromMessage(message, thisExperiment)
                if ec != 0:
                    tolog("!!WARNING!!4444!! Failed to create PFC - cannot continue, will stop all threads")
                    sendMessage("No more events")
                    break

                # Loop over the event ranges and call AthenaMP for each event range
                i = 0
                j = 0
                for event_range in event_ranges:
                    # Send the event range to AthenaMP
                    tolog("Sending a new event range to AthenaMP (id=%s)" % (currentEventRangeIDs[j]))
                    sendMessage(str([event_range]))

                    # Set the boolean to false until AthenaMP is again ready for processing more events
                    athenamp_is_ready = False

                    # Wait until AthenaMP is ready to receive another event range
                    while not athenamp_is_ready:
                        # Take a nap
                        if i%10 == 0:
                            tolog("Event range loop iteration #%d" % (i))
                            i += 1
                        time.sleep(nap)

                        # Is AthenaMP still running?
                        if athenaMPProcess.poll() is not None:
                            tolog("AthenaMP appears to have finished (aborting event processing loop for this event range)")
                            break

                        if athenamp_is_ready:
                            tolog("AthenaMP is ready for new event range")
                            break

                    # Is AthenaMP still running?
                    if athenaMPProcess.poll() is not None:
                        tolog("AthenaMP has finished (aborting event range loop for current event ranges)")
                        break

                    j += 1

                # Is AthenaMP still running?
                if athenaMPProcess.poll() is not None:
                    tolog("AthenaMP has finished (aborting event range loop)")
                    break

            else:
                time.sleep(5)

                # Is AthenaMP still running?
                if athenaMPProcess.poll() is not None:
                    tolog("!!WARNING!!2222!! AthenaMP has finished prematurely (aborting monitoring loop)")
                    break

#        n = 0
#        while True:
#            # if the AthenaMP workers are ready for event processing, download some event ranges
#            # the boolean will be set to true in the yamplListener after the "Ready for events" message is received from the client
#            if athenamp_is_ready:
#
#                # Pilot will download an event range from the Event Server
#
#                # Download events
#                n += 1
#                if n <= 1:
#                    message = downloadEventRanges(job.jobId)
#                else:
#                    message = "No more events"
#
#                # Create a new PFC for the current event range (won't be executed in case there are no more events)
#                ec, pilotErrorDiag = createPoolFileCatalogFromMessage(message, thisExperiment)
#                if ec != 0:
#                    tolog("!!WARNING!!4444!! Failed to create PFC - cannot continue, will stop all threads")
#                    sendMessage("") # Empty message means no more events
#                    break
#
#                # Pass the server response to AthenaMP
#                sendMessage(message)
#
#                # Set the boolean to false until AthenaMP is again ready for processing more events
#                athenamp_is_ready = False
#
#                # the event server has no more events, break the loop and quit
#                if message == "No more events":
#                    tolog("Server: Finished")
#
#                    # wait for the subprocess to finish
#                    tolog("Waiting for subprocess to finish")
#
#                    if athenaMPProcess.poll() is None:
#                        tolog("Subprocess has already finished")
#                    else:
#                        tolog("Subprocess has not finished yet")
#
#                        # add some code here for waiting until the subprocess has finished                                                                                        #                                                
#
#                   tolog("Ok")
#                    break
#
#                # Is AthenaMP still running?
#                if athenaMPProcess.poll() is None:
#                    tolog("AthenaMP appears to have finished")
#                    break
#
#                n += 1
#
#            else:
#                time.sleep(5)


        # Wait for AthenaMP to finish
        i = 0
        kill = False
        while athenaMPProcess.poll() is None:
            if i > 600:
                # Stop AthenaMP
                tolog("Waited long enough - Stopping AthenaMP process")
                athenaMPProcess.kill()
                tolog("(Kill signal SIGTERM sentto AthenaMP - jobReport might get lost)")
                kill = True
                break

            tolog("Waiting for AthenaMP to finish (#%d)" % (i))
            time.sleep(60)
            i += 1

        if not kill:
            tolog("AthenaMP has finished")

        # Do not stop the stageout thread until all output files have been transferred
        starttime = time.time()
        maxtime = 15*60
#        while len (stageout_queue) > 0 and (time.time() - starttime < maxtime):
#            tolog("stage-out queue: %s" % (stageout_queue))
#            tolog("(Will wait for a maximum of %d seconds, so far waited %d seconds)" % (maxtime, time.time() - starttime))
#            time.sleep(5)

        while not areAllOutputFilesTransferred():
            if len(stageout_queue) == 0:
                tolog("No files in stage-out queue, no point in waiting for transfers since AthenaMP has finished (job is failed)")
                break

            tolog("Will wait for a maximum of %d seconds for file transfers to finish (so far waited %d seconds)" % (maxtime, time.time() - starttime))
            tolog("stage-out queue: %s" % (stageout_queue))
            if (len(stageout_queue)) > 0 and (time.time() - starttime > maxtime):
                tolog("Aborting stage-out thread (timeout)")
                break
            time.sleep(30)

        # Get the datasets for the output files
        globalJob.outFiles = output_files # replace the default job output file list which is anyway not correct (it is only used by AthenaMP for generating output file names)
        tolog("output_files = %s" % (output_files))
        dsname, datasetDict = getDatasets()
        tolog("dsname = %s" % (dsname))
        tolog("datasetDict = %s" % (datasetDict))

        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(globalJob.outFiles, globalJob.logFile, globalJob.workdir, fullpath=True)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            failJob(globalJob.result[1], ec, globalJob, pilotErrorDiag=pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # Create metadata for all successfully staged-out output files (include the log file as well, even if it has not been created yet)
        ec, outputFileInfo = createFileMetadata2(list(output_files), outsDict, dsname, datasetDict, jobSite.sitename, analJob=analysisJob)
        if ec:
            runJob.failJob(0, ec, globalJob, pilotErrorDiag=globalJob.pilotErrorDiag)

        tolog("Stopping stage-out thread")
        asyncOutputStager_thread.stop()
        asyncOutputStager_thread.join()

        # Stop Token Extractor
        if tokenExtractorProcess:
            tolog("Stopping Token Extractor process")
            tokenExtractorProcess.kill()
            tolog("(Kill signal SIGTERM sent)")
        else:
            tolog("No Token Extractor process running")

        # Close stdout/err streams
        if tokenextractor_stdout:
            tokenextractor_stdout.close()
        if tokenextractor_stderr:
            tokenextractor_stderr.close()

        # Close stdout/err streams
        if athenamp_stdout:
            athenamp_stdout.close()
        if athenamp_stderr:
            athenamp_stderr.close()

        tolog("Stopping yampl thread")
        yampl_thread.stop()
        yampl_thread.join()

        # Rename the metadata produced by the payload
        # if not pUtil.isBuildJob(outs):
        moveTrfMetadata(pworkdir)
        
        # Check the job report for any exit code that should replace the res_tuple[0]
        res0, exitAcronym, exitMsg = getTrfExitInfo(0, globalJob.workdir)
        res = (res0, exitMsg, exitMsg)

        # If payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(globalJob.workdir, ins)

        # Payload error handling
        ed = ErrorDiagnosis()
        globalJob = ed.interpretPayload(globalJob, res, False, 0, runCommandList, failureCode)
        if globalJob.result[1] != 0 or globalJob.result[2] != 0:
            failJob(globalJob.result[1], globalJob.result[2], globalJob, pilotserver, pilotport, pilotErrorDiag=globalJob.pilotErrorDiag)

#        globalJob.setState(["finished", 0, 0])

#        # Run main loop until event_loop_running is set to False
#        while event_loop_running:
#
#            # Ask event server for more events using the event range downloaded with the PanDA job by the pilot
#            if thisEventService.getEvents(job):
#
#                # Prepare the setup and get the run command list
#
#                # Set up the invocation and execution of the athenaMP payload initialization step
#
#                # Set up POOL catalog file creation for event type jobs (include in previous step)
#                if thisEventService.createPFC4Event():
#
#                    # Start the payload
#                    payload_running = True
#                    time.sleep(10) # Wait until the payload process has seen that payload_running is set
#
#                    # Execute payload (process events)
#                    thisEventService.processEvents()
#
#                    # Implement the communication from pilot to PanDA/JEDI to inform PanDA of completed events:
#                    # status of completion and transmission to aggregation point, retry number (include in previous step)
#                    thisEventService.updatePandaServer()
#                else:
#                    pass
#
#            else:
#                # if no more events, quit
#                event_loop_running = False
#                tolog("No more events")
#
#                # no more events, wait for the staging thread to finish
#                asyncOutputStager_thread.join()
#                tolog("Asynchronous output stager thread has finished")

        # wrap up ..........................................................................................

        globalJob.setState(["finished", 0, 0])
        rt = RunJobUtilities.updatePilotServer(globalJob, pilotserver, pilotport, final=True)

        tolog("Done")
        sysExit(globalJob)
    
    except Exception, errorMsg:

        error = PilotErrors()

        if globalPilotErrorDiag != "":
            pilotErrorDiag = "Exception caught in runEvent: %s" % (globalPilotErrorDiag)
        else:
            pilotErrorDiag = "Exception caught in runEvent: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()    

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (str(e), pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in runEvent: %s" % str(e)
                tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))

        job = Job.Job()
        job.setJobDef(newJobDef.job)
        job.pilotErrorDiag = pilotErrorDiag
        job.result[0] = "failed"
        if globalErrorCode != 0:
            job.result[2] = globalErrorCode
        else:
            job.result[2] = error.ERR_RUNEVENTEXC
        tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        failJob(0, job.result[2], job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag, docleanup=False)

    # end of runEvent
