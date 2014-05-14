import os, sys, commands, getopt, time
import traceback
import atexit, signal
import stat
import Site, pUtil, Job, Node, RunJobUtilities
import Mover as mover
from pUtil import debugInfo, tolog, isAnalysisJob, readpar, createLockFile, getDatasetDict, getAtlasRelease, getChecksumCommand,\
     tailPilotErrorDiag, getFileAccessInfo, addToJobSetupScript, processDBRelease, getCmtconfig, getExtension, getExperiment
from JobRecovery import JobRecovery
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from ProxyGuard import ProxyGuard
from shutil import copy2

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
testLevel = 0                      # test suite control variable (0: no test, 1: put error, 2: ...)
pilot_initdir = ""                 # location of where the pilot is untarred and started
proxycheckFlag = True              # True (default): perform proxy validity checks, False: no check
globalPilotErrorDiag = ""          # global pilotErrorDiag used with signal handler (only)
globalErrorCode = 0                # global error code used with signal handler (only)
inputDir = ""                      # location of input files (source for mv site mover)
outputDir = ""                     # location of output files (destination for mv site mover)
lfcRegistration = True             # should the pilot perform LFC registration?
experiment = "ATLAS"               # Current experiment (can be set with pilot option -F <experiment>)

def usage():
    """
    usage: python runJob.py -s <sitename> -d <workdir> -a <appdir> -l <pilotinitdir> -w <url> -p <port> -q <dq2url> -g <inputdir> -m <outputdir> -o <pworkdir> -i <guid> -b <debuglevel> -k <pilotlogfilename> -x <stageintries> -v <testlevel> -t <proxycheckflag> -B <lfcRegistration> -E <stageouttries -F <experiment>
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
               <testlevel> 0: no test, 1: simulate put error, 2: ...
               <proxycheckflag> True (default): perform proxy validity checks, False: no check
               <outputDir> location of output files (destination for mv site mover)
               <inputdir> location of input files (source for mv site mover)
               <lfcRegistration> True[False]: pilot will [not] perform LFC registration (default: True)
               <experiment> Current experiment (default: ATLAS)
    """
    print usage.__doc__

def argParser(argv):
    """ parse command line arguments for the main script """
    global pilotserver, pilotport, uflag, pworkdir, logguid, debugLevel, pilotlogfilename, stageinretry,\
           stageoutretry, testLevel, pilot_initdir, proxycheckFlag, inputDir, outputDir, lfcRegistration, experiment

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
        elif o == "-v": testLevel = a
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
            tolog('job.workdir is %s pworkdir is %s ' % (job.workdir, pworkdir)) # Eddie
            copy2("%s/metadata-%d.xml" % (job.workdir, job.jobId), "%s/metadata-%d.xml" % (pworkdir, job.jobId))
        except Exception, e:
            tolog("Warning: Could not copy metadata-%d.xml to site work dir - ddm Adder problems will occure in case of job recovery" % (job.jobId))
            tolog('job.workdir is %s pworkdir is %s ' % (job.workdir, pworkdir)) # Eddie
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
    tolog("runJob (payload wrapper) has finished")
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
    jobAtlasRelease = getAtlasRelease(job.atlasRelease)

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
        job.atlasRelease = _swRelease
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

def stageIn(job, jobSite, analJob, pilot_initdir, pworkdir):
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
            mover.get_data(job, jobSite, ins, stageinretry, analysisJob=analJob, usect=useCT,\
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
    if os.path.exists(filename):
        tolog("Found job report: %s" % (filename))

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

#def executePayloadGLExec(cmd, job):
#    """ Execute trf under glexec """
#
#    tolog('Executing transformation script under glexec...')
#
#    import myproxyUtils
#
#    # retrieving the end-user credentials from a MyProxy server...
#    MyPI = myproxyUtils.MyProxyInterface(job.myproxy)
#    MyPI.userDN = job.prodUserID
#    MyPI.credname = job.credname
#
#    # using those retrieved credentials to switch identity...
#    glexec_obj = myproxyUtils.executeGlexec(MyPI)
#    glexec_obj.payload = cmd 
#
#    # ... and run the transformation script with gLExec
#    glexec_obj.execute()
#
#    return glexec_obj.status, glexec_obj.output

def executePayload(runCommandList, job):
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
            addToJobSetupScript(to_script, job.workdir)

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
            if failureCode:
                job.result[2] = failureCode
                tolog("!!FAILED!!3000!! Failure code: %d" % (failureCode))
                break
        else:
            if res_tuple[0] == 0:
                tolog("Job command %d/%d finished" % (current_job_number, number_of_jobs))
            else:
                tolog("Job command %d/%d failed: res = %s" % (current_job_number, number_of_jobs, str(res_tuple)))
                break

    t1 = os.times()
    t = map(lambda x, y:x-y, t1, t0) # get the time consumed
    job.cpuConsumptionUnit, job.cpuConsumptionTime, job.cpuConversionFactor = pUtil.setTimeConsumed(t)
    tolog("Job CPU usage: %s %s" % (job.cpuConsumptionTime, job.cpuConsumptionUnit))
    tolog("Job CPU conversion factor: %1.10f" % (job.cpuConversionFactor))
    job.timeExe = int(round(t1[4] - t0[4]))

    tolog("Original exit code: %d" % (res_tuple[0]))
    tolog("Exit code: %d (returned from OS)" % (res_tuple[0]%255))

    # check the job report for any exit code that should replace the res_tuple[0]
    res0, exitAcronym, exitMsg = getTrfExitInfo(res_tuple[0], job.workdir)
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

def moveTrfMetadata(workdir, jobId, pworkdir):
    """ rename and copy the trf metadata """

    oldMDName = "%s/metadata.xml" % (workdir)
    _filename = "metadata-%s.xml.PAYLOAD" % (repr(jobId))
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
            copy2(newMDName, "%s/%s" % (pworkdir, _filename))
        except Exception, e:
            tolog("Warning: Could not copy %s to site work dir: %s" % (_filename, str(e)))
        else:
            tolog("Metadata was transferred to site work dir: %s/%s" % (pworkdir, _filename))

def createFileMetadata(outFiles, job, outsDict, dsname, datasetDict, sitename, analJob=False):
    """ create the metadata for the output + log files """

    ec = 0

    # get/assign guids to the output files
    if outFiles:
        tolog("outFiles=%s"%str(outFiles))
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
        failJob(job.result[1], ec, job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)

    if logguid:
        guid = logguid
    else:
        guid = job.tarFileGuid

    # create preliminary metadata (no metadata yet about log file - added later in pilot.py)
    _fname = "%s/metadata-%d.xml" % (job.workdir, job.jobId)
    try:
        _status = pUtil.PFCxml(job.experiment, _fname, list(job.outFiles), fguids=job.outFilesGuids, fntag="lfn", alog=job.logFile, alogguid=guid,\
                               fsize=fsize, checksum=checksum, analJob=analJob)
    except Exception, e:
        pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
        tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag)) 
        failJob(job.result[1], error.ERR_MISSINGGUID, job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)
    else:
        if not _status:
            pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            failJob(job.result[1], error.ERR_MISSINGGUID, job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)

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

def getDatasets(job):
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

def stageOut(job, jobSite, outs, pilot_initdir, testLevel, analJob, dsname, datasetDict, proxycheckFlag, outputFileInfo):
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
                                         ub=jobSite.dq2url, analysisJob=analJob, testLevel=testLevel, pinitdir=pilot_initdir, scopeOut=job.scopeOut,\
                                         proxycheck=proxycheckFlag, spsetup=job.spsetup, token=job.destinationDBlockToken,\
                                         userid=job.prodUserID, datasetDict=datasetDict, prodSourceLabel=job.prodSourceLabel,\
                                         outputDir=outputDir, jobId=job.jobId, jobWorkDir=job.workdir, DN=job.prodUserID,\
                                         dispatchDBlockTokenForOut=job.dispatchDBlockTokenForOut, outputFileInfo=outputFileInfo,\
                                         lfcreg=lfcRegistration, jobDefId=job.jobDefinitionID, jobCloud=job.cloud, logFile=job.logFile,\
                                         stageoutTries=stageoutretry, cmtconfig=cmtconfig, experiment=experiment, fileDestinationSE=job.fileDestinationSE)
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
            job.pilotErrorDiag = "Put error: " + tailPilotErrorDiag(job.pilotErrorDiag, size=256-len("pilot: Put error: "))

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
            createLockFile(True, pworkdir, lockfile="ALLFILESTRANSFERRED")
    
            # file transfer worked, now register the output files in the LRC if necessary (not for LFC sites)
            ub = jobSite.dq2url
            lfchost = readpar('lfchost')
            if ub != "None" and ub != None and ub != "" and lfchost == "": # ub is 'None' outside the US
                # Perform the LRC file registration
                from FileRegistration import FileRegistration
                filereg = FileRegistration()
                _islogfile = False
                _jobrec = False
                _errorLabel = "WARNING"
                ec, pilotErrorDiag, _state, _msg, latereg = filereg.registerFilesLRC(ub, rf, _islogfile, _jobrec, jobSite.workdir, _errorLabel)
                if ec != 0:
                    job.setState([_state, 0, ec])
                    job.pilotErrorDiag = pilotErrorDiag
            else:
                if lfchost != "":
                    tolog("No LRC file registration since lfchost is set")
                else:
                    tolog("No LRC file registration since dq2url is not set")
    
        if job.result[0] == "holding" and '(unrecoverable)' in job.pilotErrorDiag:
            job.result[0] = "failed"
            tolog("!!WARNING!!2999!! HOLDING state changed to FAILED since error is unrecoverable")

    return rc, job, rf, latereg

# main process starts here
if __name__ == "__main__":

    # get error handler
    error = PilotErrors()

    # protect the runJob code with exception handling
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

        # redirect stder
        sys.stderr = open("%s/runjob.stderr" % (jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)
        # get the experiment object
        thisExperiment = getExperiment(experiment)
        tolog("runJob will serve experiment: %s" % (thisExperiment.getExperiment()))

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

        # prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        job.datadir = pworkdir + "/PandaJob_%d_data" % (job.jobId)

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
        analJob = isAnalysisJob(job.trf.split(",")[0])
        if analJob:
            tolog("User analysis job")
        else:
            tolog("Production job")
        tolog("runJob received a job with prodSourceLabel=%s" % (job.prodSourceLabel))

        # setup starts here ................................................................................

        # update the job state file
        job.jobState = "setup"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = setup(job, jobSite, thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            failJob(0, ec, job, pilotserver, pilotport, pilotErrorDiag=job.pilotErrorDiag)
        tolog("Setup has finished successfully")

        # job has been updated, display it again
        job.displayJob()

        # (setup ends here) ................................................................................

        tolog("Setting stage-in state until all input files have been copied")
        job.setState(["stagein", 0, 0])
        # send the special setup string back to the pilot (needed for the log transfer on xrdcp systems)
        rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport)

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
        job, ins, statusPFCTurl, usedFAXandDirectIO = stageIn(job, jobSite, analJob, pilot_initdir, pworkdir)
        if job.result[2] != 0:
            tolog("Failing job with ec: %d" % (ec))
            failJob(0, job.result[2], job, pilotserver, pilotport, ins=ins, pilotErrorDiag=job.pilotErrorDiag)

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
        if job.inFiles != ['']:
            runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, pworkdir, job.jobId, statusPFCTurl, analJob, usedFAXandDirectIO)

        # (stage-in ends here) .............................................................................

        # change to running state since all input files have been staged
        tolog("Changing to running state since all input files have been staged")
        job.setState(["running", 0, 0])
        rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport)
    
        # update the job state file
        job.jobState = "running"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # run the job(s) ...................................................................................

        # Set ATLAS_CONDDB if necessary, and other env vars
        RunJobUtilities.setEnvVars(jobSite.sitename)

        # execute the payload
        res, job, getstatusoutput_was_interrupted, current_job_number = executePayload(runCommandList, job)

        # if payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        # payload error handling
        ed = ErrorDiagnosis()
        job = ed.interpretPayload(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode)
        if job.result[1] != 0 or job.result[2] != 0:
            failJob(job.result[1], job.result[2], job, pilotserver, pilotport, pilotErrorDiag=job.pilotErrorDiag)

        # stage-out ........................................................................................

        # update the job state file
        job.jobState = "stageout"
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

        # verify and prepare and the output files for transfer
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, job.workdir)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            failJob(job.result[1], ec, job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # update the current file states
        updateFileStates(outs, pworkdir, job.jobId, mode="file_state", state="created")
        dumpFileStates(pworkdir, job.jobId)

        # create xml string to pass to dispatcher for atlas jobs
        outputFileInfo = {}
        if outs or (job.logFile and job.logFile != ''):
            # get the datasets for the output files
            dsname, datasetDict = getDatasets(job)

            # re-create the metadata.xml file, putting guids of ALL output files into it.
            # output files that miss guids from the job itself will get guids in PFCxml function

            # first rename and copy the trf metadata file for non-build jobs
            if not pUtil.isBuildJob(outs):
                moveTrfMetadata(job.workdir, job.jobId, pworkdir)

            # create the metadata for the output + log files
            ec, job, outputFileInfo = createFileMetadata(list(outs), job, outsDict, dsname, datasetDict, jobSite.sitename, analJob=analJob)
            if ec:
                failJob(0, ec, job, pilotserver, pilotport, pilotErrorDiag=job.pilotErrorDiag)

        # move output files from workdir to local DDM area
        finalUpdateDone = False
        if outs:
            tolog("Setting stage-out state until all output files have been copied")
            job.setState(["stageout", 0, 0])
            rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport)

            # stage-out output files
            ec, job, rf, latereg = stageOut(job, jobSite, outs, pilot_initdir, testLevel, analJob,\
                                                            dsname, datasetDict, proxycheckFlag, outputFileInfo)
            # error handling
            if job.result[0] == "finished" or ec == error.ERR_PUTFUNCNOCALL:
                rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport, final=True)
            else:
                rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport, final=True, latereg=latereg)
            if ec == error.ERR_NOSTORAGE:
                # update the current file states for all files since nothing could be transferred
                updateFileStates(outs, pworkdir, job.jobId, mode="file_state", state="not_transferred")
                dumpFileStates(pworkdir, job.jobId)

            finalUpdateDone = True
            if ec != 0:
                sysExit(job, rf)
            # (stage-out ends here) .......................................................................

        job.setState(["finished", 0, 0])
        if not finalUpdateDone:
            rt = RunJobUtilities.updatePilotServer(job, pilotserver, pilotport, final=True)
        sysExit(job)
    
    except Exception, errorMsg:

        error = PilotErrors()

        if globalPilotErrorDiag != "":
            pilotErrorDiag = "Exception caught in runJob: %s" % (globalPilotErrorDiag)
        else:
            pilotErrorDiag = "Exception caught in runJob: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()    

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (str(e), pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in runJob: %s" % str(e)
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
        cmd = "pwd;ls -lF %s;ls -lF;ls -lF .." % (pilot_initdir)
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog("%s" % (out))

        job = Job.Job()
        job.setJobDef(newJobDef.job)
        job.pilotErrorDiag = pilotErrorDiag
        job.result[0] = "failed"
        if globalErrorCode != 0:
            job.result[2] = globalErrorCode
        else:
            job.result[2] = error.ERR_RUNJOBEXC
        tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        failJob(0, job.result[2], job, pilotserver, pilotport, pilotErrorDiag=pilotErrorDiag, docleanup=False)

    # end of runJob


