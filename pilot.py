#!/usr/bin/python -u 

import commands
import getopt
import os
import time
import re
import atexit
import sys
import signal
import cgi
from glob import glob

from PilotErrors import PilotErrors
from JobState import JobState
from processes import killProcesses, isCGROUPSSite
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from JobLog import JobLog # import here to avoid issues seen at EELA with missing module
import Mover as mover
import Site
import Job
import Node
import pUtil
import glexec_utils
from Configuration import Configuration
from WatchDog import WatchDog
from Monitor import Monitor
import subprocess


# Initialize the configuration singleton
import environment
environment.set_environment()
env = Configuration()
globalSite = None

def usage():
    """
    usage: python pilot.py -s <sitename> -d <workdir> -a <appdir> -w <url> -p <port> -q <dq2url> -u <user> -m <outputdir> -g <inputdir> -r <rmwkdir> -j <jrflag> -n <jrmax> -c <jrmaxatt> -f <jreqflag> -e <logfiledir> -b <debuglevel> -h <queuename> -x <stageinretry> -y <loggingMode> -z <updateserver> -k <memory> -t <proxycheckflag> -l <wrapperflag> -i <pilotreleaseflag> -o <countrygroup> -v <workingGroup> -A <allowOtherCountry> -B <lfcRegistration> -C <timefloor> -D <useCoPilot> -E <stageoutretry> -F <experiment> -G <getJobMaxTime> -H <cache> -I <schedconfigURL>
    where:
               <sitename> is the name of the site that this job is landed,like BNL_ATLAS_1
               <workdir> is the pathname to the work directory of this job on the site
               <appdir> is the pathname to the directory of the executables
               <url> is the URL of the PanDA server
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
               <cache> is an optional URL used by some experiment classes (LSST)
               <schedconfigURL> optional URL used by the pilot to download queuedata from the schedconfig server
    """
    #  <testlevel> 0: no test, 1: simulate put error, 2: ...
    print usage.__doc__

def execute(program):
    """Run a program on the command line. Return stderr, stdout and status."""
    pipe = subprocess.Popen(program, bufsize=-1, shell=True, close_fds=False,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    return stdout, stderr, pipe.wait()

def argParser(argv):
    """ parse command line arguments for the main script """

    pUtil.tolog("argParser arguments: %s" % str(argv))    

    # find the pilot ID and JSID if any from the environment variables
    try:
        jsid = os.environ["PANDA_JSID"]
    except:
        pass
    else:
        env['jobSchedulerId'] = jsid
        print "jobScheduler ID = %s" % env['jobSchedulerId']
    try:
        gtag = os.environ["GTAG"]
    except:
        pass
    else:
        env['pilotId'] = gtag
        print "pilot ID = %s" % env['pilotId']

    try:
        # warning: option o and k have diffierent meaning for pilot and runJob
        opts, args = getopt.getopt(argv, 'a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:E:F:G:H:I:')
    except getopt.GetoptError:
        print "Invalid arguments and options!"
        usage()
        os._exit(5)
        
    for o, a in opts:
        
        if o == "-a": 
            appdir = a
        
        elif o == "-b":
            try:
                env['debugLevel'] = int(a)
            except ValueError:
                print "debugLevel not an integer:", a

        elif o == "-c":
            try:
                env['maxNumberOfRecoveryAttempts'] = int(a)
            except ValueError:
                print "maxNumberOfRecoveryAttempts not an integer:", a
            else:
                if env['maxNumberOfRecoveryAttempts'] < 0:
                    env['maxNumberOfRecoveryAttempts'] = - env['maxNumberOfRecoveryAttempts']
        
        elif o == "-d":
            env['workdir'] = a.strip()
            if env['workdir'].startswith("{"): # is an env. variable
                try:
                    workdir_env = re.match('\{([^}]*)\}', env['workdir']). expand("\g<1>")
                    env['workdir'] = os.environ[workdir_env]
                    print "Getting the workdir from env. variable %s: %s" % (workdir_env, env['workdir'])
                except Exception, e:
                    print "Exception when trying to get the workdir from env. variable %s: %s" % (workdir_env, str(e))
                    # OSCER test:
                    # workdir = '/hep/data/griddata'
                    raise KeyError
        
        elif o == "-e": 
            env['logFileDir'] = a
        
        elif o == "-f":
            jreq = a
            if jreq.upper() == "JOBREC":
                env['jobrec'] = True
                env['jobRecoveryMode'] = True
            elif jreq.upper() == "TRUE":
                env['jobRequestFlag'] = True
            else:
                env['jobRequestFlag'] = False
        
        elif o == "-g": 
            env['inputDir'] = a
        
        elif o == "-h": 
            env['queuename'] = a
        
        elif o == "-i":
            if a == "PR" or a == "RC":
                env['pilot_version_tag'] = a
            else:
                print "Unknown pilot version tag: %s" % (a)
        
        elif o == "-j":
            jr = a
            if jr.upper() == "TRUE":
                env['jobrec'] = True
            else:
                env['jobrec'] = False
        
        elif o == "-k":
            try:
                env['memory'] = int(a)
            except ValueError:
                print "memory not an integer:", a

        elif o == "-l":
            wrFlag = a
            if wrFlag.upper() == "TRUE":
                env['wrapperFlag'] = True
            else:
                env['wrapperFlag'] = False
        
        elif o == "-m": 
            env['outputDir'] = a
        
        elif o == "-n":
            try:
                env['maxjobrec'] = int(a)
            except ValueError:
                print "maxjobrec not an integer:", a
            else:
                if env['maxjobrec'] < 0:
                    env['maxjobrec'] = env['maxjobrecDefaultenv']

        elif o == "-o": 
            env['countryGroup'] = a
        
        elif o == "-p":
            try:
                env['psport'] = int(a)
            except ValueError:
                print "psport not an integer:", a

        elif o == "-q": 
            env['dq2url'] = a
        
        elif o == "-r":
            rmwd = a
            if rmwd.upper() == "TRUE":
                env['rmwkdirenv'] = True
            elif rmwd.upper() == "FALSE":
                env['rmwkdir'] = False
        
        elif o == "-s": 
            env['sitename'] = a
        
        elif o == "-t":
            pc_flag = str(a)
            if pc_flag.upper() == "TRUE":
                env['proxycheckFlag'] = True
            else:
                env['proxycheckFlag'] = False

        elif o == "-u": 
            env['uflag'] = a
        
        elif o == "-v": 
            env['workingGroup'] = a
        
        elif o == "-w": 
            env['pshttpurl'] = a #"https://voatlas220.cern.ch"
        
        elif o == "-x":
            try:
                env['stageinretry'] = int(a)
            except ValueError:
                print "stageinretry not an integer:", a

        elif o == "-y":
            yflag = a
            if yflag.upper() == "TRUE":
                env['loggingMode'] = True
            elif yflag.upper() == "FALSE":
                env['loggingMode'] = False
            else:
                env['loggingMode'] = None
        
        elif o == "-z":
            update_flag = str(a)
            if update_flag.upper() == "TRUE":
                env['updateServerFlag'] = True
            else:
                env['updateServerFlag'] = False
        
        elif o == "-A":
            if a.upper() == "TRUE":
                env['allowOtherCountry'] = True
            else:
                env['allowOtherCountry'] = False
        
        elif o == "-B":
            if a.upper() == "FALSE":
                env['lfcRegistration'] = False
            else:
                env['lfcRegistration'] = True
        
        elif o == "-C":
            try:
                env['timefloor_default'] = int(a)
            except ValueError:
                print "timefloor_default not an integer:", a

        elif o == "-D":
            if a.upper() == "FALSE":
                env['useCoPilot'] = False
            else:
                env['useCoPilot'] = True
        
        elif o == "-E":
            try:
                env['stageoutretry'] = int(a)
            except ValueError:
                print "stageoutretry not an integer:", a

        elif o == "-F": 
            env['experiment'] = a

        elif o == "-G": 
            try:
                _getjobmaxtime = int(a)*60 # convert to seconds
            except ValueError:
                print "getjobmaxtime not an integer:", a
            else:
                if _getjobmaxtime > 1:
                    env['getjobmaxtime'] = _getjobmaxtime

        elif o == "-H":
            env['cache'] = a

        elif o == "-I":
            env['schedconfigURL'] = a

        else:
            print "Unknown option: %s (ignoring)" % o
            usage()

    # use sitename as queuename if queuename == ""
    if env['queuename'] == "":
        env['queuename'] = env['sitename']

    # force user jobs for ANALY sites
    if env['sitename'].startswith('ANALY_'):
        if env['uflag'] != 'user' and env['uflag'] != 'self' and env['uflag'] != 'ptest' and env['uflag'] != 'rucio_test':
            env['uflag'] = 'user'
            pUtil.tolog("Pilot user flag has been reset for analysis site (to value: %s)" % (env['uflag']))
        else:
            pUtil.tolog("Pilot user flag: %s" % str(env['uflag']))

def moveLostOutputFiles(job, thisSite, remaining_files):
    """
    Move all output files from lost job workdir to local DDM area
    """

    ec = 0
    error = PilotErrors()
    pilotErrorDiag = ""

    transExitCode = job.result[1]
    pUtil.chdir(job.datadir)

    # create the dataset dictionary before outFiles is overwritten
    # (if None, the dsname above will be used for all output files)
    datasetDict = pUtil.getDatasetDict(job.outFiles, job.destinationDblock, job.logFile, job.logDblock)
    if datasetDict:
        pUtil.tolog("Dataset dictionary has been verified")
    else:
        pUtil.tolog("Dataset dictionary could not be verified, output files will go to default dsname (below)")

    # reset the output file information
    job.outFiles = remaining_files
    job.outFilesGuids = []

    # see if it's an analysis job or not
    analJob = pUtil.isAnalysisJob(job.trf.split(",")[0])

    # recreate the guids
    for i in range (0, len(job.outFiles)):
        job.outFilesGuids.append(None)

    # open and parse xml to find the guids
    from xml.dom import minidom
    _filename = "%s/metadata-%s.xml" % (thisSite.workdir, job.jobId)
    if os.path.isfile(_filename):
        try:
            xmldoc = minidom.parse(_filename)
            _fileList = xmldoc.getElementsByTagName("File")
            pUtil.tolog("Processing %d output files" % (len(job.outFiles)))
            for thisfile in _fileList:
                gpfn = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
                guid = str(thisfile.getAttribute("ID"))
                for i in range(0, len(job.outFiles)):
                    if job.outFiles[i] == gpfn:
                        job.outFilesGuids[i] = guid
                        pUtil.tolog("Guid %s belongs to file %s" % (guid, gpfn))
        except Exception, e:
            pUtil.tolog("!!FAILED!!1105!! Could not parse the metadata - guids unknown")
            job.setState(["failed", transExitCode, error.ERR_LOSTJOBPFC])
            pUtil.chdir(thisSite.workdir)
            return -1
        else:
            pUtil.tolog("Successfully read %s" % (_filename))
    else:
        pUtil.tolog("!!FAILED!!1105!! Could not find %s - guids unknown" % (_filename))
        job.setState(["failed", transExitCode, error.ERR_LOSTJOBPFC])
        pUtil.chdir(thisSite.workdir)
        return -1

    pUtil.tolog("Remaining files:")
    pUtil.dumpOrderedItems(remaining_files)
    pUtil.tolog("Guids for remaining files:")
    pUtil.dumpOrderedItems(job.outFilesGuids)

    # get the experiment name
    experiment = job.experiment

    # recreate the OutPutFileCatalog.xml
    file_name = "OutPutFileCatalog.xml"
    file_path = os.path.join(thisSite.workdir, file_name)
    try:
        guids_status = pUtil.PFCxml(experiment, file_path, remaining_files, fguids=job.outFilesGuids, fntag="pfn", analJob=analJob, jr=True)
    except Exception, e:
        pUtil.tolog("!!FAILED!!1105!! Exception caught (Could not generate xml for the remaining output files): %s" % str(e))
        job.setState(["failed", transExitCode, error.ERR_LOSTJOBXML])
        pUtil.chdir(thisSite.workdir)
        return -1
    else:
        if not guids_status:
            pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
            pUtil.tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_MISSINGGUID
        else:
            pUtil.tolog("Successfully read: %s" % (file_path))
    if job.destinationDblock and job.destinationDblock[0] != 'NULL' and job.destinationDblock[0] != ' ':
        dsname = job.destinationDblock[0]
    else:
        dsname = "%s-%s-%s" % (time.localtime()[0:3])

    if not datasetDict:
        pUtil.tolog("Output files will go to default dataset: %s" % (dsname))

    # the cmtconfig is needed by at least the xrdcp site mover
    cmtconfig = pUtil.getCmtconfig(job.cmtconfig)

    tin_0 = os.times()
    rf = None
    _state = ""
    _msg = ""
    try:
        # Note: alt stage-out numbers are not saved in recovery mode (job object not returned from this function)
        rc, pilotErrorDiag, rf, rs, job.filesNormalStageOut, job.filesAltStageOut = mover.mover_put_data("xmlcatalog_file:%s" % (file_path), dsname, 
                                                          thisSite.sitename, ub=thisSite.dq2url, analysisJob=analJob,
                                                          proxycheck=env['proxycheckFlag'], spsetup=job.spsetup,scopeOut=job.scopeOut, scopeLog=job.scopeLog,
                                                          token=job.destinationDBlockToken, pinitdir=env['pilot_initdir'],
                                                          datasetDict=datasetDict, prodSourceLabel=job.prodSourceLabel,
                                                          jobId=job.jobId, jobWorkDir=job.workdir, DN=job.prodUserID,
                                                          dispatchDBlockTokenForOut=job.dispatchDBlockTokenForOut, 
                                                          lfcreg=env['lfcRegistration'], jobCloud=job.cloud, logFile=job.logFile, 
                                                          stageoutTries=env['stageoutretry'], experiment=experiment, 
                                                          cmtconfig=cmtconfig, recoveryWorkDir=thisSite.workdir, 
                                                          fileDestinationSE=job.fileDestinationSE)
    except Exception, e:
        pilotErrorDiag = "Put function can not be called for staging out: %s" % str(e)
        pUtil.tolog("!!%s!!1105!! %s" % (env['errorLabel'], pilotErrorDiag))
        ec = error.ERR_PUTFUNCNOCALL
        _state = "holding"
        _msg = env['errorLabel']
    else:
        if pilotErrorDiag != "":
            pilotErrorDiag = "Put error: " + pUtil.tailPilotErrorDiag(pilotErrorDiag, size=256-len("pilot: Put error: "))

        pUtil.tolog("Put function returned code: %d" % (rc))
        if rc != 0:
            # remove any trailing "\r" or "\n" (there can be two of them)
            if rs != None:
                rs = rs.rstrip()
                pUtil.tolog(" Error string: %s" % (rs))

            # is the job recoverable?
            if error.isRecoverableErrorCode(rc):
                _state = "holding"
                _msg = "WARNING"
            else:
                _state = "failed"
                _msg = env['errorLabel']

            # look for special error in the error string
            if rs == "Error: string Limit exceeded 250":
                pUtil.tolog("!!%s!!3000!! Put error: file name string limit exceeded 250" % (_msg))
                ec = error.ERR_LRCREGSTRSIZE
            else:
                ec = rc

            pUtil.tolog("!!%s!! %s" % (_msg, error.getErrorStr(rc)))
        else:
            # create a weak lockfile meaning that file transfer worked, and all output files have now been transferred
            pUtil.createLockFile(True, thisSite.workdir, lockfile="ALLFILESTRANSFERRED")

    # finish the time measurement of the stage-out
    tin_1 = os.times()
    job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

    # set the error codes in case of failure
    job.pilotErrorDiag = pilotErrorDiag
    if ec != 0:
        pUtil.tolog("!!%s!!2999!! %s" % (_msg, pilotErrorDiag))
        job.setState([_state, transExitCode, ec])

    pUtil.chdir(thisSite.workdir)
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
        pUtil.tolog("!!WARNING!!1000!! Final state not defined - job was run by older pilot version")
        
        # job has finished if pilotErrorCode is in the allowed list or recoverable jobs
        # get the pilot error diag
        error = PilotErrors()
        if job.result[1] == 0 and error.isRecoverableErrorCode(job.result[2]):
            state = True
    if state:
        pUtil.tolog("Final job state: finished")
    else:
        pUtil.tolog("Final job state: failed")

    return state

def runJobRecovery(thisSite, _psport, extradir):
    """
    run the lost job recovery algorithm
    """

    tmpdir = os.getcwd()

    # check queuedata for external recovery directory
    recoveryDir = "" # an empty recoveryDir means that recovery should search local WN disk for lost jobs
    try:
        recoveryDir = pUtil.readpar('recoverdir')
    except:
        pass
    else:
        # make sure the recovery directory actually exists (will not be added to dir list if empty)
        recoveryDir = pUtil.verifyRecoveryDir(recoveryDir)

    # run job recovery
    dirs = [ "" ]
    if recoveryDir != "":
        dirs.append(recoveryDir)
        pUtil.tolog("Job recovery will scan both local disk and external disk")
    if extradir != "":
        if extradir not in dirs:
            dirs.append(extradir)
            pUtil.tolog("Job recovery will also scan extradir (%s)" % (extradir))

    dircounter = 0
    for _dir in dirs:
        dircounter += 1
        pUtil.tolog("Scanning for lost jobs [pass %d/%d]" % (dircounter, len(dirs)))
        try:
            found_lost_jobs = RecoverLostJobs(_dir, thisSite, _psport)
        except Exception, e:
            pUtil.tolog("!!WARNING!!1999!! Failed during search for lost jobs: %s" % str(e))
        else:
            pUtil.tolog("Recovered/Updated %d lost job(s)" % (found_lost_jobs))
    pUtil.chdir(tmpdir)

def testExternalDir(recoveryDir):
    """
    try to write a temp file in the external recovery dir
    """

    status = True

    testFile = "%s/testFile-%s.tmp" % (recoveryDir, pUtil.getGUID())
    ec, rv = commands.getstatusoutput("touch %s" % (testFile))
    if ec != 0:
        pUtil.tolog("!!WARNING!!1190!! Could not write test file to recovery dir (%s): %d, %s" % (testFile, ec, rv))
        pUtil.tolog("!!WARNING!!1190!! Aborting move to external disk. Holding job will remain on local disk")
        status = False
    else:
        pUtil.tolog("Successfully created a test file on the external disk (will now proceed with transferring the holding job)")
        ec, rv = commands.getstatusoutput("ls -l %s; rm %s" % (testFile, testFile))
        if ec != 0:
            pUtil.tolog("!!WARNING!!1190!! Could not remove test file (%s): %d, %s (ignore since write succeeded)" %\
                  (testFile, ec, rv))

    return status

def createAtomicLockFile(file_path):
    """ Create an atomic lockfile while probing this dir to avoid a possible race-condition """

    lockfile_name = os.path.join(os.path.dirname(file_path), "ATOMIC_LOCKFILE")
    try:
        # acquire the lock
        fd = os.open(lockfile_name, os.O_EXCL|os.O_CREAT)
    except OSError:
        # work dir is locked, so exit
        pUtil.tolog("Found lock file: %s (skip this dir)" % (lockfile_name))
        fd = None
    else:
        pUtil.tolog("Created lock file: %s" % (lockfile_name))
    return fd, lockfile_name

def releaseAtomicLockFile(fd, lockfile_name):
    """ Release the atomic lock file """

    try:
        os.close(fd)
        os.unlink(lockfile_name)
    except Exception, e:
        if "Bad file descriptor" in str(e):
            pUtil.tolog("Lock file already released")
        else:
            pUtil.tolog("WARNING: Could not release lock file: %s" % str(e))
    else:
        pUtil.tolog("Released lock file: %s" % (lockfile_name))

def RecoverLostJobs(recoveryDir, thisSite, _psport):
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
        pUtil.tolog("Recovery algorithm will search external dir for lost jobs: %s" % (dir_path))
    else:
        dir_path = thisSite.wntmpdir
        if dir_path == "":
            dir_path = "/tmp" # reset to default
        pUtil.tolog("Recovery algorithm will search local WN disk for lost jobs: %s" % (dir_path))

    currentDir = os.getcwd()
    try:
        os.path.isdir(dir_path)
    except:
        pUtil.tolog("!!WARNING!!1100!! No such dir path (%s)" % (dir_path))
    else:
        JS = JobState()
        # grab all job state files in all work directories
        job_state_files = glob(dir_path + "/Panda_Pilot_*/jobState-*.pickle")

        # purge any test job state files (testing for new job rec algorithm)
        job_state_files = pUtil.removeTestFiles(job_state_files, mode="default")

        pUtil.tolog("Number of found job state files: %d" % (len(job_state_files)))

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
                            pUtil.tolog("Found %s created at %d" % (lockFileName, os.path.getmtime(dirname + "/LOCKFILE")))
                        except Exception, e:
                            pUtil.tolog("!!WARNING!!1100!! (could not read modification time of %s): %s" % (dirname + "/LOCKFILE", str(e)))

                file_nr += 1
                if file_nr > env['maxjobrec']:
                    pUtil.tolog("Maximum number of job recoveries exceeded for this pilot: %d" % (env['maxjobrec']))
                    # release the atomic lockfile and go to the next directory
                    releaseAtomicLockFile(fd, lockfile_name)
                    break
                pUtil.tolog("Processing job state file %d/%d: %s" % (file_nr, len(job_state_files), file_path))
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
                    if (current_time - file_modification_time) > 2*env['heartbeatPeriod']:
                        # found lost job recovery file
                        pUtil.tolog("File was last modified %d seconds ago (limit=%d, t=%d, tmod=%d)" %\
                              (current_time - file_modification_time, 2 * env['heartbeatPeriod'], current_time, file_modification_time))
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
                                if not pUtil.isSameType(_job.trf.split(",")[0], env['uflag']):
                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue

                                #PN
                                # uncomment this code for recovery of certain panda ids only
#                                allowedJobIds = ['1435974131']
#                                if _job.jobId not in allowedJobIds:
#                                    pUtil.tolog("Job id %s not in allowed id list: %s" % (_job.jobId, str(allowedJobIds)))
#                                    continue

                            if _job and _site and _node:
                                pUtil.tolog("Stored job state: %s" % (_job.result[0]))
                                # query the job state file for job information
                                if _job.result[0] == 'holding' or _job.result[0] == 'lostheartbeat':
                                    pUtil.tolog("(1)")

                                    pUtil.tolog("Job %s is currently in state \'%s\' (according to job state file - recover)" %\
                                          (_job.jobId, _job.result[0]))
                                elif _job.result[0] == 'failed':
                                    pUtil.tolog("(2)")
                                    pUtil.tolog("Job %s is currently in state \'%s\' (according to job state file - skip)" %\
                                          (_job.jobId, _job.result[0]))

                                    pUtil.tolog("Further recovery attempts will be prevented for this job (will leave work dir)")
                                    if not JS.rename(_site, _job):
                                        pUtil.tolog("(Fate of job state file left for next pilot)")

                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue
                                else:
                                    pUtil.tolog("(3) Not enough information in job state file, query server")
                                    
                                    # get job status from server
                                    jobStatus, jobAttemptNr, jobStatusCode = pUtil.getJobStatus(_job.jobId, env['pshttpurl'], _psport, env['pilot_initdir'])

                                    # recover this job?
                                    if jobStatusCode == 20:
                                        pUtil.tolog("Received general error code from dispatcher call (leave job for later pilot)")
                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue
                                    elif not (jobStatus == 'holding' and jobStatusCode == 0):
                                        pUtil.tolog("Job %s is currently in state \'%s\' with attemptNr = %d (according to server - will not be recovered)" %\
                                              (_job.jobId, jobStatus, jobAttemptNr))

                                        if _job.attemptNr != jobAttemptNr or jobStatus == "transferring" or jobStatus == "failed" or \
                                               jobStatus == "notfound" or jobStatus == "finished" or "tobekilled" in _job.action:
                                            pUtil.tolog("Further recovery attempts will be prevented for this job")
                                            if not JS.rename(_site, _job):
                                                pUtil.tolog("(Fate of job state file left for next pilot)")
                                            else:
                                                if not JS.cleanup():
                                                    pUtil.tolog("!!WARNING!!1110!! Failed to cleanup")
                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue
                                    else:
                                        # is the attemptNr defined?
                                        try:
                                            attemptNr = _job.attemptNr
                                        except Exception, e:
                                            pUtil.tolog("!!WARNING!!1100!! Attempt number not defined [ignore]: %s" % str(e))
                                        else:
                                            # check if the attemptNr (set during initial getJob command) is the same
                                            # as the current jobAttemptNr from the server (protection against failed lost
                                            # heartbeat jobs due to reassigned panda job id numbers)
                                            if attemptNr != jobAttemptNr:
                                                pUtil.tolog("!!WARNING!!1100!! Attempt number mismatch for job %s (according to server - will not be recovered)" %\
                                                      (_job.jobId))
                                                pUtil.tolog("....Initial attempt number: %d" % (attemptNr))
                                                pUtil.tolog("....Current attempt number: %d" % (jobAttemptNr))
                                                pUtil.tolog("....Job status (server)   : %s" % (jobStatus))
                                                pUtil.tolog("....Job status (state)    : %s" % (_job.result[0]))
                                                pUtil.tolog("Further recovery attempts will be prevented for this job")
                                                if not JS.rename(_site, _job):
                                                    pUtil.tolog("(Fate of job state file left for next pilot)")
                                                else:
                                                    if not JS.cleanup():
                                                        pUtil.tolog("!!WARNING!!1110!! Failed to cleanup")
                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue
                                            else:
                                                pUtil.tolog("Attempt numbers from server and job state file agree: %d" % (attemptNr))
                                        # the job state as given by the dispatcher should only be different from that of
                                        # the job state file for 'lostheartbeat' jobs. This state is only set like this
                                        # in the job state file. The dispatcher will consider it as a 'holding' job.
                                        pUtil.tolog("Job %s is currently in state \'%s\' (according to job state file: \'%s\') - recover" %\
                                              (_job.jobId, jobStatus, _job.result[0]))

                                # only attempt recovery if the lost job ran on the same site as the current pilot
                                # (to avoid problems on two sites with shared WNs)
                                if _site.sitename == thisSite.sitename:
                                    pUtil.tolog("Verified that lost job ran on the same site as the current pilot")
                                else:
                                    pUtil.tolog("Aborting job recovery since the lost job ran on site %s but the current pilot is running on %s" % (_site.sitename, thisSite.sitename))
                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue

                                pUtil.chdir(_site.workdir)

                                # abort if max number of recovery attempts has been exceeded
                                if _recoveryAttempt > env['maxNumberOfRecoveryAttempts'] - 1:
                                    pUtil.tolog("!!WARNING!!1100!! Max number of recovery attempts exceeded: %d" % (env['maxNumberOfRecoveryAttempts']))
                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBMAXEDOUT])
                                    rt, retNode = updatePandaServer(_job, _site, _psport, ra = _recoveryAttempt, schedulerID = env['jobSchedulerId'], pilotID = env['pilotId'])
                                    if rt == 0:
                                        number_of_recoveries += 1
                                        pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                        # did the server send back a command?
                                        if "tobekilled" in _job.action:
                                            pUtil.tolog("!!WARNING!!1100!! Panda server returned a \'tobekilled\' command")

                                        pUtil.tolog("NOTE: This job has been terminated. Will now remove workdir.")
                                        if not JS.cleanup():
                                            pUtil.tolog("!!WARNING!!1110!! Failed to cleanup")

                                    else:
                                        pUtil.tolog("Panda server returned a %d" % (rt))
                                        pUtil.tolog("(Failed to update panda server - leave for next pilot)")

                                    # release the atomic lockfile and go to the next directory
                                    releaseAtomicLockFile(fd, lockfile_name)
                                    continue
                                else:
                                    # increase recovery attempt counter
                                    _recoveryAttempt += 1
                                pUtil.tolog("Recovery attempt: %d" % (_recoveryAttempt))

                                # update job state file at this point to prevent a parallel pilot from doing a simultaneous recovery
                                _retjs = pUtil.updateJobState(_job, _site, _node, _recoveryAttempt)

                                # should any file be registered? (data dirs will not exist in the following checks
                                # since late registration requires that all files have already been transferred)
                                # (Note: only for LRC sites)
#                                rc = checkForLateRegistration(thisSite.dq2url, _job, _site, _node, type="output")
#                                if rc == False:
#                                    pUtil.tolog("Resume this rescue operation later due to the previous errors")
#                                    # release the atomic lockfile and go to the next directory
#                                    releaseAtomicLockFile(fd, lockfile_name)
#                                    continue
#                                rc = checkForLateRegistration(thisSite.dq2url, _job, _site, _node, type="log")
#                                if rc == False:
#                                    pUtil.tolog("Resume this rescue operation later due to the previous errors")
#                                    # release the atomic lockfile and go to the next directory
#                                    releaseAtomicLockFile(fd, lockfile_name)
#                                    continue

                                # does log exist?
                                logfile = "%s/%s" % (_site.workdir, _job.logFile)
                                if os.path.exists(logfile):

                                    logfileAlreadyCopied = pUtil.isLogfileCopied(_site.workdir)
                                    if logfileAlreadyCopied:
                                        pUtil.tolog("Found log file        : %s (already transferred)" % (logfile))
                                    else:
                                        pUtil.tolog("Found log file        : %s" % (logfile))

                                    # does data dir exist?
                                    if os.path.isdir(_job.datadir):

                                        pUtil.tolog("Found data dir        : %s" % (_job.datadir))
                                        pUtil.chdir(_job.datadir)

                                        # do output files exist?
                                        remaining_files = pUtil.getRemainingOutputFiles(_job.outFiles)
                                        pUtil.tolog("Number of data files  : %d" % (len(remaining_files)))
                                        if remaining_files:

                                            # get the metadata
                                            strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                            # extract the outFilesGuids from the xml in case of build jobs
                                            # (_job.outFilesGuids has not been set for those, the guid is kept in the xml)
                                            if pUtil.isBuildJob(remaining_files):
                                                # match the guid in the metadata with the single file in remaining_files
                                                _guids = pUtil.getGuidsFromXML(_site.workdir, id=_job.jobId, filename=remaining_files[0])
                                                if len(_guids) == 1:
                                                    _job.outFilesGuids = _guids
                                                else:
                                                    pUtil.tolog("Warning: could not identify guid in metadata")
                                                    pUtil.tolog("Remaining files: %s" % str(remaining_files))
                                                    pUtil.tolog("_guids: %s" % str(_guids))
                                                    pUtil.tolog("metadata: \n%s" % (strXML))

                                            # can output files be moved?
                                            pUtil.tolog("Trying to move data files")
                                            ec = moveLostOutputFiles(_job, _site, remaining_files)

                                            skip_for_now = False
                                            if ec != 0:
                                                if ec == error.ERR_MISSINGGUID:
                                                    pUtil.tolog("!!FAILED!!1110!! Could not move lost output files to local DDM due to missing guid")
                                                    _job.finalstate = "failed"
                                                elif _job.result[2] == error.ERR_LOSTJOBPFC:
                                                    pUtil.tolog("!!WARNING!!1110!! Could not move lost output files to local DDM due to PoolFileCatalog read failure")
                                                    _job.finalstate = "failed"
                                                elif _job.result[2] == error.ERR_LOSTJOBXML:
                                                    pUtil.tolog("!!WARNING!!1110!! Could not move lost output files to local DDM due to xml generation failure")
                                                    _job.finalstate = "failed"
                                                else:
                                                    pUtil.tolog("!!WARNING!!1110!! Could not move lost output files to local DDM (leave for next pilot)")
                                                    skip_for_now = True
                                                pass
                                            else:
                                                pUtil.tolog("Remaining data files moved to SE")

                                                pUtil.chdir(_site.workdir)

                                                # remove data dir
                                                if pUtil.remove([_job.datadir]):
                                                    pUtil.tolog("Removed data dir")
                                                else:
                                                    pUtil.tolog("!!WARNING!!1110!! Failed to remove data dir")
    
                                            # can log be registered?
                                            ret, _job = transferLogFile(_job, _site, jr=True)
                                            if not ret:
                                                pUtil.tolog("!!WARNING!!1110!! Could not register lost job log file (state set to holding)")
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
                                                            pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                    else:
                                                        pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                              (_job.result[1], _job.result[2]))
                                                        if _job.result[2] != error.ERR_LOSTJOBPFC and _job.result[2] != error.ERR_LOSTJOBXML: # state already set for these codes
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                else:
                                                    # there were problems with the file transfer to the local DDM
                                                    _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBFILETRANSFER])

                                            # update the server
                                            rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr=strXML, 
                                                                            ra=_recoveryAttempt, schedulerID = env['jobSchedulerId'], 
                                                                            pilotID = env['pilotId'])
                                            if rt == 0:
                                                number_of_recoveries += 1
                                                pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                # did the server send back a command?
                                                if "tobekilled" in _job.action:
                                                    pUtil.tolog("!!WARNING!!1110!! Panda server returned a \'tobekilled\' command")
                                                    _job.result[0] = "failed"

                                                # only cleanup work dir if no error code has been set
                                                if _job.result[0] == "finished" or _job.result[0] == "failed":
                                                    pUtil.chdir(currentDir)
                                                    if not JS.cleanup():
                                                        pUtil.tolog("!!WARNING!!1110!! Failed to cleanup")
                                                        # release the atomic lockfile and go to the next directory
                                                        releaseAtomicLockFile(fd, lockfile_name)
                                                        continue

                                            else:
                                                pUtil.tolog("!!WARNING!!1110!! Panda server returned a %d" % (rt))

                                                # store the final state so that the next pilot will know
                                                # store the metadata xml
                                                retNode['xml'] = strXML

                                                # update the job state file with the new state information
                                                _retjs = pUtil.updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        else: # output files do not exist

                                            pUtil.chdir(_site.workdir)

                                            # can log be registered?
                                            ret, _job = transferLogFile(_job, _site, jr=True)
                                            if not ret:
                                                pUtil.tolog("!!WARNING!!1110!! Could not register lost job log file (state set to holding)")
                                                _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBLOGREG])
                                            else:
                                                # set new exit code
                                                if FinishedJob(_job):
                                                    if pUtil.verifyTransfer(_site.workdir):
                                                        _job.setState(['finished', 0, 0])
                                                    else:
                                                        pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                        _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                else:
                                                    pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" 
                                                          %(_job.result[1], _job.result[2]))
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                
                                            # get the metadata
                                            strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                            # update the server
                                            rt, retNode = updatePandaServer(_job, _site, _psport, 
                                                                            xmlstr=strXML, ra=_recoveryAttempt, 
                                                                            schedulerID = env['jobSchedulerId'], 
                                                                            pilotID = env['pilotId'])
                                            if rt == 0:
                                                number_of_recoveries += 1

                                                # did the server send back a command?
                                                if "tobekilled" in _job.action:
                                                    pUtil.tolog("!!WARNING!!1110!! Panda server returned a \'tobekilled\' command")
                                                    _job.result[0] = "failed"

                                                pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                                if _job.result[0] == 'finished' or _job.result[0] == "failed":
                                                    if not JS.cleanup():
                                                        pUtil.tolog("!!WARNING!!1110!! Failed to cleanup")
                                                        # release the atomic lockfile and go to the next directory
                                                        releaseAtomicLockFile(fd, lockfile_name)
                                                        continue

                                            else:
                                                pUtil.tolog("!!WARNING!!1110!! Panda server returned a %d" % (rt))

                                                # store the final state so that the next pilot will know
                                                # store the metadata xml
                                                retNode['xml'] = strXML

                                                # update the job state file with the new state information
                                                _retjs = pUtil.updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue

                                    else: # data dir does not exist

                                        pUtil.tolog("Data dir already deleted for this lost job (or never existed)")

                                        # can log be registered?
                                        ret, _job = transferLogFile(_job, _site, jr=True)
                                        if not ret:
                                            pUtil.tolog("!!WARNING!!1120!! Could not register lost job log file (state set to holding)")
                                            _job.setState(['holding', _job.result[1], error.ERR_LOSTJOBLOGREG])
                                        else:
                                            # set exit code if lost job exited correctly
                                            if FinishedJob(_job):
                                                if pUtil.verifyTransfer(_site.workdir):
                                                    _job.setState(['finished', 0, 0])
                                                else:
                                                    pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                            else:
                                                pUtil.tolog("!!WARNING!!1120!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                      (_job.result[1], _job.result[2]))
                                                _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])

                                        # was xml saved?
                                        strXML = ''
                                        try:
                                            strXML = _node['xml']
                                        except:
                                            pUtil.tolog("XML could not be found - try to read from file")
                                            strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                        # update the server
                                        rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr=strXML, 
                                                                        ra = _recoveryAttempt, 
                                                                        schedulerID = env['jobSchedulerId'],
                                                                        pilotID = env['pilotId'])
                                        if rt == 0:
                                            number_of_recoveries += 1
                                            pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                            # only cleanup work dir if no error code has been set
                                            if _job.result[0] == 'finished':
                                                if not JS.cleanup():
                                                    pUtil.tolog("!!WARNING!!1120!! Failed to cleanup")

                                            # did the server send back a command?
                                            if "tobekilled" in _job.action:
                                                pUtil.tolog("!!WARNING!!1120!! Panda server returned a \'tobekilled\' command")
                                                _job.result[0] = "failed"
                                                
                                            # further recovery attempt unnecessary, but keep the work dir for debugging
                                            if _job.result[0] == "failed":
                                                pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                if not JS.rename(_site, _job):
                                                    pUtil.tolog("(Fate of job state file left for next pilot)")

                                            # release the atomic lockfile and go to the next directory
                                            releaseAtomicLockFile(fd, lockfile_name)
                                            continue

                                        else:
                                            pUtil.tolog("!!WARNING!!1120!! Panda server returned a %d" % (rt))

                                            # store the final state so that the next pilot will know

                                            # store the metadata xml
                                            retNode['xml'] = strXML

                                            # update the job state file with the new state information
                                            _retjs = pUtil.updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue

                                else: # log file does not exist
                                    pUtil.tolog("Log file already deleted for this job")

                                    # does work dir exist?
                                    if os.path.isdir(_job.newDirNM):

                                        pUtil.tolog("Found renamed work dir: %s" % (_job.newDirNM))
                                        # is exit code set?
                                        ecSet = False
                                        if not _job.result[2]:
                                            # can exit code be read from file?
                                            _ec = pUtil.getExitCode(_job.newDirNM, "pilotlog.txt")
                                            if _ec == -1:
                                                pUtil.tolog("!!WARNING!!1130!! Could not read exit code from file: %s" % (_job.newDirNM + "pilotlog.txt"))
                                                _job.setState(['failed', 0, error.ERR_LOSTJOBNOTFINISHED]) # lost job never finished
                                            else:
                                                ecSet = True
                                        else:
                                            _ec = _job.result[2]
                                            ecSet = True

                                        # EC was not set and could not be read from file
                                        if not ecSet:
                                            pUtil.tolog("Exit code not found")

                                            # get the metadata
                                            # this metadata does not contain the metadata for the log
                                            strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                            # update the server
                                            rt, retNode = updatePandaServer(_job, _site, _psport, 
                                                                            xmlstr = strXML, ra = _recoveryAttempt, 
                                                                            schedulerID = env['jobSchedulerId'], 
                                                                            pilotID = env['pilotId'])
                                            if rt == 0:
                                                number_of_recoveries += 1
                                                pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                # this job can never be recovered - delete work dir
                                                if not JS.cleanup():
                                                    pUtil.tolog("!!WARNING!!1130!! Failed to cleanup")
                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                                # did the server send back a command?
                                                if "tobekilled" in _job.action:
                                                    pUtil.tolog("!!WARNING!!1130!! Panda server returned a \'tobekilled\' command")
                                                    _job.result[0] = "failed"

                                                # further recovery attempt unnecessary, but keep the work dir for debugging
                                                if _job.result[0] == "failed":
                                                    pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                    if not JS.rename(_site, _job):
                                                        pUtil.tolog("(Fate of job state file left for next pilot)")

                                            else:
                                                pUtil.tolog("!!WARNING!!1130!! Panda server returned a %d" % (rt))

                                                # store the final state so that the next pilot will know
                                                # store the metadata xml
                                                retNode['xml'] = strXML

                                                # update the job state file with the new state information
                                                _retjs = pUtil.updateJobState(_job, _site, retNode, _recoveryAttempt)

                                        else: # EC was set or could be read from file
                                            pUtil.tolog("Found exit code       : %d" % (_ec))
                                            if _ec == 0:
                                                # does data directory exist?
                                                if os.path.isdir(_job.datadir):
                                                    pUtil.tolog("Found data dir        : %s" % (_job.datadir))
                                                    pUtil.chdir(_job.datadir)

                                                    # do output files exist?
                                                    remaining_files = pUtil.getRemainingOutputFiles(_job.outFiles)
                                                    pUtil.tolog("Number of data files  : %d" % (len(remaining_files)))
                                                    if remaining_files:
                                                        # can output files be moved?
                                                        pUtil.tolog("Trying to move data files")
                                                        ec = moveLostOutputFiles(_job, _site, remaining_files)
                                                        pUtil.chdir(_site.workdir)
                                                        if ec != 0:
                                                            if ec == error.ERR_MISSINGGUID:
                                                                pUtil.tolog("!!FAILED!!1130!! Could not move lost output files to local DDM due to missing guid")
                                                                _job.finalstate = "failed"
                                                            elif _job.result[2] == error.ERR_LOSTJOBPFC: # missing PoolFileCatalog
                                                                pUtil.tolog("!!WARNING!!1130!! Could not move lost output files to local DDM due to PoolFileCatalog read failure")
                                                                _job.finalstate = "failed"
                                                            elif _job.result[2] == error.ERR_LOSTJOBXML: # could not generate xml file
                                                                pUtil.tolog("!!WARNING!!1130!! Could not move lost output files to local DDM due xml generation failure")
                                                                _job.finalstate = "failed"
                                                            else:
                                                                pUtil.tolog("!!WARNING!!1130!! Could not move lost output files to local DDM (leave for next pilot)")
                                                                _job.setState(['holding', 0, error.ERR_LOSTJOBFILETRANSFER])

                                                            # do not delete work dir (leave it for the next pilot to try again)

                                                            # get the metadata
                                                            strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                                            # update the server
                                                            rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr = strXML, 
                                                                                            ra = _recoveryAttempt, 
                                                                                            schedulerID = env['jobSchedulerId'], 
                                                                                            pilotID = env['pilotId'])
                                                            if rt == 0:
                                                                pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                                # did the server send back a command?
                                                                if "tobekilled" in _job.action:
                                                                    pUtil.tolog("!!WARNING!!1130!! Panda server returned a \'tobekilled\' command")
                                                                    _job.result[0] = "failed"

                                                                # further recovery attempt unnecessary, but keep the work dir for debugging
                                                                if _job.result[0] == "failed":
                                                                    pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                                    if not JS.rename(_site, _job):
                                                                        pUtil.tolog("(Fate of job state file left for next pilot)")

                                                            else:
                                                                pUtil.tolog("!!WARNING!!1130!! Failed to update Panda server for job %s (exit code %d)" %\
                                                                      (_job.jobId, _job.result[2]))
                                                            # release the atomic lockfile and go to the next directory
                                                            releaseAtomicLockFile(fd, lockfile_name)
                                                            continue

                                                        else: # output files could be moved

                                                            pUtil.tolog("Remaining data files moved to SE")
                                                            pUtil.chdir(_site.workdir)

                                                            # remove data dir
                                                            if pUtil.remove([_job.datadir]):
                                                                pUtil.tolog("Removed data dir")
                                                            else:
                                                                pUtil.tolog("!!WARNING!!1130!! Failed to remove data dir")

                                                            # create log file and update panda server
                                                            pUtil.postJobTask(_job, _site, env['workerNode'], env['experiment'], jr=True, ra=_recoveryAttempt)                                                            
                                                            number_of_recoveries += 1
                                                            # release the atomic lockfile and go to the next directory
                                                            releaseAtomicLockFile(fd, lockfile_name)
                                                            continue

                                                    else: # output files do not exist

                                                        if FinishedJob(_job):
                                                            if pUtil.verifyTransfer(_site.workdir):
                                                                _job.setState(['finished', 0, 0])
                                                            else:
                                                                pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                                _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                        else:
                                                            pUtil.tolog("!!WARNING!!1130!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                                  (_job.result[1], _job.result[2]))
                                                            # failed since output files do not exist
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBFILETRANSFER])

                                                        # create log file and update panda server
                                                        pUtil.postJobTask(_job, _site, env['workerNode'], env['experiment'], jr=True, ra=_recoveryAttempt)
                                                        number_of_recoveries += 1
                                                        # release the atomic lockfile and go to the next directory
                                                        releaseAtomicLockFile(fd, lockfile_name)
                                                        continue

                                                else: # data dir does not exist
                                                    pUtil.tolog("No data dir for this lost job")

                                                    # create log file and update panda server
                                                    pUtil.postJobTask(_job, _site, env['workerNode'], env['experiment'], jr=True, ra=_recoveryAttempt)
                                                    number_of_recoveries += 1
                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                            else: # EC != 0 
                                                # create log file and update panda server
                                                pUtil.postJobTask(_job, _site, env['workerNode'], env['experiment'], jr=True, ra=_recoveryAttempt)
                                                number_of_recoveries += 1
                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue

                                        # release the atomic lockfile and go to the next directory
                                        releaseAtomicLockFile(fd, lockfile_name)
                                        continue

                                    else: # work dir does not exist

                                        pUtil.tolog("Work dir does not exist (log probably already transferred)")

                                        # should an old log be registered? (log file will not exist since late registration requires
                                        # that the log has already been transferred)
#                                        rc = checkForLateRegistration(thisSite.dq2url, _job, _site, _node, type="log")
#                                        if rc == False:
#                                            pUtil.tolog("Resume this rescue operation later since the LRC is not working (log could not be registered)")
#                                            continue

                                        # does data directory exist?
                                        if os.path.isdir(_job.datadir):

                                            pUtil.tolog("Found data dir: %s" % (_job.datadir))
                                            pUtil.chdir(_job.datadir)

                                            # do output files exist?
                                            remaining_files = pUtil.getRemainingOutputFiles(_job.outFiles)
                                            pUtil.tolog("Number of data files: %d" % (len(remaining_files)))
                                            if remaining_files:
                                                # can output files be moved?
                                                pUtil.tolog("Trying to move data files")
                                                ec = moveLostOutputFiles(_job, _site, remaining_files)
                                                pUtil.chdir(_site.workdir)
                                                if ec != 0:
                                                    if ec == error.ERR_MISSINGGUID:
                                                        pUtil.tolog("!!WARNING!!1140!! Could not move lost output files to local DDM due to missing guid")
                                                        _job.finalstate = "failed"
                                                    elif _job.result[2] == error.ERR_LOSTJOBPFC: # missing PoolFileCatalog
                                                        pUtil.tolog("!!WARNING!!1140!! Could not move lost output files to local DDM due to PoolFileCatalog read failure")
                                                        _job.finalstate = "failed"
                                                    elif _job.result[2] == error.ERR_LOSTJOBXML: # could not generate xml file
                                                        pUtil.tolog("!!WARNING!!1140!! Could not move lost output files to local DDM due to xml generation failure")
                                                        _job.finalstate = "failed"
                                                    else:
                                                        pUtil.tolog("!!WARNING!!1140!! Could not move lost output files to local DDM (leave for next pilot)")
                                                        _job.setState(['holding', 0, error.ERR_LOSTJOBFILETRANSFER])

                                                    # do not delete data dir (leave it for the next pilot to try again) unless failed

                                                    # get the metadata
                                                    strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                                    # update the server
                                                    rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr = strXML, 
                                                                                    ra = _recoveryAttempt, 
                                                                                    schedulerID = env['jobSchedulerId'],
                                                                                    pilotID = env['pilotId'])
                                                    if rt == 0:
                                                        pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                        # did the server send back a command?
                                                        if "tobekilled" in _job.action:
                                                            pUtil.tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                            _job.result[0] = "failed"

                                                        # further recovery attempt unnecessary, but keep the work dir for debugging
                                                        if _job.result[0] == "failed":
                                                            pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                            if not JS.rename(_site, _job):
                                                                pUtil.tolog("(Fate of job state file left for next pilot)")

                                                    else:
                                                        pUtil.tolog("!!WARNING!!1140!! Failed to update Panda server for job %s (exit code %d)" % (_job.jobId, _job.result[2]))

                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                                else: # output files could be moved

                                                    pUtil.tolog("Remaining data files moved to SE")
                                                    pUtil.chdir(_site.workdir)

                                                    # remove data dir
                                                    if pUtil.remove([_job.datadir]):
                                                        pUtil.tolog("Removed data dir")
                                                    else:
                                                        pUtil.tolog("!!WARNING!!1140!! Failed to remove data dir")

                                                    if FinishedJob(_job):
                                                        if pUtil.verifyTransfer(_site.workdir):
                                                            # set new exit code
                                                            _job.setState(['finished', 0, 0])
                                                        else:
                                                            pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                    else:
                                                        pUtil.tolog("!!WARNING!!1140!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                              (_job.result[1], _job.result[2]))

                                                    # get the metadata
                                                    strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                                    # update the server
                                                    rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr = strXML, 
                                                                                    ra = _recoveryAttempt, 
                                                                                    schedulerID = env['jobSchedulerId'], 
                                                                                    pilotID = env['pilotId'])
                                                    if rt == 0:
                                                        number_of_recoveries += 1
                                                        pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

                                                        # did the server send back a command?
                                                        if "tobekilled" in _job.action:
                                                            pUtil.tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                            _job.result[0] = "failed"

                                                        # further recovery attempt unnecessary, but keep the work dir for debugging
                                                        if _job.result[0] == "failed":
                                                            pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                            if not JS.rename(_site, _job):
                                                                pUtil.tolog("(Fate of job state file left for next pilot)")
                                                                
                                                        # only cleanup work dir if no error code has been set
                                                        if _job.result[0] == 'finished':
                                                            if not JS.cleanup():
                                                                pUtil.tolog("!!WARNING!!1140!! Failed to cleanup")

                                                    else:
                                                        pUtil.tolog("!!WARNING!!1140!! Failed to update Panda server for job %s (exit code %d)" % (_job.jobId, _job.result[2]))

                                                    # release the atomic lockfile and go to the next directory
                                                    releaseAtomicLockFile(fd, lockfile_name)
                                                    continue

                                            else: # output files do not exist

                                                if FinishedJob(_job):
                                                    if pUtil.verifyTransfer(_site.workdir):
                                                        _job.setState(['finished', 0, 0])
                                                    else:
                                                        pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                        _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                                else:
                                                    pUtil.tolog("!!WARNING!!1140!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                          (_job.result[1], _job.result[2]))
                                                    # failed since output files do not exist
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBFILETRANSFER])
                                                pUtil.tolog("Data files do not exist")

                                                # was xml saved?
                                                strXML = ''
                                                try:
                                                    strXML = _node['xml']
                                                except:
                                                    pUtil.tolog("XML could not be found - try to read from file")
                                                    strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                                # update the server
                                                rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr = strXML, 
                                                                                ra = _recoveryAttempt, 
                                                                                schedulerID = env['jobSchedulerId'], 
                                                                                pilotID = env['pilotId'])
                                                if rt == 0:
                                                    pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                                    number_of_recoveries += 1

                                                    # did the server send back a command?
                                                    if "tobekilled" in _job.action:
                                                        pUtil.tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                        _job.result[0] = "failed"

                                                    # further recovery attempt unnecessary, but keep the work dir for debugging
                                                    if _job.result[0] == "failed":
                                                        pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                        if not JS.rename(_site, _job):
                                                            pUtil.tolog("(Fate of job state file left for next pilot)")

                                                else:
                                                    pUtil.tolog("!!WARNING!!1140!! Failed to update Panda server for job %s (exit code %d)" % (_job.jobId, _job.result[2]))

                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue

                                        else: # data dir does not exist
                                            pUtil.tolog("No data dir for this lost job")

                                        # store results in case there's another server hickup
                                        # (since they might be overwritten)
                                        _result1 = _job.result[1]
                                        _result2 = _job.result[2]

                                        # lost heartbeat job?
                                        # (this state is only set for finished job when there was a temporary
                                        # problem with the server during dispatcher update)
                                        if _job.result[0] == 'lostheartbeat':
                                            pUtil.tolog("Recovering lost heartbeat job")
                                            if FinishedJob(_job):
                                                if pUtil.verifyTransfer(_site.workdir):
                                                    _job.setState(['finished', 0, 0])
                                                else:
                                                    pUtil.tolog("!!WARNING!!1110!! Job recovery can not recover this job! Fate of output files unknown")
                                                    _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])
                                            else:
                                                pUtil.tolog("!!WARNING!!1140!! Job recovery can not recover this job! transExitCode=%d, pilotErrorCode=%d" %\
                                                      (_job.result[1], _job.result[2]))
                                                # should never happen since 'lostheartbeat' jobs are finished..
                                                _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])

                                        # the job might be finished and the code has entered this point due to a
                                        # non responding server in a prior recovery attempt
                                        elif _job.result[0] == 'finished':
                                            pUtil.tolog("Job was finished, log and output files already registered. Will remind dispatcher")
                                        else:
                                            pUtil.tolog("!!WARNING!!1140!! Neither log, data nor work dir exist (setting EC 1156: Pilot could not recover job)")
                                            _job.setState(['failed', _job.result[1], error.ERR_LOSTJOBRECOVERY])

                                        # was xml saved?
                                        strXML = ''
                                        try:
                                            strXML = _node['xml']
                                        except:
                                            pUtil.tolog("XML could not be found - try to read from file")
                                            strXML = pUtil.getMetadata(_site.workdir, _job.jobId)

                                        # update the server
                                        rt, retNode = updatePandaServer(_job, _site, _psport, xmlstr = strXML, 
                                                                        ra = _recoveryAttempt, 
                                                                        schedulerID = env['jobSchedulerId'], 
                                                                        pilotID = env['pilotId'])
                                        if rt == 0:
                                            number_of_recoveries += 1
                                            pUtil.tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))
                                            if not JS.cleanup():
                                                pUtil.tolog("!!WARNING!!1140!! Failed to cleanup")
                                                # release the atomic lockfile and go to the next directory
                                                releaseAtomicLockFile(fd, lockfile_name)
                                                continue    

                                            # did the server send back a command?
                                            if "tobekilled" in _job.action:
                                                pUtil.tolog("!!WARNING!!1140!! Panda server returned a \'tobekilled\' command")
                                                _job.result[0] = "failed"

                                            # further recovery attempt unnecessary, but keep the work dir for debugging
                                            if _job.result[0] == "failed":
                                                pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                                                if not JS.rename(_site, _job):
                                                    pUtil.tolog("(Fate of job state file left for next pilot)")
                                        else:
                                            pUtil.tolog("!!WARNING!!1140!! Panda server returned a %d" % (rt))

                                            # store the final state so that the next pilot will know
                                            # store the metadata xml
                                            retNode['xml'] = strXML

                                            # update the job state file with the new state information
                                            _job.setState(['lostheartbeat', _result1, _result2])
                                            _retjs = pUtil.updateJobState(_job, _site, retNode, _recoveryAttempt)

                    # Job state file was recently modified
                    else:
                        pUtil.tolog("(Job state file was recently modified - skip it)")
                        # atomic lockfile will be released below

                # (end main "for file_path in job_state_files"-loop)
                # release the atomic lockfile and go to the next directory
                releaseAtomicLockFile(fd, lockfile_name)

    pUtil.chdir(currentDir)
    return number_of_recoveries

def getProperNodeName(nodename):
    """ Get the proper node name (if possible, containing the _CONDOR_SLOT (SlotID)) """

    # if possible (on a condor system) add the SlotID to the nodename: _CONDOR_SLOT@nodename
    if os.environ.has_key("_CONDOR_SLOT"):
        nodename = "%s@%s" % (os.environ["_CONDOR_SLOT"], nodename)

    return nodename

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

    rc = pUtil.lateRegistration(dq2url, job, type=type) 
    if rc:
        # update job state file to prevent any further registration attempts for these files
        try:
            if type == "output":
                job.output_latereg = "False"
                job.output_fields = None
                _retjs = pUtil.updateJobState(job, site, node)
                status = True
            elif type == "log":
                job.log_latereg = "False"
                job.log_field = None
                _retjs = pUtil.updateJobState(job, site, node)
                status = True
            else:
                pUtil.tolog("!!WARNING!!1150!! Unknown registration type (must be either output or log): %s" % (type))
        except Exception, e:
            pUtil.tolog("!!WARNING!!1150!! Exception caught (ignore): %s" % str(e))
            pass
        else:
            pUtil.tolog("Late registration performed for non-registered %s file(s)" % (type))
            if status and type == "log":
                # create a weak lock file for the log registration
                pUtil.createLockFile(env['jobrec'], site.workdir, lockfile="LOGFILEREGISTERED")
    elif rc == False:
        pUtil.tolog("Late registration failed, job will remain in holding state")
    else: # None
        pUtil.tolog("Late registration had nothing to do")
        status = None

    return status

def updatePandaServer(job, site, port, xmlstr = None, spaceReport = False, 
                      log = None, ra = 0, jr = False, schedulerID = None, pilotID = None, 
                      updateServer = True, stdout_tail = ""):
    """ Update the panda server with the latest job info """

    # create and instantiate the client object
    from PandaServerClient import PandaServerClient
    client = PandaServerClient(pilot_version = env['version'],
                               pilot_version_tag = env['pilot_version_tag'], 
                               pilot_initdir = env['pilot_initdir'], 
                               jobSchedulerId = schedulerID, 
                               pilotId = pilotID,
                               updateServer = env['updateServerFlag'], 
                               jobrec = env['jobrec'], 
                               pshttpurl = env['pshttpurl'])

    # update the panda server
    return client.updatePandaServer(job, site, env['workerNode'], port, xmlstr = xmlstr, 
                                    spaceReport = spaceReport, log = log, ra = ra, 
                                    jr = jr, useCoPilot = env['useCoPilot'], 
                                    stdout_tail = stdout_tail)

def transferLogFile(job, site, dest=None, jr=False):
    """
    save log tarball into DDM and register it to catalog, or copy it to 'dest'.
    the job recovery will use the current site info known by the current
    pilot and will override the old jobs' site.dq2url in case the dq2url
    has been updated
    """

    # create and instantiate the job log object
    from JobLog import JobLog
    joblog = JobLog()

    # transfer the log
    return joblog.transferLogFile(job, site, env['experiment'], dest=dest, jr=jr)
    

def dumpVars(thisSite):
    """ dump argParser variables """

    pUtil.tolog("Pilot options:................................................")
    pUtil.tolog("appdir: %s" % (thisSite.appdir))
    pUtil.tolog("debugLevel: %s" % str(env['debugLevel']))
    pUtil.tolog("dq2url: %s" % str(thisSite.dq2url))
    pUtil.tolog("jobrec: %s" % str(env['jobrec']))
    pUtil.tolog("jobRequestFlag: %s" % str(env['jobRequestFlag']))
    pUtil.tolog("jobSchedulerId: %s" % str(env['jobSchedulerId']))
    pUtil.tolog("maxjobrec: %s" % str(env['maxjobrec']))
    pUtil.tolog("maxNumberOfRecoveryAttempts: %s" % str(env['maxNumberOfRecoveryAttempts']))
    pUtil.tolog("pilotId: %s" % str(env['pilotId']))
    pUtil.tolog("pshttpurl: %s" % (env['pshttpurl']))
    pUtil.tolog("psport: %s" % (env['psport']))
    pUtil.tolog("queuename: %s" % (env['queuename']))
    pUtil.tolog("rmwkdir: %s" % str(env['rmwkdir']))
    pUtil.tolog("sitename: %s" % (thisSite.sitename))
    pUtil.tolog("stageinretry: %s" % str(env['stageinretry']))
    pUtil.tolog("stageoutretry: %s" % str(env['stageoutretry']))
    pUtil.tolog("uflag: %s" % str(env['uflag']))
    pUtil.tolog("workdir: %s" % (thisSite.workdir))
    pUtil.tolog("logFileDir: %s" % (env['logFileDir']))
    pUtil.tolog("..............................................................")

def getInOutDirs():
    """ return the input and output directories """

    if env['inputDir'] != "":
        pUtil.tolog("Requested input file dir: %s" % (env['inputDir']))
    else:
        # default inputDir only releveant for mv site mover
        env['inputDir'] = env['pilot_initdir']
    if env['outputDir'] != "":
        pUtil.tolog("Requested output file dir: %s" % (env['outputDir']))
    else:
        # default outputDir only releveant for mv site mover
        env['outputDir'] = env['pilot_initdir']

def diskCleanup(wntmpdir, _uflag):
    """ Perform disk cleanup """

    pUtil.tolog("Preparing to execute Cleaner")
    from Cleaner import Cleaner

    dirs = [wntmpdir]
    _wntmpdir = pUtil.readpar('wntmpdir')
    if wntmpdir != _wntmpdir and _wntmpdir != "":
        dirs.append(_wntmpdir)

    for _dir in dirs:
        pUtil.tolog("Cleaning %s" % (_dir))
        cleaner = Cleaner(limit = env['cleanupLimit'], path = _dir, uflag = _uflag)
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
        pUtil.tolog("!!WARNING!!1111!! Detected problem with the local SE")
        pUtil.tolog("Taking a nap for %d s before trying again" % (_delay))
        time.sleep(_delay)
        if not mover.checkLocalSE(analyJob):
            pUtil.tolog("!!WARNING!!1111!! Detected problem with the local SE (again) - giving up")
            ec = error.ERR_SEPROBLEM
    return ec

def storePilotInitdir(targetdir, pilot_initdir):
    """ Store the pilot launch directory in a file used by environment.py """

    # This function is used to store the location of the init directory in the init directory itself as well as in the
    # site work directory. The location file is used by environment.py to set the global env['pilot_initdir'] used
    # by the pilot and the Monitor

    # This function must be called before the global env variable is instantiated in the pilot

    path = os.path.join(targetdir, "PILOT_INITDIR")
    pUtil.tolog("Creating file %s with content %s" % (path, pilot_initdir))
    pUtil.writeToFile(path, pilot_initdir)

def createSiteWorkDir(workdir, error):
    """ Create the pilot workdir and write the path to file """

    ec = 0

    pUtil.tolog("Will attempt to create workdir: %s" % (workdir))
    try:
        # note: do not set permissions in makedirs since they will not come out correctly, 0770 -> 0750
        os.makedirs(workdir)
        os.chmod(workdir, 0770)
    except Exception, e:
        pUtil.tolog("!!WARNING!!1999!! Exception caught: %s (will try os.mkdir instead)" % str(e))
        # a bug in makedirs can attempt to create existing basedirs, try to use mkdir instead
        try:
            # change to absolute permissions, requested by QMUL
            # note: do not set permissions in makedirs since they will not come out correctly, 0770 -> 0750
            os.mkdir(workdir)
            os.chmod(workdir, 0770)
        except Exception, e:
            errorText = "Exception caught: %s" % str(e)
            pUtil.tolog("!!FAILED!!1999!! %s" % (errorText))
            ec = error.ERR_MKDIRWORKDIR
        else:
            ec = 0

            # verify permissions
            cmd = "stat %s" % (workdir)
            pUtil.tolog("(1) Executing command: %s" % (cmd))
            rc, rs = commands.getstatusoutput(cmd)
            pUtil.tolog("\n%s" % (rs))

    if ec == 0:
        path = os.path.join(env['pilot_initdir'], "CURRENT_SITEWORKDIR")
        if os.path.exists(path):
            # remove the old file
            try:
                os.remove(path)
            except Exception, e:
                pUtil.tolog("!!WARNING!!2999!! Could not remove old file: %s, %s (attempt to overwrite)" % (path, str(e)))
            else:
                pUtil.tolog("Removed old file: %s" % (path))
        pUtil.tolog("Creating file: %s" % (path))
        pUtil.writeToFile(path, workdir)

    return ec

def getMaxtime():
    """ Get the maximum time this pilot is allowed to run """

    _maxtime = pUtil.readpar('maxtime')
    if not _maxtime or _maxtime == "0":
        maxtime = 999999
    else:
        try:
            maxtime = int(_maxtime)
        except:
            maxtime = 999999

    return maxtime

def setUpdateFrequencies():
    """ Set the update frequency of user workdir etc checks """

    env['update_freq_proc'] = 5*60              # Update frequency, process checks [s], 5 minutes
    env['update_freq_space'] = 10*60            # Update frequency, space checks [s], 10 minutes

    if os.environ.has_key('NON_LOCAL_ATLAS_SCRATCH'):
        if os.environ['NON_LOCAL_ATLAS_SCRATCH'].lower() == "true":
            if os.environ.has_key('NON_LOCAL_ATLAS_SCRATCH_SPACE'):
                try:
                    space_n = int(os.environ['NON_LOCAL_ATLAS_SCRATCH_SPACE'])
                except Exception, e:
                    pUtil.tolog("!!WARNING!!1234!! Exception caught: %s" % (e))
                else:
                    if space_n > 0 and space_n < 10:
                        env['update_freq_space'] = 10*60*space_n
                    else:
                        pUtil.tolog("!!WARNING!!1234!! NON_LOCAL_ATLAS_SCRATCH_SPACE is out or range: %d (0 < n < 10)" % (space_n))
            else:
                env['update_freq_space'] = 30*60

    pUtil.tolog("Update frequencies:")
    pUtil.tolog("...Processes: %d s" % (env['update_freq_proc']))
    pUtil.tolog(".......Space: %d s" % (env['update_freq_space']))
    pUtil.tolog("......Server: %d s" % (env['update_freq_server']))

def getProdSourceLabel():
    """ determine the job type """

    prodSourceLabel = None

    # not None value; can be user (user analysis job), ddm (panda mover job, sitename should contain DDM)
    # test will return a testEvgen/testReco job, ptest will return a job sent with prodSourceLabel ptest
    if env['uflag']:
        if env['uflag'] == 'self' or env['uflag'] == 'ptest':
            if env['uflag'] == 'ptest':
                prodSourceLabel = env['uflag']
            elif env['uflag'] == 'self':
                prodSourceLabel = 'user'
        else:
            prodSourceLabel = env['uflag']

    # for PandaMover jobs the label must be ddm
    if "DDM" in env['thisSite'].sitename or (env['uflag'] == 'ddm' and env['thisSite'].sitename == 'BNL_ATLAS_test'):
        prodSourceLabel = 'ddm'
    elif "Install" in env['thisSite'].sitename:  # old, now replaced with prodSourceLabel=install
        prodSourceLabel = 'software'
    if pUtil.readpar('status').lower() == 'test' and env['uflag'] != 'ptest' and env['uflag'] != 'ddm':
        prodSourceLabel = 'test'

    # override for release candidate pilots
    if env['pilot_version_tag'] == "RC":
        prodSourceLabel = "rc_test"
    if env['pilot_version_tag'] == "DDM":
        prodSourceLabel = "ddm"

    return prodSourceLabel

def getDispatcherDictionary(_diskSpace, tofile):
    """ Construct a dictionary for passing to jobDispatcher """

    pilotErrorDiag = ""

    # glExec proxy key
    _getProxyKey = "False"
    # Eddie - commented out
    # if pUtil.readpar('glexec').lower() in ['true', 'uid']:
    #     _getProxyKey = "True"

    nodename = env['workerNode'].nodename
    pUtil.tolog("Node name: %s" % (nodename))

    jNode = {'siteName':         env['thisSite'].sitename,
             'cpu':              env['workerNode'].cpu,
             'mem':              env['workerNode'].mem,
             'diskSpace':        _diskSpace,
             'node':             nodename,
             'computingElement': env['thisSite'].computingElement,
             'getProxyKey':      _getProxyKey,
             'workingGroup':     env['workingGroup']}

    if env['countryGroup'] == "":
        pUtil.tolog("No country group selected")
    else:
        jNode['countryGroup'] = env['countryGroup']
        pUtil.tolog("Using country group: %s" % (env['countryGroup']))

    if env['workingGroup'] == "":
        pUtil.tolog("No working group selected")
    else:
        pUtil.tolog("Using working group: %s" % (jNode['workingGroup']))

    if env['allowOtherCountry']:
        pUtil.tolog("allowOtherCountry is set to True (will be sent to dispatcher)")
        jNode['allowOtherCountry'] = env['allowOtherCountry']

    # should the job be requested for a special DN?
    if env['uflag'] == 'self':
        # get the pilot submittor DN, and only process this users jobs
        DN, pilotErrorDiag = getDN()
        if DN == "":
            return {}, "", pilotErrorDiag
        else:
            jNode['prodUserID'] = DN

        pUtil.tolog("prodUserID: %s" % (jNode['prodUserID']))

    # determine the job type
    prodSourceLabel = getProdSourceLabel()
    if prodSourceLabel:
        jNode['prodSourceLabel'] = prodSourceLabel
        pUtil.tolog("prodSourceLabel: %s" % (jNode['prodSourceLabel']), tofile=tofile)

    # send the pilot token
    # WARNING: do not print the jNode dictionary since that will expose the pilot token
    if env['pilotToken']:
        jNode['token'] = env['pilotToken']

    return jNode, prodSourceLabel, pilotErrorDiag

def getDN():
    """ Return the DN for the pilot submitter """

    DN = ""
    pilotErrorDiag = ""

    # Try to use arcproxy first since voms-proxy-info behaves poorly under SL6
    # cmd = "arcproxy -I |grep 'subject'| sed 's/.*: //'"
    cmd = "arcproxy -i subject"
    pUtil.tolog("Executing command: %s" % (cmd))
    err, out = commands.getstatusoutput(cmd)
    if "command not found" in out:
        pUtil.tolog("!!WARNING!!1234!! arcproxy is not available")
        pUtil.tolog("!!WARNING!!1235!! Defaulting to voms-proxy-info (can lead to memory problems with the command in case of low schedconfig.memory setting)")

        # Default to voms-proxy-info
        cmd = "voms-proxy-info -subject"
        pUtil.tolog("Executing command: %s" % (cmd))
        err, out = commands.getstatusoutput(cmd)

    if err == 0:
        DN = out
        pUtil.tolog("Got DN = %s" % (DN))

        CN = "/CN=proxy"
        if not DN.endswith(CN):
            pUtil.tolog("!!WARNING!!1234!! DN does not end with %s (will be added)" % (CN))
            DN += CN
    else:
        pilotErrorDiag = "User=self set but cannot get proxy: %d, %s" % (err, out)

    return DN, pilotErrorDiag

def writeDispatcherEC(EC):
    """ write the dispatcher exit code to file """    
    filename = os.path.join(env['pilot_initdir'], "STATUSCODE")
    if os.path.exists(filename):
        try:
            os.remove(filename)
        except Exception, e:
            pUtil.tolog("Warning: Could not remove file: %s" % str(e))
        else:
            pUtil.tolog("Removed existing STATUSCODE file")
    pUtil.writeToFile(os.path.join(filename), str(EC))    

def getStatusCode(data):
    """ get and write the dispatcher status code to file """

    pUtil.tolog("Parsed response: %s" % str(data))

    try:
        StatusCode = data['StatusCode']
    except Exception, e:
        pilotErrorDiag = "Can not receive any job from jobDispatcher: %s" % str(e)
        pUtil.tolog("!!WARNING!!1200!! %s" % (pilotErrorDiag))
        StatusCode = '45'

    # Put the StatusCode in a file (used by some pilot wrappers), erase if it already exists
    writeDispatcherEC(StatusCode)

    return StatusCode

def backupDispatcherResponse(response, tofile):
    """ Backup response (will be copied to workdir later) """        
    try:
        fh = open(env['pandaJobDataFileName'], "w")
        fh.write(response)
        fh.close()
    except Exception, e:
        pUtil.tolog("!!WARNING!!1999!! Could not store job definition: %s" % str(e), tofile=tofile)
    else:
        pUtil.tolog("Job definition stored (for later backup) in file %s" % (env['pandaJobDataFileName']), tofile=tofile)

def getNewJob(tofile=True):
    """ Get a new job definition from the jobdispatcher or from file """

    pilotErrorDiag = ""
    StatusCode = ''

    # determine which disk space to send to dispatcher (only used by dispatcher so no need to send actual available space)
    _maxinputsize = pUtil.getMaxInputSize(MB=True)
    _disk = env['workerNode'].disk
    pUtil.tolog("Available WN disk space: %d MB" % (_disk))
    _diskSpace = min(_disk, _maxinputsize)
    pUtil.tolog("Sending disk space %d MB to dispatcher" % (_diskSpace))

    # construct a dictionary for passing to jobDispatcher and get the prodSourceLabel
    jNode, prodSourceLabel, pilotErrorDiag = getDispatcherDictionary(_diskSpace, tofile)
    if jNode == {}:
        errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
        pUtil.tolog(errorText, tofile=tofile)
        # send to stderr
        print >> sys.stderr, errorText
        return None, pilotErrorDiag

    # should we ask the server for a job or should we read it from a file (as in the case of the test pilot)
    if not env['jobRequestFlag']:
        # read job from file
        pUtil.tolog("Looking for a primary job (reading from file)", tofile=tofile)
        _pandaJobDataFileName = os.path.join(env['pilot_initdir'], env['pandaJobDataFileName'])
        if os.path.isfile(_pandaJobDataFileName):
            try:
                f = open(_pandaJobDataFileName)
            except Exception,e:
                pilotErrorDiag = "[pilot] Can not open the file %s: %s" % (_pandaJobDataFileName, str(e))
                errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
                pUtil.tolog(errorText, tofile=tofile)
                # send to stderr
                print >> sys.stderr, errorText
                return None, pilotErrorDiag
            else:
                # get the job definition from the file
                response = f.read()

                if len(response) == 0:
                    pilotErrorDiag = "[pilot] No job definition found in file: %s" % (_pandaJobDataFileName)
                    errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
                    pUtil.tolog(errorText, tofile=tofile)
                    # send to stderr
                    print >> sys.stderr, errorText
                    return None, pilotErrorDiag

                env['jobRequestFlag'] = True
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
            pUtil.tolog(errorText, tofile=tofile)
            # send to stderr
            print >> sys.stderr, errorText
            return None, pilotErrorDiag
    else:
        # get a random server
        url = '%s:%s/server/panda' % (env['pshttpurl'], str(env['psport']))
        pUtil.tolog("Looking for a primary job (contacting server at %s)" % (url), tofile=tofile)

        # make http connection to jobdispatcher
        # format: status, parsed response (data), response
        ret = pUtil.httpConnect(jNode, url, mode = "GETJOB", path = env['pilot_initdir'], experiment = env['experiment']) # connection mode is GETJOB

        # get and write the dispatcher status code to file
        StatusCode = str(ret[0])

        # the original response will be put in a file in this function
        data = ret[1] # dictionary
        response = ret[2] # text

        # write the dispatcher exit code to file
        writeDispatcherEC(StatusCode)

        if ret[0]: # non-zero return
            return None, pUtil.getDispatcherErrorDiag(ret[0])
        
    if StatusCode != '0':
        pilotErrorDiag = "No job received from jobDispatcher, StatusCode: %s" % (StatusCode)
        pUtil.tolog("%s" % (pilotErrorDiag), tofile=tofile)
        return None, pilotErrorDiag

    # test if he attempt number was sent
    try:
        attemptNr = int(data['attemptNr'])
    except Exception,e:
        pUtil.tolog("!!WARNING!!1200!! Failed to get attempt number from server: %s" % str(e), tofile=tofile)
    else:
        pUtil.tolog("Attempt number from server: %d" % attemptNr)

    # should further job recovery be switched off? (for gangarobot jobs)
    if "gangarobot" in data['processingType'] and env['jobrec']:
        pUtil.tolog("Switching off further job recovery for gangarobot job")

        # get the site information object
        env['si'] = pUtil.getSiteInformation(env['experiment'])

        env['jobrec'] = False
        ec = env['si'].replaceQueuedataField("retry", "False")
    else:
        if env['jobrec']:
            pUtil.tolog("Job recovery is still switched on after job download")
        else:
            pUtil.tolog("Job recovery is still switched off after job download")

    # should there be a delay before setting running state?
    try:
        env['nSent'] = int(data['nSent'])
    except Exception,e:
        env['nSent'] = 0
    else:
        pUtil.tolog("Received nSent: %d" % (env['nSent']))

    # backup response (will be copied to workdir later)
    backupDispatcherResponse(response, tofile)

    if data.has_key('prodSourceLabel'):
        if data['prodSourceLabel'] == "":
            pUtil.tolog("Setting prodSourceLabel in job def data: %s" % (prodSourceLabel))
            data['prodSourceLabel'] = prodSourceLabel
        else:
            pUtil.tolog("prodSourceLabel already set in job def data: %s" % (data['prodSourceLabel']))

            # override ptest value if install job to allow testing using dev pilot
            if prodSourceLabel == "ptest" and "atlpan/install/sw-mgr" in data['transformation']:
                pUtil.tolog("Dev pilot will run test install job (job.prodSourceLabel set to \'install\')")
                data['prodSourceLabel'] = "install"
    else:
        pUtil.tolog("Adding prodSourceLabel to job def data: %s" % (prodSourceLabel))
        data['prodSourceLabel'] = prodSourceLabel

    # look for special commands in the job parameters (can be set by HammerCloud jobs; --overwriteQueuedata, --disableFAX)
    # if present, queuedata needs to be updated (as well as jobParameters - special commands need to be removed from the string)
    data['jobPars'], transferType = env['si'].updateQueuedataFromJobParameters(data['jobPars'])
    if transferType != "":
        # we will overwrite whatever is in job.transferType using jobPars
        data['transferType'] = transferType
                                
    # update the copytoolin if transferType is set to fax/xrd
    if data.has_key('transferType'):
        if data['transferType'] == 'fax' or data['transferType']== 'xrd':
            if pUtil.readpar('faxredirector') != "":
                pUtil.tolog("Encountered transferType=%s, will use FAX site mover for stage-in" % (data['transferType']))
                ec = env['si'].replaceQueuedataField("copytoolin", "fax")
                ec = env['si'].replaceQueuedataField("allowfax", "True")
                ec = env['si'].replaceQueuedataField("timefloor", "")
            else:
                pilotErrorDiag = "Cannot switch to FAX site mover for transferType=%s since faxredirector is not set" % (data['transferType'])
                pUtil.tolog("!!WARNING!!1234!! %s" % (pilotErrorDiag))
                return None, pilotErrorDiag

    # convert the data into a file for child process to pick for running real job later
    try:
        f = open("Job_%s.py" % data['PandaID'], "w")
        print >>f, "job=", data
        f.close()
    except Exception,e:
        pilotErrorDiag = "[pilot] Exception caught: %s" % str(e)
        pUtil.tolog("!!WARNING!!1200!! %s" % (pilotErrorDiag), tofile=tofile)
        return None, pilotErrorDiag

    # create the new job
    newJob = Job.Job()
    newJob.setJobDef(data)  # fill up the fields with correct values now
    newJob.datadir = env['thisSite'].workdir + "/PandaJob_%s_data" % (newJob.jobId)
    newJob.experiment = env['experiment']

    if data.has_key('logGUID'):
        logGUID = data['logGUID']
        if logGUID != "NULL" and logGUID != "":
            newJob.tarFileGuid = logGUID
            pUtil.tolog("Got logGUID from server: %s" % (logGUID), tofile=tofile)
        else:
            pUtil.tolog("!!WARNING!!1200!! Server returned NULL logGUID", tofile=tofile)
            pUtil.tolog("Using generated logGUID: %s" % (newJob.tarFileGuid), tofile=tofile)
    else:
        pUtil.tolog("!!WARNING!!1200!! Server did not return logGUID", tofile=tofile)
        pUtil.tolog("Using generated logGUID: %s" % (newJob.tarFileGuid), tofile=tofile)

    if newJob.prodSourceLabel == "":
        pUtil.tolog("Giving new job prodSourceLabel=%s" % (prodSourceLabel))
        newJob.prodSourceLabel = prodSourceLabel
    else:
        pUtil.tolog("New job has prodSourceLabel=%s" % (newJob.prodSourceLabel))
        
    # should we use debug mode?
    if data.has_key('debug'):
        if data['debug'].lower() == "true":
            env['update_freq_server'] = 5*30
            pUtil.tolog("Debug mode requested: Updating server update frequency to %d s" % (env['update_freq_server']))

    # Eddie: try to get user proxy from data['userproxy']                                                                                                                           
    if data.has_key('userProxy'):
        pUtil.tolog('Retrieving userproxy from panda-server')
        env['userProxy'] = data['userProxy']
    else:
        pUtil.tolog('no user proxy in data')
        # Eddie: what do we do when there is no user proxy? Do we use the proxy that started the pilot?                                                                         
        env['userProxy'] = ''
 
    return newJob, ""

def getJob():
    """ Download a new job from the dispatcher """    
    ec = 0
    job = None
    error = PilotErrors()

    # loop over getNewJob to allow for multiple attempts
    trial = 1
    t0 = time.time()
    pUtil.tolog("Pilot will attempt single job download for a maximum of %d seconds" % (env['getjobmaxtime']))
    while int(time.time() - t0) < env['getjobmaxtime']:
        job, pilotErrorDiag = getNewJob()
        if not job:
            if env['getjobmaxtime'] - int(time.time() - t0) > 60:
                pUtil.tolog("[Trial %d] Could not find a job! (will try again after 60 s)" % (trial))
                time.sleep(60)
                trial += 1
            else:
                pUtil.tolog("(less than 60 s left of the allowed %d s for job downloads, so not a good time for a nap!)" % (env['getjobmaxtime']))
                break
        else:
            env['number_of_jobs'] += 1
            pUtil.tolog("Increased job counter to %d" % (env['number_of_jobs']))
            os.environ["PanDA_TaskID"] = job.taskID
            pUtil.tolog("Task ID set to: %s" % (job.taskID))
            break

    if not job:
        if "No job received from jobDispatcher" in pilotErrorDiag or "Dispatcher has no jobs" in pilotErrorDiag:
            errorText = "!!FINISHED!!0!!Dispatcher has no jobs"
        else:
            errorText = "!!FAILED!!1999!!%s" % (pilotErrorDiag)

        # only set an error code if it's the first job
        if env['number_of_jobs'] == 0:
            ec = error.ERR_GENERALERROR
        else:
            errorText += "\nNot setting any error code since %d job(s) were already executed" % (env['number_of_jobs'])
            ec = -1 # temporary

        # send to stderr
        pUtil.tolog(errorText)
        print >> sys.stderr, errorText

    return ec, job, env['number_of_jobs']

def checkLocalDiskSpace(error):
    """ Do we have enough local disk space left to run the job? """

    ec = 0

    # Convert local space to B and compare with the space limit
    spaceleft = int(env['workerNode'].disk)*1024**2 # B (node.disk is in MB)
    _localspacelimit = env['localspacelimit0']*1024 # B
    pUtil.tolog("Local space limit: %d B" % (_localspacelimit))
    if spaceleft < _localspacelimit:
        pUtil.tolog("!!FAILED!!1999!! Too little space left on local disk to run job: %d B (need > %d B)" % (spaceleft, _localspacelimit))
        ec = error.ERR_NOLOCALSPACE
    else:
        pUtil.tolog("Remaining local disk space: %d B" % (spaceleft))

    return ec

# warning!!! duplicate with similar method in Monitor.py
def getsetWNMem(memory):
    """ Get the memory limit from queuedata or from the -k pilot option and set it """

    wn_mem = 0

    # Get the memory limit primarily from queuedata
    # Note: memory will soon be changed to maxmemory
    _maxmemory = pUtil.readpar('maxmemory')
    if _maxmemory == "":
        _maxmemory = pUtil.readpar('memory')

    if _maxmemory != "":
        try:
            maxmemory = int(_maxmemory) # Should already be an int
        except Exception, e:
            pUtil.tolog("Could not convert maxmemory to an int: %s" % (e))
            maxmemory = -1
        else:
            pUtil.tolog("Got max memory limit: %d MB (from queuedata)" % (maxmemory))
    else:
        maxmemory = -1

    # Get the max memory limit from the -k pilot option if specified
    if maxmemory == -1 and memory:
        try:
            maxmemory = int(memory)
        except Exception, e:
            pUtil.tolog("Could not convert memory to an int: %s" % (e))
            maxmemory = -1
        else:
            pUtil.tolog("Got max memory limit: %d MB (from pilot option -k)" % (maxmemory))

    # Set the memory limit
    if maxmemory > 0:
    
        # Convert MB to Bytes for the setrlimit function
        _maxmemory = maxmemory*1024**2

        # Only proceed if not a CGROUPS site
        if not isCGROUPSSite():
            pUtil.tolog("Not a CGROUPS site, proceeding with setting the memory limit")
            try:
                import resource
                resource.setrlimit(resource.RLIMIT_AS, [_maxmemory, _maxmemory])
            except Exception, e:
                pUtil.tolog("!!WARNING!!3333!! resource.setrlimit failed: %s" % (e))
            else:
                pUtil.tolog("Max memory limit set to: %d B" % (_maxmemory))
        else:
            pUtil.tolog("Detected a CGROUPS site, will not set the memory limit")

        cmd = "ulimit -a"
        pUtil.tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        pUtil.tolog("\n%s" % (out))
    else:
        pUtil.tolog("Max memory will not be set")

    return maxmemory

# main process starts here
def runMain(runpars):

    global env
    # keep track of when the pilot was started
    env['pilot_startup'] = int(time.time())

    # get error handler
    error = PilotErrors()

    # protect the bulk of the pilot code with exception handling
    env['isJobDownloaded'] = False
    env['isServerUpdated'] = False
    try:
        # dump some pilot info, version id, etc to stdout
        pUtil.dumpPilotInfo(env['version'], env['pilot_version_tag'], env['pilotId'], env['jobSchedulerId'], env['pilot_initdir'], tofile=True)

        # read the pilot token
        global pilotToken
        pilotToken = pUtil.getPilotToken(tofile=False)

        # extend PYTHONPATH to include the local workdir path
        sys.path.insert(1, env['pilot_initdir'])

        # add the current dir to the path to make sure pilot modules can be found
        sys.path.append(os.path.abspath(os.curdir))

        # parse the pilot argument list (e.g. queuename is updated)
        argParser(runpars)
        args = [env['sitename'], env['appdir'], env['workdir'], env['dq2url'], env['queuename']]

        # fill in the site information by parsing the argument list
        env['thisSite'] = Site.Site()
        env['thisSite'].setSiteInfo(args)

        # verify inputDir and outputDir
        getInOutDirs()

        ec, env['thisSite'], env['jobrec'], env['hasQueuedata'] = pUtil.handleQueuedata(env['queuename'], env['schedconfigURL'], error, env['thisSite'], env['jobrec'], 
                                                                                        env['experiment'], forceDownload = False, forceDevpilot = env['force_devpilot'])
        if ec != 0:
            return pUtil.shellExitCode(ec)

        # the maximum time this pilot is allowed to run
        env['maxtime'] = getMaxtime()

        # get the experiment object
        thisExperiment = pUtil.getExperiment(env['experiment'])
        if thisExperiment:
            pUtil.tolog("Pilot will serve experiment: %s" % (thisExperiment.getExperiment()))

            # set the cache if necessary (e.g. for LSST)
            if env['cache']:
                thisExperiment.setCache(env['cache'])
        else:
            pUtil.tolog("!!FAILED!!1234!! Did not get an experiment object from the factory")
            return pUtil.shellExitCode(error.ERR_GENERALERROR)

        # perform special checks for given experiment
        if not thisExperiment.specialChecks():
            return pUtil.shellExitCode(error.ERR_GENERALERROR)

        if not env['jobrec']:
            env['errorLabel'] = "FAILED"

        # set node info
        env['workerNode'] = Node.Node()
        env['workerNode'].setNodeName(getProperNodeName(os.uname()[1]))

        # collect WN info .........................................................................................
        # do not include the basename in the path since it has not been created yet
        # i.e. remove Panda_Pilot* from the workdir path
        # pUtil.tolog("Collecting WN info from: %s" % (os.path.dirname(thisSite.workdir)))
        # env['workerNode'].collectWNInfo(os.path.dirname(env['thisSite'].workdir))

        # overwrite mem since this should come from either pilot argument or queuedata
        # workerNode.mem = getWNMem(env['hasQueuedata'])

        # update the globals used in the exception handler
        globalSite = env['thisSite']
        globalWorkNode = env['workerNode']

        # create the initial pilot workdir
        ec = createSiteWorkDir(env['thisSite'].workdir, error)
        if ec != 0:
            return pUtil.shellExitCode(ec)

        # create the watch dog
        wdog = WatchDog()   

        # register cleanup function
        atexit.register(pUtil.cleanup, wdog, env['pilot_initdir'], env['wrapperFlag'], env['rmwkdir'])

        # check special environment variables
        ec = thisExperiment.checkSpecialEnvVars(env['thisSite'].sitename)
        if ec != 0:
            return pUtil.shellExitCode(ec)
    
        signal.signal(signal.SIGTERM, pUtil.sig2exc)
        signal.signal(signal.SIGQUIT, pUtil.sig2exc)
        signal.signal(signal.SIGSEGV, pUtil.sig2exc)
        signal.signal(signal.SIGXCPU, pUtil.sig2exc)
        signal.signal(signal.SIGUSR1, pUtil.sig2exc)
        signal.signal(signal.SIGBUS, pUtil.sig2exc)

        # perform job recovery ....................................................................................

        if env['jobrec']:
            runJobRecovery(env['thisSite'], env['psport'], pUtil.readpar('wntmpdir'))
            if env['jobRecoveryMode']:
                pUtil.tolog("Pilot is in Job Recovery Mode, no payload will be downloaded, will now finish")
                return pUtil.shellExitCode(0)

        # perform disk cleanup ....................................................................................
        diskCleanup(env['thisSite'].wntmpdir, env['uflag'])

        # multi job loop will begin here...........................................................................

        # master job counter
        env['number_of_jobs'] = 0

        # set the update frequency for process monitoring and output file size and user workdir size checks
        setUpdateFrequencies()

        # get the timefloor from the queuedata, the pilot is allowed to run multi-jobs within this limit
        # if set to zero, only one job will be executed
        env['timefloor'] = pUtil.getTimeFloor(env['timefloor_default'])

        # get the site information object
        env['si'] = pUtil.getSiteInformation(env['experiment'])
        if env['si']:
            pUtil.tolog("Using site information for experiment: %s" % (env['si'].getExperiment()))
        else:
            pUtil.tolog("!!FAILED!!1234!! Did not get an experiment object from the factory")
            return pUtil.shellExitCode(error.ERR_GENERALERROR)

        # loop until pilot has run out of time (defined by timefloor)
        env['multijob_startup'] = int(time.time())
        env['hasMultiJob'] = False
        
        while True:

            # create the pilot workdir (if it was not created before, needed for the first job)
            if env['number_of_jobs'] > 0:
                # update the workdir (i.e. define a new workdir and create it)
                env['thisSite'].workdir = env['thisSite'].getWorkDir()
                ec = createSiteWorkDir(env['thisSite'].workdir, error)
                if ec != 0:
                    return pUtil.shellExitCode(ec)
                globalSite = env['thisSite']

            # make sure we are in the current work dir
            pUtil.chdir(env['thisSite'].workdir)
            dumpVars(env['thisSite'])

            # do we have a valid proxy?
            if env['proxycheckFlag']:
                ec, pilotErrorDiag = thisExperiment.verifyProxy(envsetup="")
                if ec != 0:
                    pUtil.fastCleanup(env['thisSite'].workdir, env['pilot_initdir'], env['rmwkdir']) 
                    return pUtil.shellExitCode(ec)

            pUtil.tolog("Collecting WN info from: %s" % (os.path.dirname(env['thisSite'].workdir)))
            env['workerNode'].collectWNInfo(os.path.dirname(env['thisSite'].workdir))
            env['workerNode'].mem = getsetWNMem(env['memory'])

            # do we have enough local disk space to run the job?
            ec = checkLocalDiskSpace(error)
            if ec != 0:
                pUtil.tolog("Pilot was executed on host: %s" % (env['workerNode'].nodename))
                pUtil.fastCleanup(env['thisSite'].workdir, env['pilot_initdir'], env['rmwkdir']) 
                return pUtil.shellExitCode(ec)

            # getJob begins here....................................................................................

            # create the first job, usually a production job, but analysis job is ok as well
            # we just use the first job as a MARKER of the "walltime" of the pilot
            env['isJobDownloaded'] = False # (reset in case of multi-jobs)
            ec, env['job'], env['number_of_jobs'] = getJob()
            if ec != 0:
                # remove the site workdir before exiting
                # pUtil.writeExitCode(thisSite.workdir, error.ERR_GENERALERROR)
                # raise SystemError(1111)
                pUtil.fastCleanup(env['thisSite'].workdir, env['pilot_initdir'], env['rmwkdir']) 
                if ec == -1: # reset temporary error code (see getJob)
                    ec = 0
                return pUtil.shellExitCode(ec)
            else:
                env['isJobDownloaded'] = True
                pUtil.tolog("Using job definition id: %s" % (env['job'].jobDefinitionID))            

            # verify any contradicting job definition parameters here
            try:
                ec, pilotErrorDiag = thisExperiment.postGetJobActions(env['job'])
                if ec == 0:
                    pUtil.tolog("postGetJobActions: OK")
                else:
                    pUtil.tolog("!!WARNING!!1231!! Post getJob() actions encountered a problem - job will fail")

                    try:
                        # job must be failed correctly
                        pUtil.tolog("Updating PanDA server for the failed job (error code %d)" % (ec))
                        env['job'].result[0] = 'failed'
                        env['job'].currentState = env['job'].result[0]
                        env['job'].result[2] = ec
                        # note: job.workdir has not been created yet so cannot create log file
                        env['pilotErrorDiag'] = "Post getjob actions failed - workdir does not exist, cannot create job log, see batch log"
                        pUtil.tolog("!!WARNING!!2233!! Work dir has not been created yet so cannot create job log in this case - refer to batch log")
                        updatePandaServer(env['job'], env['thisSite'], env['psport'], schedulerID = env['jobSchedulerId'], pilotID = env['pilotId'])
#                        pUtil.postJobTask(env['job'], env['thisSite'], env['workerNode'], env['experiment'], jr=False)
                        pUtil.fastCleanup(env['thisSite'].workdir, env['pilot_initdir'], env['rmwkdir'])
                        return pUtil.shellExitCode(ec)
                    except Exception, e:
                        pUtil.tolog("Caught exception: %s" % (e))
            except Exception, e:
                pUtil.tolog("Caught exception: %s" % (e))

            if env['glexec'] == 'False':
                monitor = Monitor(env)
                monitor.monitor_job()
	    elif env['glexec'] == 'test':
		pUtil.tolog('glexec is set to test, we will hard-fail miserably in case of errors')
                payload = 'python -m glexec_aux'
                my_proxy_interface_instance = glexec_utils.MyProxyInterface(env['userProxy'])
                glexec_interface = glexec_utils.GlexecInterface(my_proxy_interface_instance, payload=payload)
                glexec_interface.setup_and_run()
            else:
                # Try to ping the glexec infrastructure to test if it is ok.
                # If it is ok, go ahead with glexec, if not, use the normal pilot mode without glexec.

                if os.environ.has_key('OSG_GLEXEC_LOCATION'):
			if os.environ['OSG_GLEXEC_LOCATION'] != '':
				glexec_path = os.environ['OSG_GLEXEC_LOCATION']
     			else:
			        glexec_path = '/usr/sbin/glexec'
                                os.environ['OSG_GLEXEC_LOCATION'] = '/usr/sbin/glexec'
                elif os.environ.has_key('GLEXEC_LOCATION'):
			if os.environ['GLEXEC_LOCATION'] != '':
	     			glexec_path = os.path.join(os.environ['GLEXEC_LOCATION'],'sbin/glexec')
     			else:
             			glexec_path = '/usr/sbin/glexec'
                                os.environ['GLEXEC_LOCATION'] = '/usr'
		elif os.path.exists('/usr/sbin/glexec'):
			glexec_path = '/usr/sbin/glexec'
	                os.environ['GLEXEC_LOCATION'] = '/usr'
                elif os.environ.has_key('GLITE_LOCATION'):
                        glexec_path = os.path.join(os.environ['GLITE_LOCATION'],
                                             'sbin/glexec')
                else:
			pUtil.tolog("!!WARNING!! gLExec is probably not installed at the WN!")
                        glexec_path = '/usr/sbin/glexec'

                cmd = 'export GLEXEC_CLIENT_CERT=$X509_USER_PROXY;'+glexec_path + ' /bin/true'
                stdout, stderr, status = execute(cmd)
                pUtil.tolog('cmd: %s' % cmd)
                pUtil.tolog('status: %s' % status)
                if not (status or stderr):
                        pUtil.tolog('glexec infrastructure seems to be working fine. Running in glexec mode!')
                        payload = 'python -m glexec_aux'
                        my_proxy_interface_instance = glexec_utils.MyProxyInterface(env['userProxy'])
                        glexec_interface = glexec_utils.GlexecInterface(my_proxy_interface_instance, payload=payload)
                        glexec_interface.setup_and_run()
                else:
                        pUtil.tolog('!!WARNING!! Problem with the glexec infrastructure! Will run the pilot in normal mode')
                        monitor = Monitor(env)
                        monitor.monitor_job()

            #Get the return code (Should be improved)
            if env['return'] == 'break':
                break
            elif env['return'] == 'continue':
                continue
            elif env['return'] != 0:
                return pUtil.shellExitCode(env['return'])

        pUtil.tolog("No more jobs to execute")

        # wait for the stdout to catch up (otherwise the full log is cut off in the batch stdout dump)
        time.sleep(10)
        pUtil.tolog("End of the pilot")

        # flush buffers
        sys.stdout.flush()
        sys.stderr.flush()

    # catch any uncaught pilot exceptions
    except Exception, errorMsg:

        error = PilotErrors()

        if len(str(errorMsg)) == 0:
            errorMsg = "(empty error string)"
                                                                
        import traceback
        if 'format_exc' in traceback.__all__:
            pilotErrorDiag = "Exception caught in pilot: %s, %s" % (str(errorMsg), traceback.format_exc())
        else:
            pUtil.tolog("traceback.format_exc() not available in this python version")
            pilotErrorDiag = "Exception caught in pilot: %s" % (str(errorMsg))
        pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))

        if env['isJobDownloaded']:
            if env['isServerUpdated']:
                pUtil.tolog("Do a full cleanup since job was downloaded and server updated")

                # was the process id added to env['jobDic']?
                bPID = False
                try:
                    for k in env['jobDic'].keys():
                        pUtil.tolog("Found process id in env['jobDic']: %d" % (env['jobDic'][k][0]))
                except:
                    pUtil.tolog("Process id not added to env['jobDic']")
                else:
                    bPID = True

                if bPID:
                    pUtil.tolog("Cleanup using env['jobDic']")
                    for k in env['jobDic'].keys():
                        env['jobDic'][k][1].result[0] = "failed"
                        env['jobDic'][k][1].currentState = env['jobDic'][k][1].result[0]
                        if env['jobDic'][k][1].result[2] == 0:
                            env['jobDic'][k][1].result[2] = error.ERR_PILOTEXC
                        if env['jobDic'][k][1].pilotErrorDiag == "":
                            env['jobDic'][k][1].pilotErrorDiag = pilotErrorDiag
                        if globalSite:
                            pUtil.postJobTask(env['jobDic'][k][1], globalSite, globalWorkNode, env['experiment'], jr=False)
                            env['logTransferred'] = True
                        pUtil.tolog("Killing process: %d" % (env['jobDic'][k][0]))
                        killProcesses(env['jobDic'][k][0], env['jobDic'][k][1].pgrp)
                        # move this job from env['jobDic'] to zombieJobList for later collection
                        env['zombieJobList'].append(env['jobDic'][k][0]) # only needs pid of this job for cleanup
                        del env['jobDic'][k]

                    # collect all the zombie processes
                    wdog.collectZombieJob(tn=10)
                else:
                    pUtil.tolog("Cleanup using globalJob")
                    env['globalJob'].result[0] = "failed"
                    env['globalJob'].currentState = env['globalJob'].result[0]
                    env['globalJob'].result[2] = error.ERR_PILOTEXC
                    env['globalJob'].pilotErrorDiag = pilotErrorDiag
                    if globalSite:
                        pUtil.postJobTask(env['globalJob'], globalSite, globalWorkNode, env['experiment'], jr=False)
            else:
                if globalSite:
                    pUtil.tolog("Do a fast cleanup since server was not updated after job was downloaded (no log)")
                    pUtil.fastCleanup(globalSite.workdir, env['pilot_initdir'], env['rmwkdir'])
        else:
            if globalSite:
                pUtil.tolog("Do a fast cleanup since job was not downloaded (no log)")
                pUtil.fastCleanup(globalSite.workdir, env['pilot_initdir'], env['rmwkdir'])
        return pUtil.shellExitCode(error.ERR_PILOTEXC)

    # end of the pilot
    else:
        return pUtil.shellExitCode(0)

# main
if __name__ == "__main__":
    
    runMain(sys.argv[1:])
