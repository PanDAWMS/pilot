__author__ = 'complynx'
"""
I don't call this Job Recovery for one simple reason, it's name is it's purpose and job.

For me, Job Recovery should try to restart the job in each case of error, even when an error appeared during staging in.
However, this code is only for one purpose: to perform staging out procedure with files haven't been staged out.

Also I had too much time to spend it on writing all of these comments
"""

from glob import glob

import os
import pUtil
import time
from JobState import JobState
from JobLog import JobLog
from pilot import createAtomicLockFile, releaseAtomicLockFile, updatePandaServer
from Configuration import Configuration
from PilotErrors import PilotErrors
import Mover
import Site
import Job
import Node

import environment
environment.set_environment()
env = Configuration()
globalSite = None

acceptedJobStatesFromFile = ['holding', 'lostheartbeat', 'failed']
finalJobStates = ['transferring', 'failed', 'notfound', 'finished']
pilotLogFileInNewWD="pilotlog.txt"


class ReturnCode:
    OK = 0
    SkipJob = 1
    Cleanup = 2
    PostJobOnly = 3
    FailedJob = 4
    Holding = 5


def GetDefaultDeferredStageoutDir(**kwargs):
    """
    Resolves standard deferred stageout path.
    :param `thisSite` can be set
    :return: (string) deferred stageout directory
    """
    d = DorE(kwargs, "thisSite").wntmpdir
    return d if d != '' else "/tmp"


def DeferredStageout(deferred_stageout_dirs, max_stageout_jobs=0,
                     **kwargs):
    """
    The procedure loops through provided dirs and performs staging out on each job in these dirs.
    Usage:
        `DeferredStageout(['path/to/dir1','path/to/dir2',...])`

    :param deferred_stageout_dirs:  (array of strings)  list of directories to search for jobs.
                                    mandatory parameter
    :param max_stageout_jobs:   (integer)   maximum stageout jobs to be finished, if zero, every job will be processed
                                defaults to zero

    Other parameters are passed into DeferredStageoutDir

    :return: (integer) number of staged out jobs
    """
    pUtil.tolog("Starting deferred stageout procedure. This procedure will scan directories for any left job files"
                " and try to preform stageout for each file that haven't been sent.")
    stageout_jobs = 0
    d = dict(kwargs)

    # reduce duplicates
    dirs_set = set(map(os.path.abspath, deferred_stageout_dirs))

    for deferred_stageout_dir in dirs_set:
        # increment in two steps, because maybe I'll send some other params
        d.update({'max_stageout_jobs': max_stageout_jobs-stageout_jobs if max_stageout_jobs > 0 else 0})
        stageout_jobs_d = DeferredStageoutDir(deferred_stageout_dir, **d)
        # We don't need to do anything except of staging out every job in these dirs
        # If there is something to do here -- think, maybe there should be another place to do it?
        stageout_jobs += stageout_jobs_d

        if stageout_jobs >= max_stageout_jobs > 0:
            return stageout_jobs

    return stageout_jobs


def DeferredStageoutLocal(**kwargs):
    """
    This is a simple alias to
        `DeferredStageout.DeferredStageoutDir('.', lockfile_name="LOCKFILE")`

    :return: (integer) number of staged out jobs
    """
    pUtil.tolog("Starting deferred stageout procedure. This procedure will scan current dir for any left job files"
                " and try to preform stageout for each file that haven't been sent.")

    kwargs.setdefault('lockfile_name', "LOCKFILE")
    return DeferredStageoutDir('.', **kwargs)


def DeferredStageoutDir(deferred_stageout_dir, max_stageout_jobs=0,
                        **kwargs):
    """
    The procedure loops through the directory and performs staging out on each job.
    Usage
        `DeferredStageout.DeferredStageoutDir('path/to/dir'[, param_name=param_value])`

    :param deferred_stageout_dir:   (string) directory to scan for deferred stageout jobs
                                    mandatory parameter
    :param max_stageout_jobs:   (integer)   maximum stageout jobs to be finished, if zero, every job will be processed
                                defaults to zero
    :param job_state_mode:  ("default"|"test")  Mode of job state file
                            defaults to "default"

    Other parameters are passed into DeferredStageoutJob

    :return: (integer) number of staged out jobs
    """
    pUtil.tolog("Scanning directory \"%s\" for deferred stageout jobs." % deferred_stageout_dir)
    stageout_jobs = 0
    d = dict(kwargs)

    job_state_files = glob(deferred_stageout_dir + "/*/jobState-*")

    job_state_files = pUtil.removeTestFiles(job_state_files, mode=kwargs.get("job_state_mode", "default"))

    for job_state_file in job_state_files:
        # increment in two steps, because maybe I'll send some other params
        d.update({'job_state_file': job_state_file})
        was_stageout = DeferredStageoutJob(os.path.dirname(job_state_file),
                                           **kwargs)

        if was_stageout:
            stageout_jobs += 1

        if stageout_jobs >= max_stageout_jobs > 0:
            return stageout_jobs

    return stageout_jobs


def DeferredStageoutJob(job_dir, job_state_file="",
                        **kwargs):
    """
    Performs stageing out preparation and stages out the job in specified directory.

    :param job_dir:     (string)    directory with a job.
                        mandatory parameter
    :param job_state_file:  (string)    path to job state file or other file containing job state. If empty, job
                                        state file is located as job_dir+'/jobState-*.*'.
                            defaults to ""

    Other parameters are passed into other functions

    :return: (bool) the fact of stageout being performed
    """
    pUtil.tolog("Deferred stageout from job directory \"%s\"" % job_dir)
    job_state = JobState()

    if job_state_file == "":
        try:
            job_state_file = glob(job_dir + "/jobState-*")[0]
        except:
            pUtil.tolog("There is no job state file in the provided directory, exiting")
            return False

    pUtil.tolog("Job state file is %s"%job_state_file)

    lockfd, lockfn = createAtomicLockFile(job_dir)

    try:
        if not TestJobDirForDeferredStageoutNecessity(job_dir, job_state_file, **kwargs):
            pUtil.tolog("Job \"%s\" does not need deferred stageout procedure (yet)" % job_dir)
            releaseAtomicLockFile(lockfd, lockfn)
            return False

        if not job_state.get(job_state_file):
            pUtil.tolog("Job state file reading failed, exiting")
            releaseAtomicLockFile(lockfd, lockfn)
            return False

        pUtil.tolog("Working with job in \"%s\"" % job_dir)
        _job, _site, _node, _recoveryAttempt = job_state.decode()

        if not (_job and _site and _node):
            pUtil.tolog("Can not decode jobState file, exiting")
            releaseAtomicLockFile(lockfd, lockfn)
            return False

        rc = PrepareJobForDeferredStageout(job_state, **kwargs)

        if rc == ReturnCode.PostJobOnly:
            pUtil.postJobTask(job_state.job, job_state.site, DorE(kwargs, 'workerNode'), DorE(kwargs, 'experiment'),
                              jr=True, ra=job_state.recoveryAttempt)
            releaseAtomicLockFile(lockfd, lockfn)
            return True

        if rc > 0:
            pUtil.tolog("Job is not prepared for stageout, exiting")
            if rc == ReturnCode.Cleanup:
                cleanup(job_state)
            releaseAtomicLockFile(lockfd, lockfn)
            return False

        rc, logfile, datadir, filelist = CreateTransferFileList(job_state, **kwargs)

        XMLStr = ''
        if datadir == "":
            try:
                XMLStr = job_state.node['xml']
            except:
                pass

        if XMLStr == '':
            XMLStr = pUtil.getMetadata(job_state.site.workdir, job_state.job.jobId)

        currentdir = os.getcwd()
        pUtil.chdir(job_state.site.workdir)

        if len(filelist):
            pUtil.tolog("Stageout will now transfer the files")
            rc = TransferFiles(job_state, datadir, filelist, **kwargs)

            if rc == ReturnCode.Holding:
                job_state.job.result[0] = "holding"
            if rc == ReturnCode.FailedJob:
                job_state.job.result[0] = "failed"

            job_state.job.setState(job_state.job.result)

        pUtil.chdir(job_state.site.workdir)
        ret = True
        if logfile != "":
            pUtil.tolog("Stageout will now transfer the log")
            log = JobLog()
            ret, _ = log.transferLogFile(job_state.job, job_state.site, DorE(kwargs, 'experiment'), dest=None, jr=True)

        if not ret:
            rc = ReturnCode.Holding  # We need to transfer log file regardless the files

        if rc == ReturnCode.OK:
            if pUtil.verifyTransfer(job_state.site.workdir):
                job_state.job.result[0] = "finished"
            else:
                job_state.job.result[0] = "failed"
            job_state.job.setState(job_state.job.result)

        if job_state.job.result[0] in finalJobStates:
            job_state.job.final_state = job_state.job.result[0]

        pUtil.tolog("Stageout will now update the server with new status")

        rt, retNode = updatePandaServer(job_state.job, job_state.site, DorE(kwargs, 'psport'), xmlstr=XMLStr,
                                        ra=job_state.recoveryAttempt, schedulerID=DorE(kwargs, 'jobSchedulerId'),
                                        pilotID=DorE(kwargs, 'pilotId'))

        if rt == 0:
            pUtil.tolog("Job %s updated (exit code %d)" % (job_state.job.jobId, job_state.job.result[2]))

            # did the server send back a command?
            if "tobekilled" in job_state.job.action:
                pUtil.tolog("!!WARNING!!1120!! Panda server returned a \'tobekilled\' command")
                job_state.job.result[0] = "failed"

            # further recovery attempt unnecessary, but keep the work dir for debugging
            if job_state.job.result[0] == "failed":
                pUtil.tolog("Further recovery attempts will be prevented for failed job (will leave work dir)")
                if not job_state.rename(job_state.site, job_state.job):
                    pUtil.tolog("(Fate of job state file left for next pilot)")

        else:
            pUtil.tolog("!!WARNING!!1120!! Panda server returned a %d" % (rt))

            # store the final state so that the next pilot will know

            # store the metadata xml
            retNode['xml'] = XMLStr

            # update the job state file with the new state information
            _retjs = pUtil.updateJobState(job_state.job, job_state.site, retNode, job_state.recoveryAttempt)

        pUtil.tolog("Stageout will now proceed to post-job actions")

        if job_state.job.result[0] in finalJobStates:
            pUtil.postJobTask(job_state.job, job_state.site,
                              DorE(kwargs, 'workerNode'), DorE(kwargs, 'experiment'), jr=True, ra=job_state.recoveryAttempt)

        pUtil.chdir(currentdir)

        releaseAtomicLockFile(lockfd, lockfn)

        if job_state.job.result[0] == "finished":
            pUtil.tolog("Stageout will now remove the job, it is in finished state and can be removed")
            job_state.cleanup()

        return True
    except:
        releaseAtomicLockFile(lockfd, lockfn)
        raise


def TestJobDirForDeferredStageoutNecessity(job_dir, job_state_file,
                                           test_mtime=True, test_lockfile="", lockfile_logic=True, min_time=0,
                                           **kwargs):
    """
    Tests, whether a directory and a state file provided are in right condition to perform staging out

    :param job_dir:     (string)    directory with a job.
                        mandatory parameter
    :param job_state_file:  (string)    path to job state file or other file containing job state.
                            mandatory parameter
    :param test_mtime:  (bool)  does the stageout procedure need to be sure that the job is old enough to start the
                                staging out procedure
                        defaults to True
    :param test_lockfile:   (string)    should the procedure test the lock file before staging out. The presence of it
                                        is compared to lockfile_logic
                                        If the string is empty, does not perform a test.
                            defaults to ""
    :param lockfile_logic:  (bool)  defines the condition of lockfile for staging out procedure to perform staging out
                            defaults to True
    :param min_time:    (integer)   minimum time for job to be untouched to perform deferred stageout. If is less then
                                    or equal to zero, default value is used
                        defaults to 2*heartbeatPeriod
    :param heartbeatPeriod: (integer)   used to calculate min_time default value
                            defaults to env['heartbeatPeriod']

    Other params ommited.

    :return: (bool) a confirmation of staging out for a job
    """

    if job_state_file.endswith(".MAXEDOUT"):
        return lognfalse("Job already exceeded maximum number of attempts.")

    if test_lockfile != "" and \
                    bool(pUtil.checkLockFile(os.path.dirname(job_state_file), test_lockfile)) != bool(lockfile_logic):
        return lognfalse("Lockfile test prevents from stageout")

    current_time = int(time.time())
    if test_mtime:
        try:
            file_modification_time = os.path.getmtime(job_state_file)
        except:  # no modification time -- no staging out (if mtime needed)
            return lognfalse("Couldn't get modification time for job state file, maybe better to run the code without"
                             " this test?")

        if min_time <= 0:
            min_time = 2*DorE(kwargs, 'heartbeatPeriod')
        if (current_time - file_modification_time) <= min_time:
            return lognfalse("File was modified not so long ago, skipping.")

    return True


def lognfalse(_str):
    return lognret(False,_str)


def lognret(ret,_str):
    pUtil.tolog(_str)
    return ret


def DorE(dictionary, key):
    return dictionary.get(key, env[key])


def cleanup(job_state):
    pUtil.tolog("Cleanup job directory called")
    if not job_state.rename(job_state.site, job_state.job):
        return lognfalse("Failed to rename (Fate of job state file left for next pilot)")
    else:
        if not job_state.cleanup():
            return lognfalse("!!WARNING!!1110!! Failed to cleanup")


def PrepareJobForDeferredStageout(job_state, **kwargs):
    """
    Function prepares job for staging out
    :param job_state:   (JobState) decoded job state file
                        mandatory

    Other params can be passed into functions:
        1. To overwrite environment variables:
            1. uflag -- pUtil.isSameType
            2. pshttpurl, psport, pilot_initdir -- pUtil.getJobStatus
            3. thisSite -- compare the site names
            4. maxNumberOfRecoveryAttempts -- check recoveryAttempt
            5. psport, jobSchedulerId, pilotId -- server updates

    Unused parameters are ommited.

    :return: (integer)
        0. job is prepared
        1. job is not prepared, skip
        2. job is to be removed, proceed to cleanup
    """

    # Next test is testing trf type and PROD/ANAL job vs pilot comparison
    if not pUtil.isSameType(job_state.job.trf.split(",")[0], DorE(kwargs, 'uflag')):
        return lognret(ReturnCode.SkipJob, "Job is not the same type as current pilot")

    # Next test ensures that the pilot is started on the same site.
    # Maybe we should add env var to switch this test on and off, there can be a number of equal nodes sharing one FS
    # Which can equally perform recovery of the lost jobs from each other
    if job_state.site.sitename != DorE(kwargs, 'thisSite').sitename:
        return lognret(ReturnCode.SkipJob, "Job is not running on the same site")

    # This test ensures that the number of recovery attempts did not exceeded and if exceeded, updates the server and
    # the state file
    if job_state.recoveryAttempt >= DorE(kwargs, 'maxNumberOfRecoveryAttempts'):
        pUtil.tolog("!!WARNING!!1100!! Max number of recovery attempts exceeded: %d" %
                    (env['maxNumberOfRecoveryAttempts']))
        job_state.job.setState(['failed', job_state.job.result[1], PilotErrors().ERR_LOSTJOBMAXEDOUT])
        rt, retNode = updatePandaServer(job_state.job, job_state.site,
                                        DorE(kwargs, 'psport'),
                                        ra=job_state.recoveryAttempt,
                                        schedulerID=DorE(kwargs, 'jobSchedulerId'),
                                        pilotID=DorE(kwargs, 'pilotId'))
        if rt == 0:
            pUtil.tolog("Job %s updated (exit code %d)" % (job_state.job.jobId, job_state.job.result[2]))

        else:
            pUtil.tolog("Panda server returned a %d" % (rt))
            return lognret(ReturnCode.SkipJob, "(Failed to update panda server - leave for next pilot)")

    jobStatus = job_state.job.result[0]
    jobStatusCode = 0
    jobAttemptNr = job_state.job.attemptNr

    # There can be unclear state, consult the server
    if jobStatus not in acceptedJobStatesFromFile:
        pUtil.tolog("Job state is unclear (%s), checking with the server" % jobStatus)
        jobStatus, jobAttemptNr, jobStatusCode = pUtil.getJobStatus(job_state.job.jobId,
                                                                    DorE(kwargs, 'pshttpurl'),
                                                                    DorE(kwargs, 'psport'),
                                                                    DorE(kwargs, 'pilot_initdir'))
        if jobStatusCode != 0:
            return lognret(ReturnCode.SkipJob, "Received general error code from dispatcher call (leave job for later"
                                                " pilot)")
        else:
            pUtil.tolog("Job state is %s" % jobStatus)

    # If any inconvenience occurs or job is finalised, cleanup
    if job_state.job.attemptNr != jobAttemptNr or\
                     jobStatus in finalJobStates or\
                     "tobekilled" in job_state.job.action:
        return lognret(ReturnCode.Cleanup, "Further recovery attempts will be prevented for this job")

    if jobStatus != 'holding':
        # is the attemptNr defined?
        try:
            attemptNr = job_state.job.attemptNr
        except Exception, e:
            pUtil.tolog("!!WARNING!!1100!! Attempt number not defined [ignore]: %s" % str(e))
        else:
            # check if the attemptNr (set during initial getJob command) is the same
            # as the current jobAttemptNr from the server (protection against failed lost
            # heartbeat jobs due to reassigned panda job id numbers)
            if attemptNr != jobAttemptNr:
                pUtil.tolog("!!WARNING!!1100!! Attempt number mismatch for job %s (according to server - will not be"
                            " recovered)" % job_state.job.jobId)
                pUtil.tolog("....Initial attempt number: %d" % attemptNr)
                pUtil.tolog("....Current attempt number: %d" % jobAttemptNr)
                pUtil.tolog("....Job status (server)   : %s" % jobStatus)
                pUtil.tolog("....Job status (state)    : %s" % job_state.job.result[0])
                return lognret(ReturnCode.Cleanup, "Further recovery attempts will be prevented for this job")
            else:
                pUtil.tolog("Attempt numbers from server and job state file agree: %d" % attemptNr)

    job_state.recoveryAttempt += 1

    currentDir = os.getcwd()
    pUtil.chdir(job_state.site.workdir)
    # Update state to prevent another pilot to try to recover this job
    # This was at the main logic, but I don't see, why should we do that while lockfile is present
    pUtil.updateJobState(job_state.job, job_state.site, job_state.node, job_state.recoveryAttempt)
    pUtil.chdir(currentDir)

    ec = job_state.job.result[2]
    logfile = "%s/%s" % (job_state.site.workdir, job_state.job.logFile)
    if not ec:
        if (not os.path.isfile(logfile)) and os.path.isdir(job_state.job.newDirNM)\
                and os.path.isfile(os.path.join(job_state.job.newDirNM, pilotLogFileInNewWD)):
            ec = pUtil.getExitCode(job_state.job.newDirNM, "pilotlog.txt")

            # Too big nesting, but I don't know how to do better.
            # We search an error code, if not found, we get it from the server
            if ec == -1:
                job_state.job.setState(['failed', 0, PilotErrors().ERR_LOSTJOBNOTFINISHED])
                pUtil.tolog("Exit code not found")

                # get the metadata
                # this metadata does not contain the metadata for the log
                strXML = pUtil.getMetadata(job_state.site.workdir, job_state.job.jobId)

                # update the server
                rt, retNode = updatePandaServer(job_state.job, job_state.site, DorE(kwargs, 'psport'),
                                                xmlstr=strXML, ra=job_state.recoveryAttempt,
                                                schedulerID=DorE(kwargs, 'jobSchedulerId'),
                                                pilotID=DorE(kwargs, 'pilotId'))
                if rt == 0:
                    return lognret(ReturnCode.Cleanup, "Lost job %s updated (exit code %d)" % (job_state.job.jobId,
                                                                                               job_state.job.result[2]))
                else:
                    pUtil.tolog("!!WARNING!!1130!! Panda server returned a %d" % (rt))

                    # store the final state so that the next pilot will know
                    # store the metadata xml
                    retNode['xml'] = strXML

                    # update the job state file with the new state information
                    pUtil.updateJobState(job_state.job, job_state.site, retNode, job_state.recoveryAttempt)
                    return lognret(ReturnCode.SkipJob, "It's a task for the next pilot now")

    # # I think, this is useless and wrong piece of code. Maybe will be removed, or changed in the future.
    # # Copied from previous jobrec.
    # if ec != 0 and not os.path.isfile(logfile):
    #     pUtil.tolog("ec: %s, log: %s" % (ec, logfile))
    #     # Here I don't understand, why we shouldn't transfer pilotlog.txt file also, but nevertheless
    #     return lognret(ReturnCode.PostJobOnly, "It is failed job, doing only post-job.")

    return lognret(ReturnCode.OK, "Job preparations finished, job is ready.")


def CreateTransferFileList(job_state, **kwargs):
    """
    Function prepares job for staging out
    :param job_state:   (JobState) decoded job state file
                        mandatory

    Other params can be passed into functions:
        1. To overwrite environment variables:

    Unused parameters are ommited.

    :return:
        1. (integer)    return code
        2. (string)     logfile path
        3. (string)     dir with files
        2. (list)       list of files to be transferred
    """

    currentDir = os.getcwd()
    pUtil.chdir(job_state.site.workdir)

    logfile = "%s/%s" % (job_state.site.workdir, job_state.job.logFile)
    logfile = os.path.abspath(logfile)\
        if os.path.isfile(logfile) and not pUtil.isLogfileCopied(job_state.site.workdir) else ""

    remaining_files = []
    filesDir = ''
    if os.path.isdir(job_state.job.datadir):
        pUtil.chdir(job_state.job.datadir)
        remaining_files = pUtil.getRemainingOutputFiles(job_state.job.outFiles)
        if not remaining_files:
            remaining_files = []
        filesDir = os.path.abspath(job_state.job.datadir)

    pUtil.chdir(currentDir)

    return ReturnCode.OK, logfile, filesDir, remaining_files


def setGuids(job_state, files, **kwargs):
    job = job_state.job
    try:
        job.outFilesGuids = map(lambda x: pUtil.getGuidsFromXML(job_state.site.workdir, id=job.jobId, filename=x)[0],
                                files)
        job.outFiles = files
    except Exception, e:
        job.outFilesGuids = None
        pUtil.tolog("!!FAILED!!1105!! Exception caught (Could not read guids for the remaining output files): %s" %
                    str(e))
        return False

    pUtil.tolog("Successfully fetched guids")
    return True


def updateOutPFC(job, **kwargs):
    file_name = "OutPutFileCatalog.xml"
    file_path = os.path.join(DorE(kwargs, 'thisSite').workdir, file_name)
    try:
        guids_status = pUtil.PFCxml(job.experiment, file_path, job.outFiles, fguids=job.outFilesGuids, fntag="pfn",
                                    analJob=pUtil.isAnalysisJob(job.trf.split(",")[0]), jr=True)
    except Exception, e:
        pUtil.tolog("!!FAILED!!1105!! Exception caught (Could not generate xml for the remaining output files): %s" %
                    str(e))
        job.result[2] = PilotErrors().ERR_LOSTJOBXML
        return False
    else:
        if not guids_status:
            pUtil.tolog("!!FAILED!!2999!! Missing guid(s) for output file(s) in metadata")
            job.result[2] = PilotErrors().ERR_MISSINGGUID
            return False

    return file_path


def defaultDSname(dblock):
    if dblock and dblock[0] != 'NULL' and dblock[0] != ' ':
        return dblock[0]
    return "%s-%s-%s" % (time.localtime()[0:3])


def TransferFiles(job_state, datadir, files, **kwargs):
    """
    Transfers files from list 'files'

    May change CWD with pUtil.chdir (several times)

    :param job_state:
    :param datadir: job data dir
    :param files: list of filenames
    :param kwargs: specific arguments for other purposes
    :return:
    """
    job = job_state.job

    pUtil.chdir(datadir)

    XMLMetadata = pUtil.getMetadata(job_state.site.workdir, job.jobId)
    thisSite = DorE(kwargs, 'thisSite')

    if not setGuids(job_state, files, **kwargs):
        job.result[2] = PilotErrors().ERR_LOSTJOBPFC
        return ReturnCode.FailedJob

    outPFC = updateOutPFC(job, **kwargs)
    if not outPFC:
        return ReturnCode.FailedJob

    dsname = defaultDSname(job.destinationDblock)

    datasetDict = pUtil.getDatasetDict(job.outFiles, job.destinationDblock, job.logFile, job.logDblock)
    if not datasetDict:
        pUtil.tolog("Output files will go to default dataset: %s" % (dsname))

    # the cmtconfig is needed by at least the xrdcp site mover
    cmtconfig = pUtil.getCmtconfig(job.cmtconfig)

    tin_0 = os.times()
    rf = None
    _state = ReturnCode.OK
    _msg = ""
    ec = -1
    try:
        # Note: alt stage-out numbers are not saved in recovery mode (job object not returned from this function)
        rc, pilotErrorDiag, rf, rs, job.filesNormalStageOut, job.filesAltStageOut = Mover.mover_put_data(
            "xmlcatalog_file:%s" % outPFC, dsname,
            thisSite.sitename, thisSite.computingElement, analysisJob=pUtil.isAnalysisJob(job.trf.split(",")[0]),
            proxycheck=DorE(kwargs, 'proxycheckFlag'), spsetup=job.spsetup, scopeOut=job.scopeOut,
            scopeLog=job.scopeLog, token=job.destinationDBlockToken, pinitdir=DorE(kwargs, 'pilot_initdir'),
            datasetDict=datasetDict, prodSourceLabel=job.prodSourceLabel,
            jobId=job.jobId, jobWorkDir=job.workdir, DN=job.prodUserID,
            dispatchDBlockTokenForOut=job.dispatchDBlockTokenForOut,
            jobCloud=job.cloud, logFile=job.logFile,
            stageoutTries=DorE(kwargs, 'stageoutretry'), experiment=job.experiment,
            cmtconfig=cmtconfig, recoveryWorkDir=thisSite.workdir,
            fileDestinationSE=job.fileDestinationSE, job=job)
    except Exception, e:
        pilotErrorDiag = "Put function can not be called for staging out: %s" % str(e)
        pUtil.tolog("!!%s!!1105!! %s" % (env['errorLabel'], pilotErrorDiag))
        ec = PilotErrors().ERR_PUTFUNCNOCALL
        _state = ReturnCode.Holding
        _msg = env['errorLabel']
    else:
        if pilotErrorDiag != "":
            pilotErrorDiag = "Put error: " + pUtil.tailPilotErrorDiag(pilotErrorDiag,
                                                                      size=256-len("pilot: Put error: "))

        ec = rc
        pUtil.tolog("Put function returned code: %d" % (rc))
        if rc != 0:
            # remove any trailing "\r" or "\n" (there can be two of them)
            if rs is not None:
                rs = rs.rstrip()
                pUtil.tolog(" Error string: %s" % (rs))

            # is the job recoverable?
            if PilotErrors().isRecoverableErrorCode(rc):
                _state = ReturnCode.Holding
                _msg = "WARNING"
            else:
                _state = ReturnCode.FailedJob
                _msg = env['errorLabel']

            # look for special error in the error string
            if rs == "Error: string Limit exceeded 250":
                pUtil.tolog("!!%s!!3000!! Put error: file name string limit exceeded 250" % (_msg))
                ec = PilotErrors().ERR_LRCREGSTRSIZE

            pUtil.tolog("!!%s!! %s" % (_msg, PilotErrors().getErrorStr(rc)))
        else:
            # create a weak lockfile meaning that file transfer worked, and all output files have now been transferred
            pUtil.createLockFile(True, job_state.site.workdir, lockfile="ALLFILESTRANSFERRED")

    # finish the time measurement of the stage-out
    tin_1 = os.times()
    job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

    # set the error codes in case of failure
    job.pilotErrorDiag = pilotErrorDiag
    if ec != 0:
        pUtil.tolog("!!%s!!2999!! %s" % (_msg, pilotErrorDiag))

    job.result[2] = ec

    return _state


