__author__ = 'complynx'
"""
I don't call this Job Recovery for one simple reason, it's name is it's purpose and job.

For me, Job Recovery should try to restart the job in each case of error, even when an error appeared during staging in.
However, this code is only for one purpose: to perform staging out procedure with files haven't been staged out.

Also I had too much time to spend it on writing all of these comments
"""

from glob import glob

import os, sys
import pUtil
import time
from JobState import JobState
from JobLog import JobLog
from Configuration import Configuration
from PilotErrors import PilotErrors
import Mover
from Monitor import Monitor
import Site
import Job
import Node
import traceback
import commands
from Logger import Logger

from pUtil import tologNew as log

import environment
environment.set_environment()
env = Configuration()
globalSite = None

acceptedJobStatesFromFile = ['holding', 'lostheartbeat', 'failed']
finalJobStates = ['transferring', 'failed', 'notfound', 'finished']
pilotLogFileInNewWD = "pilotlog.txt"

jobState_file_wildcart = "jobState-*"
hpc_jobState_file_wildcart = "HPCManagerState.json"
number_of_lost_heartbeats_for_pilot_to_be_dead = 2 #20

log_useful = False
# deferredStageoutLogFileTpl = "pilotlog-deferredstageout-{jobid}.txt"


class LogWrapper(object):

    def __init__(self, nameTpl, id):
        if not (type(nameTpl) is str) or nameTpl == "":
            self.__name = False
        else:
            self.__name = nameTpl.format(job_id=id)
        self.__old_name = ''
        self.useful = False
        self.same = False

    def __enter__(self):
        global log_useful
        self.__old_name = pUtil.getPilotlogFilename()

        if not self.__name or self.__name == self.__old_name:
            self.same = True
        else:
            log("Staging up log file, new log file is %s" % self.__name)

            self.__global_old = os.path.abspath(self.__old_name)
            self.__global = os.path.abspath(self.__name)
            pUtil.setPilotlogFilename(self.__global)

            self.__old_useful = log_useful
            log_useful = False

        return self

    def __exit__(self, *args):
        global log_useful

        if not self.same:
            log("Staging down log file")

            self.useful = self.useful and log_useful
            log_useful = self.__old_useful

            os.chdir(os.path.dirname(self.__global_old))
            pUtil.setPilotlogFilename(self.__old_name)

            if not self.useful:
                os.remove(self.__global)


class LockFileWrapper(object):

    def __init__(self, file_path):
        self.__name = os.path.join(os.path.dirname(file_path), "ATOMIC_LOCKFILE")

    def __enter__(self):
        # acquire the lock
        self.__fd = os.open(self.__name, os.O_EXCL|os.O_CREAT)

        return True

    def __exit__(self, *args):
        try:
            os.close(self.__fd)
            os.unlink(self.__name)
        except Exception, e:
            if "Bad file descriptor" in e:
                log("Lock file already released")
            else:
                log("WARNING: Could not release lock file: %s" % (e))
        else:
            log("Released lock file: %s" % (self.__name))


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
    log("Starting deferred stageout procedure. This procedure will scan directories for any left job files"
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
    log("Starting deferred stageout procedure. This procedure will scan current dir for any left job files"
                " and try to preform stageout for each file that haven't been sent.")

    kwargs.setdefault('lockfile_name', "LOCKFILE")
    return DeferredStageoutDir('.', **kwargs)


def DeferredStageoutDir(deferred_stageout_dir, max_stageout_jobs=0, remove_empty_dir=False,
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

    :param remove_empty_dir:  (bool)    Remove the directory or not
                            defaults to False

    Other parameters are passed into DeferredStageoutJob

    :return: (integer) number of staged out jobs
    """
    log("Scanning directory \"%s\" for deferred stageout jobs." % deferred_stageout_dir)
    stageout_jobs = 0
    d = dict(kwargs)

    job_state_files = glob(deferred_stageout_dir + "/*/" + jobState_file_wildcart)
    job_state_files = pUtil.removeTestFiles(job_state_files, mode=kwargs.get("job_state_mode", "default"))

    hpc_job_state_dirs = map(os.path.dirname, glob(deferred_stageout_dir + "/*/" + hpc_jobState_file_wildcart))

    job_state_files = filter(lambda jsf: os.path.dirname(jsf) not in hpc_job_state_dirs, job_state_files)

    for hpc_job_state_dir in hpc_job_state_dirs:
        # increment in two steps, because maybe I'll return some other params
        was_stageout = DeferredStageoutHPCJob(hpc_job_state_dir, **kwargs)

        if was_stageout:
            stageout_jobs += 1

        if stageout_jobs >= max_stageout_jobs > 0:
            return stageout_jobs

    for job_state_file in job_state_files:
        # increment in two steps, because maybe I'll return some other params
        d.update({'job_state_file': job_state_file})
        was_stageout = DeferredStageoutJob(os.path.dirname(job_state_file),
                                           **kwargs)

        if was_stageout:
            stageout_jobs += 1

        if stageout_jobs >= max_stageout_jobs > 0:
            return stageout_jobs

    # if not all dirs were processed, don't remove
    # remove only if there is no dir unprocessed

    log("Finished processing directory \"%s\"." % deferred_stageout_dir)

    if remove_empty_dir:
        log("Directory \"%s\" is to be removed." % deferred_stageout_dir)
        log("Contents:")
        o, e = commands.getstatusoutput("ls -la "+deferred_stageout_dir)
        log("%s" % o)
        dirs = filter(os.path.isdir, glob(deferred_stageout_dir + "/*"))

        if len(dirs) < 1:
            log("It is OK to remove it, proceeding." % deferred_stageout_dir)
            o, e = commands.getstatusoutput("rm -rf "+deferred_stageout_dir)
        else:
            log("There are subdirs in this dir, can not remove.")
            log("Remaining subdirs: %s" % dirs)

    return stageout_jobs


def DeferredStageoutHPCJob(job_dir, deferred_stageout_logfile=False, **kwargs):
    """
    Performs stageing out preparation for the HPC job in specified directory.

    :param job_dir:     (string)    directory with a job.
                        mandatory parameter

    :param deferred_stageout_logfile: (string|False)    template name for deferred log stageout
                                                        Replaces "{job_id}" with current job id like
                                                        "log-{job_id}.txt" -> "log-124124.txt"
                                        Default False

    Other parameters are passed into other functions

    :return: (bool) the fact of stageout being performed
    """
    log("Deferred stageout from HPC job directory \"%s\"" % job_dir)

    file_path = job_dir+"/"+hpc_jobState_file_wildcart
    current_dir = os.getcwd()
    log("Working on %s" % file_path)
    log("Chdir from current dir %s to %s" % (current_dir, job_dir))
    pUtil.chdir(job_dir)

    try:
        with LockFileWrapper(file_path) as is_locked:
            if not is_locked:
                return False

            from json import load
            with open(file_path) as data_file:
                HPC_state = load(data_file)
            job_state_file = HPC_state['JobStateFile']
            job_command = HPC_state['JobCommand']
            # global_work_dir = HPC_state['GlobalWorkingDir']
            JS = JobState()
            JS.get(job_state_file)
            _job, _site, _node, _recoveryAttempt = JS.decode()

            with LogWrapper(deferred_stageout_logfile, _job.jobId) as logger:
                jobStatus, jobAttemptNr, jobStatusCode = pUtil.getJobStatus(_job.jobId, DorE(kwargs,'pshttpurl'),
                                                                            DorE(kwargs,'psport'),
                                                                            DorE(kwargs,'pilot_initdir'))
                # recover this job?
                if jobStatusCode == 20:
                    log("Received general error code from dispatcher call (leave job for later pilot)")
                    # release the atomic lockfile and go to the next directory
                    # releaseAtomicLockFile(fd, lockfile_name)
                    return False
                elif jobStatus in finalJobStates or "tobekilled" in _job.action:
                    log("Job %s is currently in state \'%s\' with attemptNr = %d (according to server - will not"
                                " perform staging out)" % (_job.jobId, jobStatus, jobAttemptNr))
                    # releaseAtomicLockFile(fd, lockfile_name)
                    return False

                # update job state file at this point to prevent a parallel pilot from doing a simultaneous recovery
                _retjs = pUtil.updateJobState(_job, _site, _node, _recoveryAttempt)
                # releaseAtomicLockFile(fd, lockfile_name)

        monitor = Monitor(env)
        monitor.monitor_recovery_job(_job, _site, _node, job_command, job_state_file, recover_dir=job_dir)

        log("Chdir back to %s" % current_dir)
        pUtil.chdir(current_dir)

        panda_jobs = glob(job_dir + "/PandaJob_*_*")
        panda_logs = glob(job_dir + "/*.log.tgz.*")
        if panda_jobs or panda_logs:
            log("Number of founded panda jobs: %d, number of panda log tar file %d, will not remove job dir"
                        % (len(panda_jobs), len(panda_logs)))
        else:
            log("Number of founded panda jobs: %d, number of panda log tar file %d, will remove job dir"
                        % (len(panda_jobs), len(panda_logs)))
            log("Remove job dir %s" % job_dir)
            os.system("rm -rf %s" % job_dir)
        return True
    except:
        log("Failed to start deferred stage out for HPC job: %s" % traceback.format_exc())
        return False


def DeferredStageoutJob(job_dir, job_state_file="", deferred_stageout_logfile=False,
                        **kwargs):
    """
    Performs stageing out preparation and stages out the job in specified directory.

    :param job_dir:     (string)    directory with a job.
                        mandatory parameter
    :param job_state_file:  (string)    path to job state file or other file containing job state. If empty, job
                                        state file is located as job_dir+'/jobState-*.*'.
                            defaults to ""

    :param deferred_stageout_logfile: (string|False)    template name for deferred log stageout
                                                        Replaces "{job_id}" with current job id like
                                                        "log-{job_id}.txt" -> "log-124124.txt"
                                        Default False

    Other parameters are passed into other functions

    :return: (bool) the fact of stageout being performed
    """
    log("Deferred stageout from job directory \"%s\"" % job_dir)

    job_state = JobState()

    if job_state_file == "":
        try:
            job_state_file = glob(job_dir + "/" + jobState_file_wildcart)[0]
        except:
            log("There is no job state file in the provided directory, exiting")
            return False

    log("Job state file is %s"%job_state_file)

    # lockfd, lockfn = createAtomicLockFile(job_dir)

    with LockFileWrapper(job_dir):
        if not TestJobDirForDeferredStageoutNecessity(job_dir, job_state_file, **kwargs):
            log("Job \"%s\" does not need deferred stageout procedure (yet)" % job_dir)
            # releaseAtomicLockFile(lockfd, lockfn)
            return False

        if not job_state.get(job_state_file):
            log("Job state file reading failed, exiting")
            # releaseAtomicLockFile(lockfd, lockfn)
            return False

        log("Working with job in \"%s\"" % job_dir)
        _job, _site, _node, _recoveryAttempt = job_state.decode()

        if not (_job and _site and _node):
            log("Can not decode jobState file, exiting")
            # releaseAtomicLockFile(lockfd, lockfn)
            return False

        with LogWrapper(deferred_stageout_logfile, _job.jobId) as logger:

            rc = PrepareJobForDeferredStageout(job_state, **kwargs)

            if rc == ReturnCode.PostJobOnly:
                pUtil.postJobTask(job_state.job, job_state.site, DorE(kwargs, 'workerNode'), DorE(kwargs, 'experiment'),
                                  jr=True, ra=job_state.recoveryAttempt)
                # releaseAtomicLockFile(lockfd, lockfn)
                return True

            if rc > 0:
                log("Job is not prepared for stageout, exiting")
                if rc == ReturnCode.Cleanup:
                    cleanup(job_state)
                # releaseAtomicLockFile(lockfd, lockfn)
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
                log("Stageout will now transfer the files")
                rc = TransferFiles(job_state, datadir, filelist, **kwargs)

                if rc == ReturnCode.Holding:
                    job_state.job.result[0] = "holding"
                if rc == ReturnCode.FailedJob:
                    job_state.job.result[0] = "failed"

                job_state.job.setState(job_state.job.result)

            pUtil.chdir(job_state.site.workdir)
            ret = True
            if logfile != "" and not pUtil.isLogfileCopied(job_state.site.workdir):
                log("Stageout will now transfer the log")
                _log = JobLog()
                ret, _ = _log.transferLogFile(job_state.job, job_state.site, DorE(kwargs, 'experiment'), dest=None,
                                              jr=True)

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

            log("Stageout will now update the server with new status")

            rt, retNode = updatePandaServer(job_state, xmlstr=XMLStr, **kwargs)

            if rt == 0:
                log("Job %s updated (exit code %d)" % (job_state.job.jobId, job_state.job.result[2]))

                # did the server send back a command?
                if "tobekilled" in job_state.job.action:
                    log("!!WARNING!!1120!! Panda server returned a \'tobekilled\' command")
                    job_state.job.result[0] = "failed"

                # further recovery attempt unnecessary, but keep the work dir for debugging
                if job_state.job.result[0] == "failed":
                    log("Further recovery attempts will be prevented for failed job (will leave work dir)")
                    if not job_state.rename(job_state.site, job_state.job):
                        log("(Fate of job state file left for next pilot)")

            else:
                log("!!WARNING!!1120!! Panda server returned a %d" % (rt))

                # store the final state so that the next pilot will know

                # store the metadata xml
                retNode['xml'] = XMLStr

                # update the job state file with the new state information
                _retjs = pUtil.updateJobState(job_state.job, job_state.site, retNode, job_state.recoveryAttempt)

            log("Stageout will now proceed to post-job actions")

            if job_state.job.result[0] in finalJobStates:
                pUtil.postJobTask(job_state.job, job_state.site,
                                  DorE(kwargs, 'workerNode'), DorE(kwargs, 'experiment'), jr=True,
                                  ra=job_state.recoveryAttempt)

            pUtil.chdir(currentdir)

            # releaseAtomicLockFile(lockfd, lockfn)

            if job_state.job.result[0] == "finished":
                log("Stageout will now remove the job, it is in finished state and can be removed")
                cleanup(job_state)

            return True


def TestJobDirForDeferredStageoutNecessity(job_dir, job_state_file,
                                           test_mtime=True, test_lockfile="", lockfile_logic=True, minimum_death_time=0,
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
    :param minimum_death_time:    (integer) minimum time for job to be untouched to perform deferred stageout. If is
                                            less then or equal to zero, default value is used
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

        if minimum_death_time <= 0:
            minimum_death_time = number_of_lost_heartbeats_for_pilot_to_be_dead*DorE(kwargs, 'heartbeatPeriod')
        if (current_time - file_modification_time) <= minimum_death_time:
            return lognfalse("File was modified not so long ago, skipping.")

        lognfalse("File was not modified for a number of heartbeats. Supposing the pilot to be dead.")
    return lognret(True, "Job state file is tested and is OK to run stageout.")


def lognfalse(_str):
    return lognret(False, _str)


def lognret(ret, _str):
    log(_str)
    return ret


def DorE(dictionary, key):
    return dictionary.get(key, env[key])


def cleanup(job_state):
    log("Cleanup job directory called")

    cmd = "rm -rf %s" % os.path.dirname(job_state.filename)
    if os.path.isdir(job_state.job.newDirNM):
        cmd += (" %s" % job_state.job.newDirNM)
    if os.path.isdir(job_state.job.datadir):
        cmd += (" %s" % job_state.job.datadir)
    if os.path.isdir(job_state.site.workdir):
        cmd += (" %s" % job_state.job.workdir)
    log("Executing command: %s" % (cmd))
    try:
        ec, rs = commands.getstatusoutput(cmd)
    except Exception, e:
        log("FAILURE: JobState cleanup failed to cleanup: %s " % str(e))


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
        log("!!WARNING!!1100!! Max number of recovery attempts exceeded: %d" %
                    (env['maxNumberOfRecoveryAttempts']))
        job_state.job.setState(['failed', job_state.job.result[1], PilotErrors().ERR_LOSTJOBMAXEDOUT])
        rt, retNode = updatePandaServer(job_state, **kwargs)
        if rt == 0:
            log("Job %s updated (exit code %d)" % (job_state.job.jobId, job_state.job.result[2]))

        else:
            log("Panda server returned a %d" % (rt))
            return lognret(ReturnCode.SkipJob, "(Failed to update panda server - leave for next pilot)")

    jobStatus = job_state.job.result[0]
    jobStatusCode = 0
    jobAttemptNr = job_state.job.attemptNr

    # There can be unclear state, consult the server
    if jobStatus not in acceptedJobStatesFromFile:
        log("Job state may be unclear (found state %s), checking with the server" % jobStatus)
        jobStatus, jobAttemptNr, jobStatusCode = pUtil.getJobStatus(job_state.job.jobId,
                                                                    DorE(kwargs, 'pshttpurl'),
                                                                    DorE(kwargs, 'psport'),
                                                                    DorE(kwargs, 'pilot_initdir'))
        if jobStatusCode != 0:
            return lognret(ReturnCode.SkipJob, "Received general error code from dispatcher call (leave job for later"
                                                " pilot)")
        else:
            log("Job state is %s" % jobStatus)

    # If any inconvenience occurs or job is finalised, cleanup
    if job_state.job.attemptNr != jobAttemptNr or\
                     jobStatus in finalJobStates or\
                     "tobekilled" in job_state.job.action:
        if job_state.job.attemptNr != jobAttemptNr:
            return lognret(ReturnCode.Cleanup, "Further recovery attempts will be prevented for this job. It has a "
                                               "mismatch in the attempt number record.")
        if "tobekilled" in job_state.job.action:
            return lognret(ReturnCode.Cleanup, "Further recovery attempts will be prevented for this job. It was"
                                               " marked to be killed.")
        return lognret(ReturnCode.Cleanup, "Further recovery attempts will be prevented for this job, it is in final "
                                           "state: %s." % jobStatus)

    if jobStatus != 'holding':
        # is the attemptNr defined?
        try:
            attemptNr = job_state.job.attemptNr
        except Exception, e:
            log("!!WARNING!!1100!! Attempt number not defined [ignore]: %s" % str(e))
        else:
            # check if the attemptNr (set during initial getJob command) is the same
            # as the current jobAttemptNr from the server (protection against failed lost
            # heartbeat jobs due to reassigned panda job id numbers)
            if attemptNr != jobAttemptNr:
                log("!!WARNING!!1100!! Attempt number mismatch for job %s (according to server - will not be"
                            " recovered)" % job_state.job.jobId)
                log("....Initial attempt number: %d" % attemptNr)
                log("....Current attempt number: %d" % jobAttemptNr)
                log("....Job status (server)   : %s" % jobStatus)
                log("....Job status (state)    : %s" % job_state.job.result[0])
                return lognret(ReturnCode.Cleanup, "Further recovery attempts will be prevented for this job")
            else:
                log("Attempt numbers from server and job state file agree: %d" % attemptNr)

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
                log("Exit code not found")

                # get the metadata
                # this metadata does not contain the metadata for the log
                strXML = pUtil.getMetadata(job_state.site.workdir, job_state.job.jobId)

                # update the server
                rt, retNode = updatePandaServer(job_state, xmlstr=strXML, **kwargs)

                if rt == 0:
                    return lognret(ReturnCode.Cleanup, "Lost job %s updated (exit code %d)" % (job_state.job.jobId,
                                                                                               job_state.job.result[2]))
                else:
                    log("!!WARNING!!1130!! Panda server returned a %d" % (rt))

                    # store the final state so that the next pilot will know
                    # store the metadata xml
                    retNode['xml'] = strXML

                    # update the job state file with the new state information
                    pUtil.updateJobState(job_state.job, job_state.site, retNode, job_state.recoveryAttempt)
                    return lognret(ReturnCode.SkipJob, "It's a task for the next pilot now")

    # # I think, this is useless and wrong piece of code. Maybe will be removed, or changed in the future.
    # # Copied from previous jobrec.
    # if ec != 0 and not os.path.isfile(logfile):
    #     log("ec: %s, log: %s" % (ec, logfile))
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
        log("!!FAILED!!1105!! Exception caught (Could not read guids for the remaining output files): %s" %
                    str(e))
        return False

    log("Successfully fetched guids")
    return True


def updateOutPFC(job, **kwargs):
    file_name = "OutPutFileCatalog.xml"
    file_path = os.path.join(DorE(kwargs, 'thisSite').workdir, file_name)
    try:
        guids_status = pUtil.PFCxml(job.experiment, file_path, job.outFiles, fguids=job.outFilesGuids, fntag="pfn",
                                    analJob=pUtil.isAnalysisJob(job.trf.split(",")[0]), jr=True)
    except Exception, e:
        log("!!FAILED!!1105!! Exception caught (Could not generate xml for the remaining output files): %s" %
                    str(e))
        job.result[2] = PilotErrors().ERR_LOSTJOBXML
        return False
    else:
        if not guids_status:
            log("!!FAILED!!2999!! Missing guid(s) for output file(s) in metadata")
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
        log("Output files will go to default dataset: %s" % (dsname))

    # the cmtconfig is needed by at least the xrdcp site mover
    cmtconfig = pUtil.getCmtconfig(job.cmtconfig)

    tin_0 = os.times()
    rf = None
    _state = ReturnCode.OK
    _msg = ""
    ec = -1
    try:
        # Note: alt stage-out numbers are not saved in recovery mode (job object not returned from this function)
        rc, pilotErrorDiag, rf, rs, job.filesNormalStageOut, job.filesAltStageOut, os_bucket_id = Mover.mover_put_data(
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
        log("!!%s!!1105!! %s" % (env['errorLabel'], pilotErrorDiag))
        ec = PilotErrors().ERR_PUTFUNCNOCALL
        _state = ReturnCode.Holding
        _msg = env['errorLabel']
    else:
        if pilotErrorDiag != "":
            pilotErrorDiag = "Put error: " + pUtil.tailPilotErrorDiag(pilotErrorDiag,
                                                                      size=256-len("pilot: Put error: "))

        ec = rc
        log("Put function returned code: %d" % (rc))
        if rc != 0:
            # remove any trailing "\r" or "\n" (there can be two of them)
            if rs is not None:
                rs = rs.rstrip()
                log(" Error string: %s" % (rs))

            # is the job recoverable?
            if PilotErrors().isRecoverableErrorCode(rc):
                _state = ReturnCode.Holding
                _msg = "WARNING"
            else:
                _state = ReturnCode.FailedJob
                _msg = env['errorLabel']

            # look for special error in the error string
            if rs == "Error: string Limit exceeded 250":
                log("!!%s!!3000!! Put error: file name string limit exceeded 250" % (_msg))
                ec = PilotErrors().ERR_LRCREGSTRSIZE

            log("!!%s!! %s" % (_msg, PilotErrors().getErrorStr(rc)))
        else:
            # create a weak lockfile meaning that file transfer worked, and all output files have now been transferred
            pUtil.createLockFile(True, job_state.site.workdir, lockfile="ALLFILESTRANSFERRED")

    # finish the time measurement of the stage-out
    tin_1 = os.times()
    job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

    # set the error codes in case of failure
    job.pilotErrorDiag = pilotErrorDiag
    if ec != 0:
        log("!!%s!!2999!! %s" % (_msg, pilotErrorDiag))

    job.result[2] = ec

    return _state


def createAtomicLockFile(file_path):
    """ Create an atomic lockfile while probing this dir to avoid a possible race-condition """

    lockfile_name = os.path.join(os.path.dirname(file_path), "ATOMIC_LOCKFILE")
    try:
        # acquire the lock
        fd = os.open(lockfile_name, os.O_EXCL|os.O_CREAT)
    except OSError:
        # work dir is locked, so exit                                                                                                                                                                                                                            
        log("Found lock file: %s (skip this dir)" % (lockfile_name))
        fd = None
    else:
        log("Created lock file: %s" % (lockfile_name))
    return fd, lockfile_name


def releaseAtomicLockFile(fd, lockfile_name):
    """ Release the atomic lock file """

    try:
        os.close(fd)
        os.unlink(lockfile_name)
    except Exception, e:
        if "Bad file descriptor" in e:
            log("Lock file already released")
        else:
            log("WARNING: Could not release lock file: %s" % (e))
    else:
        log("Released lock file: %s" % (lockfile_name))


def updatePandaServer(job_state, xmlstr=None, spaceReport=False,
                      log=None, jr=False, stdout_tail="", **kwargs):
    """ Update the panda server with the latest job info """

    from PandaServerClient import PandaServerClient
    client = PandaServerClient(pilot_version=DorE(kwargs, 'version'),
                               pilot_version_tag=DorE(kwargs, 'pilot_version_tag'),
                               pilot_initdir=DorE(kwargs, 'pilot_initdir'),
                               jobSchedulerId=DorE(kwargs, 'jobSchedulerId'),
                               pilotId=DorE(kwargs, 'pilotId'),
                               updateServer=DorE(kwargs, 'updateServerFlag'),
                               jobrec=DorE(kwargs, 'jobrec'),
                               pshttpurl=DorE(kwargs, 'pshttpurl'))

    # update the panda server
    return client.updatePandaServer(job_state.job, job_state.site, DorE(kwargs, 'workerNode'),
                                           DorE(kwargs, 'psport'), xmlstr=xmlstr,
                                           spaceReport=spaceReport, log=log, ra=job_state.recoveryAttempt,
                                           jr=jr, useCoPilot=DorE(kwargs, 'useCoPilot'),
                                           stdout_tail=stdout_tail)
