import os
import time
import commands
from glob import glob

from PilotErrors import PilotErrors
from pUtil import tolog, isAnalysisJob, setPilotlogFilename, readpar
from JobState import JobState
from FileState import FileState

class JobRecovery:
    """
    Job recovery
    Specialized algorithm for recovering previously failed stage-out transfers
    The algorithm works by scanning all panda job directories on the work node, or in any provided external (remote or at least outside of
    the pilot work directory) directory as specified in schedconfig.recoverdir, for job state files.
    The algorithm only tries to recover jobs whose job state file (jobState-<jobId>.[pickle|json]) has not been modified for over two
    heartbeat periods. A stranded job that is found by the algorithm is referred to as a lost job. The last action
    performed by the pilot before the lost job failed is stored in the job state file variable "state". Only lost jobs
    that are in states "stageout", "finished", "failed" or "holding" will be recovered. A job can have the following
    job states:
       "startup"      : First update to the job state file
       "setup"        : Pilot is setting up the runtime options
       "stagein"      : Stage-in of input files
       "running"      : Payload execution; output files are created (no point in recovery if job found in this state)
       "stageout"     : Stage-out of output files
       "finished"     : Last update to the job state file (alternative)
       "failed"       : Last update to the job state file (alternative)
       "holding"      : Last update to the job state file (alternative)
    The job state file contains a dictionary of log and output files and the state at the last time of the update. The
    format of this file dictionary is: { filename1 : state1, filename2 : state2, ... }
    where the state variables have the list form "file_state", "reg_state".
    "file_state" can assume the following values:
      "not_created"   : initial value for all files at the beginning of the job
      "created"       : file was created and is waiting to be transferred
      "transferred"   : file has already been transferred (no further action)
      "missing"       : file was never created, the job failed (e.g. output file of a failed job; a log should never be missing)
    "reg_state" can assume the following values:
      "not_registered": file was not registered in the LFC
      "registered"    : file was already registered in the LFC (no further action)
    E.g. a file with state = "created", "not_registered" should first be transferred and then registered in the LFC (assuming that the pilot
    is responsible for LFC registrations)
    """

    # private data members
    __error = PilotErrors()                # PilotErrors object
    __errorLabel = "WARNING"               # Error string label
    __errorString = "!!WARNING!!1999!! %s" # Default error string
    __heartbeatPeriod = 30*60              # Heartbeat period in seconds
    __initDir = ""                         # Job recovery init dir
    __jobId = ""                           # Current job id
    __jobSchedulerId = ""                  # Job scheduler id
    __logFileDir = ""                      # Location of the log file
    __maxJobRec = 20                       # Default maximum number of job recoveries
    __maxNumberOfRecoveryAttempts = 15     # Max recovery attempts
    __outputDir = ""                       # Special output directory (Nordugrid, CERNVM)
    __pilot_version = ""                   # Pilot version string
    __pilot_version_tag = ""               # PR/RC    
    __pilot_initdir = ""                   # Pilot init dir
    __pilotErrorDiag = ""                  # Error diagnostics string
    __pilotId = ""                         # Pilot id
    __psport = 25443                       # Default panda server port
    __pshttpurl = ""                       # Job dispatcher
    __proxycheckFlag = True                # Should the proxy be validated?
    __recovered = ""                       # Comma separated string of all recovered job id's (even failed attempted recoveries)
    __recoveryAttempt = 0                  # Recovery attempt number
    __recoveryDir = ""                     # Current recovery dir
    __recoveryDirs = []                    # List of all base dirs to be scanned for lost jobs
    __site = None                          # Site object
    __testLevel = "0"                      # Debugging info (remove)
    __uflag = None                         # 'user job flag' if this pilot is to get a user analysis job or ordinary job
    __updateServer = True                  # Should the server be updated?
    __workNode = None                      # Node object

    __PASSED_INIT = False                  # Init boolean

    # Main file dictionary definition
    fileStateDictionary = {}

    def __init__(self,
                 errorLabel="WARNING",
                 heartbeat=30*60,
                 jobSchedulerId="",
                 logFileDir="",
                 maxjobrec=20,
                 maxNumberOfRecoveryAttempts=15,
                 outputDir="",
                 pilot_version="",
                 pilot_version_tag="",
                 pilot_initdir=None,
                 pilotId="",
                 probe=False,
                 pshttpurl=None,
                 psport=None,
                 site=None,
                 testLevel="0",
                 uflag=None,
                 updateServer=True,
                 workNode=None,
                 *args, **kwrds):
        """ Default initialization """

        self.__errorLabel = errorLabel
        self.__heartbeatPeriod = heartbeat
        self.__initDir = os.getcwd()
        self.__jobSchedulerId = jobSchedulerId
        self.__logFileDir = logFileDir
        self.__maxJobRec = maxjobrec
        self.__maxNumberOfRecoveryAttempts = maxNumberOfRecoveryAttempts
        self.__outputDir = outputDir
        self.__pilot_version = pilot_version
        self.__pilot_version_tag = pilot_version_tag
        self.__pilot_initdir = pilot_initdir
        self.__pilotId = pilotId
        self.__pshttpurl = pshttpurl
        self.__psport = psport
        self.__site = site
        self.__testLevel = testLevel
        self.__uflag = uflag
        self.__updateServer = updateServer
        self.__workNode = workNode

        # set all recovery directories
        if probe:
            self.setRecoveryDirs()
            if self.__recoveryDirs == []:
                self.__pilotErrorDiag = "Job recovery initialization failed"
                tolog(self.__errorString % self.__pilotErrorDiag)
                self.__PASSED_INIT = False
            else:
                self.__PASSED_INIT = True
        else:
            self.__PASSED_INIT = True

    def setRecoveryDirs(self):
        """ Set the recovery directories """

        dirs = []
        if self.__site:
            _dir = self.__site.wntmpdir
        else:
            _dir = ""
        if _dir == "":
            if os.environ.has_key('TMPDIR'):
                _dir = os.environ['TMPDIR']
            elif os.environ.has_key('OSG_WN_TMP'):
                _dir = os.environ['OSG_WN_TMP']
            elif os.path.exists("/tmp"):
                _dir = "/tmp"
            elif os.path.exists("/scratch"):
                _dir = "/scratch"
            else:
                self.__pilotErrorDiag = "Could not locate any scratch dirs"
                tolog(self.__errorString % self.__pilotErrorDiag)
                _dir = ""
        if _dir != "":
            dirs.append(_dir)

        extradir = readpar('wntmpdir')
        if extradir != "" and extradir != "None" and extradir not in dirs:
            dirs.append(extradir)

        # check queuedata for external recovery directory
        # an empty externalRecoveryDir means that recovery should only search local WN disk for lost jobs
        # make sure the recovery directory actually exists (will not be added to dir list if empty)
        externalRecoveryDir = self.verifyRecoveryDir(readpar('recoverdir'))
        if externalRecoveryDir != "":
            dirs.append(externalRecoveryDir)

        if dirs != []:
            tolog("Job recovery will probe: %s" % str(dirs))
            self.__recoveryDirs = dirs
        else:
            self.__pilotErrorDiag = "Did not identify any base recovery directories"
            tolog(self.__errorString % self.__pilotErrorDiag)
            
    def setRecoveryDir(self, dir):
        """ Set the current recovery dir """
        self.__recoveryDir = dir

    def getRecoveryDir(self):
        """ Return the current recovery dir """
        return self.__recoveryDir

    def getInitDir(self):
        """ Return the init dir """
        return self.__initDir

    def createAtomicLockFile(self, file_path):
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

    def releaseAtomicLockFile(self, fd, lockfile_name):
        """ Release the atomic lock file """

        try:
            os.close(fd)
            os.unlink(lockfile_name)
        except Exception, e:
            if "Bad file descriptor" in e:
                tolog("Lock file already released")
            else:
                self.__pilotErrorDiag = "Could not release lock file: %s" % (e)
                tolog(self.__errorString % self.__pilotErrorDiag)
        else:
            tolog("Released lock file: %s" % (lockfile_name))

    def verifyRecoveryDir(self, recoverdir):
        """ Make sure that the recovery directory actually exists, return empty string otherwise """

        status = False
        # does the recovery dir actually exists?
        if recoverdir and recoverdir != "":
            if os.path.exists(recoverydir):
                tolog("External recovery dir verified: %s" % (recoverdir))
                status = True
        if not status:
            recoverdir = ""

        return recoverdir

    def updatePandaServer(self, job, site, workerNode, port, xmlstr=None, spaceReport=False, log=None, ra=0, jr=False, schedulerID=None, pilotID=None, updateServer=True):
        """ Update the PanDA server """

        # create and instantiate the client object
        from PandaServerClient import PandaServerClient
        client = PandaServerClient(pilot_version=self.__pilot_version, \
                                   pilot_version_tag=self.__pilot_version_tag, \
                                   pilot_initdir=self.__pilot_initdir, \
                                   jobSchedulerId=self.__jobSchedulerId, \
                                   pilotId=self.__pilotId, \
                                   updateServer=self.__updateServer, \
                                   jobrec=True, \
                                   pshttpurl=self.__pshttpurl)

        # update the panda server
        return client.updatePandaServer(job, site, workerNode, port, xmlstr=xmlstr, spaceReport=spaceReport, log=log, ra=ra, jr=jr)

    def performRecovery(self):
        """ Run job recovery in all directories """

        status = False

        if not self.__PASSED_INIT:
            self.__pilotErrorDiag = "Aborting job recovery due to previous failure"
            tolog(self.__errorString % self.__pilotErrorDiag)
            return status

        # keep track of the starting dir
        startingDir = os.getcwd()

        # scan all recovery dirs
        JS = JobState()
        for _dir in self.__recoveryDirs:
            status = self.run(_dir, JS)

        # return to the starting dir
        chdir(startingDir)

        return status

    def run(self, _dir, JS):
        """ Run main job recovery algorithm """

        status = False
        tolog("Running job recovery in: %s" % (_dir))

        # grab all job state files in all work directories
        job_state_files_pickle = glob(os.path.join(_dir, "/Panda_Pilot_*/jobState-*.pickle"))
        job_state_files_json = glob(os.path.join(_dir, "/Panda_Pilot_*/jobState-*.json"))
        tolog("Number of found job state pickle files: %d" % (len(job_state_files_pickle)))
        tolog("Number of found job state json files: %d" % (len(job_state_files_json)))

        _rec_nr = 0
        job_state_files = job_state_files_json + job_state_files_pickle
        if not job_state_files:
            return True

        file_nr = 0
        # loop over all found job state files
        for job_state_file in job_state_files:

            # reset status for every file
            status = False

            # create an atomic lockfile while probing this dir to avoid a possible race-condition
            fd, lockfile_name = self.createAtomicLockFile(job_state_file)
            if not fd:
                # set status to True in case this was the last file
                status = True
                continue

            dirname = os.path.dirname(job_state_file)
            tolog("Probing dir: %s" % (dirname))

            # make sure the LOCKFILE is present
            lockFile = os.path.join(dirname, 'LOCKFILE')
            if not os.path.exists(lockFile):
                # release the atomic lockfile and go to the next directory
                self.releaseAtomicLockFile(fd, lockfile_name)

                # set status to True in case this was the last file
                status = True
                continue
            else:
                try:
                    tolog("Found %s created at %d" % (lockFile, os.path.getmtime(lockFile)))
                except Exception, e:
                    tolog(self.__errorString % "Could not read modification time of %s, try to continue: %s" % (lockFile, e))

            if (file_nr + 1) > self.__maxJobRec:
                tolog("Maximum number of job recoveries exceeded for this pilot: %d" % (self.__maxJobRec))
                # release the atomic lockfile and go to the next directory
                self.releaseAtomicLockFile(fd, lockfile_name)

                # set status to True in case this was the last file
                status = True
                break

            tolog("Processing job state file %d/%d: %s" % (file_nr + 1, len(job_state_files), job_state_file))
            current_time = int(time.time())

            # when was file last modified?
            try:
                file_modification_time = os.path.getmtime(job_state_file)
            except Exception, e:
                # skip this file since it was not possible to read the modification time
                tolog(self.__errorString % "Could not read modification time: %s" % (e))

                # release the atomic lockfile and go to the next directory
                self.releaseAtomicLockFile(fd, lockfile_name)

                # do not increase file counter, keep status = False
            else:
                # was the job state file updated longer than 2 heart beats ago?
                if (current_time - file_modification_time) > 2*self.__heartbeatPeriod:
                    # found lost job recovery file
                    tolog("File was last modified %d seconds ago (limit=%d, t=%d, tmod=%d)" %\
                          (current_time - file_modification_time, 2*self.__heartbeatPeriod, current_time, file_modification_time))

                    # open the job state file
                    if JS.get(job_state_file):
                        # decode the job state info
                        _job, _site, _node, self.__recoveryAttempt = JS.decode()

                        # set the jobId number
                        self.__jobId = _job.jobId

                    if not (_job and _site and _node):
                        tolog("Missing data object (job, site or node not defined)")
                        # release the atomic lockfile and go to the next directory
                        self.releaseAtomicLockFile(fd, lockfile_name)

                        # do not increase file counter, keep status = False
                        continue

                    # only continue if current pilot is of same type as lost job (to prevent potential permission problems)
                    if not self.isSameType(_job.trf.split(",")[0], self.__uflag):
                        # release the atomic lockfile and go to the next directory
                        self.releaseAtomicLockFile(fd, lockfile_name)
                        status = True
                        continue

                    # uncomment this code for recovery of certain panda ids only
                    # allowedJobIds = ['1018129684','1018129685']
                    # if _job.jobId not in allowedJobIds:
                    #     tolog("Job id %s not in allowed id list" % (_job.jobId))
                    #     continue

                    # query the job state file for job information
                    if not self.verifyJobInfo(_job, _site, JS):
                        # release the atomic lockfile and go to the next directory
                        self.releaseAtomicLockFile(fd, lockfile_name)
                        continue

                    tolog("Job %s is currently in state \'%s\' (according to job state file - recover)" %\
                          (_job.jobId, _job.result[0]))

                    # switch to the work dir of the old pilot
                    chdir(_site.workdir)

                    # abort if max number of recovery attempts has been exceeded
                    if self.__recoveryAttempt > self.__maxNumberOfRecoveryAttempts - 1:
                        self.__pilotErrorDiag = "Max number of recovery attempts exceeded: %d" % (self.__maxNumberOfRecoveryAttempts)
                        tolog(self.__errorString % self.__pilotErrorDiag)
                        _job.setState(['failed', _job.result[1], self.__error.ERR_LOSTJOBMAXEDOUT])

                        # update the server
                        rt = self.updateServer(_job, _site, JS)
                        if rt == 0:
                            number_of_recoveries += 1

                        # release the atomic lockfile and go to the next directory
                        self.releaseAtomicLockFile(fd, lockfile_name)
                        file_nr += 1
                        continue
                    else:
                        # increase recovery attempt counter
                        self.__recoveryAttempt += 1

                    tolog("Recovery attempt: %d" % (self.__recoveryAttempt))

                    # update job state file at this point to prevent a parallel pilot from doing a simultaneous recovery
                    # (note: should not really be necessary due to the atomic lock file)
                    _retjs = updateJobState(_job, _site, _node, _recoveryAttempt)

####


                # Job state file was recently modified
                else:
                    tolog("(Job state file was recently modified - skip it)")
                    # atomic lockfile will be released below

            # (end main "for job_state_file in job_state_files"-loop)
            # release the atomic lockfile and go to the next directory
            self.releaseAtomicLockFile(fd, lockfile_name)

            # increase file counter
            file_nr += 1

        return status

    def updateServer(self, _job, _site, JS):
        """ Update the Panda server """

        rt, retNode = self.updatePandaServer(_job, _site, self.__workNode, self.__psport, ra=self.__recoveryAttempt)
        if rt == 0:
            tolog("Lost job %s updated (exit code %d)" % (_job.jobId, _job.result[2]))

            # did the server send back a command?
            if "tobekilled" in _job.action:
                self.__pilotErrorDiag = "Panda server returned a \'tobekilled\' command"
                tolog(self.__errorString % self.__pilotErrorDiag)

            if _job.result[0] == "failed":
                tolog("NOTE: This job has been terminated. Will now remove workdir.")
                if not JS.cleanup():
                    self.__pilotErrorDiag = "failed to cleanup"
                    tolog(self.__errorString % self.__pilotErrorDiag)
        else:
            tolog("Panda server returned a %d" % (rt))
            tolog("(Failed to update panda server - leave for next pilot)")

        return rt

    def verifyJobInfo(self, _job, _site, JS):
        """ Query the job state file for job information """

        status = False

        # only proceed with job recovery if job is in holding or lostheartbeart state
        if _job.result[0] == 'holding' or _job.result[0] == 'lostheartbeat':
            tolog("Recovery will be attempted")
            status = True
        elif _job.result[0] == 'failed':
            tolog("Recovery will be skipped")
            tolog("Further recovery attempts will be prevented for this job (will leave work dir)")
            if not JS.rename(_site, _job):
                tolog("(Fate of job state file left for next pilot)")
        else:
            tolog("Not enough information in job state file, will query server")
            if self.recoverWithServer(_job, _site, JS):
                status = True

        return status

    def recoverWithServer(self, _job, _site, JS):
        """ query server about job and decide if it should be recovered or not """

        status = False

        # get job status from server
        jobStatus, jobAttemptNr, jobStatusCode = pUtil.getJobStatus(_job.jobId, self.__pshttpurl, self.__psport, self.__pilot_initdir)

        # recover this job?
        if jobStatusCode == 20:
            tolog("Received general error code from dispatcher call (leave job for later pilot)")
        elif not (jobStatus == 'holding' and jobStatusCode == 0):
            tolog("Job %s is currently in state \'%s\' with attemptNr = %d (according to server - will not be recovered)" % \
                  (_job.jobId, jobStatus, jobAttemptNr))

            if _job.attemptNr != jobAttemptNr or jobStatus == "transferring" or jobStatus == "failed" or \
                   jobStatus == "notfound" or jobStatus == "finished" or "tobekilled" in _job.action:
                tolog("Further recovery attempts will be prevented for this job")
                if not JS.rename(_site, _job):
                    tolog("(Fate of job state file left for next pilot)")
                else:
                    if not JS.cleanup():
                        tolog("!!WARNING!!1110!! Failed to cleanup")
        else:
            # check if the attemptNr (set during initial getJob command) is the same
            # as the current jobAttemptNr from the server (protection against failed lost
            # heartbeat jobs due to reassigned panda job id numbers)
            if _job.attemptNr != jobAttemptNr:
                tolog("WARNING: Attempt number mismatch for job %s (according to server - will not be recovered)" % (_job.jobId))
                tolog("....Initial attempt number: %d" % (_job.attemptNr))
                tolog("....Current attempt number: %d" % (jobAttemptNr))
                tolog("....Job status (server)   : %s" % (jobStatus))
                tolog("....Job status (state)    : %s" % (_job.result[0]))
                tolog("Further recovery attempts will be prevented for this job")
                if not JS.rename(_site, _job):
                    tolog("(Fate of job state file left for next pilot)")
                else:
                    if not JS.cleanup():
                        tolog("!!WARNING!!1110!! Failed to cleanup")
                pass
            else:
                tolog("Attempt numbers from server and job state file agree: %d" % (_job.attemptNr))

                # the job state as given by the dispatcher should only be different from that of
                # the job state file for 'lostheartbeat' jobs. This state is only set like this
                # in the job state file. The dispatcher will consider it as a 'holding' job.
                tolog("Job %s is currently in state \'%s\' (according to job state file: \'%s\') - recover" % (_job.jobId, jobStatus, _job.result[0]))

                status = True

        return status

    def recover(self):
        """ """

        # get file state dictionary from file

        # get object from job state file

        # check job state on server

        # loop over all files in the file state dictionary
        for filename in fileStateDictionary.keys():
            state = ""
            tolog("Processing file: \'%s\' in state \'%s\'" % (filename, state))
            transferFile(filename, state)
            registerFile(filename, state)

    def transferFile(self, filename, state):
        """ Transfer file to SE if necessary """

        # should the file be transferred?
        if "created" in state:
            # file needs to be transferred
            tolog("Preparing to transfer file")
        elif "transferred" in state:
            tolog("File was already transferred")
        elif "missing" in state:
            # file was never created, so skip it
            pass
        else:
            self.__pilotErrorDiag = "Can not recovery a file in this state: %s" % (state)
            tolog(self.__errorString % self.__pilotErrorDiag)

    def registerFile(self, filename, state):
        """ Register file in LFC if necessary """

        if "not_registered" in state:
            # file needs to be registered
            tolog("Preparing to register file")
        elif "registered" in state:
            tolog("File was already registered")
        else:
            self.__pilotErrorDiag = "Can not register a file in this state: %s" % (state)
            tolog(self.__errorString % self.__pilotErrorDiag)

    def verifyFileStatus(self):
        """ """

        pass

    def isSameType(self, trf, userflag):
        """ is the lost job of same type as the current pilot? """

        # treat userflag 'self' as 'user'
        if userflag == 'self':
            userflag = 'user'

        if (isAnalysisJob(trf) and userflag == 'user') or \
               (not isAnalysisJob(trf) and userflag != 'user'):
            sametype = True
            if userflag == 'user':
                tolog("Lost job is of same type as current pilot (analysis pilot, lost analysis job trf: %s)" % (trf))
            else:
                tolog("Lost job is of same type as current pilot (production pilot, lost production job trf: %s)" % (trf))
        else:
            sametype = False
            if userflag == 'user':
                tolog("Lost job is not of same type as current pilot (analysis pilot, lost production job trf: %s)" % (trf))
            else:
                tolog("Lost job is not of same type as current pilot (production pilot, lost analysis job trf: %s)" % (trf))

        return sametype

    def setLog(self):
        """ Redirect log messages to recovery_reports/recovery_report-<jobId>-<attemptNr>.txt """

        status = False

        # does recovery_reports/ exist?
        _path = "%s/recovery_reports" % (self.__recoveryDir)
        if not os.path.exists(_path):
            try:
                os.mkdir(_path)
            except Exception, e:
                self.__pilotErrorDiag = "Failed to create recovery reports dir: %s" % (e)
                tolog(self.__errorString % self.__pilotErrorDiag)
            else:
                tolog("Created %s" % (_path))
                status = True
        else:
            status = True

        if status:
            # proceed to set the log file
            fileName = os.path.join(_path, "recovery_report-%s-%d.txt" % (self.__jobId, self.__recoveryAttempt))
            msg = "Redirecting log messages to: " % (fileName)
            print msg
            tolog(msg)
            setPilotlogFilename(fileName)

        return status

    def updateJobStateTest(self, job, thisSite, workerNode, recoveryAttempt=0, mode=""):
        """ update the job state file """

        # NOTE: this function will eventually replace pilot::updateJobState() when new job rec is in place

        status = False

        # create a job state object and give it the current job state information
        JS = JobState()
        if JS.put(job, thisSite, workerNode, recoveryAttempt=recoveryAttempt, mode=mode):
            if recoveryAttempt > 0:
                tolog("Successfully updated job state file (recovery attempt number: %d) with state: %s" % (recoveryAttempt, job.jobState))
            else:
                tolog("Successfully updated job state file with state: %s" % (job.jobState))
                status = True
        else:
            self.__pilotErrorDiag = "Failed to update job state file"
            tolog(self.__errorString % self.__pilotErrorDiag)

        return status
