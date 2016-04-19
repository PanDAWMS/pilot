"""
  dcacheSiteMover implementation
  SiteMover for dCache/dccp locally mounted SEs
  :reimplemented: Alexey Anisenkov
"""

from .base import BaseSiteMover

from TimerCommand import TimerCommand
from PilotErrors import PilotErrors, PilotException

from subprocess import Popen, PIPE, STDOUT

from datetime import datetime

import re
import os

class dcacheSiteMover(BaseSiteMover):
    """ SiteMover for dCache/dccp locally mounted SEs for both get and put operations"""

    #name = "dccp"
    copy_command = "dccp"
    checksum_type = "adler32"
    checksum_command = "adler32"

    #def __init__(self, *args, **kwargs):
    #    super(dcacheSiteMover, self).__init__(*args, **kwargs)

    def is_stagein_allowed(self, fspec, job):
        """
            check if stage-in operation is allowed for the mover
            apply additional job specific checks here if need
            Should be overwritten by custom sitemover
            :return: True in case stage-in transfer is allowed
            :raise: PilotException in case of controlled error
        """

        # for analysis jobs, failure transfer of (non lib) input file if the file is on tape (not pre-staged)
        if job.isAnalysisJob() and not '.lib.tgz' in fspec.lfn: # check if file is on tape
            if not self.isFileStaged(fspec):
                raise PilotException("File %s is not staged and will be skipped for analysis job: stage-in is not allowed" % fspec.lfn, code=PilotErrors.ERR_FILEONTAPE, state='FILE_ON_TAPE')

        return True

    def isFileStaged(self, fspec):

        is_staged = True # assume file is staged by default

        cmd = '%s -P -t -1 %s' % (self.copy_command, fspec.turl)
        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        timeout = 10
        self.log("Executing command: %s, timeout=%s" % (cmd, timeout))

        t0 = datetime.now()
        is_timeout = False
        try:
            timer = TimerCommand(cmd)
            rcode, output = timer.run(timeout=timeout)
            is_timeout = timer.is_timeout
        except Exception, e:
            self.log("WARNING: %s threw an exception: %s" % (self.copy_command, e))
            rcode, output = -1, str(e)

        dt = datetime.now() - t0
        self.log("Command execution time: %s" % dt)
        self.log("is_timeout=%s, rcode=%s, output=%s" % (is_timeout, rcode, output.replace("\n", " ")))

        if is_timeout:
            self.log("isFileStaged command self timed out after %s, timeout=%s, output=%s" % (dt, self.timeout, output))
            self.log('skip isFileStaged() test..')
        elif rcode == -1:
            self.log('WARNING: isFileStaged command (%s) failed: Status=%s Output=%s' % (cmd, rcode, output.replace("\n"," ")))
            self.log('skip isFileStaged() test..')
        elif not rcode: # zero code => file is online
            is_staged = True
            self.log("isFileStaged: is_staged=True, successfully verified file stage status for lfn=%s" % fspec.lfn)
        else:
            is_staged = False
            self.log("isFileStaged: is_staged=False, successfully verified OFFLINE file stage status for lfn=%s" % fspec.lfn)

        self.log("result: is_staged=%s" % is_staged)

        return is_staged


    def _stagefile(self, source, destination, filesize, is_stagein):
        """
            Stage the file
            mode is stagein or stageout
            :return: destination file details (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        cmd = '%s -A %s %s' % (self.copy_command, source, destination)
        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        timeout = self.getTimeOut(filesize)
        self.log("Executing command: %s, timeout=%s" % (cmd, timeout))

        t0 = datetime.now()
        is_timeout = False
        try:
            timer = TimerCommand(cmd)
            rcode, output = timer.run(timeout=timeout)
            is_timeout = timer.is_timeout
        except Exception, e:
            self.log("WARNING: %s threw an exception: %s" % (self.copy_command, e))
            rcode, output = -1, str(e)

        dt = datetime.now() - t0
        self.log("Command execution time: %s" % dt)
        self.log("is_timeout=%s, rcode = %s, output = %s" % (is_timeout, rcode, output.replace("\n", " ")))

        if is_timeout:
            raise PilotException("Copy command self timed out after %s, timeout=%s, output=%s" % (dt, self.timeout, output), code=PilotErrors.ERR_GETTIMEOUT if is_stagein else PilotErrors.ERR_PUTTIMEOUT, state='CP_TIMEOUT')

        if rcode:
            self.log('WARNING: [is_stagein=%s] Stage file command (%s) failed: Status=%s Output=%s' % (is_stagein, cmd, rcode, output.replace("\n"," ")))
            error = self.resolveStageErrorFromOutput(output, source, is_stagein=is_stagein)

            if is_stagein: # do clean up: check if file was partially transferred
                self.removeLocal(destination)

            rcode = error.get('rcode')
            if not rcode:
                rcode = PilotErrors.ERR_STAGEINFAILED if is_stagein else PilotErrors.ERR_STAGEOUTFAILED
            state = error.get('state')
            if not state:
                state = 'COPY_FAIL' #'STAGEIN_FAILED' if is_stagein else 'STAGEOUT_FAILED'

            raise PilotException(error.get('error'), code=rcode, state=state)

        # extract filesize and checksum values from output: not available for dccp in stage-in
        # check stage-out: not used at the moment

        return None, None

    def stageOutFile(self, source, destination, fspec):
        """
            Stage out the file
            Should be implementated by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        filesize = os.path.getsize(source)
        return self._stagefile(source, destination, filesize, is_stagein=False)

    def stageInFile(self, source, destination, fspec):
        """
            Stage out the file
            Should be implementated by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        return self._stagefile(source, destination, fspec.filesize, is_stagein=True)


    def getRemoteFileChecksum(self, filename):
        """
            get checksum of remote file
            should be implemented by Site Mover
            :return: (checksum, checksum_type)
            :raise: an exception in case of errors
        """

        raise Exception("getRemoteFileChecksum(): NOT IMPLEMENTED error")


    def getRemoteFileSize(self, filename):
        """
            get size of remote file
            Should be implemented by different site mover
            :return: length of file
            :raise: an exception in case of errors
        """

        raise Exception("getRemoteFileSize(): NOT IMPLEMENTED error")
