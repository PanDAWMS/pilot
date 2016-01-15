"""
  lcg-cp site mover implementation
  :reimplemented: Alexey Anisenkov
"""

from .base import BaseSiteMover

from TimerCommand import TimerCommand
from PilotErrors import PilotErrors, PilotException

from subprocess import Popen, PIPE, STDOUT

from datetime import datetime

import re
import os

class lcgcpSiteMover(BaseSiteMover):
    """ SiteMover that uses lcg-cp for both get and put """

    name = "lcgcp"
    copy_command = "lcg-cp"
    checksum_type = "adler32"
    checksum_command = "lcg-get-checksum"

    #def __init__(self, *args, **kwargs):
    #    super(lcgcpSiteMover, self).__init__(*args, **kwargs)

    def _stagefile(self, cmd, source, destination, filesize, is_stagein):
        """
            Stage the file
            mode is stagein or stageout
            :return: destination file details (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        timeout = self.getTimeOut(filesize)

        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

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
        self.log("is_timeout=%s, rcode=%s, output=%s" % (is_timeout, rcode, output))

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
        # resolve token value from fspec.ddmendpoint

        token = self.ddmconf.get(fspec.ddmendpoint, {}).get('token')
        if not token:
            raise PilotException("stageOutFile: Failed to resolve token value for ddmendpoint=%s: source=%s, destination=%s, fspec=%s .. unknown ddmendpoint" % (fspec.ddmendpoint, source, destination, fspec))
        filesize = os.path.getsize(source)
        timeout = self.getTimeOut(filesize)
        cmd = '%s --verbose --vo atlas -b -U srmv2 --connect-timeout=300 --srm-timeout=%s --sendreceive-timeout=%s -S %s %s %s' % (self.copy_command, timeout, timeout, token, source, destination)
        return self._stagefile(cmd, source, destination, filesize, is_stagein=False)


    def stageInFile(self, source, destination, fspec):
        """
            Stage out the file
            Should be implementated by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        timeout = self.getTimeOut(fspec.filesize)
        cmd = '%s --verbose --vo atlas -b -T srmv2 --connect-timeout=300 --srm-timeout=%s --sendreceive-timeout=%s %s %s' % (self.copy_command, timeout, timeout, source, destination)
        return self._stagefile(cmd, source, destination, fspec.filesize, is_stagein=True)


    def getRemoteFileChecksum(self, filename):
        """
            get checksum of remote file
            should be implemented by Site Mover
            :return: (checksum, checksum_type)
            :raise: an exception in case of errors
        """

        if self.checksum_type not in ['adler32']:
            raise Exception("getRemoteFileChecksum(): internal error: unsupported checksum_type=%s .. " % self.checksum_type)

        cmd = "%s -b -T srmv2 --checksum-type %s --connect-timeout=300 --sendreceive-timeout=3600" % (self.checksum_command, self.checksum_type)

        return self.calc_checksum(filename, cmd, setup=self.getSetup()), self.checksum_type


    def getRemoteFileSize(self, filename):
        """
            get size of remote file
            Should be implemented by different site mover
            :return: length of file
            :raise: an exception in case of errors
        """

        raise Exception("getRemoteFileSize(): NOT IMPLEMENTED error")
