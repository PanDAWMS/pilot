"""
  Local site mover implementation
  :author: Alexey Anisenkov
"""

from .base import BaseSiteMover

from TimerCommand import TimerCommand
from PilotErrors import PilotErrors, PilotException

from subprocess import Popen, PIPE, STDOUT

from datetime import datetime

import re
import os

class lsmSiteMover(BaseSiteMover):
    """ SiteMover that uses lsm-get/lsm-put for get/put operations"""

    name = "lsm"
    #copy_command = "lsm"
    checksum_type = "adler32"
    checksum_command = "adler32"

    schemes = ['srm', 'gsiftp', 'root'] # list of supported schemes for transfers


    def shouldVerifyStageIn(self):
        return True # to be disabled

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
            self.log("WARNING: %s threw an exception: %s" % (cmd, e))
            rcode, output = -1, str(e)

        dt = datetime.now() - t0
        self.log("Command execution time: %s" % dt)
        self.log("is_timeout=%s, rcode=%s, output=%s" % (is_timeout, rcode, output))

        if is_timeout or rcode: ## do clean up
            if is_stagein: # stage-in clean up: check if file was partially transferred
                self.removeLocal(destination)

        if is_timeout:
            raise PilotException("Copy command self timed out after %s, timeout=%s, output=%s" % (dt, timeout, output), code=PilotErrors.ERR_GETTIMEOUT if is_stagein else PilotErrors.ERR_PUTTIMEOUT, state='CP_TIMEOUT')

        if rcode:
            self.log('WARNING: [is_stagein=%s] Stage file command (%s) failed: Status=%s Output=%s' % (is_stagein, cmd, rcode, output.replace("\n"," ")))

            if rcode == 205: # lsm-get 205 error (Expected adler32 checksum does not match the checksum of file)
                error = {'rcode':PilotErrors.ERR_GETADMISMATCH, 'state':'AD_MISMATCH', 'error':output}
            else:
                error = self.resolveStageErrorFromOutput(output, source, is_stagein=is_stagein)

            rcode = error.get('rcode')
            if not rcode:
                rcode = PilotErrors.ERR_STAGEINFAILED if is_stagein else PilotErrors.ERR_STAGEOUTFAILED
            state = error.get('state')
            if not state:
                state = 'COPY_FAIL' #'STAGEIN_FAILED' if is_stagein else 'STAGEOUT_FAILED'

            raise PilotException(error.get('error'), code=rcode, state=state)

        # extract filesize and checksum values from output
        if not is_stagein: # check stage-out
            return self.getRemoteFileChecksumFromOutput(output)

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
            raise PilotException("stageOutFile: Failed to resolve token value for ddmendpoint=%s: source=%s, destination=%s, fspec=%s .. unknown ddmendpoint" % (fspec.ddmendpoint, source, destination, fspec), code=PilotErrors.ERR_STAGEOUTFAILED, state='UNKNOWN_DDMENDPOINT')
        filesize = os.path.getsize(source)

        checksum = fspec.get_checksum()
        if not checksum[0]: # checksum is not available => do calculate
            checksum = self.calc_file_checksum(source)
            fspec.set_checksum(checksum[0], checksum[1])

        if not checksum[1]:
            checksum = checksum[0]
        else:
            checksum = "%s:%s" % (checksum[1], checksum[0])

        opts = {'--size':filesize, '-t':token, '--checksum':checksum, '--guid':fspec.guid}
        opts = " ".join(["%s %s" % (k,v) for (k,v) in opts.iteritems()])

        cmd = 'lsm-put %s %s %s' % (opts, source, destination)

        return self._stagefile(cmd, source, destination, filesize, is_stagein=False)


    def stageInFile(self, source, destination, fspec):
        """
            Stage in the file
            Should be implementated by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        checksum = fspec.get_checksum()
        if not checksum[1]:
            checksum = checksum[0]
        else:
            checksum = "%s:%s" % (checksum[1], checksum[0])

        opts = {'--size':fspec.filesize, '--checksum':checksum}
        #opts['--guid'] = fspec.guid
        opts = " ".join(["%s %s" % (k,v) for (k,v) in opts.iteritems()])

        cmd = 'lsm-get %s %s %s' % (opts, source, destination)

        return self._stagefile(cmd, source, destination, fspec.filesize, is_stagein=True)


    def getRemoteFileChecksumFromOutput(self, output):
        """
            extract checksum value from command output
            :return: (checksum, checksum_type) or (None, None) in case of failure
        """

        pattern = ".*?size[\s:]+(?P<size>[0-9]+)[\s:]+(?P<type>md5|adler32)[\s:]+(?P<checksum>[a-zA-Z0-9]+)\s*"
        checksum, checksum_type = None, None

        m = re.search(pattern, output)
        if m:
            checksum_type = m.group('type')
            checksum = m.group('checksum')
            checksum = checksum.zfill(8) # make it at least 8 chars length
        else:
            self.log("WARNING: Checksum info not found in output: failed to match pattern=%s against output=%s" % (pattern, output))

        return checksum, checksum_type


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
