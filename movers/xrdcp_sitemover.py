"""
  xrdcpSiteMover implementation
  :reimplemented: Alexey Anisenkov
"""

from .base import BaseSiteMover

from TimerCommand import TimerCommand
from PilotErrors import PilotErrors, PilotException

from subprocess import Popen, PIPE, STDOUT

from datetime import datetime

import re
import os

class xrdcpSiteMover(BaseSiteMover):
    """ SiteMover that uses xrdcp for both get and put """

    #name = "xrdcp"
    copy_command = "xrdcp"
    checksum_type = "adler32"
    checksum_command = "xrdadler32"


    def stageOutFile(self, source, destination):
        """
            Stage out the file
            Should be implementated by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        if self.checksum_type not in ['adler32']: # exclude md5
            raise PilotException("Failed to stageOutFile(): internal error: unsupported checksum_type=%s .. " % self.checksum_type, code=PilotErrors.ERR_STAGEOUTFAILED, state='BAD_CSUMTYPE')

        cmd = "%s -h" % self.copy_command
        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        self.log("Execute command (%s) to decide which option should be used to calc file checksum.." % cmd)

        c = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
        output = c.communicate()[0]

        self.log("status: %s, output: %s" % (c.returncode, output))

        coption = ""

        if c.returncode:
            self.log('FAILED to execute command=%s: %s' % (cmd, output))
        else:
            if "--cksum" in output:
                coption = "--cksum %s:print" % self.checksum_type
            elif "-adler" in output and self.checksum_type == 'adler32':
                coption = "-adler"
            elif "-md5" in output and self.checksum_type == 'md5':
                coption = "-md5"

        if coption:
            self.log("Use %s option to get the checksum" % coption)
        else:
            self.log("Cannot find neither -adler nor --cksum. will not use checksum")

        cmd = '%s -np -f %s %s %s' % (self.copy_command, coption, source, destination)
        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        timeout = self.getTimeOut(os.path.getsize(source))
        self.log("Executing command: %s, timeout=%s" % (cmd, timeout))

        t0 = datetime.now()
        is_timeout = False
        try:
            timer = TimerCommand(cmd)
            rcode, output = timer.run(timeout=timeout)
            is_timeout = timer.is_timeout
        except Exception, e:
            self.log("WARNING: xrdcp threw an exception: %s" % e)
            rcode, output = -1, str(e)

        dt = datetime.now() - t0
        self.log("Command execution time: %s" % dt)
        self.log("is_timeout=%s, rcode = %s, output = %s" % (is_timeout, rcode, output.replace("\n", " ")))

        if is_timeout:
            raise PilotException("Copy command self timed out after %s, timeout=%s, output=%s" % (dt, self.timeout, output), code=PilotErrors.ERR_PUTTIMEOUT, state='CP_TIMEOUT')

        if rcode:
            self.log('WARNING: Stage Out command (%s) failed: Status=%s Output=%s' % (cmd, rcode, output.replace("\n"," ")))
            error = self.resolveStageOutError(output, source)

            #if rcode != PilotErrors.ERR_FILEEXIST:
            #    # check if file was partially transferred, if so, remove it
            #    #_ec, removeOutput = self.removeRemoteFile(destination)
            #    #if not _ec :
            #    #    self.log("Failed to remove file %s" % destination)
            #    #return rcode, outputRet

            raise PilotException(error.get('error'), code=error.get('rcode', PilotErrors.ERR_STAGEOUTFAILED), state=error.get('state', 'STAGEOUT_FAILED'))

        # extract remote filesize and checksum values from output

        checksum, checksum_type = self.getRemoteFileChecksumFromOutput(output)

        return checksum, checksum_type


    def getRemoteFileChecksum(self, filename):
        """
            get checksum of remote file
            should be implemented by Site Mover
            :return: (checksum, checksum_type)
            :raise: an exception in case of errors
        """

        if self.checksum_type not in ['adler32']:
            raise Exception("getRemoteFileChecksum(): internal error: unsupported checksum_type=%s .. " % self.checksum_type)

        return self.calc_checksum(filename, self.checksum_command, setup=self.getSetup()), 'adler32'


    def getRemoteFileSize(self, filename):
        """
            get size of remote file
            Should be implemented by different site mover
            :return: length of file
            :raise: an exception in case of errors
        """
        # For xrdcp site mover, not implemented yet.

        raise Exception("getRemoteFileSize(): NOT IMPLEMENTED error")


    def getRemoteFileChecksumFromOutput(self, output):
        """
            extract checksum value from xrdcp --chksum command output
            :return: (checksum, checksum_type) or (None, None) in case of failure
        """

        if not ("xrootd" in output or "XRootD" in output or "adler32" in output):
            self.log("WARNING: Failed to extract checksum: Unexpected %s output: %s" % (self.copy_command, output))
            return None, None

        pattern = "(?P<type>md5|adler32): (?P<checksum>[a-zA-Z0-9]+)"
        checksum, checksum_type = None, None

        m = re.search(pattern, output)
        if m:
            checksum_type = m.group('type')
            checksum = m.group('checksum')
            checksum = checksum.zfill(8) # make it at least 8 chars length (adler32 xrdcp fix)
            #self.log("Copy command returned checksum: %s" % checksum)
        else:
            self.log("WARNING: Checksum info not found in output: failed to match pattern=%s in output=%s" % (pattern, output))

        return checksum, checksum_type
