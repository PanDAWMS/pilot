"""
  gfal-copy site mover implementation
  :author: Alexey Anisenkov
"""

from .base import BaseSiteMover

from TimerCommand import TimerCommand
from PilotErrors import PilotErrors, PilotException

from datetime import datetime
from os.path import dirname
import time

import re
import os

class gfalcopySiteMover(BaseSiteMover):
    """ SiteMover that uses gfal-copy for both get and put """

    name = "gfalcopy"
    copy_command = "gfal-copy"
    checksum_type = "adler32"
    checksum_command = "gfal-sum"

    schemes = ['srm', 'gsiftp', 'https'] # list of supported schemes for transfers

    def _stagefile(self, cmd, source, destination, filesize, is_stagein):
        """
            Stage the file (stagein or stageout respect to is_stagein value)
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

        if is_timeout or rcode: ## do clean up
            if is_stagein: # stage-in clean up: check if file was partially transferred
                self.removeLocal(destination)

        if is_timeout:
            raise PilotException("Copy command self timed out after %s, timeout=%s, output=%s" % (dt, timeout, output), code=PilotErrors.ERR_GETTIMEOUT if is_stagein else PilotErrors.ERR_PUTTIMEOUT, state='CP_TIMEOUT')

        if rcode:
            self.log('WARNING: [is_stagein=%s] Stage file command (%s) failed: Status=%s Output=%s' % (is_stagein, cmd, rcode, output.replace("\n"," ")))
            error = self.resolveStageErrorFromOutput(output, source, is_stagein=is_stagein)
            rcode = error.get('rcode')
            if not rcode:
                rcode = PilotErrors.ERR_STAGEINFAILED if is_stagein else PilotErrors.ERR_STAGEOUTFAILED
            state = error.get('state')
            if not state:
                state = 'COPY_FAIL' #'STAGEIN_FAILED' if is_stagein else 'STAGEOUT_FAILED'

            raise PilotException(error.get('error'), code=rcode, state=state)

        # extract filesize and checksum values from output
        # check stage-out: not used at the moment

        return None, None

    def remote_cleanup(self, destination, fspec):
        """
            Apply remote clean up
            remove incomplete remote file
        """

        try:
            self.removeRemoteFile(destination)
            self.log("gfal clean up: successfully removed remote file=%s from storage" % destination)
            return True
        except PilotException, e:
            self.log("Warning: failed to remove remote file=%s from storage .. skipped: error=%s" % (destination, e))

        return False


    def removeRemoteFile(self, surl):
        """
            Do remove (remote) file from storage
            :raise: PilotException in case of controlled error
        """

        # take a 1 m nap before trying to reach the file (it might not be available immediately after a transfer)
        self.log("INFO: [gfal removeRemoteFile] Taking a 1 m nap before the file removal attempt")
        time.sleep(60)

        timeout = self.getTimeOut(0)
        cmd = 'gfal-rm --verbose -t %s %s' % (timeout, surl)

        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        self.log("Do remove RemoteFile: %s" % surl)
        self.log("Executing command: %s, timeout=%s" % (cmd, timeout))

        t0 = datetime.now()
        is_timeout = False
        try:
            timer = TimerCommand(cmd)
            rcode, output = timer.run(timeout=timeout)
            is_timeout = timer.is_timeout
        except Exception, e:
            self.log("WARNING: %s threw an exception: %s" % ('gfal-rm', e))
            rcode, output = -1, str(e)

        dt = datetime.now() - t0
        self.log("Command execution time: %s" % dt)
        self.log("is_timeout=%s, rcode=%s, output=%s" % (is_timeout, rcode, output))

        if is_timeout:
            raise PilotException("removeRemoteFile self timed out after %s, timeout=%s, output=%s" % (dt, timeout, output), code=PilotErrors.ERR_GENERALERROR, state='RM_TIMEOUT')

        if rcode:
            raise PilotException("Failed to remove remote file", code=PilotErrors.ERR_GENERALERROR, state='RM_FAILED')


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

        src_checksum, src_checksum_type = fspec.get_checksum()
        checksum_opt = ''
        if src_checksum:
            checksum_opt = '-K %s:%s' % (src_checksum_type, src_checksum)

        src = "file://%s" % os.path.abspath(source)
        cmd = '%s --verbose %s -p -f -t %s -D "SRM PLUGIN:TURL_PROTOCOLS=gsiftp" -S %s %s %s' % (self.copy_command, checksum_opt, timeout, token, src, destination)

        # Prepend the command with singularity if necessary
        from Singularity import singularityWrapper
        cmd = singularityWrapper(cmd, fspec.cmtconfig, dirname(source))

        return self._stagefile(cmd, source, destination, filesize, is_stagein=False)


    def stageInFile(self, source, destination, fspec):
        """
            Stage out the file
            Should be implementated by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        timeout = self.getTimeOut(fspec.filesize)

        src_checksum, src_checksum_type = fspec.get_checksum()
        checksum_opt = ''
        if src_checksum:
            checksum_opt = '-K %s:%s' % (src_checksum_type, src_checksum)

        dst = "file://%s" % os.path.abspath(destination)
        cmd = '%s --verbose %s -f -t %s -D "SRM PLUGIN:TURL_PROTOCOLS=gsiftp" %s %s' % (self.copy_command, checksum_opt, timeout, source, dst)

        # Prepend the command with singularity if necessary
        from Singularity import singularityWrapper
        cmd = singularityWrapper(cmd, fspec.cmtconfig, dirname(destination))

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

        timeout = self.getTimeOut(0)
        cmd = '%s -D "SRM PLUGIN:TURL_PROTOCOLS=gsiftp" -t %s %s %s' % (self.checksum_command, timeout, filename, self.checksum_type)

        return self.calc_checksum(filename, cmd=cmd, setup=self.getSetup(), pattern='.*?\s+(?P<checksum>[^\s]+)'), self.checksum_type


    def getRemoteFileSize(self, filename):
        """
            get size of remote file
            Should be implemented by different site mover
            :return: length of file
            :raise: an exception in case of errors
        """

        raise Exception("getRemoteFileSize(): NOT IMPLEMENTED error")
