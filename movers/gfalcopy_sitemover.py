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
    # rm_command and ls_command : look at the code
    
    schemes = ['srm', 'gsiftp', 'https', 'davs'] # list of supported schemes for transfers

    gfal_prop_grid = '-D "SRM PLUGIN:TURL_PROTOCOLS=gsiftp"'
    gfal_prop_dynacloud = ''
    
    def detectDynafedCloud(self, ddmendpoint):
        """
            Determine whether the storage is a cloud with dynafed frontend.
            It is necessary to distinguish cloud from the grid to treat them in different ways (plug-in properties, checksum)
        """
        ddmConf = self.ddmconf.get(ddmendpoint, {})
        is_mkdir = bool ( ddmConf.get("is_mkdir") ) # "true" or "false" -> True or False
        webdav_se_flavour = ( ddmConf.get("se_flavour") == "WEBDAV")
        isDynafedCloud = is_mkdir and webdav_se_flavour 
        self.log("gfalcopy_sitemover:  %s is defined as Dynafed with cloud backend: %s" % (ddmendpoint, isDynafedCloud) )
        return isDynafedCloud
    
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
            Should be implemented by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """
        self.log("gfalcopy_sitemover: stageOutFile() arguments: src=%s, dst=%s fspec=%s" % (source, destination, fspec) )
        # we need to store the value of isDynafedCloud it in the private variable in order to use it later in getRemoteFileChecksum()  , as 
        # getRemoteFileChecksum() without fspec will be called after stageOutFile from the base.stageOut()  
        self._isDynafedCloud = self.detectDynafedCloud(fspec.ddmendpoint)
        gfal_prop = self.gfal_prop_dynacloud if  self._isDynafedCloud else self.gfal_prop_grid
        
        # in ES workflow only fspec.pfn is correct, but it may be not set for normal workflow
        src = fspec.pfn if fspec.pfn else source
        
        # resolve token value from fspec.ddmendpoint
        token = self.ddmconf.get(fspec.ddmendpoint, {}).get('token')
        if not token:
            raise PilotException("stageOutFile: Failed to resolve token value for ddmendpoint=%s: src=%s, destination=%s, fspec=%s .. unknown ddmendpoint" % (fspec.ddmendpoint, src, destination, fspec))

        filesize = os.path.getsize(src)
        timeout = self.getTimeOut(filesize)

        src_checksum, src_checksum_type = fspec.get_checksum()
        checksum_opt = ''
        if src_checksum and not self._isDynafedCloud:
            checksum_opt = '-K %s:%s' % (src_checksum_type, src_checksum)

        
        srcUrl = "file://%s" % os.path.abspath(src) # may be omitted, gfal-utils understand local file paths
        cmd = '%s --verbose %s -p -f -t %s %s -S %s %s %s' % (self.copy_command, checksum_opt, timeout, gfal_prop, token, srcUrl, destination)

        # Prepend the command with singularity if necessary
        from Singularity import singularityWrapper
        cmd = singularityWrapper(cmd, fspec.cmtconfig, dirname(src))

        return self._stagefile(cmd, src, destination, filesize, is_stagein=False)


    def stageInFile(self, source, destination, fspec):
        """
            Stage out the file
            Should be implemented by different site mover
            :return: remote file (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """
        self.log("gfalcopy_sitemover: stageInFile() arguments: src=%s, dst=%s fspec=%s" % (source, destination, fspec) )
        self._isDynafedCloud = self.detectDynafedCloud(fspec.ddmendpoint)
        gfal_prop = self.gfal_prop_dynacloud if  self._isDynafedCloud else self.gfal_prop_grid

        dst = fspec.pfn if fspec.pfn else destination
      
        timeout = self.getTimeOut(fspec.filesize)

        src_checksum, src_checksum_type = fspec.get_checksum()
        checksum_opt = ''
        if src_checksum and not self._isDynafedCloud:
            checksum_opt = '-K %s:%s' % (src_checksum_type, src_checksum)

        dstUrl = "file://%s" % os.path.abspath(dst) # may be omitted, gfal-utils treat the absence of protocol:// as a local file path
        cmd = '%s --verbose %s -f -t %s %s %s %s' % (self.copy_command, checksum_opt, timeout, gfal_prop, source, dstUrl)

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
        # cloud storages does not support total file checksum (it exists, but depends on the upload procedure: how the file was broken into pieces for transportation)
        # Anyway gfal-utils commands do not support total file sum check for cloud storages.
        # Transfer check: the most newest versions of gfal (since 2.15.0, nov 2017), can send transfer data md5 in http which leads to verification in the cloud.
        # Maybe it can be implemented later when sites will have new gfal version.
        
        # avoid checksum checking for dynafed+cloud:
        if self._isDynafedCloud:
            return None, None
        
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
        
        # do not rely on gfal-ls internal time out
        timeout = self.getTimeOut(0)
        cmd = "gfal-ls -l -t %d %s| awk '{print $5}'" % (timeout, filename)
        self.log("getRemoteFileSize: execute command: %s" % cmd )
        
        timer = TimerCommand(cmd)
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
            raise PilotException("Failed to get remote file size: timeout")

        if rcode:
            raise PilotException("Failed to get remote file size: cmd execution error")

        size = int(output)
        self.log("getRemoteFileSize: success: file=%s size=%d " % (file, size))
        return size
