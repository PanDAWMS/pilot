"""
  Base class of site movers
  :author: Alexey Anisenkov
"""

import hashlib
import os
import time

from subprocess import Popen, PIPE, STDOUT

from pUtil import tolog #
from PilotErrors import PilotErrors, PilotException
from Node import Node

class BaseSiteMover(object):
    """
    File movers move files between a storage element (of different kinds) and a local directory
    get_data: SE->local
    put_data: local->SE
    check_space: available space in SE

    mkdirWperm -- create recursively dirs setting appropriate permissions
    getLocalFileInfo -- get size and checksum of a local file

    """

    name = "" # unique ID of the Mover implementation, if not set copy_command will be used
    copy_command = None

    timeout = 5*60 # 5 min

    checksum_type = "adler32"     # algorithm name of checksum calculation
    checksum_command = "adler32"  # command to be executed to get checksum, e.g. md5sum (adler32 is internal default implementation)

    ddmconf = {}                  # DDMEndpoints configuration from AGIS

    #has_mkdir = True
    #has_df = True
    #has_getsize = True
    #has_md5sum = True
    #has_chmod = True
    #

    def __init__(self, setup_path='', **kwargs):

        self.copysetup = setup_path
        self.timeout = kwargs.get('timeout', self.timeout)
        self.ddmconf = kwargs.get('ddmconf', self.ddmconf)
        self.workDir = kwargs.get('workDir', '')

        #self.setup_command = self.getSetup()

        self.trace_report = {}

    @classmethod
    def log(self, value): # quick stub
        #print value
        tolog(value)

    @property
    def copysetup(self):
        return self._setup

    @copysetup.setter
    def copysetup(self, value):
        value = os.path.expandvars(value.strip())
        if value and not os.access(value, os.R_OK):
            self.log("WARNING: copysetup=%s is invalid: file is not readdable" % value)
            raise PilotException("Failed to set copysetup: passed invalid file name=%s" % value, code=PilotErrors.ERR_NOSUCHFILE, state="RFCP_FAIL")
        self._setup = value

    @classmethod
    def getID(self):
        """
            return the ID/NAME string of Mover class used to resolve Mover classs
            name attribute helps to define various movers with the same copy command
        """
        return self.name or self.copy_command

    @classmethod
    def getRucioPath(self, scope, lfn, prefix='rucio'):
        """
            Construct a partial Rucio PFN using the scope and the LFN
        """

        # <prefix=rucio>/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

        hash_hex = hashlib.md5('%s:%s' % (scope, lfn)).hexdigest()

        paths = [prefix] + scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        paths = filter(None, paths) # remove empty parts to avoid double /-chars
        return '/'.join(paths)

        #scope = os.path.join(*scope.split('.')) # correct scope
        #return os.path.join(prefix, scope, hash_hex[0:2], hash_hex[2:4], lfn)

    def getSURLRucio(self, se, se_path, scope, lfn, job=None):
        """
            Get final destination SURL of file to be moved
        """

        # ANALY/PROD job specific processing ??

        prefix = 'rucio'
        if se_path.rstrip('/').endswith('/' + prefix): # avoid double prefix
            prefix = ''

        surl = se + os.path.join(se_path, self.getRucioPath(scope, lfn, prefix=prefix))

        return surl

    def getSURL(self, se, se_path, scope, lfn, job=None):
        """
            Get final destination SURL of file to be moved
            job instance is passing here for possible JOB specific processing ?? FIX ME LATER
        """

        if '/rucio' in se_path:
            return self.getSURLRucio(se, se_path, scope, lfn)

        raise Exception("getSURL(): NOT IMPLEMENTED error: processing of non Rucio transfers is not implemented yet, se_path=%s" % se_path)

    def getSetup(self):
        """
            return full setup command to be executed
            Can be customized by different site mover
        """
        if not self.copysetup:
            return ''
        return 'source %s' % self.copysetup

    def setup(self):
        """
            Prepare site specific setup initializations
            Should be implemented by different site mover
        """

        # TODO: vertify setup??
        # raise in case of errors

        return True # rcode=0, output=''

    def shouldVerifyStageIn(self):
        """
            Should the get operation perform any file size/checksum verifications?
            can be customized for specific movers
        """

        return True

    def check_availablespace(self, maxinputsize, files):
        """
            Verify that enough local space is available to stage in and run the job
        """

        if not self.shouldVerifyStageIn():
            return

        totalsize = reduce(lambda x, y: x + y.filesize, files, 0)

        # verify total filesize
        if maxinputsize and totalsize > maxinputsize:
            error = "Too many/too large input files (%s). Total file size=%s B > maxinputsize=%s B" % (len(files), totalsize, maxinputsize)
            raise PilotException(error, code=PilotErrors.ERR_SIZETOOLARGE)

        self.log("Total input file size=%s B within allowed limit=%s B (zero value means unlimited)" % (totalsize, maxinputsize))

        # get available space
        wn = Node()
        wn.collectWNInfo(self.workDir)

        available_space = int(wn.disk)*1024**2 # convert from MB to B

        self.log("Locally available space: %d B" % available_space)

        # are we wihin the limit?
        if totalsize > available_space:
            error = "Not enough local space for staging input files and run the job (need %d B, but only have %d B)" % (totalsize, available_space)
            raise PilotException(error, code=PilotErrors.ERR_NOLOCALSPACE)


    def getRemoteFileChecksum(self, filename):
        """
            get checksum of remote file
            Should be implemented by different site mover
            :return: (checksum, checksum_type)
            :raise: an exception in case of errors
        """

        return None, None

    def getRemoteFileSize(self, filename):
        """
            get size of remote file
            Should be implemented by different site mover
            :return: length of file
            :raise: an exception in case of errors
        """

        return None

    def resolve_replica(self, fspec):
        """
            fspec is FileSpec object
            :return: input file replica details: {'surl':'', 'ddmendpoint':'', 'pfn':''}
            :raise: PilotException in case of controlled error
        """

        # resolve proper surl and find related replica

        ignore_rucio_replicas = True # quick stab until protocols are properly populated in Rucio: CHANGE ME LATER
        #if '.root' in fspec.lfn:
        #    ignore_rucio_replicas = False

        protocol = self.protocol

        scheme = protocol.get('se', '').split(':')[0]
        if not scheme:
            raise Exception('Failed to resolve copytool scheme to be used, se field is corrupted?: protocol=%s' % protocol)

        replica = None # find first matched to protocol spec replica
        surl = None

        for ddmendpoint, replicas, ddm_se in fspec.replicas:
            if not replicas:
                continue
            surl = replicas[0] # assume srm protocol is first entry
            self.log("[stage-in] surl (srm replica) from Rucio: pfn=%s, ddmendpoint=%s, ddm.se=%s" % (surl, ddmendpoint, ddm_se))

            for r in replicas:
                # match Rucio replica by default protocol se (quick stub until Rucio protocols are proper populated)
                if ignore_rucio_replicas and r.startswith(ddm_se): # manually form pfn based of protocol.se
                    replica = protocol.get('se') + r.replace(ddm_se, '')
                    self.log("[stage-in] ignore_rucio_replicas=True: found replica=%s matched ddm.se=%s .. will use TURL=%s" % (surl, ddm_se, replica))
                    break
                # use exact pfn from Rucio replicas
                if not replica and r.startswith("%s://" % scheme):
                    replica = r
            if replica:
                break

        if not replica: # replica not found
            error = 'Failed to find replica for input file, protocol=%s, fspec=%s' % (protocol, fspec)
            raise PilotException(error, code=PilotErrors.ERR_REPNOTFOUND)

        return {'surl':surl, 'ddmendpoint':ddmendpoint, 'pfn':replica}

    def is_stagein_allowed(self, fspec, job):
        """
            check if stage-in operation is allowed for the mover
            apply additional job specific checks here if need
            Should be overwritten by custom sitemover
            :return: True in case stage-in transfer is allowed
            :raise: PilotException in case of controlled error
        """

        return True

    def get_data(self, fspec):
        """
            fspec is FileSpec object
            :return: file details: {'checksum': '', 'checksum_type':'', 'filesize':''}
            :raise: PilotException in case of controlled error
        """

        # resolve proper surl and find related replica

        dst = os.path.join(self.workDir, fspec.lfn)
        return self.stageIn(fspec.turl, dst, fspec)


    def stageIn(self, source, destination, fspec):
        """
            Stage in the source file: do stagein file + verify local file
            :return: file details: {'checksum': '', 'checksum_type':'', 'filesize':''}
            :raise: PilotException in case of controlled error
        """

        self.trace_report.update(relativeStart=time.time(), transferStart=time.time())

        dst_checksum, dst_checksum_type = self.stageInFile(source, destination, fspec)

        src_fsize = fspec.filesize

        if not self.shouldVerifyStageIn():
            self.log("skipped stage-in verification for lfn=%" % fspec.lfn)
            return {'checksum': dst_checksum, 'checksum_type':dst_checksum_type, 'filesize':src_fsize}

        src_checksum, src_checksum_type = fspec.get_checksum()

        dst_fsize = os.path.getsize(destination)

        # verify stagein by checksum
        self.trace_report.update(validateStart=time.time())

        try:
            if not dst_checksum:
                dst_checksum, dst_checksum_type = self.calc_file_checksum(destination)
        except Exception, e:
            self.log("verify StageIn: caught exception while getting local file=%s checksum: %s .. skipped" % (destination, e))

        try:
            if not src_checksum:
                src_checksum, src_checksum_type = self.getRemoteFileChecksum(source)
        except Exception, e:
            self.log("verify StageIn: caught exception while getting remote file=%s checksum: %s .. skipped" % (source, e))

        try:
            if dst_checksum and dst_checksum_type: # verify against source

                is_verified = src_checksum and src_checksum_type and dst_checksum == src_checksum and dst_checksum_type == src_checksum_type

                self.log("Remote checksum [%s]: %s  (%s)" % (src_checksum_type, src_checksum, source))
                self.log("Local  checksum [%s]: %s  (%s)" % (dst_checksum_type, dst_checksum, destination))
                self.log("checksum is_verified = %s" % is_verified)

                if not is_verified:
                    error = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" % \
                                            (src_checksum_type, os.path.basename(destination), dst_checksum, src_checksum)
                    if src_checksum_type == 'adler32':
                        state = 'AD_MISMATCH'
                        rcode = PilotErrors.ERR_GETADMISMATCH
                    else:
                        state = 'MD5_MISMATCH'
                        rcode = PilotErrors.ERR_GETMD5MISMATCH
                    raise PilotException(error, code=rcode, state=state)

                self.log("verifying stagein done. [by checksum] [%s]" % source)
                self.trace_report.update(clientState="DONE")
                return {'checksum': dst_checksum, 'checksum_type':dst_checksum_type, 'filesize':dst_fsize}

        except PilotException:
            raise
        except Exception, e:
            self.log("verify StageIn: caught exception while doing file checksum verification: %s ..  skipped" % e)

        # verify stageout by filesize
        try:
            if not src_fsize:
                src_fsize = self.getRemoteFileSize(source)
            is_verified = src_fsize and src_fsize == dst_fsize

            self.log("Remote filesize [%s]: %s" % (os.path.dirname(destination), src_fsize))
            self.log("Local  filesize [%s]: %s" % (os.path.dirname(destination), dst_fsize))
            self.log("filesize is_verified = %s" % is_verified)

            if not is_verified:
                error = "Remote and local file sizes do not match for %s (%s != %s)" % (os.path.basename(destination), dst_fsize, src_fsize)
                self.log(error)
                raise PilotException(error, code=PilotErrors.ERR_GETWRONGSIZE, state='FS_MISMATCH')

            self.log("verifying stagein done. [by filesize] [%s]" % source)
            self.trace_report.update(clientState="DONE")
            return {'checksum': dst_checksum, 'checksum_type':dst_checksum_type, 'filesize':dst_fsize}

        except PilotException:
            raise
        except Exception, e:
            self.log("verify StageOut: caught exception while doing file size verification: %s .. skipped" % e)

        raise PilotException("Neither checksum nor file size could be verified (failing job)", code=PilotErrors.ERR_NOFILEVERIFICATION, state='NOFILEVERIFICATION')


    def stageInFile(source, destination, fspec=None):
        """
            Stage in the file.
            Should be implemented by different site mover
            :return: destination file details (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        raise Exception('NOT IMPLEMENTED')


    def put_data(self, fspec):
        """
            fspec is FileSpec object
            :return: remote file details: {'checksum': '', 'checksum_type':'', 'filesize':'', 'surl':''}
            stageout workflow could be overwritten by specific Mover
            :raise: PilotException in case of controlled error
        """

        src = os.path.join(self.workDir, fspec.lfn)
        return self.stageOut(src, fspec.turl, fspec)


    def stageOut(self, source, destination, fspec):
        """
            Stage out the source file: do stageout file + verify remote file output
            :return: remote file details: {'checksum': '', 'checksum_type':'', 'filesize':''}
            :raise: PilotException in case of controlled error
        """

        src_checksum, src_checksum_type = None, None
        src_fsize = fspec and fspec.filesize or os.path.getsize(source)

        if fspec:
            src_checksum, src_checksum_type = fspec.get_checksum()

        # do stageOutFile
        self.trace_report.update(relativeStart=time.time(), transferStart=time.time())
        dst_checksum, dst_checksum_type = self.stageOutFile(source, destination, fspec)

        # verify stageout by checksum
        self.trace_report.update(validateStart=time.time())

        try:
            if not dst_checksum:
                dst_checksum, dst_checksum_type = self.getRemoteFileChecksum(destination)
        except Exception, e:
            self.log("verify StageOut: caught exception while getting remote file checksum.. skipped, error=%s" % e)
            import traceback
            self.log(traceback.format_exc())

        try:
            if dst_checksum and dst_checksum_type: # verify against source
                if not src_checksum: # fspec has no checksum data defined try to calculate from the source
                    src_checksum, src_checksum_type = self.calc_file_checksum(source)

                is_verified = src_checksum and src_checksum_type and dst_checksum == src_checksum and dst_checksum_type == src_checksum_type

                self.log("Local  checksum [%s]: %s" % (src_checksum_type, src_checksum))
                self.log("Remote checksum [%s]: %s" % (dst_checksum_type, dst_checksum))
                self.log("checksum is_verified = %s" % is_verified)

                if not is_verified:
                    error = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" % \
                                            (src_checksum_type, os.path.basename(destination), dst_checksum, src_checksum)
                    if src_checksum_type == 'adler32':
                        state = 'AD_MISMATCH'
                        rcode = PilotErrors.ERR_PUTADMISMATCH
                    else:
                        state = 'MD5_MISMATCH'
                        rcode = PilotErrors.ERR_PUTMD5MISMATCH
                    raise PilotException(error, code=rcode, state=state)

                self.log("verifying stageout done. [by checksum]")
                self.trace_report.update(clientState="DONE")
                return {'checksum': dst_checksum, 'checksum_type':dst_checksum_type, 'filesize':src_fsize}

        except PilotException:
            raise
        except Exception, e:
            self.log("verify StageOut: caught exception while doing file checksum verification: %s ..  skipped" % e)

        # verify stageout by filesize
        try:
            dst_fsize = self.getRemoteFileSize(destination)
            is_verified = src_fsize and src_fsize == dst_fsize

            self.log("Local  filesize [%s]: %s" % (os.path.dirname(destination), src_fsize))
            self.log("Remote filesize [%s]: %s" % (os.path.dirname(destination), dst_fsize))
            self.log("filesize is_verified = %s" % is_verified)

            if not is_verified:
                error = "Remote and local file sizes do not match for %s (%s != %s)" % (os.path.basename(destination), dst_fsize, src_fsize)
                self.log(error)
                raise PilotException(error, code=PilotErrors.ERR_PUTWRONGSIZE, state='FS_MISMATCH')

            self.log("verifying stageout done. [by filesize]")
            self.trace_report.update(clientState="DONE")
            return {'checksum': dst_checksum, 'checksum_type':dst_checksum_type, 'filesize':src_fsize}

        except PilotException:
            raise
        except Exception, e:
            self.log("verify StageOut: caught exception while doing file size verification: %s .. skipped" % e)

        raise PilotException("Neither checksum nor file size could be verified (failing job)", code=PilotErrors.ERR_NOFILEVERIFICATION, state='NOFILEVERIFICATION')


    def stageOutFile(source, destination, fspec):
        """
            Stage out the file.
            Should be implemented by different site mover
            :return: destination file details (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """

        raise Exception('NOT IMPLEMENTED')


    def resolveStageErrorFromOutput(self, output, filename=None, is_stagein=False):
        """
            resolve error code, client state and defined error mesage from the output
            :return: dict {'rcode', 'state, 'error'}
        """

        ret = {'rcode': PilotErrors.ERR_STAGEINFAILED if is_stagein else PilotErrors.ERR_STAGEOUTFAILED, 'state': 'COPY_ERROR', 'error': 'Copy operation failed [is_stagein=%s]: %s' % (is_stagein, output)}

        if "Could not establish context" in output:
            ret['rcode'] = PilotErrors.ERR_NOPROXY
            ret['state'] = 'CONTEXT_FAIL'
            ret['error'] = "Could not establish context: Proxy / VO extension of proxy has probably expired: %s" % output
        elif "File exists" in output or 'SRM_FILE_BUSY' in output or 'file already exists' in output:
            ret['rcode'] = PilotErrors.ERR_FILEEXIST
            ret['state'] = 'FILE_EXIST'
            ret['error'] = "File already exist in the destination: %s" % output
        elif "No space left on device" in output:
            ret['rcode'] = PilotErrors.ERR_NOLOCALSPACE
            ret['state'] = 'NO_SPACE'
            ret['error'] = "No available space left on local disk: %s" % output
        elif "globus_xio:" in output:
            ret['rcode'] = PilotErrors.ERR_GETGLOBUSSYSERR
            ret['state'] = 'GLOBUS_FAIL'
            ret['error'] = "Globus system error: %s" % output
        elif "No such file or directory" and "DBRelease" in filename: ## is it a stageout error??
            ret['rcode'] = PilotErrors.ERR_MISSDBREL
            ret['state'] = 'NO_DBREL'
            ret['error'] = output
        elif "No such file or directory" in output:
            ret['rcode'] = PilotErrors.ERR_NOSUCHFILE
            ret['state'] = 'NO_FILE'
            ret['error'] = output

        return ret


    def getTimeOut(self, filesize):
        """ Get a proper time-out limit based on the file size """

        timeout_max = 6*3600 # 6 hours
        timeout_min = self.timeout #5*60   # 5 mins

        timeout = timeout_min + int(filesize/0.4e6) # approx < 0.4 Mb/sec

        return max(timeout, timeout_max)


    def calc_file_checksum(self, filename):
        """
            calculate SiteMover specific checksum for a file
            :return: (checksum, checksum_type)
            raise an exception if input filename is not exist/readable
        """

        if not self.checksum_command or not self.checksum_type:
            raise Exception("Failed to get file checksum: incomplete checksum_command declaration: type=%s, command=%s" % (self.checksum_type, self.checksum_command))

        fn = getattr(self, "calc_%s_checksum" % self.checksum_type, None)
        checksum = fn(filename) if callable(fn) else self.calc_checksum(filename, self.checksum_command)

        return checksum, self.checksum_type


    @classmethod
    def calc_adler32_checksum(self, filename):
        """
            calculate the adler32 checksum for a file
            raise an exception if input filename is not exist/readable
        """

        from zlib import adler32

        asum = 1 # default adler32 starting value
        BLOCKSIZE = 64*1024*1024 # read buffer, 64 Mb

        with open(filename, 'rb') as f:
            while True:
                data = f.read(BLOCKSIZE)
                if not data:
                   break
                asum = adler32(data, asum)
                if asum < 0:
                  asum += 2**32

        return "%08x" % asum # convert to hex


    @classmethod
    def calc_checksum(self, filename, command='md5sum', setup=None):
        """
            calculate the md5 checksum for a file
            raise an exception if input filename is not exist/readable
        """

        cmd = "%s %s" % (command, filename)
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        self.log("Execute command (%s) to calc checksum of file" % cmd)

        c = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
        output = c.communicate()[0]
        if c.returncode:
            self.log('FAILED to calc_checksum for file=%s, cmd=%s, rcode=%s, output=%s' % (flename, cmd, c.returncode, output))
            raise Exception(output)

        return output.split()[0] # return final checksum

    @classmethod
    def removeLocal(self, filename):
        """
            Remove the local file in case of failure to prevent problem with get retry attempt
        :return: True in case of physical file removal
        """

        if not os.path.exists(filename): # nothing to remove
            return False

        try:
            os.remove(filename)
            self.log("Successfully removed local file=%s" % filename)
            is_removed = True
        except Exception, e:
            self.log("Could not remove the local file=%s .. skipped, error=%s" % (filename, e))
            is_removed = False

        return is_removed
