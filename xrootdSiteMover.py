# xrootdSiteMover.py
""" Site mover used at e.g. UTA, SLACXRD """

import os
import shutil
import commands
import urllib
from time import time

import SiteMover
from re import compile, findall
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation
from config import config_sm
from FileStateClient import updateFileState
from timed_command import timed_command

PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE
CMD_CHECKSUM = config_sm.COMMAND_MD5
ARCH_DEFAULT = config_sm.ARCH_DEFAULT

class xrootdSiteMover(SiteMover.SiteMover):
    """
    File movers move files between a SE (of different kind) and a local directory
    where all posix operations have to be supported and fast access is supposed
    get_data: SE->local
    put_data: local->SE
    check_space: available space in SE
    This is the Default SiteMover, the SE has to be locally accessible for all the WNs
    and all commands like cp, mkdir, md5checksum have to be available on files in the SE
    E.g. NFS exported file system
    """
    __childDict = {}

    copyCommand = "xcp"
    checksum_command = "adler32"
    permissions_DIR = PERMISSIONS_DIR
    permissions_FILE = PERMISSIONS_FILE
    arch_type = ARCH_DEFAULT
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        """ default init """
        self._setup = setup_path
        tolog("Init is using _setup: %s" % (self._setup))

    def get_timeout(self):
        return self.timeout

    def getID(self):
        """ returnd SM ID, the copy command used for it """
        return self.copyCommand

    def getSetup(self):
        """ returns the setup string (pacman setup os setup script) for the copy command """
        return self._setup

    def getCopytool(self, setup):
        """ determine which copy command to use """
        cmd = "which xcp"
        cpt = "cp"
        try:
            rs = commands.getoutput("%s which xcp" % (setup))
        except Exception, e:
            tolog("!!WARNING!!2999!! Failed the copy command test: %s" % str(e))
        else:
            if rs.find("no xcp") >= 0:
                cpt = "cp"
            else:
                cpt = "xcp"
        tolog("Will use %s to transfer file" % (cpt))
        return cpt

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        Moves a DS file the local SE (where was put from DDM) to the working directory.
        Performs the copy and, for systems supporting it, checks size and md5sum correctness
        gpfn: full source URL (e.g. method://[host[:port]/full-dir-path/filename - a SRM URL is OK)
        path: destination absolute path (in a local file system)
        returns the status of the transfer. In case of failure it should remove the partially copied destination
        """
        # The local file is assumed to have a relative path that is the same of the relative path in the 'gpfn'
        # loc_... are the variables used to access the file in the locally exported file system

        error = PilotErrors()
        pilotErrorDiag = ""

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        dsname = pdict.get('dsname', '')
        workDir = pdict.get('workDir', '')
        prodDBlockToken = pdict.get('access', '')

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'xrootd', lfn, guid)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        ec, pilotErrorDiag = verifySetupCommand(error, _setup_str)
        if ec != 0:
            self.prepareReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        tolog("xrootdSiteMover get_data using setup: %s" % (_setup_str))

        # remove any host and SFN info from PFN path
        src_loc_pfn = self.extractPathFromPFN(gpfn)

        src_loc_filename = lfn
        # source vars: gpfn, loc_pfn, loc_host, loc_dirname, loc_filename
        # dest vars: path

        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # protect against bad pfn's
        src_loc_pfn = src_loc_pfn.replace('///','/')
        src_loc_pfn = src_loc_pfn.replace('//xrootd/','/xrootd/')

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", ftype="input")
            else:
                rootFile = self.isRootFile(src_loc_pfn, setup=_setup_str)
                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", ftype="input")
                elif rootFile:
                    tolog("Found root file: %s (will not be transferred in direct reading mode)" % (src_loc_pfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.prepareReport('IS_ROOT', report)
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", ftype="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", ftype="input")
                    return error.ERR_DIRECTIOFILE, pilotErrorDiag
                else:
                    tolog("Normal file transfer")
        else:
            tolog("No direct access mode")

        ec = 0
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(src_loc_pfn, csumtype=csumtype)
        if ec != 0:
            self.prepareReport('GET_LOCAL_FILE_INFO_FAIL', report)
            return ec, pilotErrorDiag

        dest_file = os.path.join(path, src_loc_filename)

        report['relativeStart'] = time()
        # determine which copy command to use
        cpt = self.getCopytool(_setup_str)

        report['transferStart'] = time()
        cmd = "%s %s %s %s" % (_setup_str, cpt, src_loc_pfn, dest_file)
#PN
#        if ".lib." in src_loc_pfn:
#            cmd = "%s %s %s %s" % (_setup_str, cpt, src_loc_pfn, dest_file)
#        else:
#            cmd = "%s %sXXX %s %s" % (_setup_str, cpt, src_loc_pfn, dest_file)
        tolog("Executing command: %s" % (cmd))

        # execute
        timeout = 3600
        try:
            rc, telapsed, cout, cerr = timed_command(cmd, timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            rc = 1
            rs = str(e)
            telapsed = timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            rs = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))

        if rc != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            pilotErrorDiag = "Error copying the file: %d, %s" % (rc, rs)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            # did the copy command time out?
            if is_timeout(rc):
                pilotErrorDiag = "xcp get was timed out after %d seconds" % (telapsed)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.prepareReport('GET_TIMEOUT', report)
                return error.ERR_GETTIMEOUT, pilotErrorDiag

            self.prepareReport('CMD_FAIL', report)
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        report['validateStart'] = time()
        # get remote file size and checksum
        ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
        tolog("File info: %d, %s, %s" % (ec, dstfsize, dstfchecksum))
        if ec != 0:
            self.prepareReport('LOCAL_FILE_INFO_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return ec, pilotErrorDiag

        # compare remote and local file checksum
        if dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            if csumtype == "adler32":
                self.prepareReport('AD_MISMATCH', report)
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                self.prepareReport('MD5_MISMATCH', report)
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        # compare remote and local file size
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('FS_MISMATCH', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", ftype="input")
        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ Moves the file from the current local directory to a storage element
        source: full path of the file in  local directory
        destination: destination SE, method://[hostname[:port]]/full-dir-path/ (NB: no file name)
        Assumes that the SE is locally mounted and its local path is the same as the remote path
        if both fsize and fchecksum (for the source) are given and !=0 these are assumed without reevaluating them
        returns: exitcode, gpfn,fsize, fchecksum
        """

        error = PilotErrors()

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        dsname = pdict.get('dsname', '')
        analyJob = pdict.get('analyJob', False)
        extradirs = pdict.get('extradirs', '')
        experiment = pdict.get('experiment', '')
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        if prodSourceLabel == 'ddm' and analyJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analyJob = False

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'xrootd', lfn, guid)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        ec, pilotErrorDiag = verifySetupCommand(error, _setup_str)
        if ec != 0:
            self.prepareReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        report['relativeStart'] = time()

        ec = 0
        if fsize == 0 or fchecksum == 0:
            if not self.useExternalAdler32():
                # Can not use external adler32 command for remote file since the command is
                # not available (defaulting to md5sum for put operation)
                tolog("Command not found: adler32.sh (will switch to md5sum for local file checksum)")
                csumtype = "default"
            else:
                csumtype = "adler32"
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype=csumtype)
        if ec != 0:
            self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        tolog("File destination: %s" % (destination))
        dst_se = destination
        # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnfs/usatlas.bnl.gov/
        if( dst_se.find('SFN') != -1 ):
            s = dst_se.split('SFN=')
            dst_loc_se = s[1]
            dst_prefix = s[0] + 'SFN='
        else:
            _sentries = dst_se.split('/', 3)
            # 'method://host:port' is it always a ftp server? can it be srm? something else?
            dst_serv = _sentries[0] + '//' + _sentries[2]
            # dst_host = _sentries[2] # host and port
            dst_loc_se = '/'+ _sentries[3]
            dst_prefix = dst_serv

        # use bare destination when it starts with root://
        if destination.startswith('root://'):
            dst_loc_se = destination
            dst_prefix = ''

#        report['dataset'] = dsname

        # May be be a comma list but take first always
        # (Remember that se can be a list where the first is used for output but any can be used for input)
        se = readpar('se').split(",")[0]
        _dummytoken, se = self.extractSE(se)
        tolog("Using SE: %s" % (se))

        filename = os.path.basename(source)

        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analyJob, token, prodSourceLabel, dsname, filename, scope=scope, sitemover=self) # quick workaround
        if ec != 0:
            self.prepareReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        # are we transfering to a space token?
        if token != None and token != "":
            # Special case for GROUPDISK (do not remove dst: bit before this stage, needed in several places)
            if "dst:" in token:
                token = token[len('dst:'):]
                tolog("Dropped dst: part of space token descriptor; token=%s" % (token))
                token = "ATLASGROUPDISK"
                tolog("Space token descriptor reset to: %s" % (token))

            # get the proper destination
            #destination = self.getDestination(analyJob, token)

            #if destination == '':
            #    pilotErrorDiag = "put_data destination path in SE not defined"
            #    tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            #    self.prepareReport('SE_DEST_PATH_UNDEF', report)
            #    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

            #tolog("Going to store job output at destination: %s" % (destination))
            # add the space token to the destination string
            #dst_loc_sedir = os.path.join(destination, os.path.join(extradirs, dsname))
            #dst_loc_pfn = os.path.join(dst_loc_sedir, filename)
            #dst_loc_pfn += "?oss.cgroup=%s" % (token)
            dst_loc_pfn = dst_gpfn + "?oss.cgroup=%s" % (token)
        #else:
            #dst_loc_sedir = os.path.join(dst_loc_se, os.path.join(extradirs, dsname))
            #dst_loc_pfn = os.path.join(dst_loc_sedir, filename)
            dst_loc_pfn = dst_gpfn

        dst_gpfn = dst_prefix + dst_loc_pfn
        tolog("Final destination path: %s" % (dst_loc_pfn))
        tolog("dst_gpfn: %s" % (dst_gpfn))

        # get the Rucio site name from ToA
        try:
            _RSE = self.getRSE(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get RSE: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_RSE, _RSE)
            tolog("RSE: %s" % (_RSE))

        # determine which copy command to use
        cpt = self.getCopytool(_setup_str)

        cmd = "%s %s %s %s" % (_setup_str, cpt, source, dst_loc_pfn)
#        cmd = "%sXXX %s %s %s" % (_setup_str, cpt, source, dst_loc_pfn)
#PN
#        if ".log." in dst_loc_pfn:
#            cmd = "%s %s %s %s" % (_setup_str, cpt, source, dst_loc_pfn)
#        else:
#            cmd = "%sXXX %s %s %s" % (_setup_str, cpt, source, dst_loc_pfn)
        tolog("Executing command: %s" % (cmd))
        report['transferStart'] = time()

        # execute
        timeout = 3600
        try:
            rc, telapsed, cout, cerr = timed_command(cmd, timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            rc = 1
            rs = str(e)
            telapsed = timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            rs = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))

        # ready with the space token descriptor, remove it from the path if present
        if "?oss.cgroup=" in dst_loc_pfn:
            dst_loc_pfn = dst_loc_pfn[:dst_loc_pfn.find("?oss.cgroup=")]
            dst_gpfn = dst_gpfn[:dst_gpfn.find("?oss.cgroup=")]
            tolog("Removed space token part from dst_loc_pfn (not needed anymore): %s" % (dst_loc_pfn))

        if rc != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            pilotErrorDiag = "Error copying the file: %d, %s" % (rc, rs)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))

            # did the copy command time out?
            if is_timeout(rc):
                pilotErrorDiag = "xcp get was timed out after %d seconds" % (telapsed)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.prepareReport('PUT_TIMEOUT', report)
                return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag, surl=dst_gpfn)

            self.prepareReport('COPY_ERROR', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=dst_gpfn)

        report['validateStart'] = time()
        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"
        if csumtype == "adler32" and not self.useExternalAdler32():
            # Can not use external adler32 command for remote file since the command is
            # not available (defaulting to md5sum for put operation)
            tolog("Command not found: adler32.sh (will switch to md5sum for remote file checksum)")
            csumtype = "default"

        # get remote file size and checksum
        ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dst_loc_pfn, csumtype=csumtype)
        tolog("File info: %d, %s, %s" % (ec, dstfsize, dstfchecksum))
        if ec != 0:
            self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag, surl=dst_gpfn)

        # compare remote and local file checksum
        if dstfchecksum != fchecksum:
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(dst_gpfn), dstfchecksum, fchecksum)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            if csumtype == "adler32":
                self.prepareReport('AD_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=dst_gpfn)
            else:
                self.prepareReport('MD5_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=dst_gpfn)

        # compare remote and local file size
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(dst_gpfn), str(dstfsize), str(fsize))
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.prepareReport('FS_MISMATCH', report)
            return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=dst_gpfn)

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, fsize, fchecksum, ARCH_DEFAULT

    def check_space(self, ub):
        """
        Checking space availability:
        1. check DQ space URL
        2. get storage path and check local space availability
        """

        if ub == "" or ub == "None" or ub == None:
            tolog("Using alternative check space function since URL method can not be applied (URL not set)")
            retn = self._check_space(ub)
        else:
            try:
                f = urllib.urlopen(ub + '/space/free')
                ret = f.read()
                retn = int(ret)
                if retn == 0:
                    tolog(ub + '/space/free returned 0 space available, returning 999995')
                    retn = 999995
            except:
                tolog("Using alternative check space function since URL method failed")
                retn = self._check_space(ub)
        return retn

    def _check_space(self, ub):
        """Checking space of a local directory"""

        # "source setup.sh"
        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        fail = 0
        ret = ''
        if ub == "" or ub == "None" or ub == None:
            # seprodpath can have a complex structure in case of space tokens
            # although currently not supported in this site mover, prepare the code anyway
            # (use the first list item only)
            dst_loc_se = self.getDirList(readpar('seprodpath'))[0]
            if dst_loc_se == "":
                dst_loc_se = readpar('sepath')
            if dst_loc_se == "":
                tolog("WARNING: Can not perform alternative space check since sepath is not set")
                return -1
            else:
                tolog("Attempting to use df for checking SE space: %s" % (dst_loc_se))
                return self.check_space_df(dst_loc_se)
        else:
            try:
                f = urllib.urlopen(ub + '/storages/default')
            except Exception, e:
                tolog('!!WARNING!!2999!! Fetching default storage failed!')
                return -1
            else:
                ret = f.read()

        if ret.find('//') == -1:
            tolog('!!WARNING!!2999!! Fetching default storage failed!')
            fail = -1
        else:
            dst_se = ret.strip()
            # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnOAfs/usatlas.bnl.gov/
            if (dst_se.find('SFN') != -1):
                s = dst_se.split('SFN=')
                dst_loc_se = s[1]
                #dst_prefix = s[0]
            else:
                _sentries = dst_se.split('/', 3)
                # 'method://host:port' is it always a ftp server? can it be srm? something else?
                dst_loc_se = '/'+ _sentries[3]

            # Run df to check space availability
            s, o = commands.getstatusoutput('%s df %s' % (_setup_str, dst_loc_se))
            if s != 0:
                check_syserr(s, o)
                tolog("!!WARNING!!2999!! Error in running df: %s" % o)
                fail = -1
            else:
                # parse Wei's df script (extract the space info)
                df_split = o.split("\n")[1]
                p = r"XROOTD[ ]+\d+[ ]+\d+[ ]+(\S+)[ ]+"

                pattern = compile(p)
                available = findall(pattern, df_split)
                try:
                    available_space = available[0]
                except:
                    available_space = 999999999

        if fail != 0:
            return fail
        else:
            return available_space

    def getLocalFileInfo(self, fname, csumtype="default"):
        """ returns exit code (0 if OK), file size and checksum """

        error = PilotErrors()
        pilotErrorDiag = ""

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''
        tolog("getLocalFileInfo using setup: %s" % (_setup_str))

        # get the file size
        fsize = str(self.getRemoteFileSize(fname))
        if fsize == "0":
            pilotErrorDiag = "Encountered zero file size for file %s" % (fname)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_ZEROFILESIZE, pilotErrorDiag, 0, 0
#            pilotErrorDiag = "Could not get file size for file: %s" % (fname)
#            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
#            return error.ERR_FAILEDSIZELOCAL, pilotErrorDiag, 0, 0

        # get the checksum
        if csumtype == "adler32":
            if not self.useExternalAdler32():
                tolog("External adler32.sh command not found, using built-in function")
                fchecksum = self.adler32(fname)
            else:
                _CMD_CHECKSUM = "adler32.sh"
                cmd = '%s %s %s' % (_setup_str, _CMD_CHECKSUM, fname)
                tolog("Executing command: %s" % (cmd))
                s, o = commands.getstatusoutput(cmd)
                if s != 0:
                    o = o.replace('\n', ' ')
                    check_syserr(s, o)
                    pilotErrorDiag = "Error running checksum command (%s): %s" % (_CMD_CHECKSUM, o)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    # try to continue
                # confirm output
                _fchecksum_prel, pilotErrorDiag = self.parseAdler32(o, fname)
                if _fchecksum_prel == "":
                    return error.ERR_FAILEDADLOCAL, pilotErrorDiag, fsize, 0

                fchecksum = _fchecksum_prel.split()[0]

            if fchecksum == '00000001': # "%08x" % 1L
                pilotErrorDiag = "Adler32 failed (returned %s)" % (fchecksum)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDADLOCAL, pilotErrorDiag, fsize, 0

            tolog("Using checksum: %s" % (fchecksum))
        else:
            cmd = '%s which %s' % (_setup_str, CMD_CHECKSUM)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            tolog("cmd output: %s" % o)
            cmd = '%s %s %s' % (_setup_str, CMD_CHECKSUM, fname)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                o = o.replace('\n', ' ')
                check_syserr(s, o)
                pilotErrorDiag = "Error running checksum command (%s): %s" % (CMD_CHECKSUM, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDMD5LOCAL, pilotErrorDiag, fsize, 0
            fchecksum = o.split()[0]

        return 0, pilotErrorDiag, fsize, fchecksum

    def useExternalAdler32(self):
        """ check if the local adler32 command is available, if not md5sum will be used """

        status = True
        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        cmd = "%s which adler32.sh" % (_setup_str)
        tolog("Executing command: %s" % (cmd))
        s, o = commands.getstatusoutput(cmd)
        if s != 0:
            tolog("!!WARNING!!2999!! s=%d, o=%s" % (s, o))
            # Command not found: adler32.sh (will default to use md5sum for checksums
            status = False

        return status

    def parseAdler32(self, output, fname):
        """ parse the adler32.sh output in case there was an AFS hickup """
        # error in the output has the form:
        # ERROR: some message. <checksum> <file name>
        # This function should return "<checksum> <file name>"
        # In case of problems, the function will return an empty string and the error diag

        _output = ""
        pilotErrorDiag = ""
        tolog("Parsing adler32 output: %s" % (output))
        try:
            _output_prel = output.split(" ")
        except Exception, e:
            pilotErrorDiag = "Exception caught in parseAdler32: %s, %s" % (output, str(e))
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        else:
            if len(_output_prel) >= 2:
                _adler32 = _output_prel[-2]
                _filename = _output_prel[-1]

                # make sure that _adler32 and _filename make sense
                if len(_output_prel) > 2:
                    tolog("!!WARNING!!2999!! parseAdler32 found garbled output: %s" % (output))

                # try to interpret output
                if len(_adler32) != 8 or not _adler32.isalnum():
                    pilotErrorDiag = "parseAdler32: Wrong format of interpreted adler32: %s" % (_adler32)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                elif _filename != fname:
                    pilotErrorDiag = "parseAdler32: File names do not match: %s ne %s" % (_filename, fname)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                else:
                    # put back confirmed values in _output
                    _output = _adler32 + " " + _filename
                    tolog('Interpreted output ok: \"%s\"' % (_output))
            else:
                pilotErrorDiag = "parseAdler32 could not interpret output: %s" % (output)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

        return _output, pilotErrorDiag

    def getRemoteFileSize(self, fname):
        """ return the file size of the remote file """

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        size = 0
        cmd = "%s stat %s" % (_setup_str, fname)
        tolog("Executing command: %s" % (cmd))
        stat = commands.getoutput(cmd)

        # get the second line in the stat output which contains the size
        try:
            stat_split = stat.split("\n")[1]
        except Exception, e:
            tolog("!!WARNING!!2999!! Failed to execute commands:")
            tolog(".stat: %s" % (stat))
            tolog(".stat_split: %s" % (str(e)))
            size = 0
        else:
            # reg ex search pattern
            pattern = compile(r"Size:[ ]+(\d+)")

            # try to find the size in the stat output
            fsize = findall(pattern, stat_split)
            try:
                size = fsize[0]
            except:
                tolog("!!WARNING!!2999!! stat command did not return file size")
                size = 0

        return size

    def getMover(cls, *args, **kwrds):
        """
        Creates and provides exactly one instance for each required subclass of SiteMover.
        Implements the Singleton pattern
        """

        cl_name = cls.__name__
        if not issubclass(cls, SiteMover):
            log.error("Wrong Factory invocation, %s is not subclass of SiteMover" % cl_name)
        else:
            return cls(*args, **kwrds)
    getMover = classmethod(getMover)
