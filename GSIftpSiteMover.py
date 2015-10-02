import os
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog
from time import time

class GSIftpSiteMover(SiteMover.SiteMover):
    """
    SiteMover for SEs remotely accessible using globus-url-copy from the WNs
    """

    copyCommand = "gsiftp"
    checksum_command = "md5sum"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = False
    has_chmod = False
    timeout = 5*3600

    def get_timeout(self):
        return self.timeout

    # Delete everything reachable from the directory named in 'top',
    # assuming there are no symbolic links.
    # CAUTION:  This is dangerous!  For example, if top == '/', it
    # could delete all your disk files.
    def _deldirtree(top):
        "removes a whole tree of directories"
        if top != "/":
            os.system("rm -rf %s" % (top))
    _deldirtree = staticmethod(_deldirtree)
    
    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path

    def _check_space(self, ub):
        """For when space availability is not verifiable"""
        return 999999

    def convertURL_SRMtoGSIftp(srmstr):
        """Converts SRM URLs to gsiftp URLs
        srm://uct2-dc1.uchicago.edu/pnfs/uchicago.edu/data/ddm1/
        srm://uct2-dc1.uchicago.edu:8443//pnfs/uchicago.edu/data/ddm1/
        srm://uct2-dc1.uchicago.edu:8443/srm/managerv2?SFN=/pnfs/uchicago.edu/data/ddm1/
        """
        utok = srmstr.split('/', 3)
        if len(utok) < 4:
            tolog("!!WARNING!!2999!! Wrong srm URL, impossible to extract GSIftp URL, copy will most likely fail")
            return srmstr
        host_name = utok[2]
        try:
            idx = host_name.index(':')
            host_name = host_name[:idx]
        except ValueError:
            pass
        path_name = utok[3]
        try:
            idx = path_name.index("?SFN=")
            path_name = path_name[idx+5:]
        except ValueError:
            pass
        retv = "gsiftp://%s/%s" % (host_name, path_name)
        return retv
    convertURL_SRMtoGSIftp=staticmethod(convertURL_SRMtoGSIftp)

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ stage-in function """

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'GSIftp'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str)
        if s != 0:
            self.prepareReport('DONE', report)
            return s, pilotErrorDiag

        tolog("gpfn: %s" % (gpfn))
        if gpfn.startswith('srm://'):
            tolog("Converting URL SRM to GSIftp")
            _ftp_gpfn = GSIftpSiteMover.convertURL_SRMtoGSIftp(gpfn)
            tolog("_ftp_gpfn: %s" % (_ftp_gpfn))
        else:
            _ftp_gpfn = gpfn
        _cmd_str = '%sglobus-url-copy %s file://%s/%s' % (_setup_str, _ftp_gpfn, os.path.abspath(path), lfn)
        # -cd will create one level of directories; local dir should exist
        tolog('Executing command: %s' % _cmd_str)
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            pilotErrorDiag = "GUC failed: %d, %s" % (s, o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            check_syserr(s, o)
            ec = error.ERR_STAGEINFAILED
            if o.find("No such file or directory") >= 0:
                if _ftp_gpfn.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (_ftp_gpfn)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    ec = error.ERR_MISSDBREL
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (_ftp_gpfn)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    ec = error.ERR_NOSUCHFILE
            self.prepareReport('NO_FILE', report)
            return ec, pilotErrorDiag

        if fsize != 0 or fchecksum != 0:
            loc_filename = lfn
            dest_file = os.path.join(path, loc_filename)

            if fchecksum != 0 and fchecksum != "":
                csumtype = self.getChecksumType(fchecksum)
            else:
                csumtype = "default"

            # get remote file size and checksum 
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
            if ec != 0:
                self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
                return ec, pilotErrorDiag

            # compare remote and local file size
            if fsize != 0 and dstfsize != fsize:
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(dest_file), str(dstfsize), str(fsize))
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('FS_MISMATCH', report)
                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum != 0 and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                if csumtype == "adler32":
                    self.prepareReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.prepareReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, pfn, destination, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):
        """ copy output file from disk to local SE """

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the lfn
        try:
            lfn = pdict['lfn']
        except:
            lfn = ""

        # get the guid
        try:
            guid = pdict['guid']
        except:
            guid = ""

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'GSIftp'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str, limit=2)
        if s != 0:
            self.prepareReport('PROXY_FAIL', report)
            return self.put_data_retfail(s, pilotErrorDiag)

        # Make the dir structure locally and do a recursive copy
        # in future versions -bc will create full path
        if os.environ.has_key('USER'):
            user = os.environ["USER"]
        else:
            user = "dummy"
        _tmpdir = "/tmp/%s/%s" % (user, commands.getoutput('uuidgen'))
        try:
            os.makedirs(_tmpdir)
        except Exception, e:
            pilotErrorDiag = "Could not create dirs: %s" % (_tmpdir)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.prepareReport('MKDIR_FAIL', report)
            return self.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag) 

        _tmpfulldir = os.path.join(_tmpdir, os.path.join(extradirs, dsname))
        os.makedirs(_tmpfulldir)
        os.chmod(_tmpfulldir, self.permissions_DIR)
        filename = pfn.split('/')[-1]
        _tmpfname = os.path.join(_tmpfulldir, filename)
        dst_gpfn = os.path.join(destination, os.path.join(dsname, filename))
        fppfn = os.path.abspath(pfn)
        os.symlink(fppfn, _tmpfname)

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        # check local file size and checksum
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(_tmpfname)
            if ec != 0:
                self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # protect from SRM URL
        if destination.startswith('srm://'):
            tolog("Converting URL SRM to GSIftp")
            _ftp_destination = GSIftpSiteMover.convertURL_SRMtoGSIftp(destination)
        else:
            _ftp_destination = destination
        _cmd_str = '%sglobus-url-copy -r file://%s/ %s/' % (_setup_str, _tmpdir, _ftp_destination)
        tolog("Copy [Tmptree: %s (%s->%s)] cmd: %s" % (_tmpdir, fppfn, _tmpfname, _cmd_str))
        
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        
        GSIftpSiteMover._deldirtree(_tmpdir)
        if s != 0:
            check_syserr(s, o)
            if o.find('File exists') >= 0:
                tolog('!!WARNING!!2999!! File %s exists already in %s (ec: %s), trying to continue' % (filename, _ftp_destination, str(s)))
            else:
                pilotErrorDiag = "Error copying the file: %d, %s " % (s, o)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.prepareReport('COPY_FAIL', report)
                return self.put_data_retfail(s, pilotErrorDiag, surl=_ftp_destination)

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, fsize, fchecksum, self.arch_type
