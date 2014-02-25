import os
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog
from time import time

class COGSiteMover(SiteMover.SiteMover):
    """
    SiteMover for SEs remotely accessible using GridFTP from the WNs
    This uses a custom modified g-u-c part of the COG library
    /local/inst/cog-jglobus-061025mike/bin
    ./globus-url-copy -help
    export COG_INSTALL_PATH=`pwd`
    """

    copyCommand = "cog"
    checksum_command = "md5sum"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
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
        for root, dirs, files in os.walk(top, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
    _deldirtree = staticmethod(_deldirtree)
    
    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path
    
    def _check_space(self, ub):
        """For when space availability is not verifiable"""
        return 999999
    
    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        copy input file from local SE to local disk
        no MD5 or size comparison are not possible - require g-u-c modification
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'COG'
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

        # do we have a valid proxy?
        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str)
        if s != 0:
            self.__sendReport('PROXY_FAIL', report)
            return s, pilotErrorDiag

        loc_filename = lfn

        # srmcp does not create directories; local dir should exist
        _cmd_str = '%s$COG_INSTALL_PATH/bin/globus-url-copy -checksum %s file://%s/%s' %\
                   (_setup_str, gpfn, path, loc_filename)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        if s != 0:
            check_syserr(s, o)
            o = o.replace('\n', ' ')
            pilotErrorDiag = "globus-url-copy failed: %d, %s" % (s, o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('COPY_FAIL', report)
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if fsize != 0 or fchecksum != 0:
            dest_file = os.path.join(path, loc_filename)

            # get remote file size and checksum
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return ec, pilotErrorDiag

            # compare remote and local file size
            if fsize != 0 and dstfsize != fsize:
                pilotErrorDiag = "File sizes do not match for %s (%s != %s)" % (gpfn, str(fsize), str(dstfsize))
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('FS_MISMATCH', report)
                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum != 0 and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Different checksums (of type %s) for %s (%s != %s)" % (csumtype, gpfn, dstfchecksum, fchecksum)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                if csumtype == "adler32":
                    self.__sendReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag
                
        self.__sendReport('MD5_MISMATCH', report)
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
            report['protocol'] = 'COG'
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

        # do we have a valid proxy?
        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str, limit=2)
        if s != 0:
            self.__sendReport('PROXY_FAIL', report)
            return self.put_data_retfail(s, pilotErrorDiag)

        filename = pfn.split('/')[-1]
        dst_gpfn = os.path.join(destination, os.path.join(dsname, filename))
        fppfn = os.path.abspath(pfn)

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        # get local file size and checksum if not known already
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(fppfn)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        _cmd_str = '%s$COG_INSTALL_PATH/bin/globus-url-copy -checksum file://%s %s/' % (_setup_str, fppfn, dst_gpfn)
        tolog("SRM Copy (%s->%s) cmd: %s" % (fppfn, dst_gpfn, _cmd_str))
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        if s != 0:
            check_syserr(s, o)
            o = o.replace('\n', ' ')
            pilotErrorDiag = "Error copying the file: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('COPY_FAIL', report)
            return self.put_data_retfail(s, pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, fsize, fchecksum, self.arch_type


    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)
