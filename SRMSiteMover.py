import os
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar
from re import findall, compile, search
from time import time

class SRMSiteMover(SiteMover.SiteMover):
    """ SiteMover for SEs remotely accessible using SRM from the WNs """

    copyCommand = "srmcp"
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
        for root, dirs, files in os.walk(top, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
    _deldirtree = staticmethod(_deldirtree)
    
    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path
    
    def check_space(self, ub):
        """For when space availability is not verifiable"""
        return 999999

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ get function """

        tolog("SRMSiteMover: get_data()")

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'SRM'
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

        _setup_str = _setup_str.replace("//", "/")
        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str)
        if s != 0:
            self.__sendReport('PROXY_FAIL', report)
            return s, pilotErrorDiag

        se = readpar('se').split(",")[0]
        token, se = self.extractSE(se)

        # add the port number to gpfn if not present
        # se = "srm://dcsrm.usatlas.bnl.gov:8443"
        # gpfn = "srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/..."
        # => gpfn = "srm://dcsrm.usatlas.bnl.gov:8443/pnfs/usatlas.bnl.gov/..."
        gpfn = self.addPortToPath(se, gpfn)

#        if token:
#            _cmd_str = '%ssrmcp -space_token=%s %s file:///%s/%s' % (_setup_str, token, gpfn, path, lfn)
#        else:
#            _cmd_str = '%ssrmcp %s file:///%s/%s' % (_setup_str, gpfn, path, lfn)
        _cmd_str = '%ssrmcp %s file:///%s/%s' % (_setup_str, gpfn, path, lfn)

        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            pilotErrorDiag = "srmcp failed: %d, %s" % (s, o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            check_syserr(s, o)
            ec = error.ERR_STAGEINFAILED
            if o.find("No such file or directory") >= 0:
                if gpfn.find("DBRelease") >= 0:
                    pilotErrorDiag = "DBRelease file missing: %s" % (gpfn)
                    ec = error.ERR_MISSDBREL
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (gpfn)
                    ec = error.ERR_NOSUCHFILE
            self.__sendReport('NO_FILE', report)
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
            tolog("File info: %d, %s, %s" % (ec, dstfsize, dstfchecksum))
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return ec, pilotErrorDiag

            # get remote file size and checksum 
            if fsize != 0 and dstfsize != fsize:
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(dest_file), str(dstfsize), str(fsize))
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('FS_MISMATCH', report)
                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum != 0 and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(dst_gpfn), dstfchecksum, fchecksum)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                if csumtype == "adler32":
                    self.__sendReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, pfn, destination, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):

        tolog("SRMSiteMover: put_data()")

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        logFile = pdict.get('logFile', '')

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'SRM'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')
            # dsname
#            report['dataset'] = dsname

        # create the setup command
        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        # corrected double slash
        _setup_str = _setup_str.replace("//", "/")
        s, pilotErrorDiag = self.verifyProxy(envsetup=_setup_str, limit=2)
        if s != 0:
            self.__sendReport('PROXY_FAIL', report)
            return self.put_data_retfail(s, pilotErrorDiag)

        filename = pfn.split('/')[-1]

        tolog("filename: %s" % (filename))
        tolog("destination: %s" % (destination))
        m = search('^user', filename)
        # special case for BNL
        if destination.find("bnl.gov") > 0:
            tolog("(Special case for BNL; using genSubpath() to determine destination path)")
            dst_gpfn = os.path.join(destination, SRMSiteMover.genSubpath(dsname, filename, logFile))
            tolog("dst_gpfn (1): %s" % (dst_gpfn))
            # user files have no dsname automatically added to dir name
            if m == None:
                dst_gpfn = os.path.join(dst_gpfn, dsname)
                tolog("dst_gpfn (2): %s" % (dst_gpfn))

            cmd = 'mkdir -p %s' % (dst_gpfn)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                check_syserr(s, o)
                pilotErrorDiag = "Error in mkdir: %s" % (o)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.__sendReport('MKDIR_FAIL', report)
                return self.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag)

            dst_gpfn = os.path.join(dst_gpfn, filename)
            tolog("dst_gpfn (3): %s" % (dst_gpfn))
        else:
            # user files have no dsname automatically added to dir name
            if m:
                dsname = ''
            try:
                extradirs = pdict['extradirs']
            except:
                extradirs = ''
                                                
            dst_gpfn = os.path.join(destination, os.path.join(extradirs, dsname))
            tolog("dst_gpfn (1): %s" % (dst_gpfn))

            # create the destination dir
            try:
                self.mkdirWperm(dst_gpfn)
            except EnvironmentError, e:  # includes OSError (permission) and IOError (write error)
                pilotErrorDiag = "put_data mkdirWperm failed: %s" % str(e)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('MKDIR_FAIL', report)
                return self.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag)

            dst_gpfn = os.path.join(dst_gpfn, filename)
            tolog("dst_gpfn (2): %s" % (dst_gpfn))

        tolog("dst_gpfn: %s" % (dst_gpfn))

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        fppfn = os.path.abspath(pfn)

        tolog("Getting local file size")
        try:
            fsize = str(os.path.getsize(fppfn))
        except OSError, e:
            pilotErrorDiag = "Could not get file size: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('NO_FILESIZE', report)
            return self.put_data_retfail(error.ERR_FAILEDSIZELOCAL, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get and process se
        se = readpar('se').split(",")[0]
        _dummytoken, se = self.extractSE(se)

        # grab the space token
        try:
            token = pdict['token']
        except:
            token = None
        
        # add the port number to gpfn if not present
        # se = "srm://dcsrm.usatlas.bnl.gov:8443"
        # gpfn = "srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/..."
        # => gpfn = "srm://dcsrm.usatlas.bnl.gov:8443/pnfs/usatlas.bnl.gov/..."
        dst_gpfn = self.addPortToPath(se, dst_gpfn)

        if token:
            # convert the space token description to its number form
            stripped_se = self.stripPath(se)
            tolog("Using surl: %s" % (stripped_se))
            if stripped_se == "":
                stripped_se = se
            _token = self.getSpaceTokenFromDescription(_setup_str, token, stripped_se)

            if _token:
                # transfer the file to the relevant space token
                _cmd_str = '%ssrmcp -srm_protocol_version=2 -space_token=%s file:///%s %s' % (_setup_str, _token, fppfn, dst_gpfn)
            else:
                tolog("!!WARNING!!2999!! Will not use space token since description could not be converted")
                # transfer the file to the relevant destination
                _cmd_str = '%ssrmcp file:///%s %s' % (_setup_str, fppfn, dst_gpfn)
        else:
            # transfer the file to the relevant destination
            _cmd_str = '%ssrmcp file:///%s %s' % (_setup_str, fppfn, dst_gpfn)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            pilotErrorDiag = "Error copying the file: %d, %s" % (s, o)
            self.__sendReport('COPY_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, 0, 0, self.arch_type

    def getSpaceTokenFromDescription(self, setup, space_token_desc, surl):
        """ convert space token description to space token number """

        space_token = None
        cmd = "%swhich srm-get-space-tokens" % (setup)
        tolog("Executing command: %s" % (cmd))
        ec, rs = commands.getstatusoutput(cmd)
        tolog("ec = %d" % (ec))
        tolog("rs = %s" % (rs))
        cmd = "%ssrm-get-space-tokens -space_desc=%s %s" % (setup, space_token_desc, surl)
        tolog("Executing command: %s" % (cmd))
        ec, rs = commands.getstatusoutput(cmd)
        tolog("ec = %d" % (ec))
        tolog("rs = %s" % (rs))
        if ec != 0:
            tolog("!!WARNING!!2999!! Failed to execute command: %d, %s" % (ec, rs))
        else:
            # parse the command output which should be on the form:
            # Space Reservation Tokens:    750769
            pattern = compile(r'Space Reservation Tokens:+ +(\S+)')
            rs = rs.replace('\n',' ')
            st = findall(pattern, rs)
            tolog("rs: %s" % (rs))
            tolog("st: %s" % (st))
            if st != [] and st != ['']:
                space_token = st[0]
                tolog("Converted space token description %s to %s" % (space_token_desc, space_token))
            else:
                if rs == "":
                    tolog("Space token pattern not found (empty string returned from command)")
                else:
                    tolog("Space token pattern not found in: \'%s\'" % (rs))

        return space_token


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
