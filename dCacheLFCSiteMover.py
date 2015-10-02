import os
import commands
import re

import dCacheSiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand
from time import time

class dCacheLFCSiteMover(dCacheSiteMover.dCacheSiteMover):
    """
    SiteMover for dCache/dccp with LFC based replica catalog
    """
    copyCommand = "dccplfc"
    checksum_command = "adler32"
    has_mkdir = True
    has_df = False
    has_getsize = True
    has_md5sum = False
    has_chmod = True
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path
    
    def get_timeout(self):
        return self.timeout

    def _check_space(self, ub):
        """dCache specific space verification.
        There is no way at the moment to verify full dCache space availability"""
        return 999999
        
    def addMD5sum(self, lfn, md5sum):
        """ add md5sum to lfn """
        if os.environ.has_key('LD_LIBRARY_PATH'):
            tolog("LD_LIBRARY_PATH prior to lfc import: %s" % os.environ['LD_LIBRARY_PATH'])
        else:
            tolog("!!WARNING!!2999!! LD_LIBRARY_PATH not set prior to lfc import")
        try:
            import lfc
        except Exception, e:
            tolog("!!WARNING!!2999!! addMD5Sum() could not import lfc module: %s" % str(e))
            return -1

        os.environ['LFC_HOST'] = readpar('lfchost')
        #    b="."
        #    buffer = b.zfill(200)
        #    ret = lfc.lfc_seterrbuf(buffer, len(buffer))
        stat = lfc.lfc_filestatg()
        exitcode = lfc.lfc_statg(lfn, "", stat)
        if exitcode != 0:
            #    print "error:",buffer
            err_num = lfc.cvar.serrno
            tolog("!!WARNING!!2999!! lfc.lfc_statg: %d %s" % (err_num, lfn))
            return exitcode
        exitcode = lfc.lfc_setfsizeg(stat.guid, stat.filesize, 'MD', md5sum)
        if exitcode != 0:
            #    print "error:",buffer
            err_num = lfc.cvar.serrno
            tolog("[Non-fatal] ERROR: lfc.lfc_setfsizeg: %d %s %s" % (err_num, lfn, md5sum))
            return exitcode
        tolog("Successfully set md5sum for %s" % (lfn))
        return exitcode

    def put_data(self, source, ddm_storage, fsize=0, fchecksum=0, dsname='', **pdict):
        """Data transfer using dCache - generic version"""

        error = PilotErrors()
        pilotErrorDiag = ""

        tolog("put_data() got ddm_storage=%s" % (ddm_storage))

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        dsname = pdict.get('dsname', '')
        extradirs = pdict.get('extradirs', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'dCacheLFC', lfn, guid)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        # At destination append a subdirectory which is first two fields of dsname, or 'other'
        destination = readpar('sepath')
        if destination == '':
            pilotErrorDiag = "put_data destination path in SE not defined"
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('DEST_PATH_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        if dsname == '':
            pilotErrorDiag = "Dataset name not specified to put_data"
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('DSN_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
#        else:
#            dsname = self.remove_sub(dsname)
#            tolog("dsname: %s" % (dsname))

#        report['dataset'] = dsname

        pat = re.compile('([^\.]+\.[^\.]+)\..*')
        mat = pat.match(dsname)
        if mat:
            prefixdir = mat.group(1)
            destination = os.path.join(destination,prefixdir)
        else:
            pilotErrorDiag = "put_data encountered unexpected dataset name format: %s" % (dsname)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('DSN_FORMAT_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # preparing variables
        src_pfn = source
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(src_pfn, csumtype="adler32")
        if ec != 0:
            self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
            return SiteMover.SiteMover.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        dst_se = destination
        if( dst_se.find('SFN') != -1 ):  # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnfs/usatlas.bnl.gov/
            s = dst_se.split('SFN=')
            dst_loc_se = s[1]
            dst_prefix = s[0] + 'SFN='
        else:
            _sentries = dst_se.split('/', 3)
            dst_serv = _sentries[0] + '//' + _sentries[2] # 'method://host:port' is it always a ftp server? can it be srm? something else?
            dst_host = _sentries[2] #host and port            
            dst_loc_se = '/' + _sentries[3]
            dst_prefix = dst_serv

        filename = os.path.basename(source)

        # Behavior as in BNL: user files have no dsname automatically added to dir name
        m = re.search('^user', filename)
        if m:
             dsname = ''           

        dst_loc_sedir = os.path.join(dst_loc_se, os.path.join(extradirs, dsname))
        copyprefix = readpar('copyprefix')
        tolog('copyprefix: %s' % (copyprefix))
        if copyprefix != '':
            # Replace prefix on pfn
            pfrom, pto = copyprefix.split('^')
            tolog("Replacing %s with %s on %s" % (pfrom, pto, dst_loc_sedir))
            dst_loc_sedir = dst_loc_sedir.replace(pfrom, pto)

        dst_loc_pfn = os.path.join(dst_loc_sedir, filename)
        dst_gpfn = dst_prefix + dst_loc_pfn

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        try:
            self.mkdirWperm(dst_loc_sedir)
        except IOError, e:
            pilotErrorDiag = "put_data could not create dir: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('MKDIR_FAIL', report)
            return SiteMover.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag, surl=dst_gpfn)

        cmd = '%sdccp -A %s %s' % (_setup_str, source, dst_loc_sedir)
        tolog("Executing command: %s" % cmd)
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(cmd)
        report['validateStart'] = time()
        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            pilotErrorDiag = "Error in copying: %s" % (o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('DCCP_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=dst_gpfn)
        try:
            os.chmod(dst_loc_pfn, self.permissions_FILE)
        except IOError, e:
            pilotErrorDiag = "put_data could not change permission of the file: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('PERMISSION_FAIL', report)
            return SiteMover.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=dst_gpfn)

        # Comparing only file size since MD5sum is problematic
        try:
            nufsize = str(os.path.getsize(dst_loc_pfn))
        except OSError, e:
            pilotErrorDiag = "put_data could not get file size: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('NO_FS', report)
            return SiteMover.put_data_retfail(error.ERR_FAILEDSIZE, pilotErrorDiag, surl=dst_gpfn)
        if fsize != nufsize:
            pilotErrorDiag = "File sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(source), str(fsize), str(nufsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('FS_MISMATCH', report)
            return SiteMover.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=dst_gpfn)

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.prepareReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag, surl=dst_gpfn)

        # register the file in LFC
        report['catStart'] = time()
        os.environ['LFC_HOST'] = readpar('lfchost') # might already be set in envsetup
        lfcpath = readpar('lfcpath')
        lfclfn = '%s/%s/%s/%s' % (lfcpath, prefixdir, dsname, lfn)
        cmd = "%slcg-rf -g %s -l %s --vo atlas %s" % (envsetup, guid, lfclfn, putfile)
        tolog("registration command: %s" % (cmd))
        s, o = commands.getstatusoutput(cmd)
        if s != 0:
            o = o.replace('\n', ' ')
            pilotErrorDiag = "LFC registration failed: %s" % (o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            check_syserr(s, o)
            self.prepareReport('LFC_REG_FAIL', report)
            return SiteMover.put_data_retfail(error.ERR_FAILEDLFCREG, pilotErrorDiag, surl=dst_gpfn)
        else:
            # add checksum and file size to LFC
            csumtype = self.getChecksumType(fchecksum, format="short")
            exitcode, pilotErrorDiag = self.addFileInfo(lfclfn, fchecksum, csumtype=csumtype)
            if exitcode != 0:
                self.prepareReport('LFC_ADD_SUM_FAIL', report)
                return self.put_data_retfail(error.ERR_LFCADDCSUMFAILED, pilotErrorDiag, surl=dst_gpfn)
            else:
                tolog('Successfully set filesize and checksum for %s' % pfn)

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, fsize, '', self.arch_type
