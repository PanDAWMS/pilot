import os
import commands
import re

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand
from time import time
from FileStateClient import updateFileState
from timed_command import timed_command

class rfcpLFCSiteMover(SiteMover.SiteMover):
    """
    SiteMover for rfcp sites with LFC replica catalog (DPM + castor)
    """
    copyCommand = "rfcplfc"
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
        """DPM/CASTOR specific space verification.
        There is no way at the moment to verify DPM/CASTOR space availability - check info system instead"""
        return 999999
        
    def addMD5sum(self, lfn, md5sum):
        """ add md5sum to lfn """
        if os.environ.has_key('LD_LIBRARY_PATH'):
            tolog("LD_LIBRARY_PATH prior to lfc import: %s" % os.environ['LD_LIBRARY_PATH'])
        else:
            tolog("!!WARNING!!2999!! LD_LIBRARY_PATH not set prior to lfc import")
        import lfc
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

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        The local file is assubed to have a relative path that is the same of the relative path in the 'gpfn'
        loc_... are the variables used to access the file in the locally exported file system
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'rfcpLFC'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        tolog("gpfn is %s" % gpfn)

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = envsetup

        ec, pilotErrorDiag = verifySetupCommand(error, _setup_str)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        # remove any host and SFN info from PFN path
        loc_pfn = self.extractPathFromPFN(gpfn)

        try:
            if not loc_pfn.startswith(('/dpm', '/castor')):
                tolog("Potential problem with local filename. Does not start with '/dpm' or '/castor/'.")
        except TypeError:
            # Older version of python
            pass

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.__sendReport('FOUND_ROOT', report)
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                    return error.ERR_DIRECTIOFILE, pilotErrorDiag
                else:
                    tolog("Normal file transfer")

        dest_path = os.path.join(path, lfn)
        #PN
        _cmd_str = '%srfcp %s %s' % (_setup_str, loc_pfn, dest_path)
#        if ".lib." in loc_pfn:
#            _cmd_str = '%srfcp %s %s' % (_setup_str, loc_pfn, dest_path)
#        else:
#            _cmd_str = '%srfcpXXX %s %s' % (_setup_str, loc_pfn, dest_path)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()

        # execute
        timeout = 3600
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            pilotErrorDiag = 'timed_command() threw an exception: %s' % (e)
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            o = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))

        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            pilotErrorDiag = "rfcp failed: %d, %s"  % (s, o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            check_syserr(s, o)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            ec = error.ERR_STAGEINFAILED
            if o.find("No such file or directory") >= 0:
                if loc_pfn.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (loc_pfn)
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    ec = error.ERR_MISSDBREL
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (loc_pfn)
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    ec = error.ERR_NOSUCHFILE
                self.__sendReport('RFCP_FAIL', report)
            elif is_timeout(s):
                pilotErrorDiag = "rfcp get was timed out after %d seconds" % (telapsed)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('GET_TIMEOUT', report)
                ec = error.ERR_GETTIMEOUT

            return ec, pilotErrorDiag
        else:
            tolog("Copy command finished")
        if fsize == 0:
            try:
                fsize = str(os.path.getsize(loc_pfn))
            except OSError, e:
                pilotErrorDiag = "Could not get file size: %s" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.__sendReport('FS_FAIL', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(dest_path)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return error.ERR_FAILEDSIZELOCAL, pilotErrorDiag

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # get remote file size and checksum 
        ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_path, csumtype=csumtype)
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return ec, pilotErrorDiag

        # get remote file size and checksum 
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        # compare remote and local file checksum
        if fchecksum != 0 and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, ddm_storage, fsize=0, fchecksum=0, dsname='', **pdict):
        """ Data transfer using rfcp - generic version
        It's not advisable to use this right now because there's no
        easy way to register the srm space token if the file is 
        copied with rfcp"""

        error = PilotErrors()
        pilotErrorDiag = ""

        tolog("put_data() got ddm_storage=%s" % (ddm_storage))

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        dsname = pdict.get('dsname', '')
        analJob = pdict.get('analJob', False)
        extradirs = pdict.get('extradirs', '')

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'rfpLFC'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        # At destination append a subdirectory which is first two fields of dsname, or 'other'
        destination = readpar('sepath')
        if destination == '':
            pilotErrorDiag = "put_data destination path in SE not defined"
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('DEST_PATH_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        if dsname == '':
            pilotErrorDiag = "Dataset name not specified to put_data"
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('DSN_UNDEF', report)
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
            self.__sendReport('DSN_FORMAT_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # preparing variables
        src_pfn = source
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(src_pfn, csumtype="adler32")
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
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
            self.__sendReport('MKDIR_FAIL', report)
            return SiteMover.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag)

        cmd = '%srfcp %s %s' % (_setup_str, source, dst_loc_sedir)
        tolog("Executing command: %s" % cmd)
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(cmd)
        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            pilotErrorDiag = "Error in copying: %s" % (o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('RFCP_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        try:
            os.chmod(dst_loc_pfn, self.permissions_FILE)
        except IOError, e:
            pilotErrorDiag = "put_data could not change permission of the file: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            return SiteMover.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # Comparing only file size since MD5sum is problematic
        try:
            nufsize = str(os.path.getsize(dst_loc_pfn))
        except OSError, e:
            pilotErrorDiag = "put_data could not get file size: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_FAIL', report)
            return SiteMover.put_data_retfail(error.ERR_FAILEDSIZE, pilotErrorDiag)
        if fsize != nufsize:
            pilotErrorDiag = "File sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(source), str(fsize), str(nufsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)
            return SiteMover.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=dst_gpfn)

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        # register the file in LFC
        report['catStart'] = time()
        os.environ['LFC_HOST'] = readpar('lfchost') # might already be set in envsetup
        lfcpath = self.getLFCPath(analJob)
        lfclfn = '%s/%s/%s/%s' % (lfcpath, prefixdir, dsname, lfn)
        cmd = "%slcg-rf -g %s -l %s --vo atlas %s" % (envsetup, guid, lfclfn, dst_gpfn)
        tolog("registration command: %s" % (cmd))
        s, o = commands.getstatusoutput(cmd)
        if s != 0:
            o = o.replace('\n', ' ')
            pilotErrorDiag = "LFC registration failed: %s" % (o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            check_syserr(s, o)
            self.__sendReport('LFC_REG_FAIL', report)
            return SiteMover.put_data_retfail(error.ERR_FAILEDLFCREG, pilotErrorDiag)
        else:
            # add checksum and file size to LFC
            csumtype = self.getChecksumType(fchecksum, format="short")
            exitcode, pilotErrorDiag = self.addFileInfo(lfclfn, fchecksum, csumtype=csumtype)
            if exitcode != 0:
                self.__sendReport('LFC_ADD_CS_FAIL', report)
                return self.put_data_retfail(error.ERR_LFCADDCSUMFAILED, pilotErrorDiag)
            else:
                tolog('Successfully set filesize and checksum for %s' % pfn)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, fsize, '', self.arch_type


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
