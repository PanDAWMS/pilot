import os
import commands
import re
import sys
from string import printable
from time import time, sleep

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, safe_call, verifySetupCommand
from timed_command import timed_command
from FileStateClient import updateFileState

class dCacheSiteMover(SiteMover.SiteMover):
    """
    SiteMover for dCache/dccp locally mounted SEs
    dCache files are mounted in /pnfs file system accessed using dcap libraries (dccp util)
    dccp used to sopy files
    md5sum is not supported (Error running md5sum: md5sum: /pnfs/.../myfile: Input/output error)
    """
    copyCommand = "dccp"
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
        """
        dCache specific space verification.
        There is no way at the moment to verify full dCache space availability
        """

        return 999999

    def isLibFile(self, filename):
        """ check whether the file is a lib file or not """

        status = False
        if filename.find(".lib.tgz") > 0:
            status = True
        return status

    def isFileStaged(self, setup, filename, url=None, com=None):
        """
        Check whether the file is on disk or on tape
        Files that are still on tape for analysis jobs will be skipped
        """

        status = True

        if com and url:
            trial = 1
            max_trials = 3
            st = False
            doDcCheck = False
            import httplib
            while trial <= max_trials:
                # e.g. socket can throw the error "Resource temporarily unavailable"
                try:
                    doDcCheck = False
                    tolog("Contacting %s" % (url))
                    http = httplib.HTTPConnection(url)
                    http.request("GET", com)
                    res = http.getresponse()
                    if res.status == 200:
                        ans = res.read().strip()
                        if ans == "YES":
                            tolog("File %s is staged (server responded with YES)" % (filename))
                        elif ans == "NO":
                            tolog("File %s is not staged and will be skipped for analysis job (server responded with NO)" % (filename))
                            status = False
                        else:
                            tolog("WARNING: Could not interpret curl output: %s (skipping this test)" % (ans))
                    else:
                        tolog("WARNING: Bad HTTP response: return code %d" % (res.status))
                        try:
                            tolog("HTTP reponse: %s" % str(res.read()))
                        except Exception, e:
                            tolog(str(e))
                        status = False
                        doDcCheck = True
                except Exception, e:
                    tolog("!!WARNING!!2999!! [Trial %d/%d] Exception caught: %s" % (trial, max_trials, str(e)))
                    if trial < max_trials:
                        tolog("Will sleep for 60s and then try again")
                        sleep(60)
                    trial += 1
                else:
                    if doDcCheck:
                        tolog("!!WARNING!!2999!! [Trial %d/%d] Bad HTTP response" % (trial, max_trials))
                        if trial < max_trials:
                            tolog("Will sleep for 60s and then try again")
                            sleep(60)
                        trial += 1
                    else:
                        st = True
                        break

            # succeeded with the http method
            if st and not doDcCheck:
                return status
            else:
                tolog("Did not succeed with the http file stage check, will default to using dc_check")

        cmd = "%swhich dc_check" % (setup)
        tolog("Executing command: %s" % (cmd))
        ec, retv = commands.getstatusoutput(cmd)
        if ec != 0 or "Command not found" in retv:
            tolog("Skipping dc_check test: ec=%d, retv=%s" % (ec, retv))
        else:
            cmd = "%sdc_check %s" % (setup, filename)
            tolog("Executing command: %s" % (cmd))
            ec, retv = commands.getstatusoutput(cmd)
            if ec != 0:
                tolog("WARNING: Could not execute command: %s (skipping this test)" % (cmd))
            else:
                if "Check passed" in retv:
                    tolog("File %s is staged" % (filename))
                elif "Check FAILED" in retv:
                    tolog("File %s is not staged and will be skipped for analysis job: %s" % (filename, retv))
                    status = False
                else:
                    tolog("!!WARNING!!2999!! Could not interpret dc_check output: %s (skipping this test)" % (retv))

        return status

    def filter_text(self, myString):
        """ Get rid off any unprintable characters """

        try:
            if type(myString) is str:
                filteredString = ''.join([s for s in myString if s in printable])
            else:
                filteredString = str(myString)
                tolog("WARNING: Text filter did not receive a string: %s" % (filteredString))
        except Exception, e:
            filteredString = "Text filter caught exception: %s" % (e)
            tolog("WARNING: %s" % (filteredString))
            print myString
        return filteredString

    def is_number(self, number):
        """ Verify number type """

        status = False
        try:
            if type(number) is int or type(number) is float:
                status = True
            else:
                tolog("WARNING: timed_command did not return a number as expected: %s, type = %s (1)" % (self.filter_text(number), str(type(number))))
        except Exception, e:
            tolog("WARNING: timed_command did not return a number as expected: %s (2)" % (e))

        return status

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        The local file (local access to the dCache file) is assumed to have a relative path
        that is the same of the relative path in the 'gpfn'
        loc_... are the variables used to access the file in the locally exported file system
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        analJob = pdict.get('analJob', False)
        timeout = pdict.get('timeout', 5*3600)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'dCache', lfn, guid)

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

        copyprefixin = readpar('copyprefixin')
        if copyprefixin != '':
            # Extract the copy prefix
            pfrom, pto = copyprefixin.split('^')
            loc_pfn = pfrom + loc_pfn
            tolog("Added copyprefixin to file: %s" % (loc_pfn))
        else:
            copyprefix = readpar('copyprefix')
            if copyprefix != '':
                # Extract the copy prefix
                pfrom, pto = copyprefix.split('^')
                loc_pfn = pfrom + loc_pfn
                tolog("Added copyprefix to file: %s" % (loc_pfn))

        report['relativeStart'] = time()
        # for analysis jobs, skip input file if on tape or if lib file
        if analJob:
            if not self.isLibFile(loc_pfn):
                if not self.isFileStaged(_setup_str, loc_pfn):
                    pilotErrorDiag = "File %s is not staged and will be skipped for analysis job" % (loc_pfn)
                    self.__sendReport('FILE_ON_TAPE', report)
                    return error.ERR_FILEONTAPE, pilotErrorDiag
            else:
                tolog("Skipping file stage check for lib file")

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
        _cmd_str = '%sdccp %s %s' % (_setup_str, loc_pfn, dest_path)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()
        telapsed = 0
        try:
#            t0 = time()
#            s, cout = commands.getstatusoutput(_cmd_str)
#            cerr = ""
#            telapsed = time() - t0
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
            print "DEBUG: s = ", s, type(s)
            print "DEBUG: telapsed = ", telapsed, type(telapsed)
            print "DEBUG: cout = ", cout, type(cout)
            print "DEBUG: cerr = ", cerr, type(cerr) 
            cout = self.filter_text(cout)
            cerr = self.filter_text(cerr)
            if not self.is_number(s):
                s = 1
            if not self.is_number(telapsed):
                telapsed = 0
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % (e))
            s = 1
            o = self.filter_text(e)
            telapsed = timeout

            # write traceback info to stderr
            import traceback
            exc, msg, tb = sys.exc_info()
            traceback.print_tb(tb)

            _pilotErrorDiag = "Unexpected exception: %s" % (get_exc_plus())
            tolog("!!WARNING!!2999!! get_exc_plus: %s" % (_pilotErrorDiag))
            print "!!WARNING!!2999!! get_exc_plus: %s" % (_pilotErrorDiag)
        else:
            o = cout + cerr
            if telapsed:
                tolog("Elapsed time: %d" % (telapsed))
            else:
                tolog("!!WARNING!!1990!! telapsed not defined - timed_command() did not return timing info (trying to dump the other return variables)")
                try:
                    tolog("s = %d" % (s))
                    tolog("cout = %s" % (cout))
                    tolog("cerr = %s" % (cerr))
                except Exception, e:
                    tolog("Caught exception: %s" % (e))
                    s = 1
                try:
                    tolog("o = %s" % (o))
                except Exception, e:
                    tolog("Caught exception: %s" % (e))
                    o = "undefined"
                telapsed = 0 # undefined
                tolog("Note: telapsed reset to 0")
        report['validateStart'] = time()

        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(s, o)
            pilotErrorDiag = "dccp failed: %s" % (o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            # did the copy command time out?
            if is_timeout(s):
                pilotErrorDiag = "dccp get was timed out after %d seconds" % (telapsed)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('GET_TIMEOUT', report)
                return error.ERR_GETTIMEOUT, pilotErrorDiag

            ec = error.ERR_STAGEINFAILED
            if o == None:
                pilotErrorDiag = "dccp failed with output None: ec = %d" % (s)
            elif len(o) == 0:
                pilotErrorDiag = "dccp failed with no output: ec = %d" % (s)
            else:
                o = o.replace('\n', ' ')
                if o.find("No such file or directory") >= 0:
                    if loc_pfn.find("DBRelease") >= 0:
                        pilotErrorDiag = "DBRelease file missing: %s" % (loc_pfn)
                        ec = error.ERR_MISSDBREL
                    else:
                        pilotErrorDiag = "No such file or directory: %s" % (loc_pfn)
                        ec = error.ERR_NOSUCHFILE
                else:
                    pilotErrorDiag = "dccp failed with output: ec = %d, output = %s" % (s, o)

            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('DCCP_FAIL', report)
            return ec, pilotErrorDiag
        else:
            tolog("Output: %s" % (o))

            # dccp can fail without setting a non-zero exit code;
            # e.g. with output = 
            # "Failed to create a control line
            # Failed open file in the dCache.
            # Can't open source file : Unable to connect to server
            # System error: No route to host" (repeated)
            if "Failed open file in the dCache" in o or "System error: No route to host" in o:
                pilotErrorDiag = "dccp failed without setting a non-zero exit code"
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('DCACHE_SIN_FAIL', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(dest_path)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return error.ERR_STAGEINFAILED, pilotErrorDiag

        if fsize == 0 or fsize == "0":
            try:
                # PNFS is NFSv3 compliant: supported file size < 2GB
                # reading of dCache hidden files is necessary
                fsize = self.getdCacheFileSize(os.path.dirname(loc_pfn), os.path.basename(loc_pfn))
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not get file size with new method: %s" % str(e))
                tolog("Using old method to get file size")
                try:
                    fsize = str(os.path.getsize(loc_pfn))
                except Exception, e:
                    pilotErrorDiag = "Could not get file size: %s" % str(e)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    self.__sendReport('NO_FILESIZE', report)

                    # remove the local file before any get retry is attempted
                    _status = self.removeLocal(dest_path)
                    if not _status:
                        tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                    return error.ERR_FAILEDSIZELOCAL, pilotErrorDiag

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

        # compare remote and local file size
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(dest_path), str(fsize), str(dstfsize))
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        # compare remote and local file checksum
        if fchecksum and fchecksum != "0" and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(dest_path), dstfchecksum, fchecksum) 
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

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

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ Data transfer using dCache - generic version """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        timeout = pdict.get('timeout', 5*3600)
        dsname = pdict.get('dsname', '')
        extradirs = pdict.get('extradirs', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'dCache', lfn, guid)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        ec, pilotErrorDiag = verifySetupCommand(error, _setup_str)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag) 

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag)
        csumtype = "adler32"

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
            # dst_host = _sentries[2] # host and port            
            dst_loc_se = '/' + _sentries[3]
            dst_prefix = dst_serv

        filename = os.path.basename(source)
#        report['dataset'] = dsname

        # Behavior as in BNL: user files have no dsname automatically added to dir name
        m = re.search('^user', filename)
        if m:
            dsname = ''           

        dst_loc_sedir = os.path.join(dst_loc_se, os.path.join(extradirs, dsname))
        
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

        report['relativeStart'] = time()
        # create the destination dir
        try:
            self.mkdirWperm(dst_loc_sedir)
        except EnvironmentError, e:  # includes OSError (permission) and IOError (write error)
            pilotErrorDiag = "put_data mkdirWperm failed: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('MKDIRWPERM_FAIL', report)
            return self.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag)

        report['transferStart'] = time()
        # execute dccp
        cmd = "%sdccp %s %s" % (_setup_str, source, dst_loc_sedir)
        tolog("Executing command: %s" % (cmd))
        try:
#            t0 = time.time()
#            s, cout = commands.getstatusoutput(cmd)
#            cerr = ""
#            telapsed = time.time() - t0
            s, telapsed, cout, cerr = timed_command(cmd, timeout)
            print "DEBUG: s = ", s, type(s)
            print "DEBUG: telapsed = ", telapsed, type(telapsed)
            print "DEBUG: cout = ", cout, type(cout)
            print "DEBUG: cerr = ", cerr, type(cerr) 
            cout = self.filter_text(cout)
            cerr = self.filter_text(cerr)
            if not self.is_number(s):
                s = 1
            if not self.is_number(telapsed):
                telapsed = 0
        except Exception, e:
            # There may be exceptions like permission errors
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % (e))
            s = 1
            e = sys.exc_info()
            o = self.filter_text(e[1])
            telapsed = timeout

            # write traceback info to stderr
            import traceback
            exc, msg, tb = sys.exc_info()
            traceback.print_tb(tb)
        else:
            o = cout + cerr
            if telapsed:
                tolog("Elapsed time: %d" % (telapsed))
            else:
                tolog("!!WARNING!!1990!! telapsed not defined - timed_command() did not return timing info (trying to dump the other return variables)")
                try:
                    tolog("s = %d" % (s))
                    tolog("cout = %s" % (cout))
                    tolog("cerr = %s" % (cerr))
                except Exception, e:
                    tolog("Caught exception: %s" % (e))
                    s = 1
                try:
                    tolog("o = %s" % (o))
                except Exception, e:
                    tolog("Caught exception: %s" % (e))
                    o = "undefined"
                telapsed = 0 # undefined
                tolog("Note: telapsed reset to 0")
        report['validateStart'] = time()

        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            check_syserr(s, o)
            pilotErrorDiag = "Error in copying: %s" % (o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            # fail permanently any SIGPIPE errors since they leave a zero length file in dCache
            # which can not be overwritten, so any recovers attempts will fail
            if o.split(" ")[0] == "SIGPIPE":
                fail = error.ERR_SIGPIPE
            elif o.find("File is readOnly") >= 0:
                tolog("!!WARNING!!2999!! Encountered readOnly error. Will try to remove the file and re-attempt copy: %s" % str(o))
                rmCommand = 'source %s; rm %s' % (self._setup, dst_loc_pfn)
                tolog("Executing command: %s" % (rmCommand))
                s, o = commands.getstatusoutput(rmCommand)
                if s == 0:
                    tolog("Successfully removed readOnly file")
                else:
                    check_syserr(s, o)
                    if o.find("No such file or directory") >= 0:
                        tolog("(Ignoring the previous error)")
                        s = 0
                    else:
                        pilotErrorDiag += " (failed to remove readOnly file: %s)" % (o)
                        tolog('!!WARNING!!2999!! Could not remove file: %s' % o)
                        fail = error.ERR_FAILEDRM
                if s == 0:        
                    tolog("Re-attempting copy command: %s" % (cmd))
                    s, o = commands.getstatusoutput(cmd)
                    if s != 0:
                        check_syserr(s, o)
                        pilotErrorDiag += " (re-attempt to copy failed as well: %s)" % (o)
                        tolog('!!WARNING!!2999!! Error in copying: %s' % o)
                        if o.split(" ")[0] == "SIGPIPE":
                            fail = error.ERR_SIGPIPE
                        else:
                            fail = error.ERR_STAGEOUTFAILED
                    else:
                        fail = 0
            else:
                if is_timeout(s):
                    pilotErrorDiag = "dccp put was timed out after %d seconds" % (telapsed)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    fail = error.ERR_PUTTIMEOUT
                else:
                    fail = error.ERR_STAGEOUTFAILED
            self.__sendReport('DCCP_FAIL', report)
            return self.put_data_retfail(fail, pilotErrorDiag)
        try:
            os.chmod(dst_loc_pfn, self.permissions_FILE)
        except IOError, e:
            pilotErrorDiag = "Error changing permission of the file: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('PERM_CHANGE_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        tolog("dst_loc_sedir: %s" % (dst_loc_sedir))
        tolog("filename: %s" % (filename))

        # try to get the checksum
        try:
            remote_checksum = self.getdCacheChecksum(dst_loc_sedir, filename)
        except Exception, e:
            pilotErrorDiag = "Could not get checksum from dCache: %s (skip test)" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            remote_checksum = None
        else:
            if remote_checksum == "NOSUCHFILE":
                pilotErrorDiag = "The pilot will fail the job since the remote file does not exist"
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.__sendReport('NOSUCHFILE', report)
                return self.put_data_retfail(error.ERR_NOSUCHFILE, pilotErrorDiag)
            elif remote_checksum:
                tolog("Remote checksum: %s" % (remote_checksum))
            else:
                tolog("Could not get remote checksum")

        if remote_checksum:
            if remote_checksum != fchecksum:
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, filename, remote_checksum, fchecksum)
                if csumtype == "adler32":
                    self.__sendReport('AD_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=dst_gpfn)
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=dst_gpfn)
            else:
                tolog("Remote and local checksums verified")
        else:
            # alternatively, test the file size
            try:
                nufsize = self.getdCacheFileSize(dst_loc_sedir, filename)            
            except OSError, e:
                pilotErrorDiag = "Could not get file size: %s" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.__sendReport('NO_FILESIZE', report)
                return self.put_data_retfail(error.ERR_FAILEDSIZE, pilotErrorDiag)

            if fsize != nufsize:
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(source), str(nufsize), str(fsize))
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.__sendReport('FS_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=dst_gpfn)
            else:
                tolog("Remote and local file sizes verified")

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
