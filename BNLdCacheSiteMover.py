import re
import os
import sys
import time
import commands

import dCacheSiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand
from timed_command import timed_command
from FileStateClient import updateFileState

class BNLdCacheSiteMover(dCacheSiteMover.dCacheSiteMover):
    """BNL Specific site mover:
    it uses dCache (inherited, alt. would be to have a different mover as internal object and use it)
    it creates a specific directory structure
    """

    #TODO: use _mkdirWperm(newdir) to create dir threes with the correct permissions
    copyCommand = "BNLdccp"
    checksum_command = "adler32"
    has_mkdir = True
    has_df = False
    has_getsize = True
    has_md5sum = False
    has_chmod = True
    timeout = 5*3600

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        The local file (local access to the dCache file) is assumed to have a relative path
        that is the same of the relative path in the 'gpfn'
        loc_... are the variables used to access the file in the locally exported file system
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        analJob = pdict.get('analJob', False)
        timeout = pdict.get('timeout', 5*3600)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'BNLdCache', lfn, guid)

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

        report['relativeStart'] = time.time()

        pnfsid = self.getPnfsid(loc_pfn, guid)

        # for analysis jobs, skip input file if on tape or if lib file
        if analJob:
            if not self.isLibFile(loc_pfn):
                if pnfsid == None:
                    isStaged = self.isFileStaged(_setup_str, loc_pfn)
                else:
                    _com = "/cacheinfos/isFileInPool?pnfsid=%s" % (pnfsid)
                    isStaged = self.isFileStaged(_setup_str, loc_pfn, url="ddmv02.usatlas.bnl.gov:8000", com=_com)
                if not isStaged:
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
        if pnfsid == None:
            _cmd_str = '%sdccp %s %s' % (_setup_str, loc_pfn, dest_path)
        else:
            _cmd_str = '%sdccp pnfs://dcdcap.usatlas.bnl.gov:22125/%s %s' % (_setup_str, pnfsid, dest_path)
            
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time.time()
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))
        report['validateStart'] = time.time()

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
        """Data transfer: includes BNL dir creation"""

        error = PilotErrors()
        pilotErrorDiag = ""

        tolog("BNLdCacheSiteMover::put_data() called")

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        dsname = pdict.get('dsname', '')
        logFile = pdict.get('logFile', '')
        timeout = pdict.get('timeout', False)
        analJob = pdict.get('analJob', False)
        testLevel = pdict.get('testLevel', '0')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'BNLdCache', lfn, guid)

        ec, pilotErrorDiag = verifySetupCommand(error, self._setup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag) 

        filename = os.path.basename(source)
        _tmp = destination.split('/', 3)      
        locse = '/'+ _tmp[3]
        ftpserv = _tmp[0] + '//' + _tmp[2]
        subpath = self.genSubpath(dsname, filename, logFile)
        sedir = os.path.join(locse, subpath)
        m = re.search('^user', filename)
        if m == None:
            sedir = sedir + '/' + dsname

        # create higher level log directory with 775 permission since jobs writting
        # to here could be ran under usatlas1, usatlas2, ...
        if sedir.find('/user_log02/testpanda/') != -1:
            sedirh = os.path.dirname(sedir.rstrip('/'))
            if not os.path.exists(sedirh):
                cmd = 'mkdir -p -m 775 %s' % (sedirh)
                # tolog("Executing command: %s" % (cmd))
                s, o = commands.getstatusoutput(cmd)
                if s != 0:
                    check_syserr(s, o)
                    pilotErrorDiag = "Error in mkdir: %s" % (o)
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.__sendReport('MKDIR_FAIL', report)
                    return self.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag)

        cmd = 'mkdir -p %s' % (sedir)
        tolog("Executing command: %s" % (cmd))
        s, o = commands.getstatusoutput(cmd)
        if s != 0:
            check_syserr(s, o)
            pilotErrorDiag = "Error in mkdir: %s" % (o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('MKDIR_FAIL', report)
            return self.put_data_retfail(error.ERR_MKDIR, pilotErrorDiag)

        report['validateStart'] = time.time()
        tolog("Getting local file size")
        try:
            fsize = str(os.path.getsize(source))
        except OSError, e:
            pilotErrorDiag = "Could not get file size: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('NO_FILESIZE', report)
            return self.put_data_retfail(error.ERR_FAILEDSIZELOCAL, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # use adler32 checksum algorithm
        fchecksum = self.adler32(source)
        if fchecksum == '00000001': # "%08x" % 1L
            pilotErrorDiag = "Adler32 failed (returned 1)"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('AD_FAIL', report)
            return self.put_data_retfail(error.ERR_FAILEDADLOCAL, pilotErrorDiag)
        csumtype = "adler32"

        destpfn = os.path.join(sedir, filename)
        fail = 0

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=destpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        if testLevel == "1":
            source = "thisisjustatest"

        cmd = 'source %s; dccp %s %s' % (self._setup, source, sedir)
        tolog("Executing command: %s" % (cmd))
        report['transferStart'] = time.time()
        try:
            s, telapsed, cout, cerr = timed_command(cmd, timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
            s = 1
            e = sys.exc_info()
            o = str(e[1]) # set error message from exception text
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))

        if s != 0:
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            pilotErrorDiag = "Error in copying: %s" % str(o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            # fail permanently any SIGPIPE errors since they leave a zero length file in dCache
            # which can not be overwritten, so any recovers attempts will fail
            if o.split(" ")[0] == "SIGPIPE":
                fail = error.ERR_SIGPIPE
            elif o.find("File is readOnly") >= 0:
                tolog("!!WARNING!!2999!! Encountered readOnly error. Will try to remove the file and re-attempt copy: %s" % str(o))
                rmCommand = 'source %s; rm %s' % (self._setup, destpfn)
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
                    pilotErrorDiag = "dccp put timed out after %d seconds" % (telapsed)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    fail = error.ERR_PUTTIMEOUT
                else:
                    fail = error.ERR_STAGEOUTFAILED
        if fail:
            self.__sendReport('DCCP_FAIL', report)
            return self.put_data_retfail(fail, pilotErrorDiag)
        else:
            tolog("Output: %s" % (o))

        tolog("Successfully transferred file to dCache: %s" % (filename))
        time.sleep(5)

        # get the remote file size
        try:
            # PNFS is NFSv3 compliant: supported file size<2GB
            # reading of dCache hidden files is necessary
            nufsize = self.getdCacheFileSize(sedir, filename)
        except Exception, e:
            pilotErrorDiag = "Could not get file size from dCache: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('NO_GET_FILESIZE', report)
            return self.put_data_retfail(error.ERR_FAILEDSIZE, pilotErrorDiag)

        tolog("Remote file size: %s" % str(nufsize))
        if fsize != nufsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (filename, str(nufsize), str(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)
            return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag)
        else:
            tolog("Remote and local file sizes verified")

        # get the remote checksum
        try:
            remote_checksum = self.getdCacheChecksum(sedir, filename)
        except Exception, e:
            pilotErrorDiag = "Could not get checksum from dCache: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag)
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag)
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

        if remote_checksum != fchecksum:
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, filename, remote_checksum, fchecksum)
            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag)
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag)
        else:
            tolog("Remote and local checksums verified")

        r_gpfn = ftpserv + destpfn

        self.__sendReport('DONE', report)
        return (0, pilotErrorDiag, r_gpfn, fsize, fchecksum, 'P')


    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time.time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)

    def getPnfsid(self, pnfs, guid):
        """ get PNFSID from BNL LFC """

        try:
            import lfc
        except Exception, e:
            pilotErrorDiag = "getPnfsid() could not import lfc module: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return None

        os.environ['LFC_HOST'] = readpar('lfchost')
        s, replicas = lfc.lfc_getreplicax('', guid, '')
        if s != 0:
            pilotErrorDiag = "Fail to get PNFSID for guid: %s" % guid
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return None
        else:
            for replica in replicas:
                if pnfs in replica.sfn:
                    pnfsid = replica.setname
                    if pnfsid == "-1":
                        pilotErrorDiag = "getPnfsid() returned -1: File does not exist in dCache"
                        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                        return None
                    elif pnfsid == "":
                        pilotErrorDiag = "getPnfsid() returned nothing: PNFSID will not be used."
                        tolog("WARNING: %s" % (pilotErrorDiag))
                        return None
                    else:
                        return pnfsid
                    
        return None
