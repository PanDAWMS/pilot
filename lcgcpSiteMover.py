import os, re, sys
import commands
from time import time

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, getExperiment
from FileStateClient import updateFileState

# placing the import lfc here breaks compilation on non-lfc sites
# import lfc

class lcgcpSiteMover(SiteMover.SiteMover):
    """ SiteMover for lcg-cp """

    copyCommand = "lcg-cp"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
    has_chmod = False
    timeout = 3600

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def check_space(self, ub):
        """ For when space availability is not verifiable """
        return 999999

    def core_get_data(self, envsetup, token, source_surl, dest_path, experiment):
        """ stage-in core function, can be overridden (see stormSiteMover) """

        error = PilotErrors()

        # determine which timeout option to use
        if self.isNewLCGVersion("%s lcg-cp" % (envsetup)):
            timeout_option = "--srm-timeout=%d --connect-timeout=300 --sendreceive-timeout=%d" % (self.timeout, self.timeout)
        else:
            timeout_option = "-t %d" % (self.timeout)

        # used lcg-cp options:
        # --vo: specifies the Virtual Organization the user belongs to
        #   -t: time-out
        if token:
            # do not use option -b on SL3 clusters running older versions of lcg_utils
            use_b = True
            s, o = commands.getstatusoutput("%s lcg-cr --version" % (envsetup))
            if s != 0:
                # (BDII collects all information coming from site GIISes and stores them in a permanent database)
                tolog("(Probably too old lcg_utils - skipping BDII disabling)")
                use_b = False

            # for the time being
            use_b = False
            if use_b:
                _cmd_str = '%s lcg-cp --vo atlas --srcsetype srmv2 -s %s -b %s %s file://%s' %\
                           (envsetup, token, timeout_option, source_surl, dest_path)
            else:
                tolog("(Skipping space token for the time being)")
                _cmd_str = '%s lcg-cp --vo atlas %s %s file://%s' % (envsetup, timeout_option, source_surl, dest_path)
        else:
            _cmd_str = '%s lcg-cp --vo atlas %s %s file://%s' % (envsetup, timeout_option, source_surl, dest_path)

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        # add the full stage-out command to the job setup script
        to_script = _cmd_str.replace("file://%s" % os.path.dirname(dest_path), "file://`pwd`")
        to_script = to_script.lstrip(' ') # remove any initial spaces
        if to_script.startswith('/'):
            to_script = 'source ' + to_script
        thisExperiment.updateJobSetupScript(os.path.dirname(dest_path), to_script=to_script)

        tolog("Executing command: %s" % (_cmd_str))
        s = -1
        o = '(not defined)'
        t0 = os.times()
        try:
            s, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught: %s (%d, %s)" % (str(e), s, o))
            o = str(e)
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        if s == 0:
            tolog("get_data succeeded")
        else:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            tolog("!!WARNING!!2990!! get_data failed. Status=%s Output=%s" % (str(s), str(o)))
            if o.find("globus_xio:") >= 0:
                pilotErrorDiag = "Globus system error: %s" % (o)
                tolog("Globus system error encountered")
                return error.ERR_GETGLOBUSSYSERR, pilotErrorDiag
            elif o.find("No space left on device") >= 0:
                pilotErrorDiag = "No available space left on local disk: %s" % (o)
                tolog("No available space left on local disk")
                return error.ERR_NOLOCALSPACE, pilotErrorDiag
            elif o.find("No such file or directory") >= 0:
                if source_surl.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (source_surl)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_MISSDBREL, pilotErrorDiag
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (source_surl)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_NOSUCHFILE, pilotErrorDiag
            else:
                if t >= self.timeout:
                    pilotErrorDiag = "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    return error.ERR_GETTIMEOUT, pilotErrorDiag
                else:
                    if len(o) == 0:
                        pilotErrorDiag = "Copy command returned error code %d but no output" % (s)
                    else:
                        pilotErrorDiag = o
                    return error.ERR_STAGEINFAILED, pilotErrorDiag
        return None

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        token = pdict.get('token', None)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'lcg', lfn, guid)

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.prepareReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        if proxycheck:
            # do we have a valid proxy?
            s, pilotErrorDiag = thisExperiment.verifyProxy(envsetup=envsetup)
            if s != 0:
                self.prepareReport('PROXYFAIL', report)
                return s, pilotErrorDiag
        else:
            tolog("Proxy verification turned off")

        getfile = gpfn

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", ftype="input")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", ftype="input")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.prepareReport('FOUND_ROOT', report)
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", ftype="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", ftype="input")
                    return error.ERR_DIRECTIOFILE, pilotErrorDiag
                else:
                    tolog("Normal file transfer")

        # get remote filesize and checksum
        if fsize == 0 or fchecksum == 0:
            try:
                import lfc
            except Exception, e:
                pilotErrorDiag = "get_data() could not import lfc module: %s" % str(e)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.prepareReport('LFC_IMPORT', report)
                return error.ERR_GETLFCIMPORT, pilotErrorDiag

            os.environ['LFC_HOST'] = readpar('lfchost')
            try:
                ret, res = lfc.lfc_getreplicas([str(guid)],"")
            except Exception, e:
                pilotErrorDiag = "Failed to get LFC replicas: %s" % str(e)
                tolog("!!WARNING!!2990!! Exception caught: %s" % (pilotErrorDiag))
                tolog("Mover get_data finished (failed)")
                self.prepareReport('NO_LFC_REPS', report)
                return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag
            if ret != 0:
                pilotErrorDiag = "Failed to get replicas: %d, %s" % (ret, res)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('NO_REPS', report)
                return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag
            else:
                # extract the filesize and checksum
                try:
                    fsize = res[0].filesize
                    fchecksum = res[0].csumvalue
                except Exception, e:
                    pilotErrorDiag = "lfc_getreplicas did not return filesize/checksum: %s" % str(e)
                    tolog("!!WARNING!!2990!! Exception caught: %s" % (pilotErrorDiag))
                    self.prepareReport('NO_LFC_FS_CS', report)
                    return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag
                else:
                    tolog("filesize: %s" % str(fsize))
                    tolog("checksum: %s" % str(fchecksum))

        # invoke the transfer commands
        report['relativeStart'] = time()
        report['transferStart'] = time()
        result = self.core_get_data(envsetup, token, getfile, fullname, experiment)
        report['validateStart'] = time()
        if result:
            self.prepareReport('CORE_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(fullname)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return result

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if (fsize != 0 or fchecksum != 0) and self.doFileVerifications():
            loc_filename = lfn
            dest_file = os.path.join(path, loc_filename)

            # get the checksum type (md5sum or adler32)
            if fchecksum != 0 and fchecksum != "":
                csumtype = self.getChecksumType(fchecksum)
            else:
                csumtype = "default"

            # get remote file size and checksum
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
            if ec != 0:
                self.prepareReport('LOCAL_FILE_INFO_FAIL', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(fullname)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return ec, pilotErrorDiag

            # compare remote and local file size
            if long(fsize) != 0 and long(dstfsize) != long(fsize):
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(gpfn), str(dstfsize), str(fsize))
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('FS_MISMATCH', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(fullname)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))

                # report corrupted file to consistency server
                self.reportFileCorruption(gpfn)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(fullname)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                if csumtype == "adler32":
                    self.prepareReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.prepareReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", ftype="input")
        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, pfn, destination, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):
        """ copy output file from disk to local SE """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        logFile = pdict.get('logFile', '')
        sitename = pdict.get('sitename', '')
        proxycheck = pdict.get('proxycheck', False)
        experiment = pdict.get('experiment', '')
        analysisJob = pdict.get('analJob', False)
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        filename = pfn.split('/')[-1]

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'lcg', lfn, guid)

        # is the dataset defined?
        if dsname == '':
            pilotErrorDiag = "Dataset name not specified to put_data"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            self.prepareReport('DSN_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(pfn, csumtype="adler32")
            if ec != 0:
                self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.prepareReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        # do we need to check the user proxy?
        if proxycheck:
            s, pilotErrorDiag = thisExperiment.verifyProxy(envsetup=envsetup, limit=2)
            if s != 0:
                self.prepareReport('PROXY_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
        else:
            tolog("Proxy verification turned off")

        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename, scope=scope, sitemover=self) # quick workaround
        if ec != 0:
            self.prepareReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag, surl=dst_gpfn)

        lfclfn = os.path.join(lfcdir, lfn)
        # LFC LFN = /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492/
        #364aeb74-8b62-4c8f-af43-47b447192ced_0.job.log.tgz

        # putfile is the SURL
        putfile = surl
        full_surl = putfile
        if full_surl[:len('token:')] == 'token:':
            # remove the space token (e.g. at Taiwan-LCG2) from the SURL info
            full_surl = full_surl[full_surl.index('srm://'):]

        # srm://dcache01.tier2.hep.manchester.ac.uk/pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/
        #testpanda.destDB/testpanda.destDB.604b4fbc-dbe9-4b05-96bb-6beee0b99dee_sub0974647/
        #86ecb30d-7baa-49a8-9128-107cbfe4dd90_0.job.log.tgz
        tolog("putfile = %s" % (putfile))
        tolog("full_surl = %s" % (full_surl))

        # get the RSE from ToA
        try:
            _RSE = self.getRSE(surl=putfile)
        except:
            # WARNING: do not print the exception here since it can sometimes not be converted to a string! (problem seen at Taiwan)
            tolog("Warning: Failed to get RSE (can not add this info to tracing report)")
        else:
            report['localSite'], report['remoteSite'] = (_RSE, _RSE)
            tolog("RSE: %s" % (_RSE))

        # get the absolute (full) path to the file
        fppfn = os.path.abspath(pfn)
        tolog("pfn=%s" % (pfn))

        cmd = '%s echo "LFC_HOST=$LFC_HOST"; lfc-mkdir -p %s' % (envsetup, lfcdir)
        # export LFC_HOST=lfc0448.gridpp.rl.ac.uk ; echo "LFC_HOST=$LFC_HOST";
        #lfc-mkdir -p /grid/atlas/dq2/testpanda.destDB/testpanda.destDB.604b4fbc-dbe9-4b05-96bb-6beee0b99dee_sub0974647
        tolog("Executing command: %s" % (cmd))
        s, o = commands.getstatusoutput(cmd)
        if s == 0:
            tolog("LFC setup and mkdir succeeded")
            tolog("Command output: %s" % (o))
        else:
            tolog("!!WARNING!!2990!! LFC setup and mkdir failed. Status=%s Output=%s" % (s, o))
            if o == "Could not establish context":
                pilotErrorDiag = "Could not establish context: Proxy / VO extension of proxy has probably expired"
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.dumpExtendedProxy(envsetup)
                self.prepareReport('CONTEXT_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag, surl=full_surl)
            else:
                pilotErrorDiag = "LFC setup and mkdir failed: %s" % (o)
                self.prepareReport('LFC_SETUP_FAIL', report)
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=full_surl)

        # determine which timeout option to use
        if self.isNewLCGVersion("%s lcg-cr" % (envsetup)):
            timeout_option = "--srm-timeout=%d --connect-timeout=300 --sendreceive-timeout=%d" % (self.timeout, self.timeout)
        else:
            timeout_option = "-t %d" % (self.timeout)

        # used lcg-cr options:
        # --verbose: verbosity on
        #      --vo: specifies the Virtual Organization the user belongs to
        #        -T: specify SRM version
        #        -s: space token description
        #        -b: BDII disabling
        #        -t: time-out
        #        -l: specifies the Logical File Name associated with the file. If this option is present, an entry is added to the LFC
        #        -g: specifies the Grid Unique IDentifier. If this option is not present, a GUID is generated internally
        #        -d: specifies the destination. It can be the Storage Element fully qualified hostname or an SURL. In the latter case,
        #            the scheme can be sfn: for a classical SE or srm:. If only the fully qualified hostname is given, a filename is
        #            generated in the same format as with the Replica Manager
        if token:
            # Special case for GROUPDISK (do not remove dst: bit before this stage, needed in several places)
            if "dst:" in token:
                token = token[len('dst:'):]
                tolog("Dropped dst: part of space token descriptor; token=%s" % (token))
                if 'DATADISK' in token:
                    token = "ATLASDATADISK"
                else:
                    token = "ATLASGROUPDISK"
                tolog("Space token descriptor reset to: %s" % (token))

            surl = putfile[putfile.index('srm://'):]
            _cmd_str = '%s which lcg-cr; lcg-cr --version; lcg-cr --verbose --vo atlas -T srmv2 -s %s -b %s -l %s -g %s -d %s file:%s' % (envsetup, token, timeout_option, lfclfn, guid, surl, fppfn)
        else:
            surl = putfile
            _cmd_str = '%s which lcg-cr; lcg-cr --version; lcg-cr --verbose --vo atlas %s -l %s -g %s -d %s file:%s' % (envsetup, timeout_option, lfclfn, guid, surl, fppfn)

        # GoeGrid testing: _cmd_str = '%s which lcg-cr; lcg-cr --version; lcg-crXXX --verbose --vo atlas %s -l %s -g %s -d %s file:%s' % (envsetup, timeout_option, lfclfn, guid, surl, fppfn)

        tolog("Executing command: %s" % (_cmd_str))
        s = -1
        t0 = os.times()
        report['relativeStart'] = time()
        report['transferStart'] =  time()
        try:
            s, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught: %s" % (str(e)))
            o = str(e)
        report['validateStart'] = time()
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        tolog("exitcode = %d, len(stdout) = %s" % (s, len(o)))
        if s == 0:
            # add checksum and file size to LFC
            csumtype = self.getChecksumType(fchecksum, format="short")
            exitcode, pilotErrorDiag = self.addFileInfo(lfclfn, fchecksum, csumtype=csumtype)
            if exitcode != 0:
                # check if file was partially transferred, if so, remove it
                # note: --nolfc should not be used since lcg-cr succeeded in registering the file before
                ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                if ec == -2:
                    pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

            if exitcode == -1:
                self.prepareReport('LFC_IMPORT_FAIL', report)
                return self.put_data_retfail(error.ERR_PUTLFCIMPORT, pilotErrorDiag, surl=full_surl)
            elif exitcode != 0:
                self.prepareReport('LFC_ADDCSUM_FAIL', report)
                return self.put_data_retfail(error.ERR_LFCADDCSUMFAILED, pilotErrorDiag, surl=full_surl)
            else:
                tolog('Successfully set filesize and checksum for %s' % (pfn))
        else:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(s, o)
            tolog('!!WARNING!!2990!! put_data failed: exitcode = %d stdout = %s' % (s, o))

            # check if file was partially transferred, if so, remove it
            # note: --nolfc should only be used if LFC registration failed
            nolfc = self.useNoLFC(o)
            ec = self.removeFile(envsetup, self.timeout, surl, nolfc=nolfc, lfcdir=lfcdir)
            if ec == -2:
                pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

            if o.find("Could not establish context") >= 0:
                pilotErrorDiag += "Could not establish context: Proxy / VO extension of proxy has probably expired"
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('CONTEXT_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag, surl=full_surl)
            elif o.find("No such file or directory") >= 0:
                pilotErrorDiag += "No such file or directory: %s" % (o)
                self.prepareReport('NO_FILE_DIR', report)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=full_surl)
            elif o.find("globus_xio: System error") >= 0:
                pilotErrorDiag += "Globus system error: %s" % (o)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('GLOBUS_FAIL', report)
                return self.put_data_retfail(error.ERR_PUTGLOBUSSYSERR, pilotErrorDiag, surl=full_surl)
            else:
                if t >= self.timeout:
                    pilotErrorDiag += "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.prepareReport('CP_TIMEOUT', report)
                    return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag, surl=full_surl)
                else:
                    if len(o) == 0:
                        pilotErrorDiag += "Copy command returned error code %d but no output" % (s)
                    else:
                        pilotErrorDiag += o
                    self.prepareReport('CP_ERROR', report)
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=full_surl)

        verified = False

        # get the checksum using the lcg-get-checksum command
        remote_checksum = self.lcgGetChecksum(envsetup, self.timeout, full_surl)

        if not remote_checksum:
            # try to grab the remote file info using lcg-ls command
            remote_checksum, remote_fsize = self.getRemoteFileInfo(envsetup, self.timeout, surl)
        else:
            tolog("Setting remote file size to None (not needed)")
            remote_fsize = None

        # compare the checksums if the remote checksum was extracted
        tolog("Remote checksum: %s" % str(remote_checksum))
        tolog("Local checksum: %s" % (fchecksum))

        if remote_checksum:
            if remote_checksum != fchecksum:
                # report corrupted file to consistency server
                # self.reportFileCorruption(full_surl)

                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(dst_gpfn), remote_checksum, fchecksum)

                # remove the file and the LFC directory
                ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                if ec == -2:
                    pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                tolog("!!WARNING!!1800!! %s" % (pilotErrorDiag))
                if csumtype == "adler32" or csumtype == "AD":
                    self.prepareReport('AD_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=full_surl)
                else:
                    self.prepareReport('MD5_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=full_surl)
            else:
                tolog("Remote and local checksums verified")
                verified = True
        else:
            tolog("Skipped primary checksum verification (remote checksum not known)")

        # if lcg-ls could not be used
        if "/pnfs/" in dst_gpfn and not remote_checksum:
            # for dCache systems we can test the checksum with the use method
            tolog("Detected dCache system: will verify local checksum with the local SE checksum")
            # gpfn = srm://head01.aglt2.org:8443/srm/managerv2?SFN=/pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....
            path = dst_gpfn[dst_gpfn.find('/pnfs/'):]
            # path = /pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....#
            tolog("File path: %s" % (path))

            _filename = os.path.basename(path)
            _dir = os.path.dirname(path)

            # get the remote checksum
            tolog("Local checksum: %s" % (fchecksum))
            try:
                remote_checksum = self.getdCacheChecksum(_dir, _filename)
            except Exception, e:
                pilotErrorDiag = "Could not get checksum from dCache: %s (test will be skipped)" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            else:
                if remote_checksum == "NOSUCHFILE":
                    pilotErrorDiag = "The pilot will fail the job since the remote file does not exist"

                    # remove the file and the LFC directory
                    ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                    if ec == -2:
                        pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.prepareReport('NOSUCHFILE', report)
                    return self.put_data_retfail(error.ERR_NOSUCHFILE, pilotErrorDiag, surl=full_surl)
                elif remote_checksum:
                    tolog("Remote checksum: %s" % (remote_checksum))
                else:
                    tolog("Could not get remote checksum")

            if remote_checksum:
                if remote_checksum != fchecksum:
                    # report corrupted file to consistency server
                    self.reportFileCorruption(full_surl)

                    pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                     (csumtype, _filename, remote_checksum, fchecksum)

                    # remove the file and the LFC directory
                    ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                    if ec == -2:
                        pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                    if csumtype == "adler32":
                        self.prepareReport('AD_MISMATCH', report)
                        return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=full_surl)
                    else:
                        self.prepareReport('MD5_MISMATCH', report)
                        return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=full_surl)
                else:
                    tolog("Remote and local checksums verified")
                    verified = True
        else:
            tolog("Skipped secondary checksum test")

        # if the checksum could not be verified (as is the case for non-dCache sites) test the file size instead
        if not remote_checksum and remote_fsize:
            # compare remote and local file size
            tolog("Remote file size: %s" % str(remote_fsize))
            tolog("Local file size: %s" % (fsize))
            if remote_fsize and remote_fsize != "" and fsize != "" and fsize:
                if str(fsize) != remote_fsize:
                    # report corrupted file to consistency server
                    self.reportFileCorruption(full_surl)

                    pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                     (filename, remote_fsize, str(fsize))

                    # remove the file and the LFC directory
                    ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
                    if ec == -2:
                        pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.prepareReport('FS_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=full_surl)
                else:
                    tolog("Remote and local file sizes verified")
                    verified = True
            else:
                tolog("Skipped file size test")

        # was anything verified?

        #PN
#        if not '.log' in dst_gpfn:
#            verified = False

        if not verified:
            # fail at this point
            pilotErrorDiag = "Neither checksum nor file size could be verified (failing job)"

            # remove the file and the LFC directory
            ec = self.removeFile(envsetup, self.timeout, surl, nolfc=False, lfcdir=lfcdir)
            if ec == -2:
                pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out

            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('NOFILEVERIFICATION', report)
            return self.put_data_retfail(error.ERR_NOFILEVERIFICATION, pilotErrorDiag, surl=full_surl)

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, 0, 0, self.arch_type

    def useNoLFC(self, _stdout):
        """ Should --nolfc be used? """

        if "Registration failed" in _stdout:
            nolfc = True
        else:
            nolfc = False
        return nolfc
