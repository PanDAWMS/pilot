import os, re, sys
import commands
from time import time

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, getSiteInformation, getExperiment
from timed_command import timed_command
from FileStateClient import updateFileState
from SiteInformation import SiteInformation

# placing the import lfc here breaks compilation on non-lfc sites
# import lfc

class curlSiteMover(SiteMover.SiteMover):
    """ SiteMover for curl """

    copyCommand = "curl"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
    has_chmod = False
    timeout = 3600
    """ get proxy """

    si = SiteInformation()
    sslCert = si.getSSLCertificate()
    sslKey = sslCert
    sslCertDir = si.getSSLCertificatesDirectory()

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
        timeout_option = "--connect-timeout 300 --max-time %d" % (self.timeout)

        sslCert = self.sslCert
        sslKey = self.sslKey
        sslCertDir = self.sslCertDir

        # used curl options:
        # --cert: <cert[:passwd]> Client certificate file and password (SSL)
        # --capath: <directory> CA directory (made using c_rehash) to verify
        # --location: Follow Location: hints (H)
        # --output: <file> Write output to <file> instead of stdout
        # --cilent: Makes Curl mute
        # --show-error: When used with -s it makes curl show error message if it fails
        # Removed for SL6: --ciphers <list of ciphers> (SSL)  Specifies  which  ciphers  to use in the connection.

        """ define curl command string """
        _cmd_str = 'lcg-gt %s https' % (source_surl)
        try:
            s, o = commands.getstatusoutput(_cmd_str)
            tolog("Executing command: %s" % (_cmd_str))
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught: %s (%d, %s)" % (str(e), s, o))
            o = str(e)
        if s == 0:
            tolog("lcg-gt supported, get http path")
            source_surl = o.strip().split()
            source_surl = source_surl[0]
            _cmd_str = '%s curl --silent --show-error --cacert %s %s --capath %s --cert %s --key %s -L %s -o %s' % (envsetup, sslCert, timeout_option, sslCertDir, sslCert, sslKey, source_surl, dest_path)
#            _cmd_str = '%s curl --ciphers ALL:NULL --silent --show-error --cacert %s %s --capath %s --cert %s --key %s -L %s -o %s' % (envsetup, sslCert, timeout_option, sslCertDir, sslCert, sslKey, source_surl, dest_path)
        else:
            tolog("lcg-gt not supported, get http path by replacing source_surl")
            _cmd_str = '%s curl --silent --show-error --cacert %s %s --capath %s --cert %s --key %s -L %s -o %s' % (envsetup, sslCert, timeout_option, sslCertDir, sslCert, sslKey, source_surl, dest_path)
#            _cmd_str = '%s curl --ciphers ALL:NULL --silent --show-error --cacert %s %s --capath %s --cert %s --key %s -L %s -o %s' % (envsetup, sslCert, timeout_option, sslCertDir, sslCert, sslKey, source_surl, dest_path)
            _cmd_str = _cmd_str.replace("srm://", "https://")
        # add the full stage-out command to the job setup script
        #_cmd_str = _cmd_str.replace("file://", "-o ")

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        to_script = _cmd_str
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
        proxycheck = pdict.get('proxycheck', False)
        experiment = pdict.get('experiment', '')

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the Rucio tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'curl'
            # mark the relative start
            report['catStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

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
                return ec, pilotErrorDiag

            # compare remote and local file size
            if long(fsize) != 0 and long(dstfsize) != long(fsize):
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(gpfn), str(dstfsize), str(fsize))
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('FS_MISMATCH', report)
                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))

                # report corrupted file to consistency server
                self.reportFileCorruption(gpfn)

                if csumtype == "adler32":
                    self.prepareReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.prepareReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", ftype="input")
        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        # function is based on dCacheSiteMover put function

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        dsname = pdict.get('dsname', '')
        analysisJob = pdict.get('analJob', False)
        testLevel = pdict.get('testLevel', '0')
        extradirs = pdict.get('extradirs', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        tolog("put_data received prodSourceLabel=%s" % (prodSourceLabel))
        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        # get the Rucio tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'curl'
            # mark the relative start
            report['catStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get the checksum type
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        if proxycheck:
            s, pilotErrorDiag = thisExperiment.verifyProxy(envsetup=envsetup, limit=2)
            if s != 0:
                self.prepareReport('NO_PROXY', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
        else:
            tolog("Proxy verification turned off")

        filename = os.path.basename(source)

        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename, scope=scope, sitemover=self) # quick workaround
        if ec != 0:
            self.prepareReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        putfile = surl
        full_surl = putfile
        if full_surl[:len('token:')] == 'token:':
            # remove the space token (e.g. at Taiwan-LCG2) from the SURL info
            full_surl = full_surl[full_surl.index('srm://'):]

        # srm://dcache01.tier2.hep.manchester.ac.uk/pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/
        #testpanda.destDB/testpanda.destDB.604b4fbc-dbe9-4b05-96bb-6beee0b99dee_sub0974647/
        #86ecb30d-7baa-49a8-9128-107cbfe4dd90_0.job.log.tgz
        tolog("putfile: %s" % (putfile))
        tolog("full_surl: %s" % (full_surl))

        # get https surl
        full_http_surl = full_surl.replace("srm://", "https://")

        # get the RSE from ToA
        try:
            _RSE = self.getRSE(surl=putfile)
        except Exception, e:
            tolog("Warning: Failed to get RSE: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_RSE, _RSE)
            tolog("RSE: %s" % (_RSE))

        if testLevel == "1":
            source = "thisisjustatest"

        # determine which timeout option to use
        timeout_option = "--connect-timeout 300 --max-time %d" % (self.timeout)

        sslCert = self.sslCert
        sslKey = self.sslKey
        sslCertDir = self.sslCertDir

        # check htcopy if it is existed or env is set properly
        _cmd_str = 'which htcopy'
        try:
            s, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught: %s (%d, %s)" % (str(e), s, o))
            o = str(e)

        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            o = o.replace('\n', ' ')
            tolog("!!WARNING!!2990!! check PUT command failed. Status=%s Output=%s" % (str(s), str(o)))
            return 999999

        # cleanup the SURL if necessary (remove port and srm substring)
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

            # used lcg-cp options:
            # --srcsetype: specify SRM version
            #   --verbose: verbosity on
            #        --vo: specifies the Virtual Organization the user belongs to
            #          -s: space token description
            #          -b: BDII disabling
            #          -t: time-out
            # (lcg-cr) -l: specifies the Logical File Name associated with the file. If this option is present, an entry is added to the LFC
            #          -g: specifies the Grid Unique IDentifier. If this option is not present, a GUID is generated internally
            #          -d: specifies the destination. It can be the Storage Element fully qualified hostname or an SURL. In the latter case,
            #              the scheme can be sfn: for a classical SE or srm:. If only the fully qualified hostname is given, a filename is
            #              generated in the same format as with the Replica Manager
            # _cmd_str = '%s lcg-cr --verbose --vo atlas -T srmv2 -s %s -b -t %d -l %s -g %s -d %s file:%s' %\
            #           (envsetup, token, self.timeout, lfclfn, guid, surl, fppfn)
            # usage: lcg-cp [-h,--help] [-i,--insecure] [-c,--config config_file]
            #               [-n nbstreams] [-s,--sst src_spacetokendesc] [-S,--dst dest_spacetokendesc]
            #               [-D,--defaultsetype se|srmv1|srmv2] [-T,--srcsetype se|srmv1|srmv2] [-U,--dstsetype se|srmv1|srmv2]
            #               [-b,--nobdii] [-t timeout] [-v,--verbose]  [-V,--vo vo] [--version] src_file  dest_file

            # surl = putfile[putfile.index('srm://'):]
            _cmd_str = '%s htcopy --ca-path %s --user-cert %s --user-key %s "%s?spacetoken=%s"' % (envsetup, sslCertDir, sslCert, sslKey, full_http_surl, token)
            #_cmd_str = '%s lcg-cp --verbose --vo atlas -b %s -U srmv2 -S %s file://%s %s' % (envsetup, timeout_option, token, source, full_surl)
        else:
            # surl is the same as putfile
            _cmd_str = '%s htcopy --ca-path %s --user-cert %s --user-key %s "%s"' % (envsetup, sslCertDir, sslCert, sslKey, full_http_surl)
            #_cmd_str = '%s lcg-cp --vo atlas --verbose -b %s -U srmv2 file://%s %s' % (envsetup, timeout_option, source, full_surl)

        tolog("Executing command: %s" % (_cmd_str))
        ec = -1
        t0 = os.times()
        o = '(not defined)'
        report['relativeStart'] = time()
        report['transferStart'] =  time()
        try:
            ec, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2999!! lcg-cp threw an exception: %s" % (o))
            o = str(e)
        report['validateStart'] = time()
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        tolog("ec = %d, o = %s, len(o) = %d" % (ec, o, len(o)))

        if ec != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(ec, o)
            tolog('!!WARNING!!2990!! put_data failed: Status=%d Output=%s' % (ec, str(o)))

            # check if file was partially transferred, if so, remove it
            _ec = self.removeFile(envsetup, self.timeout, dst_gpfn)
            if _ec == -2:
                pilotErrorDiag += "(failed to remove file) " # i.e. do not retry stage-out

            if "Could not establish context" in o:
                pilotErrorDiag += "Could not establish context: Proxy / VO extension of proxy has probably expired"
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('CONTEXT_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag, surl=full_surl)
            elif "No such file or directory" in o:
                pilotErrorDiag += "No such file or directory: %s" % (o)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('NO_FILE_DIR', report)
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=full_surl)
            elif "globus_xio: System error" in o:
                pilotErrorDiag += "Globus system error: %s" % (o)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.prepareReport('GLOBUS_FAIL', report)
                return self.put_data_retfail(error.ERR_PUTGLOBUSSYSERR, pilotErrorDiag, surl=full_surl)
            else:
                if len(o) == 0 and t >= self.timeout:
                    pilotErrorDiag += "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.prepareReport('CP_TIMEOUT', report)
                    return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag, surl=full_surl)
                else:
                    if len(o) == 0:
                        pilotErrorDiag += "Copy command returned error code %d but no output" % (ec)
                    else:
                        pilotErrorDiag += o
                    self.prepareReport('CP_ERROR', report)
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag, surl=full_surl)

        verified = False

        # try to get the remote checksum with lcg-get-checksum
        remote_checksum = self.lcgGetChecksum(envsetup, self.timeout, full_surl)
        if not remote_checksum:
            # try to grab the remote file info using lcg-ls command
            remote_checksum, remote_fsize = self.getRemoteFileInfo(envsetup, self.timeout, full_surl)
        else:
            tolog("Setting remote file size to None (not needed)")
            remote_fsize = None

        # compare the checksums if the remote checksum was extracted
        tolog("Remote checksum: %s" % str(remote_checksum))
        tolog("Local checksum: %s" % (fchecksum))

        if remote_checksum:
            if remote_checksum != fchecksum:
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(dst_gpfn), remote_checksum, fchecksum)
                tolog("!!WARNING!!1800!! %s" % (pilotErrorDiag))
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
            tolog("Skipped primary checksum verification (remote checksum not known)")

        # if lcg-ls could not be used
        if "/pnfs/" in surl and not remote_checksum:
            # for dCache systems we can test the checksum with the use method
            tolog("Detected dCache system: will verify local checksum with the local SE checksum")
            # gpfn = srm://head01.aglt2.org:8443/srm/managerv2?SFN=/pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....
            path = surl[surl.find('/pnfs/'):]
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
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.prepareReport('NOSUCHFILE', report)
                    return self.put_data_retfail(error.ERR_NOSUCHFILE, pilotErrorDiag, surl=full_surl)
                elif remote_checksum:
                    tolog("Remote checksum: %s" % (remote_checksum))
                else:
                    tolog("Could not get remote checksum")

            if remote_checksum:
                if remote_checksum != fchecksum:
                    pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                     (csumtype, _filename, remote_checksum, fchecksum)
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
            tolog("Local file size: %s" % (fsize))

            if remote_fsize and remote_fsize != "" and fsize != "" and fsize:
                if str(fsize) != remote_fsize:
                    pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                     (_filename, remote_fsize, str(fsize))
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.prepareReport('FS_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=full_surl)
                else:
                    tolog("Remote and local file sizes verified")
                    verified = True
            else:
                tolog("Skipped file size test")

        # was anything verified?
        if not verified:
            # fail at this point
            pilotErrorDiag = "Neither checksum nor file size could be verified (failing job)"
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.prepareReport('NOFILEVERIFICATION', report)
            return self.put_data_retfail(error.ERR_NOFILEVERIFICATION, pilotErrorDiag, surl=full_surl)

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag, full_surl, fsize, fchecksum, self.arch_type
