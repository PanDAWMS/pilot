import os, re
import commands
from time import time

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, getExperiment
from FileStateClient import updateFileState

# placing the import lfc here breaks compilation on non-lcg sites
# import lfc

class lcgcp2SiteMover(SiteMover.SiteMover):
    """ SiteMover that uses lcg-cp for both get and put """
    # no registration is done
    copyCommand = "lcg-cp2"
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

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'lcg2', lfn, guid)

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

        # can not test filesize and checksum if remote values are not known
        if fsize == 0 or fchecksum == 0:
            tolog("!!WARNING!!2999!! Remote file size/checksum not known: %s/%s" % (fsize, fchecksum))

        # Maybe be a comma list but take first always
        # (Remember that se can be a list where the first is used for output but any can be used for input)
        se = readpar('se').split(",")[0]
        _dummytoken, se = self.extractSE(se)
        tolog("Using SE: %s" % (se))

        # se = srm://head01.aglt2.org:8443/srm/managerv2?SFN=
        # for srm protocol, use the full info from 'se'
        if getfile[:3] == "srm":
            try:
                # e.g. tmp = ['srm:', '', 'head01.aglt2.org', 'pnfs/aglt2.org/rucio/panda/dis/08/...']
                tmp = getfile.split('/',3)[2]
            except Exception, e:
                tolog('!!WARNING!!2999!! Could not extract srm protocol for replacement, keeping getfile variable as it is: %s (%s)' %\
                      (getfile, str(e)))
            else:
                # replace srm with 'srm://head01.aglt2.org:8443/srm/managerv2?SFN=' if not there already
                if not '?SFN=' in getfile:
                    # srm = 'srm://head01.aglt2.org'
                    srm = 'srm://' + tmp

                    # does seopt contain any matching srm's?
                    sematch = self.getSEMatchFromSEOpt(srm)
                    if sematch != "":
                        getfile = getfile.replace(srm, sematch)
                        tolog("Replaced %s with %s (from seopt) in getfile: %s" % (srm, sematch, getfile))
                    else:
                        getfile = getfile.replace(srm, se)
                        tolog("Replaced %s with %s (from se) in getfile: %s" % (srm, se, getfile))
                else:
                    tolog("Found SFN part in getfile: %s" % (getfile))

                    # add port number from se to getfile if necessary
                    getfile = self.addPortToPath(se, getfile)

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

        # determine which timeout option to use
        if self.isNewLCGVersion("%s lcg-cp" % (envsetup)):
            timeout_option = "--srm-timeout=%d --connect-timeout=300 --sendreceive-timeout=%d" % (self.timeout, self.timeout)
        else:
            timeout_option = "-t %d" % (self.timeout)

        # used lcg-cp options:
        # --vo: specifies the Virtual Organization the user belongs to
        #   -b: no bdii
        #PN
        _cmd_str = '%s lcg-cp --verbose --vo atlas -b %s -T srmv2 %s file://%s' % (envsetup, timeout_option, getfile, fullname)
#        if ".lib." in getfile:
#            _cmd_str = _cmd_str.replace("XXX","")

        tolog("Executing command: %s" % (_cmd_str))
        # getfile = gsiftp://umfs02.grid.umich.edu/atlas/data08/dq2/other/D/DBRelease-3.1.1.tar.gz
        # getfile = srm://head01.aglt2.org:8443/srm/managerv2?SFN=/pnfs/aglt2.org/dq2/panda/dis/08/06/04/panda.64d403f5-adae-42f8-8614-1fc044eb85ea_dis12076725/misal1_mc12.005802.JF17_pythia_jet_filter.digit.RDO.v12000601_tid008610._11639.pool.root.1
        s = -1
        o = '(not defined)'
        t0 = os.times()
        report['relativeStart'] = time()
        report['transferStart'] = time()
        try:
            s, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught by get_data(): %s" % (str(e)))
            o = str(e)
        report['validateStart'] = time()
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s: %s" % (t, o))

        if s == 0:
            tolog("get_data succeeded")
        else:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            tolog("!!WARNING!!2990!! get_data failed. Status=%s Output=%s" % (s, str(o)))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(fullname)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            if "globus_xio:" in o:
                pilotErrorDiag = "Globus system error: %s" % (o)
                tolog("Globus system error encountered")
                self.prepareReport('GLOBUS_FAIL', report)
                return error.ERR_GETGLOBUSSYSERR, pilotErrorDiag
            elif "No space left on device" in o:
                pilotErrorDiag = "No available space left on local disk: %s" % (o)
                tolog("No available space left on local disk")
                self.prepareReport('NO_SPACE', report)
                return error.ERR_NOLOCALSPACE, pilotErrorDiag
            elif "No such file or directory" in o:
                if "DBRelease" in getfile:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (getfile)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.prepareReport('NO_DBREL', report)
                    return error.ERR_MISSDBREL, pilotErrorDiag
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (getfile)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.prepareReport('NO_FILE', report)
                    return error.ERR_NOSUCHFILE, pilotErrorDiag
            else:
                if t >= self.timeout:
                    pilotErrorDiag = "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.prepareReport('GET_TIMEOUT', report)
                    return error.ERR_GETTIMEOUT, pilotErrorDiag
                else:
                    if len(o) == 0:
                        pilotErrorDiag = "Copy command returned error code %d but no output" % (s)
                    else:
                        pilotErrorDiag = o
                    self.prepareReport('COPY_ERROR', report)
                    return error.ERR_STAGEINFAILED, pilotErrorDiag

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if fsize != 0 or fchecksum != 0:
            loc_filename = lfn
            dest_file = os.path.join(path, loc_filename)

            # get remote file size and checksum
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
            if ec != 0:
                self.prepareReport('FILE_INFO_FAIL', report)

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
                self.prepareReport('WRONG_SIZE', report)

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

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        # function is based on dCacheSiteMover put function

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        alt = pdict.get('alt', False)
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

        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'lcg2', lfn, guid)

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
        if alt:
            # use a cvmfs setup for stage-out to alternative SE
            envsetup = si.getLocalEMISetup()
            if envsetup[-1] != ";":
                envsetup += "; "
        else:
            envsetup = self.getEnvsetup(alt=alt)

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.prepareReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag)

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
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename, scope=scope, alt=alt, sitemover=self) # quick workaround
        if ec != 0:
            self.prepareReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag, surl=dst_gpfn)

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
        except Exception, e:
            tolog("Warning: Failed to get RSE: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_RSE, _RSE)
            tolog("RSE: %s" % (_RSE))

        if testLevel == "1":
            source = "thisisjustatest"

        # determine which timeout option to use
        if self.isNewLCGVersion("%s lcg-cp" % (envsetup)):
            timeout_option = "--srm-timeout=%d --connect-timeout=300 --sendreceive-timeout=%d" % (self.timeout, self.timeout)
        else:
            timeout_option = "-t %d" % (self.timeout)

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
            _cmd_str = '%s lcg-cp --verbose --vo atlas -b %s -U srmv2 -S %s file://%s %s' % (envsetup, timeout_option, token, source, full_surl)
        else:
            # surl is the same as putfile
            _cmd_str = '%s lcg-cp --vo atlas --verbose -b %s -U srmv2 file://%s %s' % (envsetup, timeout_option, source, full_surl)

        #PN
#        if not ".log." in full_surl and not alt and not analysisJob:
#            _cmd_str = _cmd_str.replace("lcg-cp", "lcg-cpXXX")

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
        tolog("ec = %d, len(o) = %d" % (ec, len(o)))

        if ec != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(ec, o)
            tolog('!!WARNING!!2990!! put_data failed: Status=%d Output=%s' % (ec, str(o)))

            # check if file was partially transferred, if so, remove it
            _ec = self.removeFile(envsetup, self.timeout, full_surl, nolfc=True)
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
