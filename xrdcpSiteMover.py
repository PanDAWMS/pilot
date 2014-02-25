# xrdcpSiteMover.py

import os
import shutil
import commands
import urllib
import re

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, addToJobSetupScript, getCopyprefixLists, verifySetupCommand, getSiteInformation
from timed_command import timed_command
from config import config_sm
from time import time
from FileStateClient import updateFileState

PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE
CMD_CHECKSUM = config_sm.COMMAND_MD5
ARCH_DEFAULT = config_sm.ARCH_DEFAULT

class xrdcpSiteMover(SiteMover.SiteMover):
    """
    File movers move files between a SE (of different kind) and a local directory
    where all posix operations have to be supported and fast access is supposed
    get_data: SE->local
    put_data: local->SE
    check_space: available space in SE
    This is the Default SiteMover, the SE has to be locally accessible for all the WNs
    and all commands like cp, mkdir, md5checksum have to be available on files in the SE
    E.g. NFS exported file system
    """
    __childDict = {}
    
    copyCommand = "xrdcp"
    checksum_command = "adler32"
    permissions_DIR = PERMISSIONS_DIR
    permissions_FILE = PERMISSIONS_FILE
    arch_type = ARCH_DEFAULT
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path
        tolog("Init is using setup: %s" % (self._setup))

    def get_timeout(self):
        return self.timeout

    def getID(self):
        "returnd SM ID, the copy command used for it"

        return self.copyCommand
    
    def getSetup(self):
        """ Return a proper setup string """

        _setup_str = ""
        if self._setup:
            if not self._setup.endswith(";"):
                self._setup += ";"
            if not "alias" in self._setup:
                _setup_str = "source %s" % self._setup
            else:
                _setup_str = self._setup

        if _setup_str != "":
            tolog("Using setup: %s" % (_setup_str))

        return _setup_str
    
    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        Moves a DS file the local SE (where was put from DDM) to the working directory.
        Performs the copy and, for systems supporting it, checks size and md5sum correctness
        gpfn: full source URL (e.g. method://[host[:port]/full-dir-path/filename - a SRM URL is OK) 
        path: destination absolute path (in a local file system)
        returns the status of the transfer. In case of failure it should remove the partially copied destination
        """
        # The local file is assumed to have a relative path that is the same of the relative path in the 'gpfn'
        # loc_... are the variables used to access the file in the locally exported file system

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        sitename = pdict.get('sitename', '')
        cmtconfig = pdict.get('cmtconfig', '')
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'xrdcp', lfn, guid)

        # get a proper setup
        _setup_str = self.getSetup()

        ec, pilotErrorDiag = verifySetupCommand(error, _setup_str)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        # remove any host and SFN info from PFN path
        src_loc_pfn = self.extractPathFromPFN(gpfn)

        # determine which copy command to use
        cpt = "xrdcp"

        # Do pre-stagin
        copytoolin = readpar('copytoolin')
        # copytoolin = "xrdcp" #PN readpar('copytoolin')
        tolog("xrdcpSiteMover ELN copytoolin  : %s" % (copytoolin))
        if copytoolin != '':
            if copytoolin.find('^') > -1:
                cpt, pstage = copytoolin.split('^')
                if pstage != "" and pstage != 'dummy':
                    # xrdcp is anyway hardcoded below...
                    cmd = "%s %s " % (pstage, src_loc_pfn)
                    rc, rs, pilotErrorDiag = self.copy(cmd, stagein=True)
                    if rc != 0:
                        self.__sendReport('PSTAGE_FAIL', report)
                        return rc, pilotErrorDiag
                    else:
                        tolog("Successfully pre-staged file")

            else:
                cpt = readpar('copytoolin')

        tolog("Site mover will use get command: %s" % (cpt))

        # copyprefixin = 'dcap://ccdcapatlas.in2p3.fr:22125^root://ccsrb15:1094'
        # gpfn = 'srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/...'
        # src_loc_pfn = '/pnfs/in2p3.fr/data/atlas/...'
        # add 'root://ccsrb15:1094' to src_loc_pfn

        copyprefix = readpar('copyprefixin')
        if copyprefix == "":
            copyprefix = readpar('copyprefix')
            tolog("Using copyprefix = %s" % (copyprefix))            
        else:
            tolog("Using copyprefixin = %s" % (copyprefix))

        if copyprefix == "":
            pilotErrorDiag = "Empty copyprefix, cannot continue"
            tolog("!!WARNING!!1777!! %s" % (pilotErrorDiag))
            self.__sendReport('PSTAGE_FAIL', report)
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        # handle copyprefix lists
        pfroms, ptos = getCopyprefixLists(copyprefix)

        if len(pfroms) != len(ptos):
            pilotErrorDiag = "Copyprefix lists not of equal length: %s, %s" % (str(pfroms), str(ptos))
            tolog("!!WARNING!!1777!! %s" % (pilotErrorDiag))
            self.__sendReport('PSTAGE_FAIL', report)
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        # figure out which copyprefix to use (use the PFN to figure out where the file is and then use the appropriate copyprefix)
        # e.g. copyprefix=srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/.. use copyprefix root://eosatlas.cern.ch/ to build the TURL src_loc_pfn
        # full example:
        # Using copyprefixin = srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1
        # TURL=root://eosatlas.cern.ch//eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1

        for (pfrom, pto) in map(None, pfroms, ptos):
            if (pfrom != "" and pfrom != None and pfrom != "dummy") and (pto != "" and pto != None and pto != "dummy"):
                if gpfn[:len(pfrom)] == pfrom or gpfn[:len(pto)] == pto:
                    src_loc_pfn = pto + src_loc_pfn
                    src_loc_pfn = src_loc_pfn.replace('///','//')
                    break

        tolog("PFN=%s" % (gpfn))
        tolog("TURL=%s" % (src_loc_pfn))

        src_loc_filename = lfn # os.path.basename(src_loc_pfn)
        # source vars: gpfn, loc_pfn, loc_host, loc_dirname, loc_filename
        # dest vars: path

        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # protect against bad pfn's
        # src_loc_pfn = src_loc_pfn.replace('///','/')
        src_loc_pfn = src_loc_pfn.replace('//xrootd/','/xrootd/')
        if src_loc_pfn.find('//pnfs') == -1:
            src_loc_pfn = src_loc_pfn.replace('/pnfs','//pnfs')

        # should the root file be copied or read directly by athena?
        directIn = self.checkForDirectAccess(lfn, useCT, workDir, jobId, prodDBlockToken)
        if directIn:
            report['relativeStart'] = None
            report['transferStart'] = None
            self.__sendReport('FOUND_ROOT', report)
            return error.ERR_DIRECTIOFILE, pilotErrorDiag

        # in case fchecksum is not given to this function, attempt to use the md5 option to get it
        dest_file = os.path.join(path, src_loc_filename)
        if fchecksum == 0 or fchecksum == "" or fchecksum == None or fchecksum == "None":
            useMd5Option = True
            cmd = "%s %s -md5 %s %s" % (_setup_str, cpt, src_loc_pfn, dest_file)
        else:
            useMd5Option = False
            cmd = "%s %s %s %s" % (_setup_str, cpt, src_loc_pfn, dest_file)

        # is the md5 option available?
        if useMd5Option:
            cmd_test = "%s %s" % (_setup_str, cpt)
            tolog("Executing test command: %s" % (cmd_test))
            rc, rs = commands.getstatusoutput(cmd_test)
            if rs.find("-md5") > 0:
                tolog("This xrdcp version supports the md5 option")
            else:
                tolog("This xrdcp version does not support the md5 option (checksum test will be skipped)")
                useMd5Option = False
                cmd = "%s %s %s %s" % (_setup_str, cpt, src_loc_pfn, dest_file)

        # add the full stage-out command to the job setup script
        to_script = cmd.replace(dest_file, "`pwd`/%s" % os.path.basename(dest_file))
        to_script = to_script.lstrip(' ') # remove any initial spaces
        if to_script.startswith('/'):
            to_script = 'source ' + to_script
        addToJobSetupScript(to_script, path)

        # transfer the file
        report['transferStart'] = time()
        rc, rs, pilotErrorDiag = self.copy(cmd, stagein=True)
        report['validateStart'] = time()
        if rc != 0:
            self.__sendReport('COPY_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return rc, pilotErrorDiag
        else:
            tolog("Successfully transferred file")

        # get file size from the command output if not known already
        if fsize == 0:
            fsize = self.getFileSize(rs)

        # get checksum from the command output if not known already
        if useMd5Option and fchecksum == 0:
            fchecksum = self.getChecksum(rs)
        else:
            if fchecksum == 0 or fchecksum == None:
                fchecksum = ""
            else:
                tolog("fchecksum = %s" % (fchecksum))

        # get destination (local) file size and checksum 
        ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
        tolog("File info: %d, %s, %s" % (ec, dstfsize, dstfchecksum))
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return ec, pilotErrorDiag

        # compare remote and local file checksum
        if fchecksum != "" and fchecksum != 0 and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(gpfn), fchecksum, dstfchecksum)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")
            
            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        # compare remote and local file size (skip test if remote/source file size is not known)
        if dstfsize != fsize and fsize != 0 and fsize != "":
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s, type1=%s, type2=%s, len1=%s, len2=%s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize), type(dstfsize), type(fsize), len(dstfsize), len(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_file)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """
        Moves the file from the current local directory to a storage element
        source: full path of the file in  local directory
        destinaion: destination SE, method://[hostname[:port]]/full-dir-path/ (NB: no file name)
        Assumes that the SE is locally mounted and its local path is the same as the remote path
        if both fsize and fchecksum (for the source) are given and !=0 these are assumed without reevaluating them
        returns: exitcode, gpfn,fsize, fchecksum
        """

        error = PilotErrors()

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        analyJob = pdict.get('analJob', False)
        dsname = pdict.get('dsname', '')
        sitename = pdict.get('sitename', '')
        cmtconfig = pdict.get('cmtconfig', '')
        extradirs = pdict.get('extradirs', '')
        experiment = pdict.get('experiment', '')
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'xrdcp', lfn, guid)

        # get a proper setup
        _setup_str = self.getSetup()

#        if "CERN" in sitename:
#            _setup_str = "source /afs/cern.ch/project/xrootd/software/setup_stable_for_atlas.sh;"

        # PN, for now
        #_setup_str = ""
        tolog("xrdcpSiteMover put_data using setup: %s" % (_setup_str))

        lfcpath, pilotErrorDiag = self.getLFCPath(analyJob)
        if lfcpath == "":
            self.__sendReport('STAGEOUT_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        ec = 0
        # get the file size and checksum of the local file
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype="adler32")
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag)
        tolog("Local checksum: %s, local file size: %s" % (fchecksum, str(fsize)))

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get all the proper paths
        filename = os.path.basename(source)
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analyJob, token, prodSourceLabel, dsname, filename, scope=scope)
        if ec != 0:
            self.__sendReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        # correct the surl since it might contain the space token and the port info at the beginning
        surl = self.stripListSEs([surl])[0]

        tolog("dst_gpfn: %s" % (dst_gpfn))
        tolog("surl    : %s" % (surl))
        bare_dst_gpfn = dst_gpfn # ie starts with /.. (url and port will be added later, not good for rfmkdir eg)
        dst_loc_pfn = dst_gpfn
        dst_gpfn = surl

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        copyprefix = readpar('copyprefix')
        if copyprefix == "":
            pilotErrorDiag = "Copyprefix not defined"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            self.__sendReport('DEST_PATH_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        # handle copyprefix lists
        pfroms, ptos = getCopyprefixLists(copyprefix)

        if len(pfroms) != len(ptos):
            pilotErrorDiag = "Copyprefix lists not of equal length: %s, %s" % (str(pfroms), str(ptos))
            tolog("!!WARNING!!1777!! %s" % (pilotErrorDiag))
            self.__sendReport('PSTAGE_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        added_copyprefix = False
        if analyJob and prodSourceLabel != "install":
            # Extract the copy prefix
            pfrom, pto = copyprefix.split('^')
            if pto != "dummy" and not "," in pto:
                added_copyprefix = True
                dst_loc_pfn = pto + dst_loc_pfn
# use this later: dst_gpfn = dst_loc_pfn
#                dst_grpn = dst_loc_pfn
                tolog("Added copyprefix to file: %s" % (dst_loc_pfn))
            else:
                tolog("!!WARNING!!1777!! Illegal copyprefix: %s (pto = %s)" % (copyprefix, pto))
        else:
            # get the host name from seopt that matches the space token descriptor
            hostname = self.getSEFromToken(token)
            if hostname == "":
                pilotErrorDiag = "Did not find a matching SE from seopt for space token %s" % (token)
                tolog("!!WARNING!!1777!! %s" % (pilotErrorDiag))
                self.__sendReport('PSTAGE_FAIL', report)
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

            # protect against missing trailing / since that can screw up the test below (both hostname and pfrom must end with a /)
            # (easy to miss in the schedconfig)
            if not hostname.endswith('/'):
                hostname += "/"

            # use the space token matched host name to the corresponding copyprefix to create the destination path
            for (pfrom, pto) in map(None, pfroms, ptos):
                if (pfrom != "" and pfrom != None and pfrom != "dummy") and (pto != "" and pto != None and pto != "dummy"):
                    # protect against missing trailing / since that can screw up the test below
                    if not pfrom.endswith('/'):
                        pfrom += "/"

                    if pfrom == hostname:
                        dst_loc_pfn = pto + dst_loc_pfn
                        dst_loc_pfn = dst_loc_pfn.replace('///','//')
# use this later          dst_gpfn = dst_loc_pfn
##                        dst_grpn = dst_loc_pfn
                        tolog("Added copyprefix to file: %s" % (dst_loc_pfn))
                        added_copyprefix = True
                        break

        if not added_copyprefix:
            pilotErrorDiag = "Did not find a proper copyprefix for the destination path (copyprefix = %s)" % (copyprefix)
            tolog("!!WARNING!!1777!! %s" % (pilotErrorDiag))
            self.__sendReport('PSTAGE_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

#        useMd5Option = True
        useAdlerOption = True

        # on a castor site, the pilot needs to create the SE directories
        if "/castor/" in bare_dst_gpfn and not "CERN" in sitename:
            _cmd_str = '%srfmkdir -p %s' % (_setup_str, os.path.dirname(bare_dst_gpfn))
            tolog("Executing castor command: %s" % (_cmd_str))
            rc, rs = commands.getstatusoutput(_cmd_str)
            if rc != 0:
                pilotErrorDiag = "rfmkdir failed: %s" % (rs)
                tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        cpt = 'xrdcp'
        tolog("Site mover will use put command: %s" % (cpt))

        cmd = "%s %s -f -adler %s %s" % (_setup_str, cpt, source, dst_loc_pfn)

        # is the checksum option available?
        cmd_test = "%s %s" % (_setup_str, cpt)
        tolog("Executing test command: %s" % (cmd_test))
        report['transferStart'] = time()
        rc, rs = commands.getstatusoutput(cmd_test)
        report['validateStart'] = time()
#        if rs.find("-md5") > 0:
        if "-adler" in rs:
#            tolog("This xrdcp version supports the md5 option")
            tolog("This xrdcp version supports the adler option")
        else:
#            tolog("This xrdcp version does not support the md5 option (checksum test will be skipped)")
#            useMd5Option = False
            tolog("This xrdcp version does not support the adler option (checksum test will be skipped)")
            useAdlerOption = False
            cmd = "%s %s %s %s" % (_setup_str, cpt, source, dst_loc_pfn)

        #PN
#        if not ".log." in dst_loc_pfn:
#            cmd = cmd.replace("xrdcp", "xrdcpXXX")

        # transfer the file
        rc, rs, pilotErrorDiag = self.copy(cmd)
        if rc != 0:
            self.__sendReport('COPY_FAIL', report)
            pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out
            return self.put_data_retfail(rc, pilotErrorDiag)
        else:
            tolog("Successfully transferred file")

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # get remote file size from the command output
        # dstfsize = self.getFileSize(rs)

        # get remote checksum from the command output
#        if useMd5Option:
        if useAdlerOption:
            dstfchecksum = self.getChecksum(rs)
        else:
            dstfchecksum = ""

        # compare remote and local file checksum
        if dstfchecksum != fchecksum:
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(dst_gpfn), dstfchecksum, fchecksum)
            pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=dst_gpfn)
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=dst_gpfn)

        tolog("Verified checksum")

        self.__sendReport('DONE', report)

        # remove port and version
        dst_gpfn = self.stripPortAndVersion(dst_gpfn)

        # correct the surl
        if dst_gpfn[:len('root://eosatlas')] == 'root://eosatlas':
            dst_gpfn = dst_gpfn.replace('root://eosatlas', 'srm://srm-eosatlas')
            if "//eos/" in dst_gpfn:
                dst_gpfn = dst_gpfn.replace("//eos/", "/eos/")
            tolog("Updated SURL path for LFC to: %s" % (dst_gpfn))
        else:
            tolog("No need to update SURL for LFC")

        return 0, pilotErrorDiag, dst_gpfn, fsize, fchecksum, ARCH_DEFAULT

    def copy(self, cmd, stagein=False):
        """ Perfom the actual file transfer """

        ec = 0
        pilotErrorDiag = ""
        error = PilotErrors()
        tolog("Executing command: %s" % (cmd))

        try:
            s, telapsed, cout, cerr = timed_command(cmd, self.timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
            s = 1
            o = str(e)
            telapsed = self.timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))

        tolog("Command output: %s" % (o))

        # error code handling
        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (cmd))
            check_syserr(s, o)
            if is_timeout(s):
                pilotErrorDiag = "xrdcp get was timed out after %d seconds" % (telapsed)
                if stagein:
                    ec = error.ERR_GETTIMEOUT
                else:
                    ec = error.ERR_PUTTIMEOUT
            else:
                o = o.replace('\n', ' ')
                pilotErrorDiag = "cp failed with output: ec = %d, output = %s" % (s, o)
                if stagein:
                    ec = error.ERR_STAGEINFAILED
                else:
                    ec = error.ERR_STAGEOUTFAILED

        return ec, o, pilotErrorDiag

    def getFileSize(self, rs):
        """ get the file size from the xrdcp output """
    
        fsize = ""
    
        # get remote file size from the command output
        if rs.find("[xrootd]") >= 0:
            # define the search pattern
            size_pstr = r"root://[\/\.\:\_\-\$\%\#a-zA-Z0-9]+ ([0-9]+)"
            size_pattern = re.compile(size_pstr)
    
            # grab the file size from the output
            _size = re.findall(size_pattern, rs)
            if len(_size) > 0:
                fsize = _size[0]
                tolog("Copy command returned file size: %s" % (fsize))
            else:
                tolog("!!WARNING!!2999!! size search failed: pattern (%s) not found in: %s" % (size_pstr, rs))
                fsize = ""
        else:
            tolog("!!WARNING!!2999!! Unexpected xrdcp output: %s" % (rs))
    
        return fsize

    def getChecksum(self, rs):
        """ get the checksum from the xrdcp output """
    
        fchecksum = ""
    
        # get remote checksum from the command output
        if "[xrootd]" in rs:
            status = False
            # define the search patterns
            if "md5:" in rs:
                checksum_pstr = r"md5: ([a-zA-Z0-9]+)"
                checksum_pattern = re.compile(checksum_pstr)
                status = True
            elif "adler32:" in rs:
                checksum_pstr = r"adler32: ([a-zA-Z0-9]+)"
                checksum_pattern = re.compile(checksum_pstr)
                status = True
            else:
                tolog("!!WARNING!!2999!! Checksum info not found in xrdcp output: %s" % (rs))

            if status:
                # grab the checksum from the output
                _checksum = re.findall(checksum_pattern, rs)
                if len(_checksum) > 0:
                    fchecksum = _checksum[0]

                    # note: there's a bug in xrdcp which will generate non-fixed length adler checksums; checksums can be
                    # of length 7. In that case add a "0" to the beginning of the string
                    if "adler32:" in rs:
                        # verify string size length
                        if len(fchecksum) == 7:
                            tolog("!!WARNING!!1111!! Adding 0 to beginning of checksum (xrdcp returned a length 7 checksum): %s" % (fchecksum))
                            fchecksum = "0" + fchecksum
                        elif len(fchecksum) == 6:
                            tolog("!!WARNING!!1111!! Adding 00 to beginning of checksum (xrdcp returned a length 6 checksum): %s" % (fchecksum))
                            fchecksum = "00" + fchecksum
                        elif len(fchecksum) == 5:
                            tolog("!!WARNING!!1111!! Adding 000 to beginning of checksum (xrdcp returned a length 5 checksum): %s" % (fchecksum))
                            fchecksum = "000" + fchecksum
                        elif len(fchecksum) == 4:
                            tolog("!!WARNING!!1111!! Adding 0000 to beginning of checksum (xrdcp returned a length 4 checksum): %s" % (fchecksum))
                            fchecksum = "0000" + fchecksum

                    tolog("Copy command returned checksum: %s" % (fchecksum))
                else:
                    tolog("!!WARNING!!2999!! checksum search failed: pattern (%s) not found in: %s" % (checksum_pstr, rs))
                    fchecksum = ""
        else:
            tolog("!!WARNING!!2999!! Unexpected xrdcp output: %s" % (rs))

        return fchecksum
        
    def check_space(self, ub):
        """
        Checking space availability:
        1. check DQ space URL
        2. get storage path and check local space availability
        """
        # http://bandicoot.uits.indiana.edu:8000/dq2/space/free
        # http://bandicoot.uits.indiana.edu:8000/dq2/space/total
        # http://bandicoot.uits.indiana.edu:8000/dq2/space/default
        if ub == "" or ub == "None" or ub == None:
            tolog("Using alternative check space function since URL method can not be applied (URL not set)")
            retn = self._check_space(ub)
        else:
            try:
                f = urllib.urlopen(ub + '/space/free')
                ret = f.read()
                retn = int(ret)
                if retn == 0:
                    tolog(ub + '/space/free returned 0 space available, returning 999995')
                    retn = 999995
            except:
                tolog("Using alternative check space function since URL method failed")
                retn = self._check_space(ub)
        return retn

    def _check_space(self, ub):
        """ Checking space of a local directory """

        if self._setup:
            if not "alias" in self._setup:
                _setup_str = "source %s" % self._setup
            else:
                _setup_str = self._setup
        else:
            _setup_str = ''

        fail = 0
        ret = ''
        if ub == "" or ub == "None" or ub == None:
            # seprodpath can have a complex structure in case of space tokens
            # although currently not supported in this site mover, prepare the code anyway
            # (use the first list item only)
            dst_loc_se = self.getDirList(readpar('seprodpath'))[0]
            if dst_loc_se == "":
                dst_loc_se = readpar('sepath')
            if dst_loc_se == "":
                tolog("WARNING: Can not perform alternative space check since sepath is not set")
                return -1
            else:
                tolog("Attempting to use df for checking SE space: %s" % (dst_loc_se))
                return self.check_space_df(dst_loc_se)
        else:
            try:
                f = urllib.urlopen(ub + '/storages/default')
            except:
                tolog('!!WARNING!!2999!! Fetching default storage failed!')
                return -1
            else:
                ret = f.read()

        if ret.find('//') == -1:
            tolog('!!WARNING!!2999!! Fetching default storage failed!')
            fail = -1
        else:
            dst_se = ret.strip()
            # srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv1?SFN=/pnfs/usatlas.bnl.gov/
            if( dst_se.find('SFN') != -1 ):
                s = dst_se.split('SFN=')
                dst_loc_se = s[1]
            else:
                _sentries = dst_se.split('/', 3)
                # 'method://host:port' is it always a ftp server? can it be srm? something else?
                dst_loc_se = '/'+ _sentries[3]

            # Run df to check space availability
            s, o = commands.getstatusoutput('%s df %s' % (_setup_str, dst_loc_se))
            if s != 0:
                check_syserr(s, o)
                tolog("!!WARNING!!2999!! Error in running df: %s" % (o))
                fail = -1
            else:
                # parse Wei's df script (extract the space info)
                df_split = o.split("\n")[1]
                p = r"XROOTD[ ]+\d+[ ]+\d+[ ]+(\S+)[ ]+"

                pattern = re.compile(p)
                available = re.findall(pattern, df_split)
                try:
                    available_space = available[0]
                except:
                    available_space = 999999999

        if fail != 0:
            return fail
        else:
            return available_space

    def getLocalFileInfo(self, fname, csumtype="default"):
        """ returns exit code (0 if OK), file size and checksum """

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the file size
        fsize = self.getLocalFileSize(fname)
        if fsize == "0":
            pilotErrorDiag = "Encountered zero file size for file %s" % (fname)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_ZEROFILESIZE, pilotErrorDiag, 0, 0
        else:
            tolog("Size: %s (type=%s)" % (fsize, type(fsize)))

        # get the checksum
        if csumtype == "adler32":
            fchecksum = self.adler32(fname)
            if fchecksum == '00000001': # "%08x" % 1L
                pilotErrorDiag = "Adler32 failed (returned 1)"
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDADLOCAL, pilotErrorDiag, fsize, 0
        else:
            cmd = 'which %s' % (CMD_CHECKSUM)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            tolog("Using: %s" % o)
            cmd = '%s %s' % (CMD_CHECKSUM, fname)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                o = o.replace('\n', ' ')
                check_syserr(s, o)
                pilotErrorDiag = "Error running checksum command (%s) on local file: %s" % (CMD_CHECKSUM, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_FAILEDMD5LOCAL, pilotErrorDiag, fsize, 0
            fchecksum = o.split()[0]
            tolog("Checksum: %s" % (fchecksum))

        return 0, pilotErrorDiag, fsize, fchecksum

    def getRemoteFileSize(self, fname):
        """ return the file size of the remote file """

        size = 0
        cmd = "stat %s" % (fname)
        tolog("Executing command: %s" % (cmd))
        stat = commands.getoutput(cmd)

        # get the second line in the stat output which contains the size
        try:
            stat_split = stat.split("\n")[1]
        except Exception, e:
            tolog("!!WARNING!!2999!! Failed to execute commands:")
            tolog(".stat: %s" % (stat))
            tolog(".stat_split: %s" % (str(e)))
            size = 0
        else:
            # reg ex search pattern
            pattern = re.compile(r"Size:[ ]+(\d+)")

            # try to find the size in the stat output
            fsize = re.findall(pattern, stat_split)
            try:
                size = fsize[0]
            except:
                tolog("!!WARNING!!2999!! stat command did not return file size")
                size = 0

        return size
    
    def getMover(cls, *args, **kwrds):
        """
        Creates and provides exactly one instance for each required subclass of SiteMover.
        Implements the Singleton pattern
        """

        cl_name = cls.__name__
        if not issubclass(cls, SiteMover):
            log.error("Wrong Factory invocation, %s is not subclass of SiteMover" % cl_name)
        else:
            return cls(*args, **kwrds)
    getMover = classmethod(getMover)
        

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
