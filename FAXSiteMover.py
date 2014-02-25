# FAXSiteMover.py

import os
import shutil
import commands
import urllib
import re

from xrdcpSiteMover import xrdcpSiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, getSiteInformation, extractPattern
from timed_command import timed_command
from config import config_sm
from time import time
from FileStateClient import updateFileState

PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE
CMD_CHECKSUM = config_sm.COMMAND_MD5
ARCH_DEFAULT = config_sm.ARCH_DEFAULT

class FAXSiteMover(xrdcpSiteMover):
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
    
    copyCommand = "fax"
    checksum_command = "adler32"
    arch_type = ARCH_DEFAULT
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path
        if self._setup != "":
            tolog("WARNING: Init is asked to use setup: %s (WILL BE IGNORED SINCE USING LOCAL ROOT SETUP)" % (self._setup))
        else:
            tolog("No init setup is specified (would have been ignored anyway)")

    def getLocalROOTSetup(self):
        """ Build command to prepend the xrdcp command [xrdcp will in general not be known in a given site] """

        cmd = 'export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; '
        cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet; '
        cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalROOTSetup.sh --rootVersion ${rootVersionVal} --skipConfirm; '
        # cmd += 'localSetupROOT --skipConfirm; '

        return cmd

    def getGlobalPathsFileName(self, dsname):

        # protect against trailing /-signs in dsname
        if dsname.endswith('/'):
            fdsname = dsname[:-1]
        else:
            fdsname = dsname

        return 'global_paths-%s.txt' % (fdsname)

    def getGlobalFilePaths(self, surl, dsname):
        """ Get the global file paths using to_native_lfn() [dsname needed] or Rucio naming convension [surl needed to extract the scope] """

        #tolog("Guessing the global path using to_native_lfn()..")

        # this method will in fact only ever return a single path, but keep 'paths' as a list for consistency with getGlobalFilePathsDQ2()
        paths = []

        # get the global redirector
        # correct the redirector in case the protocol and/or trailing slash are missing
        redirector = self.updateRedirector(readpar('faxredirector'))

        # use the proper Rucio method to generate the path if possible (if scope is present in the SURL)
        scope = extractPattern(surl, r'\/rucio\/(.+)\/[a-zA-Z0-9]{2}\/[a-zA-Z0-9]{2}\/')
        if scope != "":
            # for Rucio convension details see https://twiki.cern.ch/twiki/bin/view/AtlasComputing/MovingToRucio
            native_path = "/atlas/rucio/" + scope + ":"
        else:
            # get the pre-path
            native_path = self.to_native_lfn(dsname, 'DUMMYLFN')
            native_path = native_path.replace('DUMMYLFN', '') # the real lfn will be added by the caller

            # remove the /grid substring
            native_path = native_path.replace('/grid', '')

        # construct the global path
        paths.append(redirector + native_path)

        tolog("Will use global path: %s" % (paths[0]))

        return paths

    def updateRedirector(self, redirector):
        """ Correct the redirector in case the protocol and/or trailing slash are missing """

        if not redirector.startswith("root://"):
            redirector = "root://" + redirector
            tolog("Updated redirector for missing protocol: %s" % (redirector))
        if not redirector.endswith("/"):
            redirector = redirector + "/"
            tolog("Updated redirector for missing trailing /: %s" % (redirector))

        return redirector

    def getGlobalFilePathsDQ2(self, dsname):
        """ Get the global file paths using dq2-list-files """

        paths = []

        if dsname == "":
            tolog("!!WARNING!!3333!! Dataset not defined")
            return paths

        filename = self.getGlobalPathsFileName(dsname)
        if os.path.exists(filename):
            try:
                f = open(filename, 'r')
            except OSError, e:
                tolog("!!WARNING!!3333!! Could not open global paths file: %s (will attempt to recreate it)" % (e))
            else:
                p = f.read()
                if p != "":
                    tolog("Cache detected (reading global paths from file)")
                    paths = p.split("\n")
                f.close()

        # if a proper file did not exist already, create and populate it
        if paths == []:
            redirector = readpar('faxredirector') # 'root://glrd.usatlas.org/'
            if redirector != "":
                # correct the redirector in case the protocol and/or trailing slash are missing
                redirector = self.updateRedirector(redirector)

                cmd = 'export STORAGEPREFIX=%s; ' % (redirector)
                cmd += 'dq2-list-files -p %s' % (dsname)

                try:
                    tolog("Executing command: %s" % (cmd))
                    s, telapsed, cout, cerr = timed_command(cmd, self.timeout)
                except Exception, e:
                    tolog("!!WARNING!!3333!! timed_command() threw an exception: %s" % str(e))
                    s = 1
                    output = str(e)
                    telapsed = self.timeout
                else:
                    output = cout + cerr
                    tolog("Elapsed time: %d" % (telapsed))
                # a lot of output: tolog("Command output: %s" % (output))

                if self.verifyGlobalPaths(output):
                    paths = output.split("\n")

                    # save the paths for later use (for the next file if necessary)
                    try:
                        f = open(filename, "w")
                    except OSError, e:
                        tolog("!!WARNING!!3333!! Could not open global paths file: %s (will attempt to recreate it)" % (e))
                    else:
                        f.write(output)
                        f.close()
                else:
                    tolog("!!WARNING!!3334!! Could not verify global paths")
            else:
                tolog("!!WARNING!!3332!! Can not get global paths without a FAX redirector (set schedconfig.faxredirector)")

        return paths

    def verifyGlobalPath(self, path, verbose=True):
        """ Verify a global path (make sure the path begins with the root file protocol) """
        # NOTE: per file check

        status = False
        protocol = 'root://'

        if path != "":
            if len(path) > len(protocol):
                if path[:len(protocol)] == protocol:
                    # path verified
                    status = True
                    if verbose:
                        tolog("Global path verified: %s" % (path))
                else:
                    tolog("!!WARNING!!3335!! Junk path detected in dq2-list-files output: %s (cannot use path)" % (path))
            else:
                tolog("!!WARNING!!3336!! Unexpected command output: %s" % (path))
        else:
            tolog("!!WARNING!!3337!! No global path found")

        return status

    def verifyGlobalPaths(self, output):
        """ Verify the global paths (make sure the output begins with the root file protocol) """
        # NOTE: this is not a per file check but an integration check to make sure the output is not garbish
        #       individual files will be verified as they are needed

        return self.verifyGlobalPath(output, verbose=False)

    def findGlobalFilePath(self, surl, dsname):
        """ Find the global path for the given file"""

        global_path = ""
        filename = os.path.basename(surl)

        # should dq2-list-files be used? If not, use to_native_lfn() directly to guess the path
        useDQ2 = False

        if useDQ2:
            # get the global file paths from file/DQ2
            paths = self.getGlobalFilePathsDQ2(dsname)

            if paths != []:
                # locate the global path
                for path in paths:
                    if filename in path:
                        # does the file path begin with 'root://'?
                        if self.verifyGlobalPath(path, verbose=True):
                            global_path = path
                            break
            else:
                # abort
                tolog("!!WARNING!!3333!! Failed to get global file path")
        else:
            # get the global file paths from file/DQ2
            paths = self.getGlobalFilePaths(surl, dsname)

            if paths[0][-1] == ":": # this is necessary to prevent rucio paths having ":/" as will be the case if os.path.join is used
                global_path = paths[0] + filename
            else: # for old style paths not using the ":" separator
                global_path = os.path.join(paths[0], filename)

        return global_path

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """
        Moves a DS file from a remote SE to the working directory.
        Performs the copy and, for systems supporting it, checks size and md5sum correctness
        gpfn: full source URL (e.g. method://[host[:port]/full-dir-path/filename) IGNORED HERE, will use dq-list-files to get it 
        path: destination absolute path (in a local file system)
        returns the status of the transfer. In case of failure it should remove the partially copied destination
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        guid = pdict.get('guid', '')
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        dsname = pdict.get('dsname', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        prodDBlockToken = pdict.get('access', '')

        # get the site information object
        tolog("get_data: experiment=%s" % (experiment))
        si = getSiteInformation(experiment)

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'fax', lfn, guid)

        src_loc_filename = lfn # os.path.basename(src_loc_pfn)
        # source vars: gpfn, loc_pfn, loc_host, loc_dirname, loc_filename
        # dest vars: path

        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # should the root file be copied or read directly by athena? (note: this section is necessary in case FAX is used as primary site mover)
        directIn = self.checkForDirectAccess(lfn, useCT, workDir, jobId, prodDBlockToken)
        if directIn:
            report['relativeStart'] = None
            report['transferStart'] = None
            self.__sendReport('FOUND_ROOT', report)
            return error.ERR_DIRECTIOFILE, pilotErrorDiag

        # local destination path
        dest_file = os.path.join(path, src_loc_filename)

        # the initial gpfn is ignored since the pilot will get it from the global redirector
        # however, the lfn can differ e.g. for files the has the __DQ2-* bit in it. In that case
        # the global redirector will not give the correct name, and the pilot need to correct for it
        # so better to use the lfn taken from the initial gpfn right away
# warning: tests at CERN has shown that this is not true. the global redirector will not find a file with __DQ2- in it
        initial_lfn = os.path.basename(gpfn)
        tolog("Initial LFN=%s" % (initial_lfn))

        # get the global path (likely to update the gpfn/SURL)
        tolog("SURL=%s" % (gpfn))
        gpfn = self.findGlobalFilePath(gpfn, dsname)
        if gpfn == "":
            ec = error.ERR_STAGEINFAILED
            pilotErrorDiag = "Failed to get global paths for FAX transfer"
            tolog("!!WARNING!!3330!! %s" % (pilotErrorDiag))
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        tolog("GPFN=%s" % (gpfn))
        global_lfn = os.path.basename(gpfn)
        if global_lfn != initial_lfn:
#            tolog("WARNING: Global LFN not the same as the initial LFN. Will try to use the initial LFN")
            tolog("WARNING: Global LFN not the same as the initial LFN. Will use the global LFN")
#            gpfn = gpfn.replace(global_lfn, initial_lfn)
#            tolog("Updated GPFN=%s" % (gpfn))

        # setup ROOT locally
        _setup_str = self.getLocalROOTSetup()

        # define the copy command
        cmd = "%s xrdcp -d 1 -f %s %s" % (_setup_str, gpfn, dest_file)

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
        if fchecksum == 0:
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
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize))
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

        # NOTE: THIS METHOD IS CURRENTLY NOT REQUIRED

        error = PilotErrors()

        pilotErrorDiag = "Put function not implemented"
        tolog("!!FAILED!!2222!! %s" % (pilotErrorDiag))
        return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

    def __sendReport(self, state, report):
        tolog("Skipping sending tracing report during testing")
        pass

if __name__ == '__main__':

    f = FAXSiteMover()

    gpfn = "nonsens_gpfn"
    lfn = "AOD.310713._000004.pool.root.1"
    path = os.getcwd()
    fsize = "4261010441"
    fchecksum = "9145af38"
    dsname = "data11_7TeV.00177986.physics_Egamma.merge.AOD.r2276_p516_p523_tid310713_00"
    report = {}

    #print f.getGlobalFilePaths(dsname)
    #print f.findGlobalFilePath(lfn, dsname)
    #print f.getLocalROOTSetup()

    print f.get_data(gpfn, lfn, path, fsize=fsize, fchecksum=fchecksum, dsname=dsname, report=report)
    
