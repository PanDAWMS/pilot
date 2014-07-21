# objectstoreSiteMover.py

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

class objectstoreSiteMover(xrdcpSiteMover):
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
    
    copyCommand = "objectstore"
    checksum_command = "adler32"
    arch_type = ARCH_DEFAULT
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path
        if self._setup != "":
            tolog("WARNING: Init is asked to use setup: %s (WILL BE IGNORED SINCE USING LOCAL ROOT SETUP)" % (self._setup))
        else:
            tolog("No init setup is specified (would have been ignored anyway)")

    def getLocalROOTSetup(self, si):
        """ Build command to prepend the xrdcp command [xrdcp will in general not be known in a given site] """

        return si.getLocalROOTSetup()

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
        jobId = pdict.get('jobId', '')
        dsname = pdict.get('dsname', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        prodDBlockToken = pdict.get('access', '')

        # get the site information object
        si = getSiteInformation(experiment)

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'objectstore', lfn, guid)

        # local destination path
        dest_file = os.path.join(path, lfn)

        # setup ROOT locally
        _setup_str = self.getLocalROOTSetup(si)

        # define the copy command
        cmd = "%s xrdcp -adler  -f %s %s" % (_setup_str, gpfn, dest_file)

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
            fsize = int(self.getFileSize(rs))

        # get checksum from the command output if not known already
        if fchecksum == 0:
            fchecksum = self.getChecksum(rs)
        else:
            if fchecksum == 0 or fchecksum == None:
                fchecksum = ""
            else:
                tolog("fchecksum = %s" % (fchecksum))

        # get destination (local) file size and checksum 
        ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype='adler32')
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
        dstfsize = int(dstfsize)
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
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        logPath = pdict.get('logPath', '')
        experiment = pdict.get('experiment', '')

        # get the site information object
        si = getSiteInformation(experiment)

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'fax', lfn, guid)
        report['filesize'] = fsize

        # get the DQ2 site name from ToA
        try:
            if logPath != "":
                surl = logPath
            else:
                surl = os.path.join(destination, lfn)
            tolog("Using SURL=%s to identify the DQ2 site name" % (surl))
            _dq2SiteName = self.getDQ2SiteName(surl=source)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        # setup ROOT locally
        _setup_str = self.getLocalROOTSetup(si)

        cmd = "%s xrdcp -f  -adler %s %s" % (_setup_str, source, surl)

        report['transferStart'] = time()

        # transfer the file
        rc, rs, pilotErrorDiag = self.copy2(cmd)
        if rc != 0:
            self.__sendReport('COPY_FAIL', report)
            pilotErrorDiag += " (failed to remove file) " # i.e. do not retry stage-out
            return self.put_data_retfail(rc, pilotErrorDiag)
        else:
            tolog("Successfully transferred file")

        dstfchecksum = self.getChecksum(rs)
        tolog("local checksum: %s" % (fchecksum))
        tolog("remote checksum: %s" % (dstfchecksum))

        if fchecksum != "" and fchecksum != 0 and dstfchecksum != fchecksum:
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             ('adler32', os.path.basename(gpfn), fchecksum, dstfchecksum)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag


        return 0, pilotErrorDiag, surl, fsize, fchecksum, ARCH_DEFAULT

    def copy2(self, cmd):
        ec, output = commands.getstatusoutput(cmd)
        tolog("ec = %d" % (ec))
        tolog("output = %s" % (output))
        if ec != 0:
            pilotErrorDiag = "Stage-out failed: %s" % (output)
        else:
            pilotErrorDiag = ""
        return ec, output, pilotErrorDiag

    def __sendReport(self, state, report):
        tolog("Skipping sending tracing report during testing")
        pass

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

if __name__ == '__main__':

    f = objectstoreSiteMover()

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

    #path = "root://atlas-objectstore.cern.ch//atlas/eventservice/2181626927" # + your .root filename"

    #source = "/bin/hostname"
    #dest = "root://eosatlas.cern.ch//eos/atlas/unpledged/group-wisc/users/wguan/"
    #lfn = "NTUP_PHOTON.01255150._000001.root.1"
    #localSize = 17848
    #localChecksum = "89b93830"
    #print f.put_data(source, dest, fsize=localSize, fchecksum=localChecksum, prodSourceLabel='ptest', experiment='ATLAS', report =report, lfn=lfn, guid='aa8ee1ae-54a5-468b-a0a0-41cf17477ffc')

    #gpfn = "root://eosatlas.cern.ch//eos/atlas/unpledged/group-wisc/users/wguan/NTUP_PHOTON.01255150._000001.root.1"
    #lfn = "NTUP_PHOTON.01255150._000001.root.1"
    #tmpDir = "/tmp/"
    #localSize = 17848
    #localChecksum = "89b93830"
    #print f.get_data(gpfn, lfn, tmpDir, fsize=localSize, fchecksum=localChecksum, experiment='ATLAS', report =report, guid='aa8ee1ae-54a5-468b-a0a0-41cf17477ffc')

    gpfn = "root://atlas-objectstore.cern.ch//atlas/logs/2188358564/7fbfd7e1-0dee-479f-a702-e46e09b511a6_0.job.log.tgz"
    lfn = "7fbfd7e1-0dee-479f-a702-e46e09b511a6_0.job.log.tgz"
    tmpDir = "/tmp/"
    localSize = 267528
    localChecksum = "b7ab96cc"
    print f.get_data(gpfn, lfn, tmpDir, fsize=localSize, fchecksum=localChecksum, experiment='ATLAS', report =report, guid='4a1736f5-5e16-4383-9a8e-748ed9fcb6a6')

