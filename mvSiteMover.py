import os
import shutil
import commands
import re
import urllib

import SiteMover
from futil import *
from pUtil import tolog, readpar, getSiteInformation
from PilotErrors import PilotErrors
from config import config_sm

PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE

# Default archival type
ARCH_DEFAULT = config_sm.ARCH_DEFAULT

class mvSiteMover(SiteMover.SiteMover):
    """
    File movers copy files between a SE (of different kind) and a local directory
    the mvSiteMover creates a symbolic link from a local dir to the local work dir
    for the input files. Output files are moved from the local work dir to the local dir
    get_data: local disk -> local work dir [symbolic link]
    put_data: local work dir -> local disk
    """
    
    copyCommand = "mv"
    checksum_command = "adler32"
    has_mkdir = True
    has_df = True
    has_getsize = True
    has_md5sum = True
    has_chmod = True
    permissions_DIR = PERMISSIONS_DIR
    permissions_FILE = PERMISSIONS_FILE
    arch_type = ARCH_DEFAULT
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def getChecksumCommand(self):
        """ return the checksum command to be used with this site mover """
        return self.checksum_command

    def getID(self):
        """ returnd SM ID, the copy command used for it """
        return self.copyCommand
    
    def getSetup(self):
        """ return the setup string for the copy command used by the mover """
        return self._setup
    
    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """        
        Perform the move and, check size and md5sum correctness.
        Parameters are: 
        gpfn -- full source URL (e.g. method://[host[:port]/full-dir-path/filename - a SRM URL is OK) - NOT USED (pinitdir replaces it)
        path -- destination absolute path (in a local file system). It is assumed to be there. get_data returns an error if the path is missing
        Return the status of the transfer. In case of failure it should remove the partially copied destination
        """
        # The local file is assumed to have a relative path that is the same of the relative path in the 'gpfn'
        # loc_... are the variables used to access the file in the locally exported file system
        # source vars: gpfn, loc_pfn, loc_host, loc_dirname, loc_filename
        # dest vars: path

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        pilot_initdir = pdict.get('pinitdir', '')
        experiment = pdict.get('experiment', "ATLAS")

        # get the site information object
        si = getSiteInformation(experiment)

        if si.isTier3():
            inputDir = os.path.dirname(gpfn)
        else:
            inputDir = pdict.get('inputDir', '')
        if inputDir == "":
            tolog("Get function will use pilot launch dir as input file dir: %s" % (pilot_initdir))
            inputDir = pilot_initdir
        else:
            tolog("Get function will use requested input file dir: %s" % (inputDir))

        if inputDir == "":
            pilotErrorDiag = "Input dir not set (can not figure out where the input files are)"
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        src_loc_pfn = os.path.join(inputDir, lfn)
        src_loc_filename = lfn
        dest_file = os.path.join(path, src_loc_filename)

        # verify that the file exists
        if not os.path.exists(src_loc_pfn):
            pilotErrorDiag = "No such file or directory: %s" % (src_loc_pfn)
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            if src_loc_pfn.find("DBRelease") >= 0:
                ec = error.ERR_MISSDBREL
            else:
                ec = error.ERR_NOSUCHFILE
            return ec, pilotErrorDiag

        # make a symbolic link to the input file in the job work dir
        cmd = "ln -s %s %s" % (src_loc_pfn, dest_file)
        tolog("Executing command: %s" % (cmd))
        ec, rv = commands.getstatusoutput(cmd)
        if ec != 0:
            pilotErrorDiag = "Error linking the file: %d, %s" % (ec, rv)
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """
        Move the file from the current local directory to the local pilot init dir

        Parameters are:
        source -- full path of the file in  local directory
        destinaion -- destination SE, method://[hostname[:port]]/full-dir-path/ (NB: no file name) NOT USED (pinitdir is used instead)
        fsize -- file size of the source file (evaluated if 0)
        fchecksum -- MD5 checksum of the source file (evaluated if 0)
        pdict -- to allow additional parameters that may make sense with specific movers
        
        Assume that the pilot init dir is locally mounted and its local path is the same as the remote path
        if both fsize and fchecksum (for the source) are given and !=0 these are assumed without reevaluating them
        returns: exitcode, pilotErrorDiag, gpfn, fsize, fchecksum
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        DN = pdict.get('DN', '')
        dsname = pdict.get('dsname', '')
        analJob = pdict.get('analJob', False)
        sitename = pdict.get('sitename', '')
        testLevel = pdict.get('testLevel', '0')
        pilot_initdir = pdict.get('pinitdir', '')
        experiment = pdict.get('experiment', "ATLAS")

        # get the site information object
        si = getSiteInformation(experiment)

        # are we on a tier 3?
        if si.isTier3():
            outputDir = self.getTier3Path(dsname, DN)
            tolog("Writing output on a Tier 3 site to: %s" % (outputDir))

            # create the dirs if they don't exist
            try:
                self.mkdirWperm(outputDir)
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not create dir: %s, %s" % (outputDir, str(e)))
        else:
            outputDir = pdict.get('outputDir', '')
        if outputDir == "":
            tolog("Put function will use pilot launch dir as output file dir: %s" % (pilot_initdir))
            outputDir = pilot_initdir
        else:
            if not os.path.isdir(outputDir):
                pilotErrorDiag = "Output directory does not exist: %s" % (outputDir)
                tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            else:
                tolog("Put function will use requested output file dir: %s" % (outputDir))

        if outputDir == "":
            pilotErrorDiag = "Pilot init dir not set (can not figure out where the output files should be moved to)"
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                return self.put_data_retfail(ec, pilotErrorDiag)

        dst_loc_sedir = outputDir
        filename = os.path.basename(source)
        dst_loc_pfn = os.path.join(dst_loc_sedir, filename)

        # for CERNVM, use dst_loc_sedir as a starting point for creating a directory structure
        if sitename == "CERNVM":
            # NOTE: LFC registration is not done here but some of the LFC variables are used to find out
            # the disk path so the code has to be partially repeated here

            lfcpath, pilotErrorDiag = self.getLFCPath(analJob)
            if lfcpath == "":
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            else:
                tolog("Got LFC path: %s" % (lfcpath))

            dst_loc_sedir, _dummy = self.getLCGPaths(outputDir, dsname, filename, lfcpath)
            tolog("Got LCG paths: %s" % (dst_loc_sedir))

            # create the sub directories
            try:
                self.mkdirWperm(dst_loc_sedir)
            except Exception, e:
                pilotErrorDiag = "Could not create dir: %s, %s" % (dst_loc_sedir, e)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            else:
                tolog("Successfully created sub-directories: %s" % (dst_loc_sedir))
        else:
            dst_loc_sedir = outputDir
                
        try:
            if testLevel == "0":
                cmd = "mv %s %s" % (source, dst_loc_pfn)
                tolog("Executing command: %s" % (cmd))
                ec, rv = commands.getstatusoutput(cmd)
                if ec != 0:
                    pilotErrorDiag = "Error copying the file: %d, %s" % (ec, rv)
                    tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            else:
                # dummy transfer to simulate transfer error
                ec, rv = commands.getstatusoutput("mv %s %s" % ("thisisjustatest", dst_loc_pfn))
                if ec != 0:
                    pilotErrorDiag = "Error copying the file: %d, %s" % (ec, rv)
                    tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            os.chmod(dst_loc_pfn, PERMISSIONS_FILE)
        except OSError, e:
            pilotErrorDiag = "OSError while copying the file (check directory permissions): %s" % str(e)
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            try:
                path = os.path.dirname(dst_loc_pfn)
                cmd = "ls -lF %s" % (path)
                tolog("Executing command: %s" % (cmd))
                output = commands.getoutput(cmd)
                tolog("\n%s" % (output))
                path = os.path.dirname(path)
                cmd = "ls -lF %s" % (path)
                tolog("Executing command: %s" % (cmd))
                output = commands.getoutput(cmd)
                tolog("\n%s" % (output))
            except Exception, e:
                tolog("Caught exception: %s" % (e))
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        except IOError, e:
            pilotErrorDiag = "IOError copying the file: %s" % str(e)
            tolog('!!WARNING!!2100!! %s' % (pilotErrorDiag))
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        return 0, pilotErrorDiag, dst_loc_pfn, fsize, fchecksum, ARCH_DEFAULT
