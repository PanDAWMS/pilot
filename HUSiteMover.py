#HUSiteMover.py
"""Base class of site 
"""
import os
import shutil
import commands
import re
import urllib

from futil import *
import SiteMover
from PilotErrors import PilotErrors
#from ErrorCodes import *
from pUtil import tolog, readpar
#from config import config_sm
from timed_command import timed_command
from time import time

class HUSiteMover(SiteMover.SiteMover):
    """Custom SiteMover for Harvard University
    it uses a custom ssh tunnel prepared by Saul. Because of network security issues ports for GridFtp or SRM could not be opened
    ls, mkdir and file size checks are supported
    md5sum is not supported (Error running md5sum: md5sum: /pnfs/.../myfile: Input/output error)
    """
    copyCommand = "necp"
    checksum_command = "md5sum"
    has_mkdir = False #True
    has_df = False
    has_getsize = False #True
    has_md5sum = False
    has_chmod = False #True
    has_timeout = True
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path
    
    def get_timeout(self):
        return self.timeout

    def _check_space(self, ub):
        """necp specific space verification.
        There is no way at the moment to verify necp space availability (remote SE)
        """
        return 999999
        
    def get_file_size(directory, filename):
        return 0
    get_file_size = staticmethod(get_file_size)

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ The local file (local access to the dCache file) is assumed to have a relative path
        that is the same of the relative path in the 'gpfn'
        loc_... are the variables used to access the file in the locally exported file system
        TODO: document GPFN format
        TODO: document better constraint
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'HU'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = envsetup

        try:
            timeout = pdict['timeout']
        except:
            timeout = 5*3600

        if( gpfn.find('SFN') != -1 ):
            s = gpfn.split('SFN=')
            loc_pfn = s[1]
        else:
            _tmp = gpfn.split('/', 3)
            loc_pfn = '/'+_tmp[3]

        _cmd_str = '%snecp %s %s/%s' % (_setup_str, loc_pfn, path, lfn)
        tolog("NECP executing (timeout %s): %s" % (timeout, _cmd_str))
        report['transferStart'] = time()
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
        report['validateStart'] = time()

        if s != 0:
            check_syserr(s, o)
            pilotErrorDiag = "necp failed: %s" % (o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

            # did the copy command time out?
            if is_timeout(s):
                pilotErrorDiag = "necp get was timed out after %d seconds" % (telapsed)
                self.prepareReport('COPY_TIMEOUT', report)
                return error.ERR_GETTIMEOUT, pilotErrorDiag

            ec = error.ERR_STAGEINFAILED
            if o == None:
                pilotErrorDiag = "necp failed with output None: ec = %d" % (s)
            elif len(o) == 0:
                pilotErrorDiag = "necp failed with no output: ec = %d" % (s)
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
                    pilotErrorDiag = "necp failed with output: ec = %d, output = %s" % (s, o)

            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.prepareReport('COPY_FAIL', report)
            return ec, pilotErrorDiag

        if fsize == 0:
            # Skipping local copy verification
            # File cannot be checked (necp uses scp tunnel to copy remote files)
            ec = 0
        else:
            # lfn (logical file name) is assumed to be the same as local file name - os.path.basename(gpfn)
            # Here fsize is supposedly never 0
            fchecksum = 0 # skip checksum test
            ec, pilotErrorDiag = self.verifyLocalFile(fsize, fchecksum, lfn, path)

        if ec != 0:
            self.prepareReport('VERIFY_FAIL', report)
            return ec, pilotErrorDiag

        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fmd5sum=0, **pdict):
        """Data transfer using necp"""

        error = PilotErrors()
        pilotErrorDiag = ""

        # get the lfn
        try:
            lfn = pdict['lfn']
        except:
            lfn = ""

        # get the guid
        try:
            guid = pdict['guid']
        except:
            guid = ""

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'HU'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = ''

        try:
            timeout = pdict['timeout']
        except:
            timeout = 5*3600

        # preparing variables
        if fsize == 0 or fmd5sum == 0:
            ec, pilotErrorDiag, fsize, fmd5sum = self.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                self.prepareReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag) 

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
        try:
            dsname = pdict['dsname']
        except:
            dsname = ''
#        report['dataset'] = dsname
            
        filename = os.path.basename(source)

        #fcname = outputpoolfcstring.split(':', 1)[1]        
        try:
            dsname = pdict['dsname']
        except KeyError:
            dsname = ''
        try:
            extradirs = pdict['extradirs']
        except KeyError:
            extradirs = ''
        dst_loc_sedir = os.path.join(dst_loc_se, os.path.join(extradirs, dsname))

        filename = os.path.basename(source)
        
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

        # Directory not created: the copy command creates the direcrory if necessary,
        # all with the right permission

        report['transferStart'] = time()
        try:
            _cmd_str = '%snecp %s %s' % (_setup_str, source, dst_loc_sedir)
            tolog("NECP executing (timeout %s): %s" % (timeout, _cmd_str))
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            # Command may have exceptions like permission errors
            # timed_command hsould block/filter out all exceptions; remove try block?
            s = 1
            e = sys.exc_info()
            o = str(e[1]) #set error message from exception text
            telapsed = timeout
        else:
            o = cout + cerr
            tolog("Elapsed time: %d" % (telapsed))
        report['validateStart'] = time()

        if s != 0:
            check_syserr(s, o)
            pilotErrorDiag = "Error in copying: %s" % (o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            # continue if file exist already (will compare sizes)
            # dCache cannot overwrite files. If file exist it returns: Server error message for [1]: "File is readOnly" (errno 1).
            if o.find('File is readOnly') != -1: 
                tolog('!!WARNING!!2999!! File %s exists already in %s (ec: %s), trying to continue' % (filename, dst_loc_sedir, str(s)))
            else:
                # fail permanently any SIGPIPE errors since they leave a zero length file in dCache
                # which can not be overwritten, so any recovers attempts will fail
                if o.split(" ")[0] == "SIGPIPE":
                    fail = error.ERR_SIGPIPE
                else:
                    fail = error.ERR_STAGEOUTFAILED
                if is_timeout(s):
                    # is the timeout interferring with SIGPIPE?
                    pilotErrorDiag = "necp put was timed out after %d seconds" % (telapsed)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    self.prepareReport('COPY_TIMEOUT', report)
                    return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag, surl=dst_gpfn)
                self.prepareReport('COPY_FAIL', report)
                return self.put_data_retfail(fail, pilotErrorDiag, surl=dst_gpfn)

        # Directory permission not set: it should be set correctly by necp 

        # Impossible to compare MD5sum or file size, trusting necp

        #return 0, dst_gpfn, fsize, '', 'P'
        self.prepareReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, fsize, fmd5sum, self.arch_type
