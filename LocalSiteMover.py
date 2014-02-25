import os, re, sys
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation
from timed_command import timed_command
from time import time
from FileStateClient import updateFileState

class LocalSiteMover(SiteMover.SiteMover):
    """ SiteMover for local copy commands etc """
    
    copyCommand = "lsm"
    checksum_command = "adler32"
    __warningStr = '!!WARNING!!2995!! %s'
    __spacetoken = '-t %s' # space token descriptor
    __localget = '%s lsm-get %s %s %s' # environment, options, lfn, target directory
    __localgetBAD = '%s lsm-getXXX %s %s %s' # environment, options, lfn, target directory
    __localput = '%s lsm-put %s %s %s' # environment, space token (optional), source directory, destination
    __localspace = '%s lsm-df %s %s' # environment, space token (optional), storage end-point
    __localerror = 'lsm-error %d' # error code
    __par_filesize = ' --size %s' # filesize in bytes
    __par_checksum = ' --checksum %s' # checksum string: "adler32:NNN", "md5:NNN", default is assumed MD5
    __timeout = 5400 # seconds
    __error = PilotErrors()
    __pilotErrorDiag = ''

    def get_timeout(self):
        return self.__timeout

    def check_space(self, storage_endpoint):
        """ available spave in the SE """

        # build setup string
        envsetup = self.getEnvsetup()

        # build the df command
        execStr = self.__localspace % (envsetup, '', storage_endpoint)
        tolog("Executing command: %s" % (execStr))

        # execute
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))

        # validate
        if status:
            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-df failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            return 999999
        try:
            return int(output.strip())
        except:
            self.__pilotErrorDiag = 'lsm-df wrong format (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            

        return 999999

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'local', lfn, guid)

        if not path:
            tolog('path is empty, using current directory')            
            path = os.getcwd()

        # build setup string
        envsetup = self.getEnvsetup(get=True)

        ec, pilotErrorDiag = verifySetupCommand(self.__error, envsetup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

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
                    return self.__error.ERR_DIRECTIOFILE, self.__pilotErrorDiag
                else:
                    tolog("Normal file transfer")
        else:
            tolog("not directIn")

        # build the get command
        _params = ""
        if fsize != 0 and fsize != "0":
            _params += self.__par_filesize % (fsize,)
        if fchecksum and fchecksum != 'None' and fchecksum != 0 and fchecksum != "0" and not self.isDummyChecksum(fchecksum):
            csumtype = self.getChecksumType(fchecksum)
            # special case for md5sum (command only understands 'md5' and 'adler32', and not 'ad' and 'md5sum')
            if csumtype == 'md5sum':
                csumtype = 'md5'
            _params += self.__par_checksum % ("%s:%s" % (csumtype, fchecksum),)

        # add the guid option
        _params += " --guid %s" % (guid)

        dest_path = os.path.join(path, lfn)
        #PN
#        if ".lib." in gpfn:
#            localGet = self.__localget
#        else:
#            localGet = self.__localgetBAD
#        execStr = localGet % (envsetup, _params, gpfn, dest_path)
        execStr = self.__localget % (envsetup, _params, gpfn, dest_path)
        tolog("Executing command: %s" % (execStr))

        report['transferStart'] = time()
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))
        tolog("Command output:\n%s" % (output))
        report['validateStart'] = time()

        if status:
            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            # did the copy command time out?
            if is_timeout(status):
                self.__pilotErrorDiag = "lsm-get failed: time out after %d seconds" % (telapsed)
                tolog(self.__warningStr % self.__pilotErrorDiag)            
                self.__sendReport('GET_TIMEOUT', report)
                return self.__error.ERR_GETTIMEOUT, self.__pilotErrorDiag

            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-get failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            self.__sendReport('COPY_FAIL', report)
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        # the lsm-get command will compare the file size and checksum with the catalog values

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, self.__pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from local dir to SE and register into dataset and catalogues """

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        dsname = pdict.get('dsname', '')
        workDir = pdict.get('workDir', '')
        analyJob = pdict.get('analJob', False)
        extradirs = pdict.get('extradirs', '')
        experiment = pdict.get('experiment', '')
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        if prodSourceLabel == 'ddm' and analyJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analyJob = False

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'local'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            report['guid'] = guid.replace('-','')
#            report['dataset'] = dsname

        filename = os.path.basename(source)

        # get the local file size and checksum
        csumtype = self.checksum_command
        if fsize == 0 or fchecksum == 0:
            ec, self.__pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, self.__pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        ec, pilotErrorDiag = verifySetupCommand(self.__error, envsetup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag) 

        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(self.__error, analyJob, token, prodSourceLabel, dsname, filename, scope=scope)
        if ec != 0:
            self.__sendReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        dst_gpfn = surl
        tolog("dst_gpfn: %s" % (dst_gpfn))

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        # build the command
        _params = ""
        if token:
            _params = self.__spacetoken % (token,)
        if fsize != 0:
            _params += self.__par_filesize % (fsize,)
        if fchecksum != 0:
            # special case for md5sum (command only understands 'md5' and 'adler32', and not 'ad' and 'md5sum')
            if csumtype == 'md5sum':
                _csumtype = 'md5'
            else:
                _csumtype = csumtype
            _params += self.__par_checksum % ("%s:%s" % (_csumtype, fchecksum),)

        # add the guid option
        _params += " --guid %s" % (guid)

        execStr = self.__localput % (envsetup, _params, source, dst_gpfn)
        tolog("Executing command: %s" % (execStr))
        report['transferStart'] = time()
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))
        tolog("Command output:\n%s" % (output))
        report['validateStart'] = time()

        # validate
        if status:
            # did the copy command time out?
            if is_timeout(status):
                self.__pilotErrorDiag = "lsm-put failed: time out after %d seconds" % (telapsed)
                tolog(self.__warningStr % self.__pilotErrorDiag)            
                self.__sendReport('PUT_TIMEOUT', report)
                return self.put_data_retfail(self.__error.ERR_PUTTIMEOUT, self.__pilotErrorDiag)

            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-put failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            self.__sendReport('COPY_FAIL', report)
            if status == 204 or status == 205: # size or checksum failed
                return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag, surl=dst_gpfn)
            else:
                return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, self.__pilotErrorDiag, dst_gpfn, fsize, fchecksum, self.arch_type

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
