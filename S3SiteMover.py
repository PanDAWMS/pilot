import os, re, sys
import commands
from time import time
import urlparse

from TimerCommand import TimerCommand
import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, extractFilePaths
from FileStateClient import updateFileState
from SiteInformation import SiteInformation
from config import config_sm

from S3ObjectstoreSiteMover import S3ObjectstoreSiteMover

CMD_CHECKSUM = config_sm.COMMAND_MD5

class S3SiteMover(S3ObjectstoreSiteMover):
    """ SiteMover that uses boto S3 client for both get and put """

    # no registration is done
    copyCommand = "S3"
    checksum_command = "adler32"
    timeout = 600


    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        error = PilotErrors()

        # Get input parameters from pdict
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'gfal-copy', lfn, guid)

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        # get the site information object
        si = getSiteInformation(experiment)
        ret_path = si.getCopyPrefixPathNew(gpfn, stageIn=True)
        if not ret_path.startswith("s3:"):
            errorLog = "Failed to use copyprefix to convert the current path to S3 path."
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            status = PilotErrors.ERR_STAGEINFAILED
            state = "PSTAGE_FAIL"
            output = errorLog
        else:
            gpfn = ret_path
            status, output = self.stageIn(gpfn, fullname, fsize, fchecksum, experiment)

        if status == 0:
            updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", ftype="input")
            state = "DONE"
        else:
            errors = PilotErrors()
            state = errors.getErrorName(status)
            if state == None:
                state = "PSTAGE_FAIL"

        self.prepareReport(state, report)
        return status, output

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

        tolog("put_data received prodSourceLabel=%s" % (prodSourceLabel))
        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 'gfal-copy', lfn, guid)

        filename = os.path.basename(source)

        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename, scope=scope, alt=alt, sitemover=self) # quick workaround
        if ec != 0:
            self.prepareReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        # get local adler32 checksum
        status, output, adler_size, adler_checksum = self.getLocalFileInfo(source, checksumType="adler32")
        if status != 0:
            errorLog = 'Failed to get local file %s adler32 checksum: %s' % (source, output)
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            status = PilotErrors.ERR_STAGEINFAILED
            state = "PSTAGE_FAIL"
            output = errorLog
            self.prepareReport(state, report)
            return self.put_data_retfail(status, output, surl)

        ret_path = si.getCopyPrefixPathNew(surl, stageIn=False)
        tolog("Convert destination: %s to new path: %s" % (surl, ret_path))
        if not ret_path.startswith("s3:"):
            errorLog = "Failed to use copyprefix to convert the current path to S3 path."
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            status = PilotErrors.ERR_STAGEINFAILED
            state = "PSTAGE_FAIL"
            output = errorLog
            size = None
            checksum = None
        else:
            status, output, size, checksum = self.stageOut(source, ret_path, token, experiment)

        if status !=0:
            errors = PilotErrors()
            state = errors.getErrorName(status)
            if state == None:
                state = "PSTAGE_FAIL"
            self.prepareReport(state, report)
            return self.put_data_retfail(status, output, surl)
        else:
            if size == adler_size:
                tolog("The file size is not changed. Will check whether adler32 changed.")
                status, output, new_adler_size, new_adler_checksum = self.getLocalFileInfo(source, checksumType="adler32")
                if status != 0:
                    errorLog = 'Failed to get local file %s adler32 checksum: %s' % (source, output)
                    tolog("!!WARNING!!1777!! %s" % (errorLog))
                    status = PilotErrors.ERR_STAGEINFAILED
                    state = "PSTAGE_FAIL"
                    output = errorLog
                    self.prepareReport(state, report)
                    return self.put_data_retfail(status, output, surl)
                else:
                    if adler_checksum == new_adler_checksum:
                        tolog("The file checksum is not changed. Will use adler32 %s to replace the md5 checksum %s" % (adler_checksum, checksum))
                        checksum = adler_checksum
                    else:
                        errorLog = "The file checksum changed from %s(before transfer) to %s(after transfer)" % (adler_checksum, new_adler_checksum)
                        tolog("!!WARNING!!1777!! %s" % (errorLog))
                        status = PilotErrors.ERR_STAGEINFAILED
                        state = "PSTAGE_FAIL"
                        output = errorLog
                        self.prepareReport(state, report)
                        return self.put_data_retfail(status, output, surl)

        state = "DONE"
        self.prepareReport(state, report)
        return 0, pilotErrorDiag, surl, size, checksum, self.arch_type
