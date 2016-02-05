import os, re, sys
import commands
import cgi
import json
from time import time, sleep
import requests
import urlparse
import traceback

from TimerCommand import TimerCommand
import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, extractFilePaths
from FileStateClient import updateFileState
from SiteInformation import SiteInformation
from config import config_sm

CMD_CHECKSUM = config_sm.COMMAND_MD5


class UploadInChunks(object):
    '''
    Class to upload by chunks.
    '''

    def __init__(self, filename, chunksize, progressbar=False):
        self.__totalsize = os.path.getsize(filename)
        self.__readsofar = 0
        self.__filename = filename
        self.__chunksize = chunksize
        self.__progressbar = progressbar

    def __iter__(self):
        try:
            with open(self.__filename, 'rb') as file_in:
                while True:
                    data = file_in.read(self.__chunksize)
                    if not data:
                        if self.__progressbar:
                           sys.stdout.write("\n")
                        break
                    self.__readsofar += len(data)
                    if self.__progressbar:
                        percent = self.__readsofar * 100 / self.__totalsize
                        sys.stdout.write("\r{percent:3.0f}%".format(percent=percent))
                    yield data
        except OSError as error:
            raise exception.SourceNotFound(error)

    def __len__(self):
        return self.__totalsize


class IterableToFileAdapter(object):
    '''
    Class IterableToFileAdapter
    '''
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.length = len(iterable)

    def read(self, size=-1):   # TBD: add buffer for `len(data) > size` case
        return next(self.iterator, b'')

    def __len__(self):
        return self.length


class S3ObjectstoreHttpSiteMover(SiteMover.SiteMover):
    """ SiteMover that uses panda proxy """
    # no registration is done
    copyCommand = "S3ObjectstoreHttp"
    checksum_command = "adler32"
    timeout = 3600
    _instance = None

    def __init__(self, setup_path, useTimerCommand=True, *args, **kwrds):
        self.setup_path = setup_path
        self.os_name = None
        self.os_endpoint = None
        self.os_bucket_endpoint = None
        self.public_key = None
        self.private_key = None
        self.pandaProxy = 'http://aipanda084.cern.ch:25064/proxy/panda'

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(S3ObjectstoreHttpsSiteMover, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def get_timeout(self):
        return self.timeout

    def log(self, errorLog):
        tolog(errorLog)

    def setup(self, experiment=None, surl=None):
        """ setup env """

        if os.environ.get("http_proxy"):
            del os.environ['http_proxy']
        if os.environ.get("https_proxy"):
            del os.environ['https_proxy']

        si = getSiteInformation(experiment)
        self.os_name = si.getObjectstoresField("os_name", "eventservice")
        self.os_endpoint = si.getObjectstoresField("os_endpoint", "eventservice")
        self.os_bucket_endpoint = si.getObjectstoresField("os_bucket_endpoint", "eventservice")
        self.public_key = si.getObjectstoresField("os_access_key", "eventservice")
        self.private_key = si.getObjectstoresField("os_secret_key", "eventservice")
        if not (self.os_name and self.os_name != "" and self.os_bucket_endpoint and self.os_bucket_endpoint != ""):
            tolog("Failed to get S3 objectstore name")
            return PilotErrors.ERR_GETKEYPAIR, "Failed to get S3 objectstore name"

        return 0, ""


    def getPandaProxyRemotePath(self, pandaID, filename, jobSetID, pandaProxySecretKey=None, stageIn=False):
        try:
            if not pandaProxySecretKey or pandaProxySecretKey == "":
                return PilotErrors.ERR_GETKEYPAIR, "Failed to get panda proxy secrekt key"

            data = {'pandaID': pandaID,
                    'secretKey': pandaProxySecretKey,
                    'publicKey': 'publicKey:%s' % self.public_key,
                    'privateKey': 'privateKey:%s' % self.private_key,
                    'url':'%s/%s/%s/%s' % (self.os_endpoint, self.os_bucket_endpoint, jobSetID, filename)}

            if stageIn:
                data['method'] = 'GET'

            res = requests.post(self.pandaProxy+'/getPresignedURL',data=data)
            if res.status_code == 200:
                tmpDict = cgi.parse_qs(res.text.encode('ascii'))
                if int(tmpDict['StatusCode'][0]) == 0:
                    return int(tmpDict['StatusCode'][0]), "", tmpDict['presignedURL'][0]
                else:
                    return int(tmpDict['StatusCode'][0]), tmpDict['ErrorMsg'][0], None
            return PilotErrors.ERR_UNKNOWN, "Failed to get remote signed path: %s" % res.text, None
        except:
            return PilotErrors.ERR_UNKNOWN, "Failed to get remote signed path: %s" % traceback.format_exc(), None

    def getPandaProxyRemoteFileInfo(self, pandaID, filename, jobSetID, pandaProxySecretKey=None):
        try:
            if not pandaProxySecretKey or pandaProxySecretKey == "":
                return PilotErrors.ERR_GETKEYPAIR, "Failed to get panda proxy secrekt key"

            data = {'pandaID': pandaID,
                    'secretKey': pandaProxySecretKey,
                    'publicKey': 'publicKey:%s' % self.public_key,
                    'privateKey': 'privateKey:%s' % self.private_key,
                    'url':'%s/%s/%s/%s' % (self.os_endpoint, self.os_bucket_endpoint, jobSetID, filename)}

            res = requests.post(self.pandaProxy+'/getFileInfo',data=data)
            if res.status_code == 200:
                tmpDict = cgi.parse_qs(res.text.encode('ascii'))
                if int(tmpDict['StatusCode'][0]) == 0:
                    size, checksum = json.loads(tmpDict['fileInfo'][0])
                    return int(tmpDict['StatusCode'][0]), "", size, checksum
                else:
                    return int(tmpDict['StatusCode'][0]), tmpDict['ErrorMsg'][0], None, None
            return PilotErrors.ERR_UNKNOWN, "Failed to get remote file info: %s" % res.text, None, None
        except:
            return PilotErrors.ERR_UNKNOWN, "Failed to get remote file info: %s" % traceback.format_exc(), None, None

    def fixStageInPath(self, path):
        """Fix the path"""
        return path

    def getLocalFileInfo(self, fileName, checksumType="md5", date=None):
        """ Return exit code (0 if OK), file size and checksum of a local file, as well as as date string if requested """
        # note that date is mutable
        self.log("Starting to get file(%s) info(checksum tyep:%s)" % (fileName, checksumType))
        size = 0
        checksum = None

        # does the file exist?
        if not os.path.isfile(fileName):
            errorLog = "No such file or directory: %s" % (fileName)
            self.log("!!WARNING!!2999!! %s" % (errorLog))
            return PilotErrors.ERR_MISSINGLOCALFILE, errorLog, size, checksum

        # get the file size
        try:
            self.log("Executing getsize() for file: %s" % (fileName))
            size = os.path.getsize(fileName)
        except OSError, e:
            errorLog = "Could not get file size: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (errorLog))
            return PilotErrors.ERR_FAILEDSIZELOCAL, errorLog, size, checksum
        else:
            if size == 0:
                errorLog = "Encountered zero file size for file %s" % (fileName)
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_ZEROFILESIZE, errorLog, size, checksum
            else:
                self.log("Got file size: %s" % (size))

        # get the checksum
        if checksumType == "adler32":
            self.log("Executing adler32() for file: %s" % (fileName))
            checksum = SiteMover.SiteMover.adler32(fileName)
            if checksum == '00000001': # "%08x" % 1L
                errorLog = "Adler32 failed (returned 1)"
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_FAILEDADLOCAL, errorLog, size, checksum
            else:
                self.log("Got adler32 checksum: %s" % (checksum))
        else:
            _cmd = '%s %s' % (CMD_CHECKSUM, fileName)
            self.log("Executing command: %s" % (_cmd))
            try:
                s, o = commands.getstatusoutput(_cmd)
            except Exception, e:
                s = -1
                o = str(e)
                self.log("!!WARNING!!2999!! Exception caught in getstatusoutput: %s" % (o))
            if s != 0:
                o = o.replace('\n', ' ')
                check_syserr(s, o)
                errorLog = "Error running checksum command (%s): %s" % (CMD_CHECKSUM, o)
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_FAILEDMD5LOCAL, errorLog, size, checksum
            checksum = o.split()[0]
            self.log("Got checksum: %s" % (checksum))

        return 0, "", size, checksum

    def getRemoteFileInfo(self, jobId, lfn, jobSetID, pandaProxySecretKey):
        try:
            status, output, size, md5 = self.getPandaProxyRemoteFileInfo(jobId, lfn, jobSetID, pandaProxySecretKey)
            if status == 0:
               return size, md5
            else:
               tolog("Failed to get remote file information: status: %s, output: %s" % (status, output))
               return None, None
        except:
            tolog("Failed to get remote file information: status: %s, output: %s" % (status, output))
            return None, None

    def stageInFile(self, source, destination, sourceSize, sourceChecksum):
        """StageIn the file. should be implementated by different site mover."""
        try:
            result = requests.get(source)
            if result and result.status_code == 200:
                chunksize = 1024
                with open(destination, 'wb') as f:
                    for chunk in result.iter_content(chunksize):
                        f.write(chunk)
                return 0, ""
            else:
                return PilotErrors.ERR_STAGEINFAILED, result.text
        except:
            tolog("Failed to stage in file: %s" % (traceback.format_exc()))
            return PilotErrors.ERR_STAGEINFAILED, "S3Objectstore failed to stage in file"

    def stageOutFile(self, source, destination, sourceSize, sourceChecksum, token, outputDir=None, timeout=3600):
        """StageIn the file. should be implementated by different site mover."""
        try:
            it = UploadInChunks(source, 10000000, progressbar=False)
            result = requests.put(destination, data=IterableToFileAdapter(it))
            if result and result.status_code == 200:
                return 0, ""
            else:
                return PilotErrors.ERR_STAGEOUTFAILED, result.text
        except:
            tolog("Failed to stage out file: %s" % (traceback.format_exc()))
            return PilotErrors.ERR_STAGEOUTFAILED, "S3Objectstore failed to stage out file"

    def verifyStage(self, localSize, localChecksum, remoteSize, remoteChecksum):
        """Verify file stag successfull"""
        self.log("Starting to verify staging.")
        self.log("Remote checksum: %s, Remote size: %s" % (str(remoteChecksum), str(remoteSize)))
        self.log("Local checksum: %s, Local size: %s" % (str(localChecksum), str(localSize)))

        errLog = None
        if remoteChecksum is None or localChecksum is None:
            errLog = "Cannot verify checksum(one of them is None)"
            self.log(errLog)
        elif remoteChecksum == localChecksum:
            errLog = "Remote checksum and local checksum are the same. verified"
            self.log(errLog)
            return 0, errLog
        else:
            errLog = "Remote and local checksum mismatch"
            self.log(errLog)
            return PilotErrors.ERR_GETMD5MISMATCH, errLog

        if remoteSize is None or localSize is None:
            errLog = "Cannot verify size(one of them is None)"
            self.log(errLog)
        elif str(remoteSize) == str(localSize):
            errLog = "Remote size and local size are the same. verified"
            self.log(errLog)
            return 0, errLog
        else:
            errLog = "Remote and local size mismatch"
            self.log(errLog)
            return PilotErrors.ERR_GETWRONGSIZE, errLog

        self.log("Finished to verify staging")
        return 0, errLog

    def stageIn(self, jobId, lfn, jobSetID, pandaProxySecretKey, destination, sourceSize=None, sourceChecksum=None, experiment=None):
        """Stage in the source file"""
        self.log("Starting to stagein file %s(size:%s, chksum:%s) to %s" % (lfn, sourceSize, sourceChecksum, destination))

        if sourceSize == 0 or sourceSize == "":
            sourceSize = None
        if sourceChecksum == "" or sourceChecksum == "NULL":
            sourceChecksum = None

        status, output = self.setup(experiment)
        if status:
            return status, output


        remoteSize = sourceSize
        remoteChecksum = sourceChecksum
        if remoteChecksum == None or remoteChecksum == "":
            remoteSize, remoteChecksum = self.getRemoteFileInfo(jobId, lfn, jobSetID, pandaProxySecretKey)
        self.log("remoteSize: %s, remoteChecksum: %s" % (remoteSize, remoteChecksum))
        if remoteChecksum == None:
            self.log("Failed to get remote file information")

        checksumType = 'md5sum'
        if remoteChecksum:
            checksumType = self.getChecksumType(remoteChecksum)

        status, output, source = self.getPandaProxyRemotePath(jobId, lfn, jobSetID, pandaProxySecretKey, stageIn=True)
        if status:
            return status, output

        if checksumType == 'adler32':
            # S3 boto doesn't support adler32, remoteChecksum set to None
            status, output = self.stageInFile(source, destination, remoteSize, None)
        else:
            status, output = self.stageInFile(source, destination, remoteSize, remoteChecksum)
        self.log("stageInFile status: %s, output: %s" % (status, output))
        if status:
             self.log("Failed to stagein this file: %s" % output)
             return  PilotErrors.ERR_STAGEINFAILED, output

        status, output, localSize, localChecksum = self.getLocalFileInfo(destination, checksumType)
        if status:
            self.log("Failed to get local file(%s) info." % destination)
            return status, output

        status, output = self.verifyStage(localSize, localChecksum, remoteSize, remoteChecksum)

        self.log("Finished to stagin file %s(status:%s, output:%s)" % (source, status, output))
        return status, output

    def stageOut(self, source, jobId, lfn, jobSetID, pandaProxySecretKey, token, experiment=None, outputDir=None, timeout=3600):
        """Stage in the source file"""
        self.log("Starting to stageout file %s with token: %s" % (source, token))

        status, output = self.setup(experiment)
        if status:
            return status, output, None, None

        status, output, localSize, localChecksum = self.getLocalFileInfo(source)
        self.log("getLocalFileInfo  status: %s, output: %s, localSize: %s, localChecksum: %s" % ( status, output, localSize, localChecksum))
        if status:
            self.log("Failed to get local file(%s) info." % destination)
            return status, output, None, None

        status, output, destination = self.getPandaProxyRemotePath(jobId, lfn, jobSetID, pandaProxySecretKey, stageIn=False)
        if status:
            return status, output, None, None

        status, output = self.stageOutFile(source, destination, localSize, localChecksum, token, outputDir=outputDir, timeout=timeout)
        self.log("stageOutFile status: %s, output: %s" % (status, output))
        if status:
             self.log("Failed to stageout this file: %s" % output)
             return  PilotErrors.ERR_STAGEOUTFAILED, output, localSize, localChecksum

        remoteSize, remoteChecksum = self.getRemoteFileInfo(jobId, lfn, jobSetID, pandaProxySecretKey)
        self.log("getRemoteFileInfo remoteSize: %s, remoteChecksum: %s" % (remoteSize, remoteChecksum))
        status, output = self.verifyStage(localSize, localChecksum, remoteSize, remoteChecksum)
        self.log("verifyStage status: %s, output: %s" % (status, output))

        self.log("Finished to stagout file %s(status:%s, output:%s)" % (source, status, output))
        return status, output, localSize, localChecksum

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        error = PilotErrors()

        # Get input parameters from pdict
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        pandaProxySecretKey = pdict.get('pandaProxySecretKey')
        jobSetID = pdict.get('jobsetID')

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 's3objectstorehttps', lfn, guid)

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        # pandaID, filename, jobSetID, pandaProxySecretKey=None, stageIn=True
        status, output = self.stageIn(jobId, lfn, jobSetID, pandaProxySecretKey, fullname, fsize, fchecksum, experiment)

        if status == 0:
            updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
            state = "DONE"
        else:
            errors = PilotErrors()
            state = errors.getErrorName(status)
            if state == None:
                state = "PSTAGE_FAIL"

        # self.__sendReport(state, report)
        self.prepareReport(state, report)
        return status, output

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        # function is based on dCacheSiteMover put function

        error = PilotErrors()
        pilotErrorDiag = ""


        # Get input parameters from pdict
        alt = pdict.get('alt', False)
        jobId = pdict.get('jobId', '')
        jobSetID = pdict.get('jobsetID', '')
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
        outputDir = pdict.get('outputDir', '')
        timeout = pdict.get('timeout', None)
        pandaProxySecretKey = pdict.get('pandaProxySecretKey')
        if not timeout:
            timeout = self.timeout

        # get the site information object
        si = getSiteInformation(experiment)

        tolog("put_data received prodSourceLabel=%s" % (prodSourceLabel))

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 's3objectstorehttps', lfn, guid)

        filename = os.path.basename(source)
        surl = destination
        status, output, size, checksum = self.stageOut(source, jobId, lfn, jobSetID, pandaProxySecretKey, token, experiment, outputDir=outputDir, timeout=timeout)
        if status !=0:
            errors = PilotErrors()
            state = errors.getErrorName(status)
            if state == None:
                state = "PSTAGE_FAIL"
            # self.__sendReport(state, report)
            self.prepareReport(state, report)
            return self.put_data_retfail(status, output, surl)

        state = "DONE"
        # self.__sendReport(state, report)
        # self.prepareReport(state, report)
        return 0, pilotErrorDiag, surl, size, checksum, self.arch_type

    def __sendReport(self, state, report):
        """
        Send Rucio tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)

