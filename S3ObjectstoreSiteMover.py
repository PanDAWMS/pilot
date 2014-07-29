import os, re
import commands
from time import time
import urlparse

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, extractFilePaths
from FileStateClient import updateFileState
from SiteInformation import SiteInformation
from config import config_sm

CMD_CHECKSUM = config_sm.COMMAND_MD5

class S3ObjectstoreSiteMover(SiteMover.SiteMover):
    """ SiteMover that uses boto S3 client for both get and put """
    # no registration is done
    copyCommand = "S3"
    checksum_command = "adler32"
    timeout = 3600

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path
        self._defaultSetup = "source /cvmfs/atlas.cern.ch/repo/sw/external/boto/setup.sh; unset http_proxy; unset https_proxy"
        self.s3Objectstore = None

    def get_timeout(self):
        return self.timeout

    def log(self, errorLog):
        tolog(errorLog)

    def setup(self, experiment=None):
        """ setup env """
        if not os.environ.get("PYTHONPATH"):
            os.environ["PYTHONPATH"] = "/cvmfs/atlas.cern.ch/repo/sw/external/boto/lib/python2.6/site-packages/"
        else:
            os.environ["PYTHONPATH"] = "/cvmfs/atlas.cern.ch/repo/sw/external/boto/lib/python2.6/site-packages/" + ":" + os.environ["PYTHONPATH"]
        if os.environ.get("http_proxy"):
            del os.environ['http_proxy']
        if os.environ.get("https_proxy"):
            del os.environ['https_proxy']

        si = getSiteInformation(experiment)
        keyPair = si.getSecurityKey('BNL_ObjectStoreKey', 'BNL_ObjectStoreKey.pub')
        if keyPair["publicKey"] == None or keyPair["privateKey"] == None:
            tolog("Failed to get the keyPair for S3 objectstore")
            return ERR_GETKEYPAIR, "Failed to get the keyPair for S3 objectstore"

        self.s3Objectstore = S3ObjctStore()
        return 0, ""

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

    def getRemoteFileInfo(self, file):
        size, md5 = self.s3Objectstore.getRemoteFileInfo(file)
        return size, md5

    def stageInFile(self, source, destination, sourceSize, sourceChecksum):
        """StageIn the file. should be implementated by different site mover."""
        status = self.s3Objectstore.stageInFile(source, destination, sourceSize, sourceChecksum)
        output = None
        if status:
            output = "S3 object store failed to stagein this file."
        return status, output

    def stageOutFile(self, source, destination, sourceSize, sourceChecksum, token):
        """StageIn the file. should be implementated by different site mover."""
        size = self.s3Objectstore.stageOutFile(source, destination, sourceSize, sourceChecksum, token)
        output = None
        if size == 0:
            output = "S3 object store failed to stagein this file."
        return size, output

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
        elif remoteSize == localSize:
            errLog = "Remote size and local size are the same. verified"
            self.log(errLog)
            return 0, errLog
        else:
            errLog = "Remote and local size mismatch"
            self.log(errLog)
            return PilotErrors.ERR_GETWRONGSIZE, errLog
        
        self.log("Finished to verify staging")
        return 0, errLog

    def stageIn(self, source, destination, sourceSize=None, sourceChecksum=None, experiment=None):
        """Stage in the source file"""
        self.log("Starting to stagein file %s(size:%s, chksum:%s) to %s" % (source, sourceSize, sourceChecksum, destination))

        status, output = self.setup(experiment)
        if status:
            return status, output

        remoteSize = sourceSize
        remoteChecksum = sourceChecksum
        if remoteChecksum == None or remoteChecksum == "":
            remoteSize, remoteChecksum = self.getRemoteFileInfo(source)
        self.log("remoteSize: %s, remoteChecksum: %s" % (remoteSize, remoteChecksum))

        status, output = self.stageInFile(source, destination, remoteSize, remoteChecksum)
        if status:
             self.log("Failed to stagein this file.")
             return  PilotErrors.ERR_STAGEINFAILED, output

        status, output, localSize, localChecksum = self.getLocalFileInfo(destination)
        if status:
            self.log("Failed to get local file(%s) info." % destination)
            return status, output

        status, output = self.verifyStage(localSize, localChecksum, remoteSize, remoteChecksum)

        self.log("Finished to stagin file %s(status:%s, output:%s)" % (source, status, output))
        return status, output

    def stageOut(self, source, destination, token, experiment=None):
        """Stage in the source file"""
        self.log("Starting to stageout file %s to %s with token: %s" % (source, destination, token))

        status, output = self.setup(experiment)
        if status:
            return status, output

        status, output, localSize, localChecksum = self.getLocalFileInfo(source)
        self.log("getLocalFileInfo  status: %s, output: %s, localSize: %s, localChecksum: %s" % ( status, output, localSize, localChecksum))
        if status:
            self.log("Failed to get local file(%s) info." % destination)
            return status, output

        remoteSize, output = self.stageOutFile(source, destination, localSize, localChecksum, token)
        self.log("stageOutFile remoteSize: %s, output: %s" % (remoteSize, output))
        if remoteSize == 0:
             self.log("Failed to stageout this file.")
             return  PilotErrors.ERR_STAGEOUTFAILED, output, localSize, localChecksum

        remoteSize, remoteChecksum = self.getRemoteFileInfo(destination)
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
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'gfal-copy', lfn, guid)

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        status, output = self.stageIn(gpfn, fullname, fsize, fchecksum, experiment)

        if status == 0:
            updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
            state = "DONE"
        else:
            errors = PilotErrors()
            state = errors.getErrorName(status)
            if state == None:
                state = "PSTAGE_FAIL"

        self.__sendReport(state, report)
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

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'gfal-copy', lfn, guid)


        filename = os.path.basename(source)
        surl = destination
        status, output, size, checksum = self.stageOut(source, surl, token, experiment)
        if status !=0:
            errors = PilotErrors()
            state = errors.getErrorName(status)
            if state == None:
                state = "PSTAGE_FAIL"
            self.__sendReport(state, report)
            return self.put_data_retfail(status, output, surl)

        state = "DONE"
        self.__sendReport(state, report)
        return 0, pilotErrorDiag, surl, size, checksum, self.arch_type

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

class S3ObjctStore:
    def __init__(self):
        self.access_key = None
        self.secret_key = None
        self.hostname = None
        self.port = None
        self.setup()

    def setup(self):
        self.access_key = '9EH9WN0NF37BLQKDJVLB'
        self.secret_key = 'lMeE69l55XTuWVEV8dWOcrTFkkb6CH1v3rUrIDlK'

    def get_key(self, url, create=False):
        import boto
        import boto.s3.connection
        from boto.s3.key import Key

        parsed = urlparse.urlparse(url)
        scheme = parsed.scheme
        self.hostname = parsed.netloc.partition(':')[0]
        self.port = int(parsed.netloc.partition(':')[2])
        path = parsed.path.strip("/")

        self.__conn = boto.connect_s3(
            aws_access_key_id = self.access_key,
            aws_secret_access_key = self.secret_key,
            host = self.hostname,
            port = self.port,
            is_secure=False,               # uncommmnt if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )

        if create:
            dir_name = os.path.dirname(path)
            key_name = os.path.basename(path)
            bucket = self.__conn.create_bucket(dir_name)
            key = Key(bucket, key_name)
        else:
            pos = path.index("/")
            bucket_name = path[:pos]
            key_name = path[pos+1:]
            bucket = self.__conn.get_bucket(bucket_name)
            key = bucket.get_key(key_name)

        return key

    def getRemoteFileInfo(self, file):
        key = self.get_key(file)
        return key.size, key.md5

    def stageInFile(self, source, destination, sourceSize=None, sourceChecksum=None):
        key = self.get_key(source)
        key.get_contents_to_filename(destination)
        return 0

    def stageOutFile(self, source, destination, sourceSize=None, sourceChecksum=None, token=None):
        key = self.get_key(destination, create=True)
        size = key.set_contents_from_filename(source)
        return size

