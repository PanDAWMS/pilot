import os, re, sys
import commands
from time import time, sleep
import random
import socket
import urlparse
import traceback

from TimerCommand import TimerCommand
import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand, getSiteInformation, extractFilePaths
from FileStateClient import updateFileState
from SiteInformation import SiteInformation
from configSiteMover import config_sm

CMD_CHECKSUM = config_sm.COMMAND_MD5

class S3ObjectstoreSiteMover(SiteMover.SiteMover):
    """ SiteMover that uses boto S3 client for both get and put """
    # no registration is done
    copyCommand = "S3Objectstore"
    checksum_command = "adler32"
    timeout = 3600
    _instance = None

    def __init__(self, setup_path, useTimerCommand=True, *args, **kwrds):
        self._setup = setup_path.strip()
        self._defaultSetup = "source /cvmfs/atlas.cern.ch/repo/sw/external/boto/setup.sh "
        self.s3Objectstore = None
        self.__isBotoLoaded = False
        self._useTimerCommand = useTimerCommand
        self.__hosts = {}

    def get_timeout(self):
        return self.timeout

    def log(self, errorLog):
        tolog(errorLog)

    def setup(self, experiment=None, surl=None, os_bucket_id=-1, label='r'):
        """ setup env """
        if not self.__isBotoLoaded:
            try:
                import boto
                import boto.s3.connection
                from boto.s3.key import Key
                self.__isBotoLoaded = True
            except ImportError:
                tolog("Failed to import boto, add /cvmfs/atlas.cern.ch/repo/sw/external/boto/lib/python2.6/site-packages/ to sys.path")
                sys.path.append('/cvmfs/atlas.cern.ch/repo/sw/external/boto/lib/python2.6/site-packages/')
                try:
                    import boto
                    import boto.s3.connection
                    from boto.s3.key import Key
                    self.__isBotoLoaded = True
                except ImportError:
                    tolog("Failed to import boto again. exit")
                    return PilotErrors.ERR_UNKNOWN, "Failed to import boto"

        si = getSiteInformation(experiment)
        # os_bucket_id will only be set if the setup function is called, if setup via the init function - get the default bucket id
        if os_bucket_id == -1:
            ddmendpoint = si.getObjectstoreDDMEndpoint(os_bucket_name='eventservice') # assume eventservice
        else:
            ddmendpoint = si.getObjectstoreDDMEndpointFromBucketID(os_bucket_id)
        endpoint_id = si.getObjectstoreEndpointID(ddmendpoint=ddmendpoint, label=label, protocol='s3')
        os_access_key, os_secret_key, os_is_secure = si.getObjectstoreKeyInfo(endpoint_id, ddmendpoint=ddmendpoint)

        if os_access_key and os_access_key != "" and os_secret_key and os_secret_key != "":
            keyPair = si.getSecurityKey(os_secret_key, os_access_key)
            if "privateKey" not in keyPair or keyPair["privateKey"] is None:
                tolog("Failed to get the keyPair for S3 objectstore")
                return PilotErrors.ERR_GETKEYPAIR, "Failed to get the keyPair for S3 objectstore"
        else:
            tolog("Failed to get the keyPair name for S3 objectstore")
            return PilotErrors.ERR_GETKEYPAIR, "Failed to get the keyPair name for S3 objectstore"

        self.s3Objectstore = S3ObjctStore(keyPair["privateKey"], keyPair["publicKey"], os_is_secure, self._useTimerCommand)

        return 0, ""

    def loadBalanceURL(self, url):
        try:
            parsed = urlparse.urlparse(url)
            hostname = parsed.netloc.partition(':')[0]
            port = int(parsed.netloc.partition(':')[2])

            if hostname not in self.__hosts:
                hosts = []
                socket_hosts = socket.getaddrinfo(hostname, port)
                for socket_host in socket_hosts:
                    if socket_host[4][0] not in hosts and not ":" in socket_host[4][0]:
                        hosts.append(socket_host[4][0])
                if hosts:
                    self.__hosts[hostname] = hosts

            if hostname in self.__hosts:
                new_host = random.choice(self.__hosts[hostname])
                if new_host:
                    return url.replace(hostname, new_host)
        except:
            tolog("Failed to load balance for url %s: %s" % (url, traceback.format_exc()))
        return url

    def fixStageInPath(self, path):
        """Fix the path"""
        return path

    def getLocalFileInfo(self, fileName, checksumType="md5", date=None):
        """ Return exit code (0 if OK), file size and checksum of a local file, as well as as date string if requested """
        # note that date is mutable
        self.log("Starting to get file(%s) info(checksum type:%s)" % (fileName, checksumType))
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
        try:
            size, md5 = self.s3Objectstore.getRemoteFileInfo(file)
        except:
            tolog("Failed to get remote file information: %s" % (sys.exc_info()[1]))
            return None, None
        return size, md5

    def stageInFile(self, source, destination, sourceSize, sourceChecksum):
        """StageIn the file. should be implementated by different site mover."""
        retSize = None
        retChecksum = None
        try:
            status, output, retSize, retChecksum = self.s3Objectstore.stageInFile(source, destination, sourceSize, sourceChecksum)
        except:
            tolog("Failed to stage in file: %s" % (sys.exc_info()[1]))
            return PilotErrors.ERR_STAGEINFAILED, "S3Objectstore failed to stage in file", None, None
        return status, output, retSize, retChecksum

    def stageOutFile(self, source, destination, sourceSize, sourceChecksum, token, outputDir=None, timeout=3600):
        """ Stage-out the file. should be implementated by different site mover """

        status = -1
        output = 'not defined'
        retSize = None
        retChecksum = None
        if outputDir and outputDir.endswith("PilotMVOutputDir"):
            timeStart = time()
            outputFile = os.path.join(outputDir, os.path.basename(source))
            mvCmd = "cp -f %s %s" % (source, outputFile)
            tolog("Executing command: %s" % (mvCmd))
            lstatus, loutput = commands.getstatusoutput(mvCmd)
            if lstatus != 0:
                status = lstatus
                output = loutput
            else:
                outputFileCmd = outputFile + ".s3cmd"
                handle = open(outputFileCmd, 'w')
                _cmd_str = "%s %s" % (outputFile, destination)
                handle.write(_cmd_str)
                handle.close()
                tolog("Write command %s to %s" % (_cmd_str, outputFileCmd))
                tolog("Waiting remote to finish transfer")
                status = -1
                output = "Remote timeout to transfer out file"
                while (time() - timeStart) < timeout:
                    sleep(5)
                    if os.path.exists(outputFile + ".s3cmdfinished"):
                        status = 0
                        output = "Remote finished transfer"
                        tolog(output)
                        os.remove(outputFile + ".s3cmdfinished")
                        break
        else:
            try:
                status, output,retSize, retChecksum = self.s3Objectstore.stageOutFile(source, destination, sourceSize, sourceChecksum, token, timeout=timeout)
            except:
                tolog("Failed to stage out file: %s" % (traceback.format_exc()))
                return PilotErrors.ERR_STAGEOUTFAILED, "S3Objectstore failed to stage out file", None, None
        return status, output, retSize, retChecksum

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

    def stageIn(self, source, destination, sourceSize=None, sourceChecksum=None, experiment=None, os_bucket_id=-1, report=None):
        """Stage in the source file"""
        self.log("Starting to stagein file %s(size:%s, chksum:%s) to %s" % (source, sourceSize, sourceChecksum, destination))

        if sourceSize:
            if sourceSize == 0 or sourceSize == "" or str(sourceSize) == '0':
                sourceSize = None
        else:
            sourceSize = None
        if sourceChecksum == "" or sourceChecksum == "NULL":
            sourceChecksum = None

        status, output = self.setup(experiment, source, os_bucket_id=os_bucket_id, label='r')
        if status:
            return status, output

        ldSource = self.loadBalanceURL(source)
        if ldSource:
            self.log("Change url %s to load balance url %s" % (source, ldSource))
            source = ldSource

        remoteSize = sourceSize
        remoteChecksum = sourceChecksum
        if report:
            report['filesize'] = remoteSize
        # if remoteChecksum == None or remoteChecksum == "":
        #     remoteSize, remoteChecksum = self.getRemoteFileInfo(source)
        # self.log("remoteSize: %s, remoteChecksum: %s" % (remoteSize, remoteChecksum))
        # if remoteChecksum == None:
        #     self.log("Failed to get remote file information")

        checksumType = 'md5sum'
        if remoteChecksum:
            checksumType = self.getChecksumType(remoteChecksum)

        if checksumType == 'adler32':
            # S3 boto doesn't support adler32, remoteChecksum set to None
            status, output, remoteSize, remoteChecksum = self.stageInFile(source, destination, remoteSize, None)
        else:
            status, output, remoteSize, remoteChecksum = self.stageInFile(source, destination, remoteSize, remoteChecksum)
        if report:
            report['filesize'] = remoteSize
        self.log("stageInFile status: %s, output: %s, remoteSize: %s, remoteChecksum: %s" % (status, output, remoteSize, remoteChecksum))
        if status:
             self.log("Failed to stagein this file: %s" % output)
             return  PilotErrors.ERR_STAGEINFAILED, output

        status, output, localSize, localChecksum = self.getLocalFileInfo(destination, checksumType)
        if status:
            self.log("Failed to get local file(%s) info." % destination)
            return status, output

        status, output = self.verifyStage(localSize, localChecksum, remoteSize, remoteChecksum)

        if report:
            report['filesize'] = remoteSize
        self.log("Finished to stagin file %s(status:%s, output:%s)" % (source, status, output))
        return status, output

    def stageOut(self, source, destination, token, experiment=None, outputDir=None, timeout=3600, os_bucket_id=-1, report=None):
        """Stage in the source file"""
        self.log("Starting to stageout file %s to %s with token: %s, os_bucket_id: %s" % (source, destination, token, os_bucket_id))

        status, output = self.setup(experiment, destination, os_bucket_id=os_bucket_id, label='w')
        if status:
            return status, output, None, None

        status, output, localSize, localChecksum = self.getLocalFileInfo(source)
        self.log("getLocalFileInfo  status: %s, output: %s, localSize: %s, localChecksum: %s" % ( status, output, localSize, localChecksum))
        if status:
            self.log("Failed to get local file(%s) info." % destination)
            return status, output, None, None

        if report:
            report['filesize'] = localSize

        ldDest = self.loadBalanceURL(destination)
        if ldDest:
            self.log("Change url %s to load balance url %s" % (destination, ldDest))
            destination = ldDest

        status, output, remoteSize, remoteChecksum = self.stageOutFile(source, destination, localSize, localChecksum, token, outputDir=outputDir, timeout=timeout)
        self.log("stageOutFile status: %s, output: %s, remoteSize: %s, remoteChecksum: %s" % (status, output, remoteSize, remoteChecksum))
        if status:
             self.log("Failed to stageout this file: %s" % output)
             return  PilotErrors.ERR_STAGEOUTFAILED, output, localSize, localChecksum

        # remoteSize, remoteChecksum = self.getRemoteFileInfo(destination)
        # self.log("getRemoteFileInfo remoteSize: %s, remoteChecksum: %s" % (remoteSize, remoteChecksum))
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
        os_bucket_id = pdict.get('os_bucket_id', -1)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 's3objectstore', lfn, guid)

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        status, output = self.stageIn(gpfn, fullname, fsize, fchecksum, experiment, os_bucket_id=os_bucket_id, report=report)
        report['eventType'] = 'get_es'

        parsed = urlparse.urlparse(gpfn)
        scheme = parsed.scheme
        hostname = parsed.netloc.partition(':')[0]
        port = int(parsed.netloc.partition(':')[2])
        report['remoteSite'] = '%s://%s:%s' % (scheme, hostname, port)

        if status == 0:
            updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", ftype="input")
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
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        dsname = pdict.get('dsname', '')
        experiment = pdict.get('experiment', '')
        outputDir = pdict.get('outputDir', '')
        os_bucket_id = pdict.get('os_bucket_id', -1)
        timeout = pdict.get('timeout', None)
        if not timeout:
            timeout = self.timeout

        # get the site information object
        si = getSiteInformation(experiment)

        # get the Rucio tracing report
        report = self.getStubTracingReport(pdict['report'], 's3objectstore', lfn, guid)

        parsed = urlparse.urlparse(destination)
        scheme = parsed.scheme
        hostname = parsed.netloc.partition(':')[0]
        port = int(parsed.netloc.partition(':')[2])
        report['remoteSite'] = '%s://%s:%s' % (scheme, hostname, port)

        filename = os.path.basename(source)
        surl = destination
        self.log("surl=%s, timeout=%s" % (surl, timeout))
        if "log.tgz" in surl:
            surl = surl.replace(lfn, "%s:%s"%(scope,lfn))
        else:
            report['eventType'] = 'put_es'

        status, output, size, checksum = self.stageOut(source, surl, token, experiment, outputDir=outputDir, timeout=timeout, os_bucket_id=os_bucket_id, report=report)
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
        self.prepareReport(state, report)
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

class S3ObjctStore(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(S3ObjctStore, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, privateKey, publicKey, is_secure, useTimerCommand):
        self.access_key = publicKey
        self.secret_key = privateKey
        self.hostname = None
        self.port = None
        self.is_secure = is_secure
        self.buckets = {}
        self._useTimerCommand = useTimerCommand

    def get_key(self, url, create=False):
        import boto
        import boto.s3.connection
        from boto.s3.key import Key

        parsed = urlparse.urlparse(url)
        scheme = parsed.scheme
        self.hostname = parsed.netloc.partition(':')[0]
        self.port = int(parsed.netloc.partition(':')[2])
        path = parsed.path.strip("/")

        pos = path.index("/")
        bucket_name = path[:pos]
        key_name = path[pos+1:]

        bucket_key = "%s_%s_%s" % (self.hostname, self.port, bucket_name)
        if bucket_key in self.buckets:
            bucket = self.buckets[bucket_key]
        else:
            self.__conn = boto.connect_s3(
                aws_access_key_id = self.access_key,
                aws_secret_access_key = self.secret_key,
                host = self.hostname,
                port = self.port,
                is_secure=self.is_secure, # False,               # uncommmnt if you are not using ssl
                calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                )

            try:
                bucket = self.__conn.get_bucket(bucket_name)
            except boto.exception.S3ResponseError, e:
                tolog("Cannot get bucket: %s" % traceback.format_exc())

                bucket = self.__conn.create_bucket(bucket_name)

        if create:
            key = Key(bucket, key_name)
            # key.set_metadata('mode',33188)
        else:
            key = bucket.get_key(key_name)

        return key

    def getRemoteFileInfo(self, file):
        # disable checksum and size checking, just return None
        # because it sends too many HEAD operations to objectstore which is not needed
        return None, None

        http_proxy = os.environ.get("http_proxy")
        https_proxy = os.environ.get("https_proxy")
        if http_proxy:
            del os.environ['http_proxy']
        if https_proxy:
            del os.environ['https_proxy']

        key = self.get_key(file)
        md5 = key.get_metadata("md5")
        key.close(fast=True)

        if http_proxy:
            os.environ['http_proxy'] = http_proxy
        if https_proxy:
            os.environ['https_proxy'] = https_proxy
        return key.size, md5

    def s3StageInFile(self, source, destination, sourceSize=None, sourceChecksum=None):
        http_proxy = os.environ.get("http_proxy")
        https_proxy = os.environ.get("https_proxy")
        if http_proxy:
            del os.environ['http_proxy']
        if https_proxy:
            del os.environ['https_proxy']

        retCode = 0
        retStr = None
        key = None
        retSize = None
        retChecksum = None
        try:
            key = self.get_key(source)
            if key is None:
                retCode = -1
                retStr = "source file(%s) cannot be found" % source

            key.get_contents_to_filename(destination)
            retSize = key.size

            # for big file with multiple parts, key.etag is not the md5
            # if key.md5 and key.md5 != key.etag.strip('"').strip("'"):
            #     return -1, "client side checksum(key.md5=%s) doesn't match server side checksum(key.etag=%s)" % (key.md5, key.etag.strip('"').strip("'"))
            if sourceSize and str(sourceSize) != '0' and str(sourceSize) != str(key.size):
                retCode = -1
                retStr = "source size(%s) doesn't match key size(%s)" % (sourceSize, key.size)
            # if sourceChecksum and sourceChecksum != key.md5:
            #     retCode = -1
            #     retStr = "source checksum(%s) doesn't match key checksum(%s)" % (sourceChecksum, key.md5)
        except Exception as e:
            retCode = -1
            retStr = str(e)
        finally:
            if key:
                key.close(fast=True)

        if http_proxy:
            os.environ['http_proxy'] = http_proxy
        if https_proxy:
            os.environ['https_proxy'] = https_proxy

        # returnCode, returnErrStr, size, checksum
        return retCode, retStr, retSize, retChecksum

    def s3StageOutFile(self, source, destination, sourceSize=None, sourceChecksum=None, token=None):
        http_proxy = os.environ.get("http_proxy")
        https_proxy = os.environ.get("https_proxy")
        if http_proxy:
            del os.environ['http_proxy']
        if https_proxy:
            del os.environ['https_proxy']

        retCode = 0
        retStr = None
        key = None
        retSize = None
        retChecksum = None
        try:
            key = self.get_key(destination, create=True)
            if key is None:
                retCode = -1
                retStr = "Failed to create S3 key on destination (%s)" % destination
            # key.set_metadata("md5", sourceChecksum)
            size = key.set_contents_from_filename(source)

            retSize = key.size
            # if key.md5 != key.etag.strip('"').strip("'"):
            #     return -1, "client side checksum(key.md5=%s) doesn't match server side checksum(key.etag=%s)" % (key.md5, key.etag.strip('"').strip("'"))
            if sourceSize and str(sourceSize) != str(key.size):
                retCode = -1
                retStr = "source size(%s) doesn't match key size(%s)" % (sourceSize, key.size)
            #if sourceChecksum and sourceChecksum != key.md5:
            #    retCode = -1
            #    retStr = "source checksum(%s) doesn't match key checksum(%s)" % (sourceChecksum, key.md5)
        except Exception as e:
            retCode = -1
            retStr = str(e)
        finally:
            if key:
                key.close(fast=True)

        if http_proxy:
            os.environ['http_proxy'] = http_proxy
        if https_proxy:
            os.environ['https_proxy'] = https_proxy

        # returnCode, returnErrStr, size, checksum
        return retCode, retStr, retSize, retChecksum

    def stageInFile(self, source, destination, sourceSize=None, sourceChecksum=None):
        if self._useTimerCommand:
            timerCommand = TimerCommand()
            ret = timerCommand.runFunction(self.s3StageInFile, args=(source, destination, sourceSize, sourceChecksum), timeout=3600)
            return ret
        else:
            return self.s3StageInFile(source, destination, sourceSize, sourceChecksum)

    def stageOutFile(self, source, destination, sourceSize=None, sourceChecksum=None, token=None, timeout=3600):
        if self._useTimerCommand:
            timerCommand = TimerCommand()
            ret = timerCommand.runFunction(self.s3StageOutFile, args=(source, destination, sourceSize, sourceChecksum, token), timeout=timeout)
            return ret
        else:
            return self.s3StageOutFile(source, destination, sourceSize, sourceChecksum, token)
