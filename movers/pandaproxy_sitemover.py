"""
  pandaproxy SiteMover 
  move files from/to S3 object storage using pre-signed links from Panda Proxy
  :author: Alexander Bogdanchikov
"""


from .base import BaseSiteMover


#from pUtil import getSiteInformation

from PilotErrors import PilotException
#from PilotErrors import PilotErrors

import os
#import traceback
import requests
import cgi

from datetime import datetime
from TimerCommand import TimerCommand
#from subprocess import Popen, PIPE, STDOUT

class pandaproxySiteMover(BaseSiteMover):
    """ sitemover to upload files with panda proxy to object store"""
    version = '20170523.001'

    name = 'pandaproxy'
    schemes = ['pandaproxy', 's3'] # list of supported schemes for transfers

    require_replicas = False       ## quick hack to avoid query Rucio to resolve input replicas

    def __init__(self, *args, **kwargs):
        super(pandaproxySiteMover, self).__init__(*args, **kwargs)
        self.log('pandaproxy mover version: %s' % self.version)
        #self.os_endpoint = None
        #self.os_bucket_endpoint = None
        self.osPublicKey = 'CERN_ObjectStoreKey.pub'
        self.osPrivateKey = 'CERN_ObjectStoreKey'
        self.pandaProxyURL = 'http://aipanda084.cern.ch:25064/proxy/panda'
        #self.pandaProxyURL = 'https://aipanda084.cern.ch:25128/proxy/panda'
    
    def unproxify(func):
        """ decorator to unproxify https and http connections """
        def wrapper(*args, **kwargs):
            http_proxy = os.environ.get("http_proxy")
            https_proxy = os.environ.get("https_proxy")
            if http_proxy:
                del os.environ['http_proxy']
            if https_proxy:
                del os.environ['https_proxy']
            funcResult = func(*args, **kwargs)
            if http_proxy:
                os.environ['http_proxy'] = http_proxy
            if https_proxy:
                os.environ['https_proxy'] = https_proxy
            return funcResult
        return wrapper
  
    def _stagefile(self, cmd, source, destination, filesize, is_stagein):
        """
            Stage the file (stagein or stageout respect to is_stagein value)
            :return: destination file details (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error

            this method was copied from lsm_sitemover (= the same in gfalcopy_sitemover)
        """
        
        timeout = self.getTimeOut(filesize)

        setup = self.getSetup()
        if setup:
            cmd = "%s; %s" % (setup, cmd)

        self.log("Executing command: %s, timeout=%s" % (cmd, timeout))

        t0 = datetime.now()
        is_timeout = False
        try:
            timer = TimerCommand(cmd)
            rcode, output = timer.run(timeout=timeout)
            is_timeout = timer.is_timeout
        except Exception, e:
            self.log("WARNING: %s threw an exception: %s" % (self.copy_command, e))
            rcode, output = -1, str(e)

        dt = datetime.now() - t0
        self.log("Command execution time: %s" % dt)
        self.log("is_timeout=%s, rcode=%s, output=%s" % (is_timeout, rcode, output))

        if is_stagein: # stage-in clean up: check if file was partially transferred
            if is_timeout or rcode: ## do clean up
                self.removeLocal(destination)

        if is_timeout:
            raise PilotException("Copy command self timed out after %s, timeout=%s, output=%s" % (dt, timeout, output), code=PilotErrors.ERR_GETTIMEOUT if is_stagein else PilotErrors.ERR_PUTTIMEOUT, state='CP_TIMEOUT')

        if rcode:
            self.log('WARNING: [is_stagein=%s] Stage file command (%s) failed: Status=%s Output=%s' % (is_stagein, cmd, rcode, output.replace("\n"," ")))
            error = self.resolveStageErrorFromOutput(output, source, is_stagein=is_stagein)
            rcode = error.get('rcode')
            if not rcode:
                rcode = PilotErrors.ERR_STAGEINFAILED if is_stagein else PilotErrors.ERR_STAGEOUTFAILED
            state = error.get('state')
            if not state:
                state = 'COPY_FAIL' #'STAGEIN_FAILED' if is_stagein else 'STAGEOUT_FAILED'

            raise PilotException(error.get('error'), code=rcode, state=state)

        # extract filesize and checksum values from output
        # check stage-out: not used at the moment

        return None, None


    def _getPresignedUrl(self, pandaProxyURL, jobId, osPrivateKey, osPublicKey, pandaProxySecretKey, s3URL, stageIn=False):
        try:
            if not pandaProxySecretKey or pandaProxySecretKey == "":
                raise PilotException("Panda proxy secret key is not set for panda proxy operations")

            data = {'pandaID': jobId,
                    'secretKey':'%s' % pandaProxySecretKey,
                    'publicKey': 'publicKey:%s' % osPublicKey,
                    'privateKey': 'privateKey:%s' % osPrivateKey,
                    'url':'%s' % s3URL}
            if stageIn:
                data['method'] = 'GET'


            requestedURL = pandaProxyURL+'/getPresignedURL'
            self.log("agb: get presinged url: requested url='%s',  data=%s" % (requestedURL, data) )

            res = requests.post(requestedURL, data=data)
            self.log("result=%s" % res)
            self.log("res.text.encode('ascii') = %s" % res.text.encode('ascii'))
            if res.status_code == 200:
                tmpDict = cgi.parse_qs(res.text.encode('ascii'))
                if int(tmpDict['StatusCode'][0]) == 0:
                    return tmpDict['presignedURL'][0]
                else:
                    raise PilotException( "get remote path presigned url from panda proxy error %s: %s" % (tmpDict['StatusCode'][0], tmpDict['ErrorMsg'][0]) )
            raise PilotException( "failed to get remote path presigned url from panda proxy, status code:  %s" % res.status_code)
        except Exception as e:
            raise PilotException( "failure when get presigned url from panda proxy: %s" % str(e))

    @unproxify
    def stageOut(self, source, destination, fspec):
        """
        get pre-signed link from Panda Proxy and upload local file to OS
        
        :param source:      local file location
        :param destination: remote location to copy file
        :param fspec:  dictionary containing destination replicas, scope, lfn
        :return:       destination file details (checksumtype, checksum, size)
        """
        self.log("pp: pandaProxySitemover stageOut parameters: src=%s, dst=%s fspec=%s" % (source, destination, fspec) )
        # !! source is not the source file name here !! source file name is fspec.pfn
        if fspec.osPrivateKey is not None and fspec.osPrivateKey != "":
            self.osPrivateKey = fspec.osPrivateKey
            self.log("pp: osPrivateKey: %s" % self.osPrivateKey)
        if fspec.osPublicKey is not None and fspec.osPublicKey != "":
            self.osPrivateKey = fspec.osPrivateKey
            self.log("pp: osPublicKey: %s" % self.osPublicKey)
        
        presignedURL = self._getPresignedUrl(self.pandaProxyURL, fspec.jobId, self.osPrivateKey, self.osPublicKey, fspec.pandaProxySecretKey, destination, stageIn=False)
        self.log("pp: presigned link was received: %s" % presignedURL)
        #raise Exception('NOT IMPLEMENTED')

        src=fspec.pfn
        self.log("pp: source file name: %s" % src )

        filesize = os.path.getsize(src)
        checksum = fspec.get_checksum()
        
        if not checksum[0]: # checksum is not available => do calculate
            checksum = self.calc_file_checksum(src)
            fspec.set_checksum(checksum[0], checksum[1])

        if not checksum[1]:
            checksum = checksum[0]
        else:
            checksum = "%s:%s" % (checksum[1], checksum[0])

        cmd = 'curl -v --request PUT  --upload-file %s "%s"' % (src, presignedURL)
        # other possibility to use rucio instead, like: cmd = 'rucio upload --no-register OTHER-PARAMETERS-HERE --pfn %s %s' % (presignedURL, src)
        self.log("pp: call stagefile to run the command: %s" % cmd)
        self._stagefile(cmd, src, destination, filesize, is_stagein=False)

        self.log('pp: stageOut was finished')
        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}

                
    def stageInFile(self, source, destination, fspec=None):
        """
            Stage in the file.
            :return: destination file details (checksum, checksum_type) in case of success, throw exception in case of failure
            :raise: PilotException in case of controlled error
        """
        self.log("pandaProxySitemover stageInFile parameters: src=%s, dst=%s fspec=%s" % (source, destination, fspec) )
        raise Exception('NOT IMPLEMENTED')

    def stageIn(self, source, destination, fspec):
        """
        :param source:      original (remote) file location - not used
        :param destination: where to create the link
        :param fspec:  dictionary containing destination replicas, scope, lfn
        :return:       destination file details (checksumtype, checksum, size)
        """
        self.log("pp: pandaProxySitemover stageIn() parameters: src=%s, dst=%s fspec=%s" % (source, destination, fspec) )
        # currently there are no use cases with panda proxy stagin (e.g. merge jobs store files on the GRID storage --> can not run on BOINC)
        raise Exception('NOT IMPLEMENTED')


    def getSURL(self, se, se_path, scope, lfn, job=None):
        """
            Get final destination SURL of file to be moved
            job instance is passing here for possible JOB specific processing ?? FIX ME LATER
        """

        ### quick fix: this actually should be reported back from Rucio upload in stageOut()
        ### surl is currently (required?) being reported back to Panda in XML

        surl = se + os.path.join(se_path, lfn)
        return surl

    def resolve_replica(self, fspec, protocol, ddm=None):
        """
        Overridden method -- unused
        """
        self.log("agb: resolve_replica: fspec=%s" % fspec)
        if ddm:
            if ddm.get('type') not in ['OS_LOGS', 'OS_ES']:
                return {}
            if ddm.get('aprotocols'):
                surl_schema = 's3'
                xprot = [e for e in ddm.get('aprotocols').get('r', []) if e[0] and e[0].startswith(surl_schema)]
                if xprot:
                    surl = self.getSURL(xprot[0][0], xprot[0][2], fspec.scope, fspec.lfn)
                    retVal = {
                        'ddmendpoint': fspec.ddmendpoint,
                        'surl': surl,
                        'pfn': surl
                    }
                    self.log("resolve_replica: return %s" % retVal)
                    return retVal
        return {}

 