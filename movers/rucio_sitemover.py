"""
  Rucio SiteMover

  :author: Mario Lassnig <mario.lassnig@cern.ch>, 2015-2018
"""

from .base import BaseSiteMover

from pUtil import tolog
from PilotErrors import PilotErrors, PilotException

from TimerCommand import getstatusoutput
from os.path import dirname
from StringIO import StringIO
import logging
import os

class Logger():
    """
    logging handler tha allows to read logger from Rucio
    """

    def __init__(self):
        self.stream = StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.log = logging.getLogger('logger')
        self.log.setLevel(logging.DEBUG)
        for handler in self.log.handlers:
            self.log.removeHandler(handler)
        self.log.addHandler(self.handler)
    def fetch(self):
        self.handler.flush()
        return self.stream.getvalue()

    def kill(self):
        self.log.removeHandler(self.handler)
        self.handler.close()


class rucioSiteMover(BaseSiteMover):
    """ SiteMover that uses rucio python API for both get and put functionality """

    name = 'rucio'
    schemes = ['srm', 'gsiftp', 'root', 'https', 's3', 's3+rucio', 'davs']
    tracing = False

    def __which(self, pgm):
        """
        Do not assume existing which command
        """
        path = os.getenv('PATH')
        for p in path.split(os.path.pathsep):
            p = os.path.join(p, pgm)
            if os.path.exists(p) and os.access(p, os.X_OK):
                return p

    def setup(self):
        """
        Basic setup
        """

        # disable rucio-clients ANSI colours - unneeded in logfiles :-)
        os.environ['RUCIO_LOGGING_FORMAT'] = '{0}%(asctime)s %(levelname)s [%(message)s]'

        # be verbose about the execution environment
        s, o = getstatusoutput('python -v -c "import gfal2" 2>&1 | grep dynamically')
        tolog('rucio_environment=%s' % str(os.environ))
        tolog('which rucio: %s' % self.__which('rucio'))
        tolog('which gfal2: %s' % o)
        tolog('which gfal-copy: %s' % self.__which('gfal-copy'))

    def shouldVerifyStageIn(self):
        """
            Should the get operation perform any file size/checksum verifications?
            can be customized for specific movers
        """

        return False

    def VerifyStageOut(self, dst, fspec):
        """
        Checks that the uploaded file is physically at the destination.

        :param dst:   destination rse
        :param fspec: file specifications
        """
        from rucio.rse import rsemanager as rsemgr
        rse_settings = rsemgr.get_rse_info(dst)
        uploaded_file = {'name':fspec.lfn, 'scope':fspec.scope}
        tolog('Checking file: %s' % str(fspec.lfn))
        return rsemgr.exists(rse_settings, [uploaded_file])

    def stageInFile(self, turl, dst, fspec):
        """
        Use the rucio download command to stage in the file.

        :param turl:  overrides parent signature -- unused
        :param dst:   overrides parent signature -- unused
        :param fspec: dictionary containing destination replicas, scope, lfn
        :return:      destination file details (ddmendpoint, surl, pfn)
        """

        num_retries = 2
        success = False
        try_counter = 0
        error_msg = None
        while not success and try_counter != num_retries: 
            try_counter += 1
            tolog('StageIn, attempt %s/%s' % (str(try_counter), str(num_retries)))
            try:
                self._stageInApi(dst, fspec)
                success = True
            except Exception as error:
                error_msg = error

        if error_msg and not success:
            raise PilotException('stageIn with API faied:  %s' % error, code=PilotErrors.ERR_STAGEINFAILED) 

        # TODO: fix in rucio download to set specific outputfile
        cmd = 'mv %s %s' % (dirname(dst) + '/%s/%s' % (fspec.scope,
                                                       fspec.lfn),
                            dst)
        tolog('stageInCmd: %s' % cmd)
        s, o = getstatusoutput(cmd)
        tolog('stageInOutput: s=%s o=%s' % (s, o))

        if s:
            raise PilotException('stageIn failed -- could not move downloaded file to destination: %s' % o.replace('\n', ''), code=PilotErrors.ERR_STAGEOUTFAILED)

        if not fspec.replicas and not fspec.filesize:
            fspec.filesize = os.path.getsize(dst)

        return None, None

    def _stageInApi(self, dst, fspec):

        # init. download client
        from rucio.client.downloadclient import DownloadClient
        download_client = DownloadClient()
        logger = Logger()
        download_client.logger = logger.log

        # traces are switched off
        if hasattr(download_client, 'tracing'):
            download_client.tracing = self.tracing

        # file specifications before the actual download
        f = {}
        f['did_scope'] = fspec.scope
        f['did_name'] = fspec.lfn
        f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
        f['rse'] = fspec.ddmendpoint
        f['base_dir'] = dirname(dst)
        if fspec.turl:
            f['pfn'] = fspec.turl

        # proceed with the download
        tolog('_stageInApi file: %s' % str(f))
        trace_pattern = {}
        if self.trace_report:
            trace_pattern = self.trace_report
        result = []
        if fspec.turl:
            result = download_client.download_pfns([f], 1, trace_custom_fields=trace_pattern)
        else:
            result = download_client.download_dids([f], trace_custom_fields=trace_pattern)

        clientState = 'FAILED'
        if result:
            clientState = result[0].get('clientState', 'FAILED') 

        # propagating rucio logger to pilot logger
        log_str = ''
        try:
            log_str = logger.fetch()
        except Exception as e:
            log_str =  e
        for msg in log_str.split('\n'):
            tolog('Rucio uploadclient: %s' % str(msg))
        try:
            logger.kill()
        except:
            tolog('Rucio logger was not closed properly.')

        return clientState 

    def stageOut(self, src, dst, fspec):
        """
        Use the rucio upload command to stage out the file.

        :param src:   overrides parent signature -- unused
        :param dst:   overrides parent signature -- unused
        :param fspec: dictionary containing destination ddmendpoint, scope, lfn
        :return:      destination file details (ddmendpoint, surl, pfn)
        """

        num_retries = 2
        success = False
        try_counter = 0
        error_msg = None
        while not success and try_counter != num_retries: 
            try_counter += 1
            tolog('StageOut, attempt %s/%s' % (str(try_counter), str(num_retries)))
            try:
                self._stageOutApi(src, fspec)
                success = True
            except Exception as error:
                error_msg = error

        #physical check after upload
        if success:
            try:
                file_exists = self.VerifyStageOut(fspec.ddmendpoint, fspec)
                tolog('File exists at the storage: %s' % str(file_exists))
            except Exception as e:
                tolog('File existence verification failed with: %s' % str(e))

        if error_msg and not success:
            raise PilotException('stageOut with API faied:  %s' % error_msg)

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}

    def _stageOutApi(self, src, fspec):

        # init. the uploadclient
        from rucio.client.uploadclient import UploadClient
        upload_client = UploadClient()
        logger = Logger()
        upload_client.logger = logger.log

        # traces are turned off
        if hasattr(upload_client, 'tracing'):
            upload_client.tracing = self.tracing

        # file specifications before the upload
        f = {}
        f['path'] = fspec.pfn if fspec.pfn else fspec.lfn
        f['rse'] = fspec.ddmendpoint
        f['did_scope'] = fspec.scope
        f['no_register'] = True

        if fspec.filesize:
            f['transfer_timeout'] = max(600, fspec.filesize*600/(100*1000*1000)) # 10 min for 100 MB file

        if fspec.storageId and int(fspec.storageId) > 0:
            if not self.isDeterministic(fspec.ddmendpoint):
                f['pfn'] = fspec.turl
        elif fspec.lfn and '.root' in fspec.lfn:
            f['guid'] = fspec.guid

        # process the upload
        tolog('_stageOutApi: %s' % str(f))
        upload_client.upload([f])

        # propagating rucio logger to pilot logger
        log_str = ''
        try:
            log_str = logger.fetch()
        except Exception as e:
            log_str =  e
        for msg in log_str.split('\n'):
            tolog('Rucio uploadclient: %s' % str(msg))
        try:
            logger.kill()
        except:
            tolog('Rucio logger was not closed properly.')

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}
