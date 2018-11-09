"""
  Rucio SiteMover

  :author: Mario Lassnig <mario.lassnig@cern.ch>, 2015-2018
"""

from .base import BaseSiteMover

from pUtil import tolog
from PilotErrors import PilotErrors, PilotException

from TimerCommand import getstatusoutput
from os.path import dirname

import os


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

        # traces are switched off
        if hasattr(download_client, 'tracing'):
            download_client.tracing = self.tracing

        # file specifications before the actual download
        f = {}
        f['scope'] = fspec.scope
        f['name'] = fspec.lfn
        f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
        f['rse'] = fspec.ddmendpoint
        f['base_dir'] = dirname(dst)
        if fspec.turl:
            f['pfn'] = fspec.turl

        # proceed with the download
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

        if error_msg and not success:
            raise PilotException('stageOut with API faied:  %s' % error_msg)

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}

    def _stageOutApi(self, src, fspec):

        # init. the uploadclient
        from rucio.client.uploadclient import UploadClient
        upload_client = UploadClient()

        # traces are turned off
        if hasattr(upload_client, 'tracing'):
            upload_client.tracing = self.tracing

        # file specifications before the upload
        f = {}
        f['path'] = fspec.pfn if fspec.pfn else fspec.lfn
        f['rse'] = fspec.ddmendpoint
        f['did_scope'] = fspec.scope
        f['no_register'] = True

        if fspec.storageId and int(fspec.storageId) > 0:
            if not self.isDeterministic(fspec.ddmendpoint):
                f['pfn'] = fspec.turl
        elif fspec.lfn and '.root' in fspec.lfn:
            f['guid'] = fspec.guid

        # proceed with the upload
        tolog('_stageOutApi: %s' % str(f))
        try:
            upload_client.upload([f])
        except Exception as err:
            tolog('_stageOutApi: upload failed - %s' % err)

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}
