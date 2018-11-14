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
    """ SiteMover that uses rucio CLI for both get and put functionality """

    name = 'rucio'
    schemes = ['srm', 'gsiftp', 'root', 'https', 's3', 's3+rucio', 'davs']

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

        trace_str_pattern = "%s%s%s%s%s%s%s"
        trace_str = ''
        trace_str = trace_str_pattern % (" --trace_appid \"%s\"" %  self.trace_report['appid'] if self.trace_report['appid'] is not None else '',
                                         " --trace_dataset \"%s\"" % self.trace_report['dataset'] if self.trace_report['dataset'] is not None else '',
                                         " --trace_datasetscope \"%s\"" % self.trace_report['scope'] if self.trace_report['scope'] is not None else '',
                                         " --trace_eventtype \"get_sm%s\"" % self.trace_report['eventType'] if self.trace_report['eventType'] else '',
                                         " --trace_pq \"%s\"" % self.trace_report['pq'] if self.trace_report['pq'] is not None else '',
                                         " --trace_taskid \"%s\"" % self.trace_report['taskid'] if self.trace_report['taskid'] is not None else '',
                                         " --trace_usrdn \"%s\"" % self.trace_report['usrdn'] if self.trace_report['usrdn'] is not None else '' )

        cmd = 'rucio -v download %s --dir %s --rse %s --pfn %s %s:%s' % (trace_str,
                                                                      dirname(dst),
                                                                      fspec.ddmendpoint,
                                                                      fspec.turl,
                                                                      fspec.scope,
                                                                      fspec.lfn)

#        cmd = 'rucio -v download --dir %s --rse %s --pfn %s %s:%s' % (dirname(dst),
#                                                                      fspec.ddmendpoint,
#                                                                      fspec.turl,
#                                                                      fspec.scope,
#                                                                      fspec.lfn)

        # Prepend the command with singularity if necessary
        from Singularity import singularityWrapper
        cmd = singularityWrapper(cmd, fspec.cmtconfig, dirname(dst))

        tolog('stageIn: %s' % cmd)
        s, o = getstatusoutput(cmd)
        tolog('stageInOutput: s=%s o=%s' % (s, o))
        if s:
            tolog('stageIn with CLI failed! Trying API. Error: %s' % o.replace('\n', ''))
            try:
                self.stageInApi(dst, fspec)
            except Exception as error:
                raise PilotException('stageIn with API failed:  %s' % error, code=PilotErrors.ERR_STAGEINFAILED)

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

    def stageInApi(self, dst, fspec):

        from rucio.client.downloadclient import DownloadClient
        download_client = DownloadClient()
        logger = Logger()
        download_client.logger = logger.log

        f = {}
        f['did_scope'] = fspec.scope
        f['did_name'] = fspec.lfn
        f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
        f['rse'] = fspec.ddmendpoint
        f['base_dir'] = dirname(dst)
        if fspec.turl:
            f['pfn'] = fspec.turl

        # proceed with the download
        tolog('_stageInApi: %s' % str(f))
        trace_pattern = {}
        if self.trace_report:
            trace_pattern = self.trace_report
        result = []
        download_client = DownloadClient()
        if fspec.turl:
            result = download_client.download_pfns([f], 1)
        else:
            result = download_client.download_dids([f])

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

        if fspec.storageId and int(fspec.storageId) > 0:
            if self.isDeterministic(fspec.ddmendpoint):
                cmd = 'rucio -v upload --no-register --rse %s --scope %s %s' % (fspec.ddmendpoint,
                                                                                fspec.scope,
                                                                                fspec.pfn if fspec.pfn else fspec.lfn)
            else:
                cmd = 'rucio -v upload --no-register --rse %s --scope %s --pfn %s %s' % (fspec.ddmendpoint,
                                                                                         fspec.scope,
                                                                                         fspec.turl,
                                                                                         fspec.pfn if fspec.pfn else fspec.lfn)
        else:
            guid = ' --guid %s' % fspec.guid if fspec.lfn and '.root' in fspec.lfn else ''
            cmd = 'rucio -v upload%s --no-register --rse %s --scope %s %s' % (guid, fspec.ddmendpoint,
                                                                              fspec.scope,
                                                                              fspec.pfn if fspec.pfn else fspec.lfn)

        # Prepend the command with singularity if necessary
        from Singularity import singularityWrapper
        cmd = singularityWrapper(cmd, fspec.cmtconfig, dirname(src))

        tolog('stageOutCmd: %s' % cmd)
        s, o = getstatusoutput(cmd)
        tolog('stageOutOutput: s=%s o=%s' % (s, o))

        if s:
            raise PilotException('stageOut failed -- rucio upload did not succeed: %s' % o.replace('\n', ''))
            #tolog('stageOut with CLI failed! Trying API. Error: %s' % o.replace('\n', ''))
            #try:
            #    self.stageOutApi(src, fspec)
            #except Exception as error:
            #    raise PilotException('stageOut with API faied:  %s' % error)

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}

    def stageOutApi(self, src, fspec):

        from rucio.client.uploadclient import UploadClient
        upload_client = UploadClient()
        logger = Logger()
        upload_client.logger = logger.log

        # traces are turned off
        if hasattr(upload_client, 'tracing'):
            upload_client.tracing = self.tracing

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
