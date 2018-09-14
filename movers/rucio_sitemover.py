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

    def stageIn(self, turl, dst, fspec):
        """
        Use the rucio download command to stage in the file.

        :param turl:  overrides parent signature -- unused
        :param dst:   overrides parent signature -- unused
        :param fspec: dictionary containing destination replicas, scope, lfn
        :return:      destination file details (ddmendpoint, surl, pfn)
        """

        cmd = 'rucio -v download --dir %s --rse %s --pfn %s %s:%s' % (dirname(dst),
                                                                      fspec.ddmendpoint,
                                                                      fspec.turl,
                                                                      fspec.scope,
                                                                      fspec.lfn)

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

        return {'ddmendpoint': fspec.replicas[0][0] if fspec.replicas else fspec.ddmendpoint,
                'surl': None,
                'pfn': fspec.lfn}

    def stageInApi(self, dst, fspec):

        from rucio.client.downloadclient import DownloadClient

        f = {}
        f['scope'] = fspec.scope
        f['name'] = fspec.lfn
        f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
        f['rse'] = fspec.ddmendpoint
        f['base_dir'] = dirname(dst)
        if fspec.turl:
            f['pfn'] = fspec.turl

        result = []
        download_client = DownloadClient()
        if fspec.turl:
            result = download_client.download_pfns([f], 1)
        else:
            result = download_client.download_dids([f])

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

        f = {}
        f['path'] = fspec.pfn if fspec.pfn else fspec.lfn
        f['rse'] = fspec.ddmendpoint
        f['scope'] = fspec.scope
        f['no_register'] = True

        if fspec.storageId and int(fspec.storageId) > 0:
            if not self.isDeterministic(fspec.ddmendpoint):
                f['pfn'] = fspec.turl
        elif fspec.lfn and '.root' in fspec.lfn:
            f['guid'] = fspec.guid

        uc = UploadClient()
        uc.upload([f])

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}
