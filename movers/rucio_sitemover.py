"""
  Rucio SiteMover

  :author: Mario Lassnig <mario.lassnig@cern.ch>, 2015
"""

from .base import BaseSiteMover

from pUtil import tolog
from PilotErrors import PilotException

from commands import getstatusoutput
from os.path import dirname


class rucioSiteMover(BaseSiteMover):
    """ SiteMover that uses rucio CLI for both get and put functionality """

    name = 'rucio'

    def __init__(self, *args, **kwargs):
        super(rucioSiteMover, self).__init__(*args, **kwargs)

    def setup(self):
        """
        Overridden method -- unused
        """
        pass

    def resolve_replica(self, fspec):
        """
        Overridden method -- unused
        """

        return {'ddmendpoint': fspec.replicas[0][0],
                'surl': None,
                'pfn': fspec.lfn}

    def stageIn(self, turl, dst, fspec):
        """
        Use the rucio download command to stage in the file.

        :param turl:  overrides parent signature -- unused
        :param dst:   overrides parent signature -- unused
        :param fspec: dictionary containing destination replicas, scope, lfn
        :return:      destination file details (ddmendpoint, surl, pfn)
        """

        cmd = 'rucio download --dir %s --rse %s %s:%s' % (dirname(dst),
                                                          fspec.replicas[0][0],
                                                          fspec.scope,
                                                          fspec.lfn)
        tolog('stageIn: %s' % cmd)
        s, o = getstatusoutput(cmd)
        if s:
            raise PilotException('stageIn failed -- rucio download did not succeed: %s' % o.replace('\n', ''))

        # TODO: fix in rucio download to set specific outputfile
        #       https://its.cern.ch/jira/browse/RUCIO-2063
        cmd = 'mv %s %s' % (dirname(dst) + '/%s/%s' % (fspec.scope,
                                                       fspec.lfn),
                            dst)
        tolog('stageIn: %s' % cmd)
        s, o = getstatusoutput(cmd)
        if s:
            raise PilotException('stageIn failed -- could not move downloaded file to destination: %s' % o.replace('\n', ''))

        return {'ddmendpoint': fspec.replicas[0][0],
                'surl': None,
                'pfn': fspec.lfn}

    def stageOut(self, src, dst, fspec):
        """
        Use the rucio upload command to stage out the file.

        :param src:   overrides parent signature -- unused
        :param dst:   overrides parent signature -- unused
        :param fspec: dictionary containing destination ddmendpoint, scope, lfn
        :return:      destination file details (ddmendpoint, surl, pfn)
        """

        cmd = 'rucio upload --rse %s --scope %s %s' % (fspec.ddmendpoint,
                                                       fspec.scope,
                                                       fspec.lfn)
        tolog('stageIn: %s' % cmd)
        s, o = getstatusoutput(cmd)
        if s:
            raise PilotException('stageOut failed -- rucio upload did not succeed: %s' % o.replace('\n', ''))

        return {'ddmendpoint': fspec.ddmendpoint,
                'surl': fspec.surl,
                'pfn': fspec.lfn}
