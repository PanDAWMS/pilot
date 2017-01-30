"""
  storm SiteMover

  :author: Mario Lassnig <mario.lassnig@cern.ch>, 2016
"""

from .base import BaseSiteMover

from TimerCommand import TimerCommand
from PilotErrors import PilotException

from rucio.client import ReplicaClient

from datetime import datetime
from xml.dom import minidom

import os, shutil

class stormSiteMover(BaseSiteMover):
    """
    SiteMover that queries HTTP etag for physcial storage location, then
    symlinks for stage in, and copies for stage out
    """

    name = 'storm'
    schemes = ['file']
    version = '20161125.005'

    require_replicas = False       ## quick hack to avoid query Rucio to resolve input replicas

    def __init__(self, *args, **kwargs):
        super(stormSiteMover, self).__init__(*args, **kwargs)
        self.log('storm sitemover version: %s' % self.version)

    def resolve_replica(self, fspec, protocol, ddm=None):
        """
        Overridden method -- unused
        """

        return {'ddmendpoint': fspec.replicas[0][0],
                'surl': None,
                'pfn': fspec.lfn}

    def stageIn(self, source, destination, fspec):
        """
        Query HTTP for etag, then symlink to the pilot working directory.

        :param source:      original file location
        :param destination: where to create the link
        :param fspec:       dictionary containing destination replicas, scope, lfn
        :return:            destination file details (checksumtype, checksum, size)
        """

        self.log('source: %s' % str(source))
        self.log('destination: %s' % str(destination))
        self.log('fspec: %s' % str(fspec))
        self.log('fspec.scope: %s' % str(fspec.scope))
        self.log('fspec.lfn: %s' % str(fspec.lfn))
        self.log('fspec.ddmendpoint: %s' % str(fspec.ddmendpoint))

        # figure out the HTTP SURL from Rucio
        rc = ReplicaClient()
        http_surl_reps = [r for r in rc.list_replicas(dids=[{'scope': fspec.scope,
                                                             'name': fspec.lfn}],
                                                      schemes=['https'],
                                                      rse_expression=fspec.ddmendpoint)]
        self.log('http_surl_reps: %s' % http_surl_reps)

        http_surl = http_surl_reps[0]['rses'][fspec.ddmendpoint][0].rsplit('_-')[0]
        self.log('http_surl: %s' % http_surl)

        # retrieve the TURL from the webdav etag
        cmd = 'davix-http --capath /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/etc/grid-security-emi/certificates --cert $X509_USER_PROXY -X PROPFIND %s' % http_surl
        self.log('ETAG retrieval: %s' % cmd)
        try:
            timer = TimerCommand(cmd)
            rcode, output = timer.run(timeout=10)
        except Exception, e:
            self.log('FATAL: could not retrieve STORM WebDAV ETag: %s' % e)
            raise PilotException('Could not retrieve STORM WebDAV ETag: %s' % e)
        p_output = minidom.parseString(output)

        # we need to strip off the quotation marks and the <timestamp> from the etag
        # but since we can have multiple underscores, we have to rely on the uniqueness
        # of the full LFN to make the split
        target = p_output.getElementsByTagName('d:getetag')[0].childNodes[0].nodeValue.replace('"', '')
        self.log('Symlink before: %s' % target)
        target = target.split(fspec.lfn)[0]+fspec.lfn
        self.log('Symlink after : %s' % target)

        # make the symlink
        try:
            self.log('Making symlink from %s to %s' % (target, destination))
            os.symlink(target, destination)
        except Exception, e:
            self.log('FATAL: could not create symlink: %s' % e)
            raise PilotException('Could not create symlink: %s' % e)

        self.log('Symlink creation successful')
        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}

    def stageOut(self, source, destination, fspec):
        """
        Copy the output file from the pilot working directory to the destination
        directory.

        :param source:      local file location
        :param destination: remote location to copy file
        :param fspec:       dictionary containing destination replicas, scope, lfn
        :return:            destination file details (checksumtype, checksum, size)
        """

        src = os.path.realpath(fspec.lfn)
        dest = os.path.join(self.init_dir, fspec.lfn)
        self.log('Moving %s to %s' % (src, dest))

        # copy the output
        try:
            shutil.move(src, dst)
        except Exception, e:
            self.log('FATAL: could not move outputfile: %s' % e)
            raise PilotException('Could not move outputfile: %s' % e)

        self.log('Move successful')

        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}
