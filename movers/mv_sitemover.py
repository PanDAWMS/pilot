"""
  mv SiteMover

  :author: David Cameron <david.cameron@cern.ch>, 2016
"""

from .base import BaseSiteMover

from PilotErrors import PilotException

import os, re, shutil

class mvSiteMover(BaseSiteMover):
    """ SiteMover that uses link for stage in and move for stage out """

    name = 'mv'
    # list of supported schemes for transfers - use them all since surl is not used
    schemes = ['file', 'srm', 'gridftp', 'https', 'root']

    require_replicas = False       ## quick hack to avoid query Rucio to resolve input replicas

    def __init__(self, *args, **kwargs):
        super(mvSiteMover, self).__init__(*args, **kwargs)
        self.init_dir = os.environ['HOME']

    def createOutputList(self, fspec, dest):

        # Calculate checksum of file - even though it is already known by pilot
        # it is not passed through to new movers
        checksum = self.calc_file_checksum(dest)[0]
        token = self.ddmconf.get(fspec.ddmendpoint, {}).get('token')
        # Add ARC options to SURL
        destsurl = re.sub(r'((:\d+)/)', r'\2;autodir=no;spacetoken=%s/' % token, fspec.turl)
        destsurl += ':checksumtype=%s:checksumvalue=%s' % (self.checksum_type, checksum)

        self.log('Adding to output.list: %s %s' % (fspec.lfn, destsurl))
        # Write output.list
        with open(os.path.join(self.init_dir, 'output.list'), 'a') as f:
            f.write('%s %s\n' % (fspec.lfn, destsurl))

    def stageIn(self, source, destination, fspec):
        """
        Override stageIn rather than stageInFile since most of stageIn is
        unnecessary.
        Make a link from the downloaded file to the pilot working directory.

        :param source:      original (remote) file location - not used
        :param destination: where to create the link
        :param fspec:  dictionary containing destination replicas, scope, lfn
        :return:       destination file details (checksumtype, checksum, size)
        """

        src = os.path.join(self.init_dir, fspec.lfn)
        self.log('Creating link from %s to %s' % (fspec.lfn, src))
        try:
            os.symlink(src, fspec.lfn)
        except OSError as e:
            raise PilotException('stageIn failed: %s' % str(e))

        if not os.path.exists(fspec.lfn):
            raise PilotException('stageIn failed: symlink points to non-existent file')

        self.log('Symlink successful')
        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}

    def stageOut(self, source, destination, fspec):
        """
        Override stageOut rather than stageOutFile since most of stageOut is
        unnecessary.
        Move the output file from the pilot working directory to the top level
        directory.
        Create the output file list for ARC CE.

        :param source:      local file location
        :param destination: remote location to copy file
        :param fspec:  dictionary containing destination replicas, scope, lfn
        :return:       destination file details (checksumtype, checksum, size)
        """

        src = os.path.realpath(fspec.lfn)
        dest = os.path.join(self.init_dir, fspec.lfn)
        self.log('Moving %s to %s' % (src, dest))
        try:
            shutil.move(src, dest)
        except IOError as e:
            raise PilotException('stageOut failed: %s' % str(e))

        self.log('Copy successful')

        # Create output list for ARC CE
        self.createOutputList(fspec, dest)

        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}
