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
    schemes = ['file'] # list of supported schemes for transfers

    def __init__(self, *args, **kwargs):
        super(mvSiteMover, self).__init__(*args, **kwargs)
        # Set top-level dir to be one level up, until real dir is available
        self.init_dir = os.path.dirname(os.getcwd())

    def createOutputList(self, fspec):

        # Write output.list
        with open(os.path.join(self.init_dir, 'output.list'), 'a') as f:
            # Add ARC options
            token = self.ddmconf.get(fspec.ddmendpoint, {}).get('token')
            dest = re.sub(r'((:\d+)/)', r'\2;autodir=no;spacetoken=%s/' % token, fspec.surl)
            f.write('%s %s' % (fspec.lfn, dest))


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
        self.createOutputList(fspec)

        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}
