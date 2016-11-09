"""
  mv SiteMover

  :author: David Cameron <david.cameron@cern.ch>, 2016
"""

from .base import BaseSiteMover

from PilotErrors import PilotException

import os, shutil

class mvSiteMover(BaseSiteMover):
    """ SiteMover that uses link for stage in and copy for stage out """

    name = 'mv'
    schemes = ['file'] # list of supported schemes for transfers

    def __init__(self, *args, **kwargs):
        super(mvSiteMover, self).__init__(*args, **kwargs)

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

        # Assume file is available in parent directory for now
        # Later get the top level dir from a paramater
        self.log('Creating link from %s to ../%s' % (fspec.lfn, fspec.lfn))
        try:
            os.symlink('../%s' % fspec.lfn, fspec.lfn)
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
        Copy the output file from the pilot working directory to the top level
        directory.

        :param source:      local file location
        :param destination: remote location to copy file - not used
        :param fspec:  dictionary containing destination replicas, scope, lfn
        :return:       destination file details (checksumtype, checksum, size)
        """

        self.log('Copying %s to ../%s' % (fspec.lfn, fspec.lfn))
        try:
            shutil.copy(fspec.lfn, '../%s' % fspec.lfn)
        except IOError as e:
            raise PilotException('stageOut failed: %s' % str(e))

        self.log('Copy successful')
        checksum, checksum_type = fspec.get_checksum()
        return {'checksum_type': checksum_type,
                'checksum': checksum,
                'filesize': fspec.filesize}
