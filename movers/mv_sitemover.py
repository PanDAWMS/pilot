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
    schemes = ['file', 'srm', 'gsiftp', 'https', 'root', 'davs', 's3']

    require_replicas = False       ## quick hack to avoid query Rucio to resolve input replicas

    def __init__(self, *args, **kwargs):
        super(mvSiteMover, self).__init__(*args, **kwargs)
        self.init_dir = os.environ['HOME']

    def createOutputList(self, fspec, dest):

        if fspec.turl.startswith('s3://'):
            # Use Rucio proxy to upload to OS
            turl = fspec.turl
            turl = re.sub(r'^s3', 's3+rucio', turl)
            # Add failureallowed option so failed upload does not fail job
            rucio = 'rucio://rucio-lb-prod.cern.ch;failureallowed=yes/objectstores'
            rse = fspec.ddmendpoint
            activity = 'write'
            destsurl = '/'.join([rucio, turl, rse, activity])
        else:
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


    def getSURL(self, se, se_path, scope, lfn, job=None, pathConvention=None, taskId=None, ddmEndpoint=None):
        """
        Override from base because it throws an exception for paths without
        '/rucio' so we need this to do OS uploads 
        """

        if ddmType and ddmType in ['OS_LOGS', 'OS_ES']:
            surl = se + os.path.join(se_path, "%s:%s" % (scope, lfn))
        else:
            surl = self.getSURLRucio(se, se_path, scope, lfn, job)
        return surl


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
        # block pre-load input file BEGIN
        # Alexander B.: the next block is necessary for testing of BOINC pilot on GRID resources.
        # it works only if the special variable "PRELOAD_STAGIN_FILES_FOR_MV_SITEMOVER" is set in external environment
        fileExpectedLocation='%s/%s' % (self.init_dir, fspec.lfn)     # the place where original mv_sitemover expect to find the file
        if not os.path.exists(fileExpectedLocation):
            preloadFilesFlag=os.environ.get("PRELOAD_STAGIN_FILES_FOR_MV_SITEMOVER")
            if preloadFilesFlag and ( preloadFilesFlag=='1' or preloadFilesFlag=="yes"  or preloadFilesFlag=="on" ) :
                # the expected behavior actions:
                # rucio download valid1:EVNT.01416937._000001.pool.root.1
                # mv valid1/EVNT.01416937._000001.pool.root.1 ./EVNT.09355665._094116.pool.root.1
                
                self.log('pp: pre-load files for mv_sitemover: download locally stageIn the file: scope=%s file=%s' % (fspec.scope, fspec.lfn))

                cmd = 'rucio download %s:%s' % (fspec.scope, fspec.lfn)
                self.log("Executing command: %s" % cmd)

                from subprocess import Popen, PIPE, STDOUT
                c = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
                output = c.communicate()[0]
                if c.returncode:
                    raise Exception(output)
                
                fileRucioLocation='%s/%s' % (fspec.scope, fspec.lfn)  # the place where Rucio downloads file
                self.log('pp: move from %s to %s' % (fileRucioLocation, fileExpectedLocation))
                try:
                    os.rename(fileRucioLocation, fileExpectedLocation)
                except OSError, e:
                    raise PilotException('stageIn failed when rename the file from rucio location: %s' % str(e))
        # block preload input file END
        
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
            # OS copy is done first so don't move
            if fspec.activity != 'pls':
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
