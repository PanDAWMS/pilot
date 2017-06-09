"""
  objectstore SiteMover

  :author: Wen Guan <wen.guan@cern.ch>, 2016
"""

from .rucio_sitemover import rucioSiteMover

from pUtil import tolog
from PilotErrors import PilotException

from commands import getstatusoutput
import os
import hashlib

class objectstoreSiteMover(rucioSiteMover):
    """ SiteMover that uses rucio sitemover for both get and put functionality """

    name = 'objectstore'
    schemes = ['srm', 'gsiftp', 'root', 'https', 's3', 's3+rucio'] # list of supported schemes for transfers

    require_replicas = True       ## if objectstoreID is set, mover.resolve_replicas() will skip the file

    def __init__(self, *args, **kwargs):
        super(objectstoreSiteMover, self).__init__(*args, **kwargs)

    def setup(self):
        """
        Overridden method -- unused
        """
        pass

    def getSURL(self, se, se_path, scope, lfn, job=None):
        """
            Get final destination SURL of file to be moved
            job instance is passing here for possible JOB specific processing ?? FIX ME LATER
        """

        ### quick fix: this actually should be reported back from Rucio upload in stageOut()
        ### surl is currently (required?) being reported back to Panda in XML

        surl = se + os.path.join(se_path, lfn)
        return surl

    def get_path(self, scope, name):
        """
        Get rucio deterministic path
        """
        hstr = hashlib.md5('%s:%s' % (scope, name)).hexdigest()
        if scope.startswith('user') or scope.startswith('group'):
            scope = scope.replace('.', '/')
        return '%s/%s/%s/%s' % (scope, hstr[0:2], hstr[2:4], name)

    def resolve_replica(self, fspec, protocol, ddm=None):
        """
        Overridden method -- unused
        """
        # tolog("To resolve replica for file (%s) protocol (%s) ddm (%s)" % (fspec, protocol, ddm))
        if ddm and fspec.storageId and fspec.storageId > 0:
            if ddm.get('type') in ['OS_LOGS', 'OS_ES']:
                if ddm.get('aprotocols'):
                    surl_schema = 's3'
                    xprot = [e for e in ddm.get('aprotocols').get('r', []) if e[0] and e[0].startswith(surl_schema)]
                    if xprot:
                        surl = self.getSURL(xprot[0][0], xprot[0][2], fspec.scope, fspec.lfn)

                        return {'ddmendpoint': fspec.ddmendpoint,
                                'surl': surl,
                                'pfn': surl}
            else:
                if ddm.get('aprotocols'):
                    min = None
                    proto = None
                    for prot in ddm.get('aprotocols').get('r', []):
                        if prot[0]:
                            if min is None:
                                min = prot[1]
                                proto = prot
                            elif prot[1] < min:
                                min = prot[1]
                                proto = prot
                    if proto:
                        surl = ''.join([proto[2], '/', self.get_path(fspec.scope, fspec.lfn)])
                        surl = surl.replace('//', '/')
                        surl = ''.join([proto[0], surl])
                        return {'ddmendpoint': fspec.ddmendpoint, 'surl': surl, 'pfn': surl}
        return {}
