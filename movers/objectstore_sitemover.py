"""
  objectstore SiteMover

  :author: Wen Guan <wen.guan@cern.ch>, 2016
"""

from .rucio_sitemover import rucioSiteMover

from pUtil import tolog
from PilotErrors import PilotException

from commands import getstatusoutput
import os

class objectstoreSiteMover(rucioSiteMover):
    """ SiteMover that uses rucio sitemover for both get and put functionality """

    name = 'objectstore'
    schemes = ['s3', 's3+rucio'] # list of supported schemes for transfers

    require_replicas = False       ## quick hack to avoid query Rucio to resolve input replicas

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

    def resolve_replica(self, fspec, protocol, ddm=None):
        """
        Overridden method -- unused
        """
        if ddm:
            if ddm.get('type') not in ['OS_LOGS', 'OS_ES']:
                return {}
            if ddm.get('aprotocols'):
                surl_schema = 's3'
                xprot = [e for e in ddm.get('aprotocols').get('r', []) if e[0] and e[0].startswith(surl_schema)]
                if xprot:
                    surl = self.getSURL(xprot[0][0], xprot[0][2], fspec.scope, fspec.lfn)

                    return {'ddmendpoint': fspec.ddmendpoint,
                            'surl': surl,
                            'pfn': surl}
        return {}
