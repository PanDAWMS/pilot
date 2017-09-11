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

    def getSURL(self, se, se_path, scope, lfn, job=None, pathConvention=None, taskId=None, ddmEndpoint=None):
        """
            Get final destination SURL of file to be moved
            job instance is passing here for possible JOB specific processing ?? FIX ME LATER
        """

        ### quick fix: this actually should be reported back from Rucio upload in stageOut()
        ### surl is currently (required?) being reported back to Panda in XML

        tolog("getSURL: pathConvention: %s, taskId: %s, ddmEndpoint: %s" % (pathConvention, taskId, ddmEndpoint))
        if pathConvention and pathConvention >= 1000:
            scope = 'transient'
            pathConvention = pathConvention - 1000
            if pathConvention == 0:
                pathConvention = None

        if not ddmEndpoint or self.isDeterministic(ddmEndpoint):
            return self.getSURLRucio(se, se_path, scope, lfn)

        ddmType = self.ddmconf.get(ddmEndpoint, {}).get('type')
        if not (ddmType and ddmType in ['OS_ES']):
            return self.getSURLRucio(se, se_path, scope, lfn)
        else:
            if pathConvention == None:
                surl = se + os.path.join(se_path, lfn)
            else:
                # If pathConvention is not None, it means multiple buckets are used.
                # If pathConvention is bigger than or equal 100:
                #     The bucket name is '<atlas-eventservice>-<taskid>-<pathConventionNumber>'
                #     Real pathConvention is pathConvention - 100
                # Else:
                #     The bucket name is '<atlas-eventservice>-<pathConventionNumber>'
                #     Real pathConvention is pathConvention.

                while se_path.endswith("/"):
                    se_path = se_path[:-1]

                if pathConvention >= 100:
                    pathConvention = pathConvention - 100
                    if taskId is None and job is None:
                        raise PilotException("getSURL with pathConvention(%s) failed becuase both taskId(%s) and job(%s) are None" % (pathConvention, taskId, job), code=PilotErrors.ERR_FAILEDLFCGETREPS)
                    if taskId is None:
                        taskId = job.taskID
                    se_path = "%s-%s-%s" % (se_path, taskId, pathConvention)
                else:
                    se_path = "%s-%s" % (se_path, pathConvention)

                surl = se + os.path.join(se_path, lfn)
        return surl

    def resolve_replica(self, fspec, protocol, ddm=None):
        """
        Overridden method -- unused
        """
        # tolog("To resolve replica for file (%s) protocol (%s) ddm (%s)" % (fspec, protocol, ddm))
        if ddm and fspec.storageId and fspec.storageId > 0:
            if fspec.pathConvention and fspec.pathConvention >= 1000:
                fspec.scope = 'transient'
            if ddm.get('type') in ['OS_LOGS', 'OS_ES']:
                if ddm.get('aprotocols'):
                    surl_schema = 's3'
                    xprot = [e for e in ddm.get('aprotocols').get('r', []) if e[0] and e[0].startswith(surl_schema)]
                    if xprot:
                        surl = self.getSURL(xprot[0][0], xprot[0][2], fspec.scope, fspec.lfn, pathConvention=fspec.pathConvention, taskId=fspec.taskId, ddmEndpoint=fspec.ddmendpoint)

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
                        surl = self.getSURL(proto[0], proto[2], fspec.scope, fspec.lfn, pathConvention=fspec.pathConvention, taskId=fspec.taskId, ddmEndpoint=fspec.ddmendpoint)
                        return {'ddmendpoint': fspec.ddmendpoint, 'surl': surl, 'pfn': surl}
        return {}
