"""
  objectstore SiteMover

  :author: Wen Guan <wen.guan@cern.ch>, 2016
"""

from .rucio_sitemover import rucioSiteMover

from pUtil import tolog
from PilotErrors import PilotException, PilotErrors

import os
import time


class objectstoreSiteMover(rucioSiteMover):
    """ SiteMover that uses rucio sitemover for both get and put functionality """

    name = 'objectstore'
    schemes = ['srm', 'gsiftp', 'root', 'https', 's3', 's3+rucio']  # list of supported schemes for transfers

    require_replicas = True       # if objectstoreID is set, mover.resolve_replicas() will skip the file

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

        # quick fix: this actually should be reported back from Rucio upload in stageOut()
        # surl is currently (required?) being reported back to Panda in XML

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
            if pathConvention is None:
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

    def stageIn(self, source, destination, fspec):
        """
        Use the rucio download command to stage in the file.

        :param source:  overrides parent signature -- unused
        :param destination:   overrides parent signature -- unused
        :param fspec: dictionary containing destination replicas, scope, lfn
        :return:      destination file details (ddmendpoint, surl, pfn)
        """
        stagein_result = super(objectstoreSiteMover, self).stageIn(source, destination, fspec)

        # verify checksum
        src_checksum, src_checksum_type = fspec.get_checksum()

        self.trace_report.update(validateStart=time.time())

        try:
            dst_checksum, dst_checksum_type = self.calc_file_checksum(destination)
        except Exception, e:
            self.log("verify StageIn: caught exception while getting local file=%s checksum: %s .. skipped" % (destination, e))

        try:
            if dst_checksum and dst_checksum_type and src_checksum and src_checksum_type:  # verify against source

                is_verified = src_checksum and src_checksum_type and dst_checksum == src_checksum and dst_checksum_type == src_checksum_type

                self.log("Remote checksum [%s]: %s  (%s)" % (src_checksum_type, src_checksum, source))
                self.log("Local  checksum [%s]: %s  (%s)" % (dst_checksum_type, dst_checksum, destination))
                self.log("checksum is_verified = %s" % is_verified)

                if type(dst_checksum) is str and type(src_checksum) is str:
                    if len(src_checksum) != len(dst_checksum):
                        self.log("Local and remote checksums have different lengths (%s vs %s), will lstrip them" % (dst_checksum, src_checksum))
                        src_checksum = src_checksum.lstrip('0')
                        dst_checksum = dst_checksum.lstrip('0')

                        is_verified = src_checksum and src_checksum_type and dst_checksum == src_checksum and dst_checksum_type == src_checksum_type

                        self.log("Remote checksum [%s]: %s  (%s)" % (src_checksum_type, src_checksum, source))
                        self.log("Local  checksum [%s]: %s  (%s)" % (dst_checksum_type, dst_checksum, destination))
                        self.log("checksum is_verified = %s" % is_verified)

                if not is_verified:
                    error = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" % \
                                            (src_checksum_type, os.path.basename(destination), dst_checksum, src_checksum)
                    if src_checksum_type == 'adler32':
                        state = 'AD_MISMATCH'
                        rcode = PilotErrors.ERR_GETADMISMATCH
                    else:
                        state = 'MD5_MISMATCH'
                        rcode = PilotErrors.ERR_GETMD5MISMATCH
                    raise PilotException(error, code=rcode, state=state)

                self.log("verifying stagein done. [by checksum] [%s]" % source)
                self.trace_report.update(clientState="DONE")
                return stagein_result

        except PilotException:
            raise
        except Exception, e:
            self.log("verify StageIn: caught exception while doing file checksum verification: %s ..  skipped" % e)

        # verify filesize
        try:
            src_fsize = fspec.filesize
            dst_fsize = os.path.getsize(destination)

            if src_fsize:
                is_verified = src_fsize and src_fsize == dst_fsize

                self.log("Remote filesize [%s]: %s" % (os.path.dirname(destination), src_fsize))
                self.log("Local  filesize [%s]: %s" % (os.path.dirname(destination), dst_fsize))
                self.log("filesize is_verified = %s" % is_verified)

                if not is_verified:
                    error = "Remote and local file sizes do not match for %s (%s != %s)" % (os.path.basename(destination), dst_fsize, src_fsize)
                    self.log(error)
                    raise PilotException(error, code=PilotErrors.ERR_GETWRONGSIZE, state='FS_MISMATCH')

                self.log("verifying stagein done. [by filesize] [%s]" % source)
                self.trace_report.update(clientState="DONE")
                return stagein_result

        except PilotException:
            raise
        except Exception, e:
            self.log("verify StageIn: caught exception while doing file size verification: %s .. skipped" % e)

        return stagein_result
