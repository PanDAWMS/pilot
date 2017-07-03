"""
  JobMover: top level mover to copy all Job files

  This functionality actually needs to be incorporated directly into RunJob,
  because it's just a workflow of RunJob
  but since RunJob at the moment is implemented as a Singleton
  it could be dangerous because of Job instance stored in the class object.
  :author: Alexey Anisenkov
"""

from . import getSiteMover
from .trace_report import TraceReport

from FileStateClient import updateFileState, dumpFileStates
from PilotErrors import PilotException, PilotErrors

from pUtil import tolog

import sys
import os
import time
import traceback
from random import shuffle, uniform
from subprocess import Popen, PIPE, STDOUT

try:
    import json # python2.6
except ImportError:
    import simplejson as json


class JobMover(object):
    """
        Top level mover to copy Job files from/to storage.
    """

    job = None  # Job object
    si = None   # Site information

    MAX_STAGEIN_RETRY = 5
    MAX_STAGEOUT_RETRY = 10

    _stageoutretry = 2 # default value
    _stageinretry = 2  # devault value

    _stagein_sleeptime_min = 20       # seconds, min allowed sleep time in case of stagein failure
    _stagein_sleeptime_max = 50       # seconds, max allowed sleep time in case of stagein failure

    _stageout_sleeptime_min = 4.9*60  # seconds, min allowed sleep time in case of stageout failure
    _stageout_sleeptime_max = 5*60    # seconds, max allowed sleep time in case of stageout failure


    def __init__(self, job, si, **kwargs):

        self.job = job
        self.si = si

        self.workDir = kwargs.get('workDir', '')

        self.stageoutretry = kwargs.get('stageoutretry', self._stageoutretry)
        self.stageinretry = kwargs.get('stageinretry', self._stageinretry)
        self.protocols = {}
        self.ddmconf = {}
        self.objectstorekeys = {}
        self.trace_report = {}

        self.useTracingService = kwargs.get('useTracingService', self.si.getExperimentObject().useTracingService())


    def log(self, value): # quick stub
        #sys.stdout.flush()
        tolog(value)
        sys.stdout.flush() # quick hack until proper central logging will be implemented

    def calc_stagein_sleeptime(self):
        return uniform(self._stagein_sleeptime_min, self._stagein_sleeptime_max)

    def calc_stageout_sleeptime(self):
        return uniform(self._stageout_sleeptime_min, self._stageout_sleeptime_max)

    @property
    def stageoutretry(self):
        return self._stageoutretry

    @stageoutretry.setter
    def stageoutretry(self, value):
        if value >= self.MAX_STAGEOUT_RETRY or value < 1:
            ival = value
            value = JobMover._stageoutretry
            self.log("WARNING: Unreasonable number of stage-out tries: %d, reset to default value=%s" % (ival, value))
        self._stageoutretry = value

    @property
    def stageinretry(self):
        return self._stageinretry

    @stageinretry.setter
    def stageinretry(self, value):
        if value >= self.MAX_STAGEIN_RETRY or value < 1:
            ival = value
            value = JobMover._stageinretry
            self.log("WARNING: Unreasonable number of stage-in tries: %d, reset to default value=%s" % (ival, value))
        self._stageinretry = value


    @classmethod
    def _prepare_input_ddm(self, ddm, localddms):
        """
            Sort and filter localddms for given preferred ddm entry
            :return: list of ordered ddmendpoint names
        """
        # set preferred ddm as first source
        # move is_tape ddm to the end

        ddms, tapes = [], []
        for e in localddms:
            ddmendpoint = e['name']
            if ddmendpoint == ddm['name']:
                continue
            if e['is_tape']:
                tapes.append(ddmendpoint)
            else:
                ddms.append(ddmendpoint)

        # randomize input
        shuffle(ddms)

        # TODO: apply more sort rules here

        ddms = [ddm['name']] + ddms + tapes

        return ddms


    def get_pfns(self, replica, protocol='root'):
        """ Extract the PFNs from the replicas dictionary"""

        pfns = {}  # FORMAT: { 'endpoint': [pfn1, ..], .. }

        for pfn in replica['pfns'].keys():
            if pfn.startswith(protocol):
                endpoint = replica['pfns'][pfn]['rse']
                if endpoint in pfns:
                    pfns[endpoint].append(pfn)
                else:
                    pfns[endpoint] = [pfn]
        return pfns


    def get_turl(self, pfns, endpoint=None):
        """ Get a turl from the pfns dictionary """

        turl = ""
        if not endpoint:
            keys = pfns.keys()
            turl = pfns[keys[0]][0]
        else:
            turl = pfns[endpoint][0]
        return turl


    def resolve_replicas(self, files, directaccesstype):
        """
            populates fdat.replicas of each entry from `files` list
            fdat.replicas = [(ddmendpoint, replica, ddm_se, ddm_path)]
            ddm_se -- integration logic -- is used to manually form TURL
            (quick stub until all protocols are properly populated in Rucio from AGIS)
            :return: `files`
        """

        # build list of local ddmendpoints grouped by site
        ddms = {}
        for ddm, dat in self.ddmconf.iteritems():
            if dat.get('state') != 'ACTIVE': # skip DISABLED ddms
                continue
            ddms.setdefault(dat['site'], []).append(dat)

        self.log("files=%s"%files)
        for fdat in files:
            if fdat.storageId and fdat.storageId > 0:
                # skip OS ddms, storageId -1 means normal RSE
                #self.log("fdat.storageId: %s" % fdat.storageId)
                #fdat.inputddms = [fdat.ddmendpoint]         ### is it used for OS?
                pass
            else:
                if fdat.storageId == 0 or fdat.storageId == -1:
                    fdat.storageId = None
                # build and order list of local ddms
                ddmdat = self.ddmconf.get(fdat.ddmendpoint)
                if not ddmdat:
                    raise Exception("Failed to resolve ddmendpoint by name=%s send by Panda job, please check configuration. fdat=%s" % (fdat.ddmendpoint, fdat))
                if not ddmdat['site']:
                    raise Exception("Failed to resolve site name of ddmendpoint=%s. please check ddm declaration: ddmconf=%s ... fdat=%s" % (fdat.ddmendpoint, ddmconf, fdat))
                localddms = ddms.get(ddmdat['site'])
                # sort and filter ddms (as possible input source)
                fdat.inputddms = self._prepare_input_ddm(ddmdat, localddms)

        # consider only normal ddmendpoints
        xfiles = [e for e in files if e.storageId is None]
        self.log("xfiles=%s"%xfiles)
        if not xfiles:
            return files

        # load replicas from Rucio
        from rucio.client import Client
        c = Client()

        dids = [dict(scope=e.scope, name=e.lfn) for e in xfiles]
        schemes = ['srm', 'root', 'https', 'gsiftp']
        self.log("dids=%s"%dids)
        self.log("schemes=%s"%schemes)
        # Get the replica list
        try:
            replicas = c.list_replicas(dids, schemes=schemes)
            result = []
            for rep in replicas:
                result.append(rep)
            replicas = result
            self.log("replicas received from rucio: %s" % replicas)
        except Exception, e:
            raise PilotException("Failed to get replicas from Rucio: %s" % e, code=PilotErrors.ERR_FAILEDLFCGETREPS)

        files_lfn = dict(((e.scope, e.lfn), e) for e in xfiles)
        self.log("files_lfn=%s"%files_lfn)

        for r in replicas:
            k = r['scope'], r['name']
            fdat = files_lfn.get(k)
            self.log("fdat=%s"%fdat)
            if not fdat: # not requested replica returned?
                continue
            fdat.replicas = [] # reset replicas list

            for ddm in fdat.inputddms:
                self.log('ddm=%s'%ddm)
                if ddm not in r['rses']: # skip not interesting rse
                    continue
                ddm_se = self.ddmconf[ddm].get('se', '')          ## FIX ME LATER: resolve from default protocol (srm?)
                self.log('ddm_se=%s'%ddm_se)
                ddm_path = self.ddmconf[ddm].get('endpoint', '')  ##
                self.log('ddm_path=%s'%ddm_path)
                if ddm_path and not (ddm_path.endswith('/rucio') or ddm_path.endswith('/rucio/')):
                    if ddm_path[-1] != '/':
                        ddm_path += '/'
                    ddm_path += 'rucio/'

                fdat.replicas.append((ddm, r['rses'][ddm], ddm_se, ddm_path))

            # if directaccess WAN, allow remote replicas
            self.log("direct access type=%s" % directaccesstype)
            if directaccesstype == "WAN":
                fdat.allowRemoteInputs = True

                # Assume the replicas to be geo-sorted, i.e. take the first root replica
                pfns = self.get_pfns(r)
                self.log("pfns=%s" % pfns)

                # Get 'random' entry
                turl = self.get_turl(pfns)
                self.log("turl=%s" % turl)

            self.log('r[rses]=%s'%r['rses'])
            if not fdat.replicas and fdat.allowRemoteInputs:
                self.log("No local replicas(%s) and allowRemoteInputs is set, looking for remote inputs" % fdat.replicas)
                for ddm in r['rses']:
                    self.log('ddm=%s'%ddm)
                    ddm_se = self.ddmconf[ddm].get('se', '')
                    ddm_path = self.ddmconf[ddm].get('endpoint', '')
                    if ddm_path and not (ddm_path.endswith('/rucio') or ddm_path.endswith('/rucio/')):
                        if ddm_path[-1] != '/':
                            ddm_path += '/'
                        ddm_path += 'rucio/'
                    break

                fdat.replicas.append((ddm, r['rses'][ddm], ddm_se, ddm_path))

            if fdat.filesize != r['bytes']:
                self.log("WARNING: filesize value of input file=%s mismatched with info got from Rucio replica:  job.indata.filesize=%s, replica.filesize=%s, fdat=%s" % (fdat.lfn, fdat.filesize, r['bytes'], fdat))
            cc_ad = 'ad:%s' % r['adler32']
            cc_md = 'md:%s' % r['md5']
            if fdat.checksum not in [cc_ad, cc_md]:
                self.log("WARNING: checksum value of input file=%s mismatched with info got from Rucio replica:  job.indata.checksum=%s, replica.checksum=%s, fdat=%s" % (fdat.lfn, fdat.filesize, (cc_ad, cc_md), fdat))

            # update filesize & checksum info from Rucio?
            # TODO

        self.log('files=%s'%files)
        return files

    def get_directaccess(self):
        """
            Check if direct access I/O is allowed. Also return the directaccess type (WAN or LAN)
            quick workaround: should be properly implemented in SiteInformation
        """

        try:
            from FileHandling import getDirectAccess
            return getDirectAccess()
        except Exception, e:
            self.log("mover.is_directaccess(): Failed to resolve direct access settings: exception=%s" % e)
            return False, None

    def handle_dbreleases(self, files):
        """
            Check if the DBRelease file(s) is locally available,
            if so, create the skeleton file and exclude file from stage-in
        """

        from DBReleaseHandler import DBReleaseHandler  ## partially reuse old implementation.. FIX ME LATER

        dbh = DBReleaseHandler(workdir=self.job.workdir)
        path = self.job.workdir

        # resolve dbreleases from files
        dbreleases = {} # by versions
        for fspec in files:
            ver = dbh.extractVersion(fspec.lfn)  ##
            if ver:
                dbreleases.setdefault(ver, []).append(fspec)

        # resolve dbrelease fom jobPars
        version = dbh.extractVersion(self.job.jobPars) ## quick hack -- fix me later (format of jobPars differs from lfn)
        if version:
            ## check if available or requested for stage-in
            if not dbh.isDBReleaseAvailable(version) and not version in dbreleases: # not available
                self.log("ERROR: Requested DBRelease from jobPars with version=%s is not available on CVMFS and not requested for stage in.. raise error." % version)
                raise PilotException("Requested DBRelease is not available: version=%s" % version, code=PilotErrors.ERR_MISSDBREL, state='ERR_MISSDBREL')
            dbreleases.setdefault(version, [])

        if not dbreleases:
            return False

        self.log("Detected DBRelease versions extracted from JobPars and input files: %s" % sorted(dbreleases))
        for version, fspecs in dbreleases.iteritems():
            if dbh.isDBReleaseAvailable(version):
                self.log("Creating the skeleton DBRelease tarball for version=%s" % version)
                if not dbh.createDBRelease(version, path):
                    self.log("Failed to create the skeleton file for DBRelease version=%s" % version)
                    raise PilotException("DBRelease: failed to create skeleton for version=%s" % version, code=PilotErrors.ERR_MISSDBREL, state='MISSDBREL_SKELFAIL')
                for fspec in fspecs:
                    fspec.status = 'no_transfer'  ## ignore for stage-in

        return True

    def get_objectstore_keys(self, ddmendpoint):
        if ddmendpoint in self.objectstorekeys:
            return

        endpoint_id = self.si.getObjectstoreEndpointID(ddmendpoint=ddmendpoint, label='r', protocol='s3')
        os_access_key, os_secret_key, os_is_secure = self.si.getObjectstoreKeyInfo(endpoint_id, ddmendpoint=ddmendpoint)

        if os_access_key and os_access_key != "" and os_secret_key and os_secret_key != "":
            keyPair = self.si.getSecurityKey(os_secret_key, os_access_key)
            if "privateKey" not in keyPair or keyPair["privateKey"] is None:
                self.log("Failed to get the keyPair for S3 objectstore from panda")
                self.objectstorekeys[ddmendpoint] = {'status': False}
            else:
                self.objectstorekeys[ddmendpoint] = {'S3_ACCESS_KEY': keyPair["publicKey"],
                                                     'S3_SECRET_KEY': keyPair["privateKey"],
                                                     'S3_IS_SECURE': os_is_secure,
                                                     'status': True}
        else:
            self.log("Failed to get the keyPair name for S3 objectstore from ddm config")
            self.objectstorekeys[ddmendpoint] = {'status': False}

    def stagein(self, files=None):
        """
            :return: (transferred_files, failed_transfers)
        """

        if files is None:
            files = self.job.inData
        self.log("To stagein files: %s" % files)

        normal_files, es_files = [], []
        transferred_files, failed_transfers = [], []

        self.ddmconf.update(self.si.resolveDDMConf([])) # load ALL ddmconf

        # resolve OS ddmendpoint
        os_ddms = {}
        for ddm, dat in self.ddmconf.iteritems():
            if dat.get('type') in ['OS_ES', 'OS_LOGS']:
                os_ddms.setdefault(int(dat.get('resource', {}).get('bucket_id', -1)), ddm)
        for ddm, dat in self.ddmconf.iteritems():
            os_ddms.setdefault(int(dat.get('id', -1)), ddm)

        self.log("os_ddms: %s" % os_ddms)

        for fspec in files:
            if fspec.prodDBlockToken and fspec.prodDBlockToken.isdigit() and int(fspec.prodDBlockToken) > 0:
                fspec.storageId = int(fspec.prodDBlockToken)
                fspec.ddmendpoint = os_ddms.get(fspec.storageId)
                self.get_objectstore_keys(fspec.ddmendpoint)
                es_files.append(fspec)
            elif fspec.prodDBlockToken and (fspec.prodDBlockToken.strip() == '-1' or fspec.prodDBlockToken.strip() == '0'):
                # es outputs in normal RSEs (not in objectstore) and registered in rucio
                fspec.allowRemoteInputs = True
                fspec.storageId = None  # resolve replicas needs to be called for it
                es_files.append(fspec)
            else:
                normal_files.append(fspec)

        if normal_files:
            self.log("Will stagin normal files: %s" % [f.lfn for f in normal_files])
            transferred_files, failed_transfers = self.stagein_real(files=normal_files, activity='pr')

        if failed_transfers:
            self.log("Failed to transfer normal files: %s" % failed_transfers)
            # if it's eventservice, can try remote stagein
            remain_files = [e for e in normal_files if e.status not in ['remote_io', 'transferred', 'no_transfer']]
            remain_non_es_input_files = [e for e in remain_files if not e.eventService]

            # there are non eventservcie input files, will not continue 
            if remain_non_es_input_files:
                return transferred_files, failed_transfers

            # all are eventservice input files, consider remote inputs
            for e in remain_files:
                e.allowRemoteInputs = True
                e.allowAllInputRSEs = True
            copytools = [('rucio', {'setup': ''})]
            transferred_files, failed_transfers = self.stagein_real(files=remain_files, activity='es_read', copytools=copytools)
            if failed_transfers:
                return transferred_files, failed_transfers

        if es_files:
            self.log("Will stagin es files: %s" % [f.lfn for f in es_files])
            self.trace_report.update(eventType='get_es')
            copytools = [('objectstore', {'setup': ''})]
            transferred_files_es, failed_transfers_es = self.stagein_real(files=es_files, activity='es_events_read', copytools=copytools)
            transferred_files += transferred_files_es
            failed_transfers += failed_transfers_es
            self.log("Failed to transfer files: %s" % failed_transfers)

        return transferred_files, failed_transfers

    def stagein_real(self, files, activity='pr', copytools=None):
        """
            :return: (transferred_files, failed_transfers)
        """

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        protocols = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])
        copytools = self.si.resolvePandaCopytools(pandaqueue, activity, copytools)[pandaqueue]

        self.log("stage-in: pq.aprotocols=%s, pq.copytools=%s" % (protocols, copytools))

        if not self.ddmconf:
            self.ddmconf.update(self.si.resolveDDMConf([])) # load ALL ddmconf

        maxinputsize = self.getMaxInputSize()
        totalsize = reduce(lambda x, y: x + y.filesize, files, 0)

        transferred_files, failed_transfers = [], []

        self.log("Found N=%s files to be transferred, total_size=%.3f MB: %s" % (len(files), totalsize/1024./1024., [e.lfn for e in files]))

        # process first PQ specific protocols settings
        # then protocols supported by copytools

        # protocol generated from aprotocols is {'copytool':'', 'copysetup':'', 'se':'', 'ddm':''}
        # protocol generated from  copytools is {'copytool':'', 'copysetup', 'scheme':''}

        # build accepted schemes from allowed copytools
        cprotocols = []
        for cp, settings in copytools:
            cprotocols.append({'resolve_scheme':True, 'copytool':cp, 'copysetup':settings.get('setup')})

        protocols = protocols + cprotocols
        if not protocols:
            raise PilotException("Failed to get files: neither aprotocols nor allowed copytools defined for input. check copytools/acopytools/aprotocols schedconfig settings for activity=%s, pandaqueue=%s" % (activity, pandaqueue), code=PilotErrors.ERR_NOSTORAGE, state='NO_PROTOCOLS_DEFINED')

        ## check files to be ignored for stage-in (DBRelease)

        self.handle_dbreleases(files)

        ignored_files = [e for e in files if e.status == 'no_transfer']

        for fdata in ignored_files: # ignored, no transfer required
            updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state=fdata.status, ftype="input")
            self.log("Stage-in will be ignored for lfn=%s .. skip transfer the file" % fdata.lfn)

        self.log("stage-in: resolved protocols=%s" % protocols)


        remain_files = [e for e in files if e.status not in ['remote_io', 'transferred', 'no_transfer']]

        nfiles = len(remain_files)
        nprotocols = len(protocols)

        # direct access settings
        allow_directaccess, directaccesstype = self.get_directaccess()
        if self.job.accessmode == 'copy':
            allow_directaccess = False
        elif self.job.accessmode == 'direct':
            if allow_directaccess:
                self.log("Direct access mode requested by task - allowed by the site (type = %s)" % directaccesstype)
            else:
                self.log("Direct access mode requested by task - but not allowed by the site (type = %s)" % directaccesstype)

        self.log("direct access settings: job.accessmode=%s, allow_direct_access=%s" % (self.job.accessmode, allow_directaccess))

        sitemover_objects = {}
        is_replicas_resolved = False

        for fnum, fdata in enumerate(remain_files, 1):

            self.log('INFO: prepare to transfer (stage-in) %s/%s file: lfn=%s' % (fnum, nfiles, fdata.lfn))

            is_directaccess = allow_directaccess and fdata.is_directaccess(ensure_replica=False) #fdata.turl is not defined at this point
            self.log("check direct access: allow_directaccess=%s, fdata.is_directaccess()=%s => is_directaccess=%s" % (allow_directaccess, fdata.is_directaccess(ensure_replica=False), is_directaccess))

            bad_copytools = True

            for protnum, dat in enumerate(protocols, 1):

                if fdata.status in ['remote_io', 'transferred', 'no_transfer']: ## success
                    break

                copytool, copysetup = dat.get('copytool'), dat.get('copysetup')

                try:
                    sitemover = sitemover_objects.get(copytool)
                    if not sitemover:
                        sitemover = getSiteMover(copytool)(copysetup, workDir=self.job.workdir)
                        sitemover_objects.setdefault(copytool, sitemover)

                        sitemover.trace_report = self.trace_report
                        sitemover.ddmconf = self.ddmconf # self.si.resolveDDMConf([]) # quick workaround  ###
                        sitemover.setup()
                    if dat.get('resolve_scheme'):
                        dat['scheme'] = sitemover.schemes
                        if is_directaccess or self.job.prefetcher:
                            if dat['scheme'] and dat['scheme'][0] != 'root':
                                dat['scheme'] = ['root'] + dat['scheme']
                            #self.log("INFO: prepare direct access mode: force to extend accepted protocol schemes to use direct access, schemes=%s" % dat['scheme'])

                except Exception, e:
                    self.log('WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                    self.trace_report.update(protocol=copytool, clientState='BAD_COPYTOOL', stateReason=str(e)[:500])
                    self.sendTrace(self.trace_report)
                    continue

                bad_copytools = False

                if sitemover.require_replicas and not is_replicas_resolved:
                    self.resolve_replicas(files, directaccesstype) ## do populate fspec.replicas for each entry in files
                    is_replicas_resolved = True

                self.log("Copy command [stage-in]: %s, sitemover=%s" % (copytool, sitemover))
                self.log("Copy setup   [stage-in]: %s" % copysetup)

                self.trace_report.update(protocol=copytool, filesize=fdata.filesize)

                updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="not_transferred", ftype="input")

                self.log("[stage-in] Prepare to get_data: [%s/%s]-protocol=%s, fspec=%s" % (protnum, nprotocols, dat, fdata))

                # check if protocol and fdata.ddmendpoint belong to same site
                #
                if dat.get('ddm'):
                    protocol_site = self.ddmconf.get(dat.get('ddm'), {}).get('site')
                    replica_site = self.ddmconf.get(fdata.ddmendpoint, {}).get('site')

                    if protocol_site != replica_site:
                        self.log('INFO: cross-sites checks: protocol_site=%s and (fdata.ddmenpoint) replica_site=%s mismatched .. skip file processing for copytool=%s (protocol=%s)' % (protocol_site, replica_site, copytool, dat))
                        continue

                try:
                    r = sitemover.resolve_replica(fdata, dat, ddm=self.ddmconf.get(fdata.ddmendpoint))
                except Exception, e:
                    if sitemover.require_replicas:
                        self.log("resolve_replica() failed for [%s/%s]-protocol.. skipped.. will check next available protocol, error=%s" % (protnum, nprotocols, e))
                        self.trace_report.update(clientState='NO_REPLICA', stateReason=str(e))
                        self.sendTrace(self.trace_report)
                        continue
                    r = {}

                # quick stub: propagate changes to FileSpec
                if r.get('surl'):
                    fdata.surl = r['surl'] # TO BE CLARIFIED if it's still used and need
                if r.get('pfn'):
                    fdata.turl = r['pfn']
                if r.get('ddmendpoint'):
                    fdata.ddmendpoint = r['ddmendpoint']

                self.log("[stage-in] found replica to be used: ddmendpoint=%s, pfn=%s" % (fdata.ddmendpoint, fdata.turl))

                # check if protocol and found replica belong to same site
                if dat.get('ddm'):
                    protocol_site = self.ddmconf.get(dat.get('ddm'), {}).get('site')
                    replica_site = self.ddmconf.get(fdata.ddmendpoint, {}).get('site')

                    if protocol_site != replica_site:
                        if fdata.allowRemoteInputs is None or not fdata.allowRemoteInputs:
                            self.log('INFO: cross-sites checks: protocol_site=%s and replica_site=%s mismatched and remote inputs is not allowed.. skip file processing for copytool=%s' % (protocol_site, replica_site, copytool))
                            continue
                        else:
                            self.log('INFO: cross-sites checks: protocol_site=%s and replica_site=%s mismatched but remote inputs is allowed.. keep processing for copytool=%s' % (protocol_site, replica_site, copytool))

                # fill trace details
                self.trace_report.update(localSite=fdata.ddmendpoint, remoteSite=fdata.ddmendpoint)
                self.trace_report.update(filename=fdata.lfn, guid=fdata.guid.replace('-', ''))
                self.trace_report.update(scope=fdata.scope, dataset=fdata.prodDBlock)

                # check direct access
                ignore_directaccess = False
                if fdata.is_directaccess() and is_directaccess and not ignore_directaccess: # direct access mode, no transfer required
                    fdata.status = 'remote_io'
                    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state=fdata.status, ftype="input")
                    self.log("Direct access mode will be used for lfn=%s .. skip transfer for this file" % fdata.lfn)
                    self.trace_report.update(url=fdata.turl, clientState='FOUND_ROOT', stateReason='direct_access')
                    self.sendTrace(self.trace_report)
                    continue

                # check prefetcher (the turl must be saved for prefetcher to use)
                # note: for files to be prefetched, there's no entry for the file_state, so the updateFileState needs
                # to be called twice (or update the updateFileState function to allow list arguments)
                # also update the file_state for the existing entry (could also be removed?)
                # note also that at least one file still needs to be staged in, or AthenaMP will not start
                if self.job.prefetcher:
                    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="file_state", state="prefetch", ftype="input")
                    fdata.status = 'remote_io'
                    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="transfer_mode", state=fdata.status, ftype="input")
                    self.log("Prefetcher will be used for turl=%s" % fdata.turl)
                    #updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state="no_transfer", ftype="input")
                    self.trace_report.update(url=fdata.turl, clientState='FOUND_ROOT', stateReason='prefetch')
                    self.sendTrace(self.trace_report)
                    #continue - if we continue here, the the file will not be staged in, but AthenaMP needs it so we still need to stage it in

                # apply site-mover custom job-specific checks for stage-in
                try:
                    is_stagein_allowed = sitemover.is_stagein_allowed(fdata, self.job)
                    if not is_stagein_allowed:
                        reason = 'SiteMover does not allow stage-in operation for the job'
                except PilotException, e:
                    is_stagein_allowed = False
                    reason = e
                except Exception:
                    raise
                if not is_stagein_allowed:
                    self.log("WARNING: sitemover=%s does not allow stage-in transfer for this job, lfn=%s with reason=%s.. skip transfer the file" % (sitemover.getID(), fdata.lfn, reason))
                    failed_transfers.append(reason)
                    self.trace_report.update(clientState='STAGEIN_NOTALLOWED', stateReason='skip stagein file')
                    self.sendTrace(self.trace_report)
                    continue

                # verify file sizes and available space for stagein
                sitemover.check_availablespace(maxinputsize, [e for e in remain_files if e.status not in ['remote_io', 'transferred']])

                self.trace_report.update(catStart=time.time())  ## is this metric still needed? LFC catalog

                self.log("[stage-in] Preparing copy for lfn=%s using copytool=%s: mover=%s" % (fdata.lfn, copytool, sitemover))

                # set environment for objectstore
                if fdata.ddmendpoint in self.objectstorekeys and self.objectstorekeys[fdata.ddmendpoint]['status']:
                    os.environ['S3_ACCESS_KEY'] = self.objectstorekeys[fdata.ddmendpoint]['S3_ACCESS_KEY']
                    os.environ['S3_SECRET_KEY'] = self.objectstorekeys[fdata.ddmendpoint]['S3_SECRET_KEY']
                    os.environ['S3_IS_SECURE'] = str(self.objectstorekeys[fdata.ddmendpoint]['S3_IS_SECURE'])
                else:
                    if 'S3_ACCESS_KEY' in os.environ: del os.environ['S3_ACCESS_KEY']
                    if 'S3_SECRET_KEY' in os.environ: del os.environ['S3_SECRET_KEY']
                    if 'S3_IS_SECURE' in os.environ: del os.environ['S3_IS_SECURE']
                self.log("Environment S3_ACCESS_KEY=%s" % os.environ.get('S3_ACCESS_KEY', None))

                #dumpFileStates(self.workDir, self.job.jobId, ftype="input")

                # loop over multple stage-in attempts
                for _attempt in xrange(1, self.stageinretry + 1):
                    if _attempt > 1: # if not first stage-in attempt, take a nap before next attempt
                        sleep_time = self.calc_stagein_sleeptime()
                        self.log(" -- Waiting %d seconds before next stage-in attempt for file=%s --" % (sleep_time, fdata.lfn))
                        time.sleep(sleep_time)

                    self.log("Get attempt %s/%s for file (%s/%s) with lfn=%s .. sitemover=%s" % (_attempt, self.stageinretry, fnum, nfiles, fdata.lfn, sitemover))

                    try:
                        result = sitemover.get_data(fdata)
                        fdata.status = 'transferred' # mark as successful
                        fdata.status_code = 0
                        if result.get('ddmendpoint'):
                            fdata.ddmendpoint = result.get('ddmendpoint')
                        if result.get('surl'):
                            fdata.surl = result.get('surl')
                        if result.get('pfn'):
                            fdata.turl = result.get('pfn')

                        #self.trace_report.update(url=fdata.surl) ###
                        self.trace_report.update(url=fdata.turl) ###
                        # for files without replication registered in rucio, the filesize need to be got from local file
                        self.trace_report.update(filesize=fdata.filesize)

                        break # transferred successfully
                    except PilotException, e:
                        result = e
                        self.log(traceback.format_exc())
                    except Exception, e:
                        result = PilotException("stageIn failed with error=%s" % e, code=PilotErrors.ERR_STAGEINFAILED, state='STAGEIN_ATTEMPT_FAILED')
                        self.log(traceback.format_exc())
                        self.log('WARNING: Error in copying file (fspec %s/%s) (protocol %s/%s) (attempt %s/%s) (exception): skip further retry (if any)' % (fnum, nfiles, protnum, nprotocols, _attempt, self.stageinretry))
                        break

                    self.log('WARNING: Error in copying file (fspec %s/%s) (protocol %s/%s) (attempt %s/%s): %s' % (fnum, nfiles, protnum, nprotocols, _attempt, self.stageinretry, result))

                    accepted_codes = [PilotErrors.ERR_GETADMISMATCH, PilotErrors.ERR_GETMD5MISMATCH, PilotErrors.ERR_GETWRONGSIZE, PilotErrors.ERR_NOSUCHFILE]
                    if isinstance(result, PilotException) and result.code in accepted_codes:
                        self.log("[stage-in] WARNING: BAD input file detected at storage side (code=%s).. will skip all remaining retry attempts (if any) .." % result.code)
                        break

                if not isinstance(result, PilotException): # transferred successfully

                    # finalize and send trace report
                    self.trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time.time())
                    self.sendTrace(self.trace_report)

                    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="transferred", ftype="input")
                    dumpFileStates(self.workDir, self.job.jobId, ftype="input")

                    ## self.updateSURLDictionary(guid, surl, self.workDir, self.job.jobId) # FIX ME LATER

                    fdat = result.copy()
                    #fdat.update(lfn=lfn, pfn=pfn, guid=guid, surl=surl)
                    transferred_files.append(fdat)
                else:
                    fdata.status = 'error'
                    fdata.status_code = result.code
                    fdata.status_message = result.message
                    self.trace_report.update(clientState=result.state or 'STAGEIN_ATTEMPT_FAILED', stateReason=result.message, timeEnd=time.time())
                    self.sendTrace(self.trace_report)
                    failed_transfers.append(result)

                    badfile_codes = [PilotErrors.ERR_GETADMISMATCH, PilotErrors.ERR_GETMD5MISMATCH, PilotErrors.ERR_GETWRONGSIZE, PilotErrors.ERR_NOSUCHFILE]
                    if fdata.status_code in badfile_codes:
                        break

                # TEMPORARY: SHOULD BE REMOVED IF DIRECT I/O ACTUALLY WORKS WITH ATHENAMP, WHICH IT SEEMS IT DOESN'T
                # AS OF NOW, THE INITIAL INPUT FILE IS STILL TRANSFERRED, OTHERWISE ATHENAMP FAILS IMMEDIATELY SINCE
                # IT DOESN'T FIND THE INPUT FILE
                # check prefetcher (no transfer is required, but the turl must be saved for prefetcher to use)
                # note: for files to be prefetched, there's no entry for the file_state, so the updateFileState needs
                # to be called twice (or update the updateFileState function to allow list arguments)
                # also update the file_state for the existing entry (could also be removed?)
                #if self.job.prefetcher:
                #    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="file_state", state="prefetch", ftype="input")
                #    fdata.status = 'remote_io'
                #    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="transfer_mode", state=fdata.status, ftype="input")
                #    self.log("Prefetcher will be used for turl=%s .. skip transfer for this file" % fdata.turl)
                #    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state="no_transfer", ftype="input")
                #    continue

            if fdata.status == 'error':
                self.log('stage-in of file (%s/%s) with lfn=%s failed: code=%s .. skip transferring of remain data..' % (fnum, nfiles, fdata.lfn, fdata.status_code))
                dumpFileStates(self.workDir, self.job.jobId, ftype="input")
                raise PilotException("STAGEIN FAILED: %s: lfn=%s, error=%s" % (PilotErrors.getErrorStr(fdata.status_code), fdata.lfn, getattr(fdata, 'status_message', '')), code=fdata.status_code, state='STAGEIN_FILE_FAILED')

            if bad_copytools:
                raise PilotException("STAGEIN FAILED: bad copytools: no one copytools supported", code=PilotErrors.ERR_NOSTORAGE, state='STAGEIN_BAD_COPYTOOLS')

        self.log('INFO: all input files have been successfully processed')

        dumpFileStates(self.workDir, self.job.jobId, ftype="input")

        #self.log('transferred_files= %s' % transferred_files)
        self.log('Summary of transferred files:')
        for e in transferred_files:
            self.log(" -- %s" % e)

        if failed_transfers:
            self.log('Summary of failed transfers:')
            for e in failed_transfers:
                self.log(" -- %s" % e)

        self.log("stagein finished")

        if not self.job.prefetcher:
            self.job.print_infiles()

        return transferred_files, failed_transfers

    def _prepare_destinations(self, files, activities):
        """
            check fspec.ddmendpoint entry and fullfill it if need by applying Pilot side logic
            :param files: list of FileSpec entries to be processed
            :param activities: ordered list of activities to be used to resolve storages
            :return: updated fspec entries
        """

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        astorages = self.si.resolvePandaAssociatedStorages(pandaqueue).get(pandaqueue, {})

        if isinstance(activities, (str, unicode)):
            activities = [activities]

        if not activities:
            raise PilotException("Failed to resolve destination: passed empty activity list. Internal error.", code=PilotErrors.ERR_NOSTORAGE, state='INTERNAL_ERROR')

        storages = None
        activity = activities[0]
        for a in activities:
            storages = astorages.get(a, {})
            if storages:
                break

        if not storages:
            raise PilotException("Failed to resolve destination: no associated storages defined for activity=%s (%s)" % (activity, ','.join(activities)), code=PilotErrors.ERR_NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        # take fist choice for now, extend the logic later if need
        ddm = storages[0]

        self.log("[prepare_destinations][%s]: allowed (local) destinations: %s" % (activity, storages))
        self.log("[prepare_destinations][%s]: resolved default destination ddm=%s" % (activity, ddm))

        for e in files:
            if not e.ddmendpoint: ## no preferences => use default destination
                self.log("[prepare_destinations][%s]: fspec.ddmendpoint is not set for lfn=%s .. will use default ddm=%s as (local) destination" % (activity, e.lfn, ddm))
                e.ddmendpoint = ddm
            elif e.ddmendpoint not in storages: ### fspec.ddmendpoint is not in associated storages ==> assume it as final (non local) alternative destination
                self.log("[prepare_destinations] [%s]: Requested fspec.ddmendpoint=%s is not in the list of allowed (local) destinations .. will consider default ddm=%s for transfers and tag %s as alternative location" % (activity, e.ddmendpoint, ddm, e.ddmendpoint))
                e.ddmendpoint = ddm
                e.ddmendpoint_alt = e.ddmendpoint  ###

        return files


    def stageout_outfiles(self):
        """
            Do stage-out of output data files
        """

        activities = ['pw', 'w']

        # apply pilot side decision about which destination should be used
        data = self._prepare_destinations(self.job.outData, activities)

        return self.stageout(activities[0], data)

    def stageout_logfiles(self):
        """
            Do stage-out of log files
        """

        activities = ['pl', 'pw', 'w']

        # apply pilot side decision about which destination should be used
        data = self._prepare_destinations(self.job.logData, activities)

        return self.stageout(activities[0], data, skip_transfer_failure=True)


    def stageout_logfiles_os(self):
        """
            Special log transfers (currently to ObjectStores)
        """

        activity = "pls" ## pilot log special/second transfer

        # resolve accepted OS DDMEndpoints

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        os_ddms = self.si.resolvePandaOSDDMs(pandaqueue).get(pandaqueue, [])

        ddmconf = self.ddmconf
        if os_ddms and (set(os_ddms) - set(self.ddmconf)): # load DDM conf
            ddmconf = self.si.resolveDDMConf(os_ddms)
            self.ddmconf.update(ddmconf)

        osddms = [e for e in os_ddms if ddmconf.get(e, {}).get('type') == 'OS_LOGS']

        self.log("[stage-outlog-special] [%s] resolved os_ddms=%s => logs=%s" % (activity, os_ddms, osddms))

        if osddms:
            ddmendpoint = osddms[0]
        else:
            self.log("[stage-outlog-special] no osddms defined, looking for associated storages with activity: %s" % (activity))
            associate_storages = self.si.resolvePandaAssociatedStorages(pandaqueue).get(pandaqueue, {})
            esDDMEndpoints = associate_storages.get(activity, [])
            if esDDMEndpoints:
                self.log("[stage-outlog-special] found associated storages %s with activity: %s" % (esDDMEndpoints, activity))
                ddmendpoint = esDDMEndpoints[0]
            else:
                raise PilotException("Failed to stage-out logs to OS: no OS_LOGS ddmendpoint attached to the queue, os_ddms=%s" % (os_ddms), code=PilotErrors.ERR_NOSTORAGE, state='NO_OS_DEFINED')

        self.get_objectstore_keys(ddmendpoint)

        # set environment for objectstore
        if ddmendpoint in self.objectstorekeys and self.objectstorekeys[ddmendpoint]['status']:
            os.environ['S3_ACCESS_KEY'] = self.objectstorekeys[ddmendpoint]['S3_ACCESS_KEY']
            os.environ['S3_SECRET_KEY'] = self.objectstorekeys[ddmendpoint]['S3_SECRET_KEY']
            os.environ['S3_IS_SECURE'] = str(self.objectstorekeys[ddmendpoint]['S3_IS_SECURE'])
        else:
            if 'S3_ACCESS_KEY' in os.environ: del os.environ['S3_ACCESS_KEY']
            if 'S3_SECRET_KEY' in os.environ: del os.environ['S3_SECRET_KEY']
            if 'S3_IS_SECURE' in os.environ: del os.environ['S3_IS_SECURE']
        self.log("Environment S3_ACCESS_KEY=%s" % os.environ.get('S3_ACCESS_KEY', None))

        self.log("[stage-out] [%s] resolved OS ddmendpoint=%s for special log transfer" % (activity, ddmendpoint))

        import copy
        data = copy.deepcopy(self.job.logData)
        for e in data:
            e.ddmendpoint = ddmendpoint

        self.job.logSpecialData = data

        #copytools = [('objectstore', {'setup': '/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/setup.sh'})]
        copytools = [('objectstore', {'setup': ''})]
        ret = self.stageout(activity, self.job.logSpecialData, copytools, skip_transfer_failure=True)
        self.job.logBucketID = self.ddmconf.get(ddmendpoint, {}).get('resource', {}).get('bucket_id', -1)
        self.job.logDDMEndpoint = ddmendpoint

        return ret

    def stageout(self, activity, files, copytools=None, skip_transfer_failure=False):
        """
            Copy files to dest SE:
            main control function, it should care about alternative stageout and retry-policy for diffrent ddmendpoints
        :param copytools: default copytools to be used
        :param skip_transfer_failure: if enabled then all errors with previous file transfers will be ignored and the logic will continue processing of all remaining files
        :return: list of entries (is_success, success_transfers, failed_transfers, exception) for each ddmendpoint
        :return: (transferred_files, failed_transfers)
        :raise: PilotException in case of error
        """

        if not files:
            raise PilotException("Failed to put files: empty file list to be transferred", code=PilotErrors.ERR_MISSINGOUTPUTFILE, state="NO_OUTFILES")

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        protocols = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])
        if copytools:
            self.log("Mover.stageout() [new implementation] [%s]: default copytools=%s" % (activity, copytools))

        copytools = self.si.resolvePandaCopytools(pandaqueue, activity, copytools)[pandaqueue]

        self.log("Mover.stageout() [new implementation] started for activity=%s, files=%s, protocols=%s, copytools=%s" % (activity, files, protocols, copytools))

        # check if file exists before actual processing
        # populate filesize if need

        for fspec in files:
            pfn = fspec.pfn if fspec.pfn else os.path.join(self.job.workdir, fspec.lfn)
            if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                error = "Error: output pfn file does not exist: %s" % pfn
                self.log(error)
                raise PilotException(error, code=PilotErrors.ERR_MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            fspec.filesize = os.path.getsize(pfn)

        totalsize = reduce(lambda x, y: x + y.filesize, files, 0)

        transferred_files, failed_transfers = [],[]

        self.log("Found N=%s files to be transferred, total_size=%.3f MB: %s" % (len(files), totalsize/1024./1024., [e.lfn for e in files]))

        # first resolve protocol settings from PQ specific aprotocols settings
        # then resolve settings from default ddm.protocols supported by copytools

        # group protocols, files by ddmendpoint
        ddmprotocols, ddmfiles = {}, {}
        for e in files:
            ddmfiles.setdefault(e.ddmendpoint, []).append(e)

        # load DDM conf/protocols
        self.ddmconf.update(self.si.resolveDDMConf(ddmfiles.keys()))

        for e in protocols:
            if e['ddm'] not in ddmfiles: # skip not affected protocols settings
                continue
            e['copytools'] = [{'copytool':e['copytool'], 'copysetup':e['copysetup']}]
            ddmprotocols.setdefault(e['ddm'], []).append(e)

        # generate default protocols from copytools/schemes and ddmconf
        unknown_ddms = set(ddmfiles) - set(ddmprotocols)
        for ddmendpoint in unknown_ddms:
            dd = self.ddmconf.get(ddmendpoint, {}).get('aprotocols', {})
            dat = dd.get(activity, []) or dd.get('w', [])
            dprotocols = [dict(se=e[0], path=e[2], resolve_scheme=True) for e in sorted(dat, key=lambda x: x[1])]
            ddmprotocols.setdefault(ddmendpoint, dprotocols)

        unknown_ddms = set(ddmfiles) - set(ddmprotocols)
        if unknown_ddms:
            raise PilotException("Failed to put files: no protocols defined for output ddmendpoints=%s .. check aprotocols schedconfig settings for activity=%s or default ddm.aprotocols entries" % (unknown_ddms, activity), code=PilotErrors.ERR_NOSTORAGE, state="NO_PROTOCOLS_DEFINED")

        self.log("[stage-out] [%s] filtered protocols to be used to transfer files: protocols=%s" % (activity, ddmprotocols))

        # get SURL endpoint for Panda callback registration
        # resolve from special protocol activity='SE' or fallback to activity='a', then to 'r'

        surl_protocols, no_surl_ddms = {}, set()

        for fspec in files:
            if not fspec.surl: # initialize only if not already set
                dconf = self.ddmconf.get(fspec.ddmendpoint, {})
                d = dconf.get('aprotocols', {})
                xprot = d.get('SE', [])
                if not xprot:
                    surl_schema = ['s3'] if dconf.get('type') in ['OS_LOGS', 'OS_ES'] else ['srm', 'gsiftp', 'root', 'http']
                    xprot = [e for e in d.get('a', d.get('r', [])) if e[0] and True in set([e[0].startswith(sc) for sc in surl_schema])]

                surl_prot = [dict(se=e[0], path=e[2]) for e in sorted(xprot, key=lambda x: x[1])]
                if surl_prot:
                    surl_protocols.setdefault(fspec.ddmendpoint, surl_prot[0])
                else:
                    no_surl_ddms.add(fspec.ddmendpoint)

        if no_surl_ddms: # failed to resolve SURLs
            self.log('FAILED to resolve default SURL path for ddmendpoints=%s' % list(no_surl_ddms))
            raise PilotException("Failed to put files: no SE/SURL protocols defined for output ddmendpoints=%s .. check ddmendpoints aprotocols settings for activity=SE/a/r" % list(no_surl_ddms), code=PilotErrors.ERR_NOSTORAGE, state="NO_SURL_PROTOCOL")

        for ddmendpoint, iprotocols in ddmprotocols.iteritems():
            for dat in iprotocols:
                if not 'copytools' in dat:
                    # use allowed copytools
                    cdat = []
                    for cp, settings in copytools:
                        cdat.append({'copytool':cp, 'copysetup':settings.get('setup')})
                    dat['copytools'] = cdat

                if not dat['copytools']:
                    msg = 'FAILED to resolve final copytools settings for ddmendpoint=%s, please check schedconf.copytools settings: copytools=%s, iprotocols=%s' % (ddmendpoint, copytools, iprotocols)
                    self.log(msg)
                    raise PilotException(msg, code=PilotErrors.ERR_NOSTORAGE, state="NO_COPYTOOLS")

        sitemover_objects = {}

        remain_files = [e for e in ddmfiles.get(ddmendpoint) if e.status not in ['transferred']]
        nfiles = len(remain_files)

        for fnum, fdata in enumerate(remain_files, 1):

            self.log('INFO: prepare to transfer (stage-out) %s/%s file: lfn=%s, fspec.ddmendpoint=%s, activity=%s' % (fnum, nfiles, fdata.lfn, fdata.ddmendpoint, activity))

            ddmendpoint = fdata.ddmendpoint
            iprotocols = ddmprotocols.get(fdata.ddmendpoint)
            nprotocols = len(iprotocols)

            bad_copytools = True

            for protnum, dat in enumerate(iprotocols, 1):

                if fdata.status in ['transferred']:
                    break

                self.log('[stage-out] [%s]: checking protocol-%s/%s to transfer file %s/%s: lfn=%s, copytools=%s' % (activity, protnum, nprotocols, fnum, nfiles, fdata.lfn, dat.get('copytools', [])))

                for cpsettings in dat.get('copytools', []):

                    if fdata.status in ['transferred']:
                        break

                    copytool, copysetup = cpsettings.get('copytool'), cpsettings.get('copysetup')

                    try:
                        sitemover = sitemover_objects.get(copytool)
                        if not sitemover:
                            sitemover = getSiteMover(copytool)(copysetup, workDir=self.job.workdir)
                            sitemover_objects.setdefault(copytool, sitemover)

                            sitemover.trace_report = self.trace_report
                            sitemover.protocol = dat # ## ?
                            sitemover.ddmconf = self.ddmconf # quick workaround  ###
                            sitemover.setup()
                        if dat.get('resolve_scheme'):
                            dat['scheme'] = sitemover.schemes
                    except Exception, e:
                        self.log('WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                        self.trace_report.update(protocol=copytool, clientState='BAD_COPYTOOL', stateReason=str(e)[:500])
                        self.sendTrace(self.trace_report)
                        continue

                    bad_copytools = False

                    if dat.get('scheme'): # filter protocols by accepted scheme from copytool
                        should_skip = True
                        for scheme in dat.get('scheme'):
                            if dat['se'].startswith(scheme):
                                should_skip = False
                                break
                        if should_skip:
                            self.log("[stage-out] [%s] protocol=%s of ddmendpoint=%s is skipped since copytool=%s does not support it, accepted schemes=%s" % (activity, dat['se'], ddmendpoint, copytool, dat['scheme']))

                            continue

                    self.log("Copy command [stage-out][%s]: %s, sitemover=%s" % (activity, copytool, sitemover))
                    self.log("Copy setup   [stage-out][%s]: %s" % (activity, copysetup))

                    self.trace_report.update(protocol=copytool, localSite=ddmendpoint, remoteSite=ddmendpoint)

                    # validate se value?
                    se, se_path = dat.get('se', ''), dat.get('path', '')

                    if not fdata.surl:
                        # job is passing here for possible JOB specific processing
                        fdata.surl = sitemover.getSURL(surl_protocols[fdata.ddmendpoint].get('se'),
                                                       surl_protocols[fdata.ddmendpoint].get('path'),
                                                       fdata.scope,
                                                       fdata.lfn,
                                                       self.job,
                                                       pathConvention=fdata.pathConvention,
                                                       ddmType=self.ddmconf.get(fdata.ddmendpoint, {}).get('type'))

                    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="not_transferred", ftype="output")

                    # job is passing here for possible JOB specific processing
                    fdata.turl = sitemover.getSURL(se, se_path, fdata.scope, fdata.lfn, self.job, pathConvention=fdata.pathConvention, ddmType=self.ddmconf.get(fdata.ddmendpoint, {}).get('type'))

                    self.log("[stage-out] [%s] resolved SURL=%s to be used for lfn=%s, ddmendpoint=%s" % (activity, fdata.surl, fdata.lfn, fdata.ddmendpoint))
                    self.log("[stage-out] [%s] resolved TURL=%s to be used for lfn=%s, ddmendpoint=%s" % (activity, fdata.turl, fdata.lfn, fdata.ddmendpoint))
                    self.log("[stage-out] [%s] Prepare to put_data: ddmendpoint=%s, %s/%s-protocol=%s, fspec=%s" % (activity, ddmendpoint, protnum, nprotocols, dat, fdata))

                    self.trace_report.update(catStart=time.time(), filename=fdata.lfn, guid=fdata.guid.replace('-', '') if fdata.guid else None)
                    self.trace_report.update(scope=fdata.scope, dataset=fdata.destinationDblock, url=fdata.turl)
                    self.trace_report.update(filesize=fdata.filesize)

                    self.log("[stage-out] [%s] Preparing copy for lfn=%s using copytool=%s: mover=%s" % (activity, fdata.lfn, copytool, sitemover))
                    #dumpFileStates(self.workDir, self.job.jobId, ftype="output")

                    # loop over multple stage-out attempts
                    for _attempt in xrange(1, self.stageoutretry + 1):
                        if _attempt > 1: # if not first stage-out attempt, take a nap before next attempt
                            sleep_time = self.calc_stageout_sleeptime()
                            self.log(" -- Waiting %d seconds before next stage-out attempt for file=%s --" % (sleep_time, fdata.lfn))
                            time.sleep(sleep_time)

                        self.log("Put attempt %s/%s for file (%s/%s) with lfn=%s .. sitemover=%s" % (_attempt, self.stageoutretry, fnum, nfiles, fdata.lfn, sitemover))

                        try:
                            result = sitemover.put_data(fdata)
                            fdata.status = 'transferred' # mark as successful
                            fdata.status_code = 0
                            if result.get('surl'):
                                fdata.surl = result.get('surl')
                            #if result.get('pfn'):
                            #    fdata.turl = result.get('pfn')

                            #self.trace_report.update(url=fdata.surl) ###
                            self.trace_report.update(url=fdata.turl) ###

                            # finalize and send trace report
                            self.trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time.time())
                            self.sendTrace(self.trace_report)

                            updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="transferred", ftype="output")
                            dumpFileStates(self.workDir, self.job.jobId, ftype="output")

                            self.updateSURLDictionary(fdata.guid, fdata.surl, self.workDir, self.job.jobId) # FIXME LATER: isolate later

                            fdat = result.copy()
                            #fdat.update(lfn=lfn, pfn=pfn, guid=guid, surl=surl)
                            transferred_files.append(fdat)

                            break # transferred successfully
                        except PilotException, e:
                            result = e
                            self.log(traceback.format_exc())
                        except Exception, e:
                            result = PilotException("stageOut failed with error=%s" % e, code=PilotErrors.ERR_STAGEOUTFAILED, state="STAGEOUT_ATTEMPT_FAILED")
                            self.log(traceback.format_exc())
                            self.log('WARNING: Error in copying file (fspec %s/%s) (protocol %s/%s) (attempt %s/%s) (exception): skip further retry (if any)' % (fnum, nfiles, protnum, nprotocols, _attempt, self.stageoutretry))
                            break

                        self.log('WARNING: Error in copying file (fspec %s/%s) (protocol %s/%s) (attempt %s/%s): %s' % (fnum, nfiles, protnum, nprotocols, _attempt, self.stageoutretry, result))

                    if isinstance(result, Exception): # failed transfer
                        fdata.status = 'error'
                        fdata.status_code = result.code
                        fdata.status_message = result.message

                        self.trace_report.update(clientState=result.state or 'STAGEOUT_ATTEMPT_FAILED', stateReason=result.message, timeEnd=time.time())
                        self.sendTrace(self.trace_report)

                        failed_transfers.append(result)


            if fdata.status == 'error' and not skip_transfer_failure:
                self.log('[stage-out] [%s] failed to transfer file (%s/%s) with lfn=%s: code=%s .. skip transferring of remain data..' % (activity, fnum, nfiles, fdata.lfn, fdata.status_code))
                dumpFileStates(self.workDir, self.job.jobId, ftype="output")
                raise PilotException("STAGEOUT FAILED: %s: lfn=%s, error=%s" % (PilotErrors.getErrorStr(fdata.status_code), fdata.lfn, getattr(fdata, 'status_message', '')), code=fdata.status_code, state='STAGEOUT_FILE_FAILED')

            if bad_copytools:
                raise PilotException("STAGEOUT FAILED: bad copytools: no one copytools supported", code=PilotErrors.ERR_NOSTORAGE, state='STAGEOUT_BAD_COPYTOOLS')


        dumpFileStates(self.workDir, self.job.jobId, ftype="output")

        self.log('Summary of transferred files:')
        for e in transferred_files:
            self.log(" -- %s" % e)

        if failed_transfers:
            self.log('Summary of failed transfers:')
            for e in failed_transfers:
                self.log(" -- %s" % e)

        self.log("stageout finished")
        self.job.print_files(files)

        return transferred_files, failed_transfers


    def put_outfiles(self, files): # old function : TO BE DEPRECATED ...
        """
        Copy output files to dest SE
        :files: list of files to be moved
        :raise: an exception in case of errors
        """

        activity = 'pw'
        ddms = self.job.ddmEndPointOut

        if not ddms:
            raise PilotException("Output ddmendpoint list (job.ddmEndPointOut) is not set", code=PilotErrors.ERR_NOSTORAGE)

        return self.put_files(ddms, activity, files)


    def put_logfiles(self, files): # old function : TO BE DEPRECATED ...
        """
        Copy log files to dest SE
        :files: list of files to be moved
        """

        activity = 'pl'
        ddms = self.job.ddmEndPointLog

        if not ddms:
            raise PilotException("Output ddmendpoint list (job.ddmEndPointLog) is not set", code=PilotErrors.ERR_NOSTORAGE)

        return self.put_files(ddms, activity, files)


    def put_files(self, ddmendpoints, activity, files): # old function : TO BE DEPRECATED ...
        """
        Copy files to dest SE:
           main control function, it should care about alternative stageout and retry-policy for diffrent ddmenndpoints
        :ddmendpoint: list of DDMEndpoints where the files will be send (base DDMEndpoint SE + alternative SEs??)
        :return: list of entries (is_success, success_transfers, failed_transfers, exception) for each ddmendpoint
        :raise: PilotException in case of error
        """

        if not ddmendpoints:
            raise PilotException("Failed to put files: Output ddmendpoint list is not set", code=PilotErrors.ERR_NOSTORAGE)
        if not files:
            raise PilotException("Failed to put files: empty file list to be transferred")

        missing_ddms = set(ddmendpoints) - set(self.ddmconf)

        if missing_ddms:
            self.ddmconf.update(self.si.resolveDDMConf(missing_ddms))

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        prot = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])

        # group by ddmendpoint
        ddmprot = {}
        for e in prot:
            ddmprot.setdefault(e['ddm'], []).append(e)

        output = []

        for ddm in ddmendpoints:
            protocols = ddmprot.get(ddm)
            if not protocols:
                self.log('Failed to resolve protocols data for ddmendpoint=%s and activity=%s.. skipped processing..' % (ddm, activity))
                continue

            success_transfers, failed_transfers = [], []

            try:
                success_transfers, failed_transfers = self.do_put_files(ddm, protocols, files)
                is_success = len(success_transfers) == len(files)
                output.append((is_success, success_transfers, failed_transfers, None))

                if is_success:
                    # NO additional transfers to another next DDMEndpoint/SE ?? .. fix me later if need
                    break

            #except PilotException, e:
            #    self.log('put_files: caught exception: %s' % e)
            except Exception, e:
                self.log('put_files: caught exception: %s' % e)
                # is_success, success_transfers, failed_transfers, exception
                import traceback
                self.log(traceback.format_exc())
                output.append((False, [], [], e))

            ### TODO: implement proper logic of put-policy: how to handle alternative stage out (processing of next DDMEndpoint)..

            self.log('put_files(): Failed to put files to ddmendpoint=%s .. successfully transferred files=%s/%s, failures=%s: will try next ddmendpoint from the list ..' % (ddm, len(success_transfers), len(files), len(failed_transfers)))

        # check transfer status: if any of DDMs were successfull
        n_success = reduce(lambda x, y: x + y[0], output, False)

        if not n_success:  # failed to put file
            self.log('put_outfiles failed')
        else:
            self.log("Put successful")


        return output


    def do_put_files(self, ddmendpoint, protocols, files): # old function : TO BE DEPRECATED ...
        """
        Copy files to dest SE
        :ddmendpoint: DDMEndpoint name used to store files
        :return: (list of transferred_files details, list of failed_transfers details)
        :raise: PilotException in case of error
        """

        self.log('[deprecated do_put_files()]Prepare to copy files=%s to ddmendpoint=%s using protocols data=%s' % (files, ddmendpoint, protocols))
        self.log("[deprecated do_put_files()]Number of stage-out tries: %s" % self.stageoutretry)

        # get SURL for Panda calback registration
        # resolve from special protocol activity=SE # fix me later to proper name of activitiy=SURL (panda SURL, at the moment only 2-letter name is allowed on AGIS side)
        # if SE is not found, try to fallback to a
        surl_prot = [dict(se=e[0], path=e[2]) for e in sorted(self.ddmconf.get(ddmendpoint, {}).get('aprotocols', {}).get('SE', self.ddmconf.get(ddmendpoint, {}).get('aprotocols', {}).get('a', [])), key=lambda x: x[1])]

        if not surl_prot:
            self.log('FAILED to resolve default SURL path for ddmendpoint=%s' % ddmendpoint)
            return [], []
        surl_prot = surl_prot[0] # take first
        self.log("[do_put_files] SURL protocol to be used: %s" % surl_prot)

        self.trace_report.update(localSite=ddmendpoint, remoteSite=ddmendpoint)

        transferred_files, failed_transfers = [], []

        for dat in protocols:

            copytool, copysetup = dat.get('copytool'), dat.get('copysetup')

            try:
                sitemover = getSiteMover(copytool)(copysetup, workDir=self.job.workdir)
                sitemover.trace_report = self.trace_report
                sitemover.protocol = dat # ##
                sitemover.ddmconf = self.ddmconf # quick workaround  ###
                sitemover.setup()
            except Exception, e:
                self.log('[do_put_files] WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                continue

            self.log("[do_put_files] Copy command: %s, sitemover=%s" % (copytool, sitemover))
            self.log("[do_put_files] Copy setup: %s" % copysetup)

            self.trace_report.update(protocol=copytool)

            se, se_path = dat.get('se', ''), dat.get('path', '')

            self.log("[do_put_files] Found N=%s files to be transferred: %s" % (len(files), [e.get('pfn') for e in files]))

            for fdata in files:
                scope, lfn, pfn = fdata.get('scope', ''), fdata.get('lfn'), fdata.get('pfn')
                guid = fdata.get('guid', '')

                # job is passing here for possible JOB specific processing
                surl = sitemover.getSURL(surl_prot.get('se'), surl_prot.get('path'), scope, lfn, self.job, pathConvention=fdata.pathConvention, ddmType=self.ddmconf.get(fdata.ddmendpoint, {}).get('type'))
                # job is passing here for possible JOB specific processing
                turl = sitemover.getSURL(se, se_path, scope, lfn, self.job, pathConvention=fdata.pathConvention, ddmType=self.ddmconf.get(fdata.ddmendpoint, {}).get('type'))

                self.trace_report.update(scope=scope, dataset=fdata.get('dsname_report'), url=surl)
                self.trace_report.update(catStart=time.time(), filename=lfn, guid=guid.replace('-', ''))

                self.log("[do_put_files] Preparing copy for pfn=%s to ddmendpoint=%s using copytool=%s: mover=%s" % (pfn, ddmendpoint, copytool, sitemover))
                self.log("[do_put_files] lfn=%s: SURL=%s" % (lfn, surl))
                self.log("[do_put_files] TURL=%s" % turl)

                if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                    error = "Erron: output pfn file does not exist: %s" % pfn
                    self.log(error)
                    raise PilotException(error, code=PilotErrors.ERR_MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")

                filename = os.path.basename(pfn)

                # update the current file state
                updateFileState(filename, self.workDir, self.job.jobId, mode="file_state", state="not_transferred")
                dumpFileStates(self.workDir, self.job.jobId)

                # loop over multple stage-out attempts
                for _attempt in xrange(1, self.stageoutretry + 1):

                    if _attempt > 1: # if not first stage-out attempt, take a nap before next attempt
                        sleep_time = self.calc_stageout_sleeptime()
                        self.log(" -- Waiting %d seconds before next stage-out attempt for file=%s --" % (sleep_time, filename))
                        time.sleep(sleep_time)

                    self.log("[do_put_files] Put attempt %d/%d for filename=%s" % (_attempt, self.stageoutretry, filename))

                    try:
                        # quick work around
                        from Job import FileSpec
                        stub_fspec = FileSpec(ddmendpoint=ddmendpoint, guid=guid, scope=scope, lfn=lfn, cmtconfig=self.job.cmtconfig)
                        result = sitemover.stageOut(pfn, turl, stub_fspec)
                        break # transferred successfully
                    except PilotException, e:
                        result = e
                        self.log(traceback.format_exc())

                    except Exception, e:
                        self.log(traceback.format_exc())
                        result = PilotException("stageOut failed with error=%s" % e, code=PilotErrors.ERR_STAGEOUTFAILED)

                    self.log('WARNING [do_put_files]: Error in copying file (attempt %s): %s' % (_attempt, result))

                if not isinstance(result, Exception): # transferred successfully

                    # finalize and send trace report
                    self.trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time.time())
                    self.sendTrace(self.trace_report)

                    updateFileState(filename, self.workDir, self.job.jobId, mode="file_state", state="transferred")
                    dumpFileStates(self.workDir, self.job.jobId)

                    self.updateSURLDictionary(guid, surl, self.workDir, self.job.jobId) # FIX ME LATER

                    fdat = result.copy()
                    fdat.update(lfn=lfn, pfn=pfn, guid=guid, surl=surl)
                    transferred_files.append(fdat)
                else:
                    failed_transfers.append(result)


        dumpFileStates(self.workDir, self.job.jobId)

        self.log('transferred_files= %s' % transferred_files)

        if failed_transfers:
            self.log('Summary of failed transfers:')
            for e in failed_transfers:
                self.log(" -- %s" % e)

        return transferred_files, failed_transfers


    @classmethod
    def updateSURLDictionary(self, guid, surl, directory, jobId): # OLD functuonality: FIX ME LATER: avoid using intermediate buffer
        """
            add the guid and surl to the surl dictionary
        """

        # temporary quick workaround: TO BE properly implemented later
        # the data should be passed directly instead of using intermediate JSON/CPICKLE file buffers
        #

        from SiteMover import SiteMover
        return SiteMover.updateSURLDictionary(guid, surl, directory, jobId)

    @classmethod
    def getMaxInputSize(self):

        # Get a proper maxinputsize from schedconfig/default
        # quick stab: old implementation, fix me later
        from pUtil import getMaxInputSize
        return getMaxInputSize()


    def sendTrace(self, report):
        """
            Go straight to the tracing server and post the instrumentation dictionary
            :return: True in case the report has been successfully sent
        """

        if not self.useTracingService:
            self.log("Experiment is not using Tracing service. skip sending tracing report")
            return False

        url = 'https://rucio-lb-prod.cern.ch/traces/'

        self.log("Tracing server: %s" % url)
        self.log("Sending tracing report: %s" % report)

        try:
            # take care of the encoding
            #data = urlencode({'API':'0_3_0', 'operation':'addReport', 'report':report})

            data = json.dumps(report)
            data = data.replace('"','\\"') # escape quote-char

            sslCertificate = self.si.getSSLCertificate()

            cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -v -k -d "%s" %s' % (sslCertificate, data, url)
            self.log("Executing command: %s" % cmd)

            c = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
            output = c.communicate()[0]
            if c.returncode:
                raise Exception(output)
        except Exception, e:
            self.log('WARNING: FAILED to send tracing report: %s' % e)
            return False

        self.log("Tracing report successfully sent to %s" % url)
        return True
