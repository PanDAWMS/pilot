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

from pUtil import tolog, get_metadata_from_xml

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

    _stageout_sleeptime_min = 1*60  # seconds, min allowed sleep time in case of stageout failure
    _stageout_sleeptime_max = 5*60    # seconds, max allowed sleep time in case of stageout failure

    direct_remoteinput_allowed_schemas = ['root']  ## list of allowed schemas to be used for direct acccess mode from REMOTE replicas
    direct_input_allowed_schemas = ['root', 'dcache', 'dcap', 'file', 'https']  ## list of allowed schemas to be used for direct acccess mode from local replicas

    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'davs', 'srm'] ## extend me later if need


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

    def calc_stageout_sleeptime(self, attempt=1):
        return uniform(self._stageout_sleeptime_min, min(self._stageout_sleeptime_max, (attempt + 1) * self._stageout_sleeptime_min))

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

    def detect_client_location(self):
        """
        Open a UDP socket to a machine on the internet, to get the local IPv4 and IPv6
        addresses of the requesting client.
        resolve sitename from PanDA Schedconfig (ATLAS_SITE_NAME)
        """

        site = self.si.readpar('gstat')  ## ATLAS Site name

        ip = '0.0.0.0'
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            pass

        ip6 = '::'
        try:
            s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            s.connect(("2001:4860:4860:0:0:0:0:8888", 80))
            ip6 = s.getsockname()[0]
        except Exception:
            pass

        return {'ip': ip,
                'ip6': ip6,
                'fqdn': socket.getfqdn(),
                'site': site}

    def resolve_replicas(self, files):
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

        xfiles = [] # consider only normal ddmendpoints (skip OS)

        for fdat in files:
            ddmdat = self.ddmconf.get(fdat.ddmendpoint)
            if not ddmdat:
                raise Exception("Failed to resolve input ddmendpoint by name=%s sent by Panda job, please check configuration. fdat=%s" % (fdat.ddmendpoint, fdat))
            if not ddmdat['site']:
                raise Exception("Failed to resolve input site name of ddmendpoint=%s. please check ddm declaration: ddmconf=%s ... fdat=%s" % (fdat.ddmendpoint, ddmconf, fdat))

            if fdat.storageId and fdat.storageId > 0 and ddmdat.get('type') in ['OS_ES']:  ## redundant check by storageId?
                self.log('file(%s:%s) in os ddms(%s), skip resolving replicas' % (fdat.scope, fdat.lfn, fdat.ddmendpoint))
                # skip OS ddms, storageId -1 means normal RSE
                #self.log("fdat.storageId: %s" % fdat.storageId)
                #fdat.inputddms = [fdat.ddmendpoint]         ### is it used for OS?
                continue

            if fdat.storageId in [0, -1]:
                fdat.storageId = None   ### redundant, is it required for further workflow outside the function?

            # build and order list of local ddms
            #localddms = ddms.get(ddmdat['site'])
            # sort and filter ddms (as possible input source)
            #fdat.inputddms = self._prepare_input_ddm(ddmdat, localddms)
            pandaqueue = self.si.getQueueName()
            fdat.inputddms = self.si.resolvePandaAssociatedStorages(pandaqueue).get(pandaqueue, {}).get('pr', {})

            #if fdat.storageId in [-1, None] or ddmdat.type not in ['OS_ES']: ## redundant, double check -- just ported from old workflow ?
            xfiles.append(fdat)

        if not xfiles: # no files for replica look-up
            return files

        # load replicas from Rucio
        from rucio.client import Client
        c = Client()

        ## for the time being until Rucio bug with geo-ip sorting is resolved
        ## do apply either simple query list_replicas() without geoip sort to resolve LAN replicas in case of directaccesstype=[None, LAN]
        # otherwise in case of directaccesstype=WAN mode do query geo sorted list_replicas() with location data passed

        bquery = {'schemes':['srm', 'root', 'davs', 'gsiftp', 'https'],
                  'dids': [dict(scope=e.scope, name=e.lfn) for e in xfiles]
                 }

        allowRemoteInputs = True in set(e.allowRemoteInputs for e in xfiles)

        try:
            query = bquery.copy()
            #if allowRemoteInputs:
            location = self.detect_client_location()
            if not location:
                raise Exception("Failed to get client location")
            query.update(sort='geoip', client_location=location)

            try:
                self.log('Call rucio.list_replicas() with query=%s' % query)
                replicas = c.list_replicas(**query)
            except TypeError, e:
                if query == bquery:
                    raise
                self.log("WARNING: Detected outdated Rucio list_replicas(), cannot do geoip-sorting: %s .. fallback to old list_replicas() call" % e)
                replicas = c.list_replicas(**bquery)

        except Exception, e:
            raise PilotException("Failed to get replicas from Rucio: %s" % e, code=PilotErrors.ERR_FAILEDLFCGETREPS)

        replicas = list(replicas)
        self.log("replicas received from Rucio: %s" % replicas)

        files_lfn = dict(((e.scope, e.lfn), e) for e in xfiles)
        #self.log("files_lfn=%s" % files_lfn)

        for r in replicas:
            k = r['scope'], r['name']
            fdat = files_lfn.get(k)
            if not fdat: # not requested replica returned?
                continue

            fdat.replicas = [] # reset replicas list

            # manually sort replicas by priority value .. can be removed once Rucio server-side fix will be delivered
            ordered_replicas = {}
            for pfn, xdat in sorted(r.get('pfns', {}).iteritems(), key=lambda x: x[1]['priority']):
                ordered_replicas.setdefault(xdat.get('rse'), []).append(pfn)

            def get_preferred_replica(replicas, allowed_schemas):
                for replica in replicas:
                    for schema in allowed_schemas:
                        if replica and replica.startswith('%s://' % schema):
                            return replica
                return None

            has_direct_remoteinput_replicas = False

            # local replicas
            for ddm in fdat.inputddms: ## iterate over local ddms and check if replica is exist here

                #pfns = r.get('rses', {}).get(ddm)  ## use me when Rucio server-side sort fix will be deployed
                pfns = ordered_replicas.get(ddm)    ## quick workaround, use mannually sorted data

                if not pfns: # no replica found for given local ddm
                    continue

                ddm_se, ddm_path = '',''
                def_protocol = self.ddmconf[ddm].get('aprotocols', {}).get('r', [])  ## fix me later: use 'read_lan' then fallback to 'read_wan'
                def_protocol = def_protocol[0] if def_protocol else None  ## take first entry
                if def_protocol:
                    ddm_se, ddm_path = def_protocol[0], def_protocol[2]

                fdat.replicas.append((ddm, pfns, ddm_se, ddm_path))

                if not has_direct_remoteinput_replicas:
                    has_direct_remoteinput_replicas = bool(get_preferred_replica(pfns, self.direct_remoteinput_allowed_schemas))

            if ((not fdat.replicas or (fdat.accessmode == 'direct' and not has_direct_remoteinput_replicas)) and fdat.allowRemoteInputs) or (not fdat.replicas and fdat.storageId >= 0):
                if fdat.accessmode == 'direct':
                    allowed_schemas = self.direct_remoteinput_allowed_schemas
                else:
                    allowed_schemas = self.remoteinput_allowed_schemas
                self.log("No local replicas found for lfn=%s or direct access is set but no local direct access files, but allowRemoteInputs is set, looking for remote inputs" % (fdat.lfn))
                self.log("consider first/closest replica, accessmode=%s, remoteinput_allowed_schemas=%s" % (fdat.accessmode, allowed_schemas))

                for ddm, pfns in r['rses'].iteritems():
                    pfns = ordered_replicas.get(ddm) or [] ## quick workaround, use manually sorted data, REMOVE ME when Rucio server-side sort fix will be deployed

                    replica = get_preferred_replica(pfns, self.remoteinput_allowed_schemas)
                    if not replica:
                        continue

                    ddm_se, ddm_path = '', ''

                    # remoteinput supported replica (root) replica has been found
                    fdat.replicas.append((ddm, pfns, ddm_se, ddm_path))
                    # break # ignore other remote replicas/sites

            # verify filesize and checksum values

            if fdat.filesize in [None, 'NULL', '', 0]:
                self.log("WARNING: filesize is not defined, assigning info got from Rucio to it.")
                fdat.filesize = r['bytes']
            elif fdat.filesize != r['bytes']:
                self.log("WARNING: filesize value of input file=%s mismatched with info got from Rucio replica:  job.indata.filesize=%s, replica.filesize=%s, fdat=%s" % (fdat.lfn, fdat.filesize, r['bytes'], fdat))

            cc_ad = 'ad:%s' % r['adler32']
            cc_md = 'md:%s' % r['md5']
            if fdat.checksum in [None, 'NULL', '']:
                self.log("WARNING: checksum is not defined, assigning info got from Rucio to it.")
                if r['adler32']:
                    fdat.checksum = cc_ad
                elif r['md5']:
                    fdat.checksum = cc_md
            elif fdat.checksum not in [cc_ad, cc_md]:
                self.log("WARNING: checksum value of input file=%s mismatched with info got from Rucio replica:  job.indata.checksum=%s, replica.checksum=%s, fdat=%s" % (fdat.lfn, fdat.checksum, (cc_ad, cc_md), fdat))

        self.log('Number of resolved replicas:\n' + '\n'.join(["lfn=%s: replicas=%s, allowRemoteInputs=%s, is_directaccess=%s" % (e.lfn, len(e.replicas), e.allowRemoteInputs,  e.is_directaccess(ensure_replica=False)) for e in files]))

        return files


    def get_directaccess(self):
        """
            Check if direct access I/O is allowed by Site. It actually exposes direct_access_lan/wan schedconfig settings
            Also return the directaccess type (WAN or LAN).
            quick workaround: should be properly implemented in SiteInformation
            :return: (is_directaccess, directaccesstype)
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

        endpoint_id = self.si.getObjectstoreEndpointID(ddmendpoint=ddmendpoint, label='read_wan', protocol='s3')
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

    def is_local_storage(self, ddmendpoint, activity='pr'):
        pandaqueue = self.si.getQueueName() # FIX ME LATER
        associate_storages = self.si.resolvePandaAssociatedStorages(pandaqueue).get(pandaqueue, {})
        ddmendpoints = associate_storages.get(activity, [])
        return ddmendpoint in ddmendpoints

    def stagein(self, files=None, analyjob=False):
        """
            :return: (transferred_files, failed_transfers)
        """

        if files is None:
            files = self.job.inData
        _files = files

        # only print out 'files' for debugging purposes since it might contain explicit pandaProxySecretKey values
        # self.log("To stagein files: %s" % files)

        normal_files, es_files, es_local_files = [], [], []
        transferred_files, failed_transfers = [], []

        self.ddmconf.update(self.si.resolveDDMConf([])) # load ALL ddmconf

        # resolve OS ddmendpoint
        os_ddms = {}
        for ddm, dat in self.ddmconf.iteritems():
            if dat.get('type') in ['OS_ES', 'OS_LOGS']:
                os_ddms.setdefault(int(dat.get('resource', {}).get('bucket_id', -1)), ddm)
        for ddm, dat in self.ddmconf.iteritems():
            os_ddms.setdefault(int(dat.get('id', -1)), ddm)

        for fspec in files:
            if fspec.pathConvention and fspec.pathConvention >= 1000:
                fspec.scope = 'transient'
            if fspec.prodDBlockToken and fspec.prodDBlockToken.isdigit() and int(fspec.prodDBlockToken) > 0:
                fspec.storageId = int(fspec.prodDBlockToken)
                fspec.ddmendpoint = os_ddms.get(fspec.storageId)
                self.get_objectstore_keys(fspec.ddmendpoint)
                es_files.append(fspec)
                if self.is_local_storage(fspec.ddmendpoint, activity='pr'):
                    es_local_files.append(fspec)
            elif fspec.prodDBlockToken and (fspec.prodDBlockToken.strip() == '-1' or fspec.prodDBlockToken.strip() == '0'):
                # es outputs in normal RSEs (not in objectstore) and registered in rucio
                fspec.allowRemoteInputs = True
                fspec.storageId = None  # resolve replicas needs to be called for it
                es_files.append(fspec)
            else:
                normal_files.append(fspec)

        allowRemoteInputs = True in set(e.allowRemoteInputs for e in normal_files)
        self.log("AllowRemoteInputs: %s" % allowRemoteInputs)
        if normal_files:
            self.log("Will stagin normal files: %s" % [f.lfn for f in normal_files])
            transferred_files, failed_transfers = self.stagein_real(files=normal_files, activity='pr', analyjob=analyjob, skip_transfer_failure=allowRemoteInputs)

        remain_normal_files = [e for e in normal_files if e.status not in ['remote_io', 'transferred', 'no_transfer']]
        self.log("Remain normal files: %s" % (remain_normal_files))
        if remain_normal_files and allowRemoteInputs:
            self.log("Will stagin remain normal files with remote inputs: %s, allowRemoteInputs: %s" % ([f.lfn for f in remain_normal_files], allowRemoteInputs))
            copytools = [('rucio', {'setup': ''})]
            transferred_files_normal, failed_transfers_normal = self.stagein_real(files=normal_files, activity='pr_remote', copytools=copytools, analyjob=analyjob)
            transferred_files += transferred_files_normal
            failed_transfers += failed_transfers_normal
            self.log("Failed to transfer files: %s" % failed_transfers)


        if es_local_files:
            try:
                self.log("Will stagin es local files: %s" % [f.lfn for f in es_local_files])
                self.trace_report.update(eventType='get_es')
                transferred_files_es, failed_transfers_es = self.stagein_real(files=es_local_files, activity='pr', analyjob=analyjob)
                transferred_files += transferred_files_es
                failed_transfers += failed_transfers_es
                self.log("Failed to transfer files: %s" % failed_transfers)
            except Exception, e:
                self.log("Failed to stagein eventservice local files: %s" % traceback.format_exc())

        remain_es_files = [e for e in es_files if e.status not in ['remote_io', 'transferred', 'no_transfer']]
        if remain_es_files:
            self.log("Will stagin remain es files: %s" % [f.lfn for f in remain_es_files])
            self.trace_report.update(eventType='get_es')
            copytools = [('objectstore', {'setup': ''})]
            transferred_files_es, failed_transfers_es = self.stagein_real(files=es_files, activity='es_events_read', copytools=copytools, analyjob=analyjob)
            transferred_files += transferred_files_es
            failed_transfers += failed_transfers_es
            self.log("Failed to transfer files: %s" % failed_transfers)

        return transferred_files, failed_transfers

    def stagein_real(self, files, activity='pr', copytools=None, analyjob=False, skip_transfer_failure=False):
        """
            :param: analyjob -- not used, to be cleaned
            :return: (transferred_files, failed_transfers)
        """

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        protocols = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])

        overwrite = dict([k,v] for k,v in self.job.overwriteAGISData.iteritems() if k in ['copytools', 'acopytools'])
        copytools = self.si.resolvePandaCopytools(pandaqueue, activity, copytools, masterdata={pandaqueue:overwrite})[pandaqueue]

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
        allow_directaccess, directaccesstype = self.get_directaccess() ## resolve Site depended direct_access settings
        self.log("direct access mode requested by task: job.accessmode=%s" % self.job.accessmode)
        self.log("direct access mode supported by the site: allow_directaccess=%s (type=%s)" % (allow_directaccess, directaccesstype))

        if self.job.accessmode != 'direct': ## task forbids direct access
            allow_directaccess = False

        self.log("final direct access settings: job.accessmode=%s => allow_directaccess=%s" % (self.job.accessmode, allow_directaccess))

        if allow_directaccess:
            # sort files to get candidates for remote_io coming first in order to exclude them from checking of available space for stage-in
            remain_files = sorted(remain_files, key=lambda x: x.is_directaccess(ensure_replica=False), reverse=True)

            # populate allowRemoteInputs for each fdata
            for fdata in remain_files:
                is_directaccess = allow_directaccess and fdata.is_directaccess(ensure_replica=False) #fdata.turl is not defined at this point
                if is_directaccess and directaccesstype == 'WAN':  ## is it the same for ES workflow ?? -- test and verify/FIXME LATER
                    fdata.allowRemoteInputs = True
                self.log("check direct access for lfn=%s: allow_directaccess=%s, fdata.is_directaccess()=%s => is_directaccess=%s, allowRemoteInputs=%s" % (fdata.lfn, allow_directaccess, fdata.is_directaccess(ensure_replica=False), is_directaccess, fdata.allowRemoteInputs))

        sitemover_objects = {}
        is_replicas_resolved = False

        # remember the original tracing choise
        useTracingService = self.useTracingService

        for fnum, fdata in enumerate(remain_files, 1):

            self.log('INFO: prepare to transfer (stage-in) %s/%s file: lfn=%s' % (fnum, nfiles, fdata.lfn))

            is_directaccess = allow_directaccess and fdata.is_directaccess(ensure_replica=False) #fdata.turl is not defined at this point
            #self.log("check direct access: allow_directaccess=%s, fdata.is_directaccess()=%s => is_directaccess=%s" % (allow_directaccess, fdata.is_directaccess(ensure_replica=False), is_directaccess))

            bad_copytools = True

            for protnum, dat in enumerate(protocols, 1):

                if fdata.status in ['remote_io', 'transferred', 'no_transfer']: ## success
                    break

                copytool, copysetup = dat.get('copytool'), dat.get('copysetup')

                # switch off tracing if copytool=rucio, as this is handled internally by rucio
                #if copytool == 'rucio':
                #    self.useTracingService = False
                #else:
                #    # re-activate tracing in case rucio is not used for staging
                #    self.useTracingService = useTracingService

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
                        self.log("is_directaccess=%s" % is_directaccess)
                        self.log("self.job.usePrefetcher=%s"%str(self.job.usePrefetcher))
                        dat.pop('primary_scheme', None)
                        if is_directaccess or self.job.usePrefetcher:
                            if dat['scheme'] and dat['scheme'][0] != self.remoteinput_allowed_schemas[0]:  ## ensure that root:// is coming first in allowed schemas required for further resolve_replica()
                                dat['scheme'] = self.remoteinput_allowed_schemas + dat['scheme'] ## add supported schema for direct access
                            self.log("INFO: prepare direct access mode: force to extend accepted protocol schemes to use direct access, schemes=%s" % dat['scheme'])
                            dat['primary_scheme'] = self.direct_input_allowed_schemas  ## will be used to look up first the replicas allowed for direct access mode

                except Exception, e:
                    self.log('WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                    self.trace_report.update(protocol=copytool, clientState='BAD_COPYTOOL', stateReason=str(e)[:500])
                    self.sendTrace(self.trace_report)
                    continue

                bad_copytools = False

                if sitemover.require_replicas and not is_replicas_resolved:
                    self.log("mover resolving replicas")
                    self.resolve_replicas(files) ## do populate fspec.replicas for each entry in files
                    is_replicas_resolved = True

                self.log("Copy command [stage-in]: %s, sitemover=%s" % (copytool, sitemover))
                self.log("Copy setup   [stage-in]: %s" % copysetup)

                self.trace_report.update(protocol=copytool, filesize=fdata.filesize)

                updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="not_transferred", ftype="input")

                self.log("[stage-in] Prepare to get_data: [%s/%s]-protocol=%s, fspec=%s" % (protnum, nprotocols, dat, fdata))

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
                localSite = os.environ.get('DQ2_LOCAL_SITE_ID', None)
                localSite = localSite if localSite else fdata.ddmendpoint
                self.trace_report.update(localSite=localSite, remoteSite=fdata.ddmendpoint)
                self.trace_report.update(filename=fdata.lfn, guid=fdata.guid.replace('-', ''))
                self.trace_report.update(scope=fdata.scope, dataset=fdata.prodDBlock)

                # check direct access
                if fdata.is_directaccess() and is_directaccess: # direct access mode, no transfer required
                    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="file_state", state="direct_access", ftype="input")
                    fdata.status = 'remote_io'
                    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state=fdata.status, ftype="input")
                    self.log("Direct access mode will be used for lfn=%s .. skip transfer for this file" % fdata.lfn)
                    self.trace_report.update(url=fdata.turl, clientState='FOUND_ROOT', stateReason='direct_access')
                    self.sendTrace(self.trace_report)
                    continue
                else:
                    self.log('Direct access will not be user for lfn=%s since fdata.is_directaccess()=%s, is_directaccess=%s' % (fdata.lfn, fdata.is_directaccess(), is_directaccess))

                # check prefetcher (the turl must be saved for prefetcher to use)
                # note: for files to be prefetched, there's no entry for the file_state, so the updateFileState needs
                # to be called twice (or update the updateFileState function to allow list arguments)
                # also update the file_state for the existing entry (could also be removed?)
                # note also that at least one file still needs to be staged in, or AthenaMP will not start
                if self.job.usePrefetcher and self.job.eventService:
                    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="file_state", state="prefetch", ftype="input")
                    fdata.status = 'remote_io'
                    updateFileState(fdata.turl, self.workDir, self.job.jobId, mode="transfer_mode", state=fdata.status, ftype="input")
                    self.log("Added TURL to file state dictionary: %s" % fdata.turl)
                    #updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state="no_transfer", ftype="input")
                    self.trace_report.update(url=fdata.turl, clientState='FOUND_ROOT', stateReason='prefetch')
                    self.sendTrace(self.trace_report)
                    continue  # - if we continue here, the the file will not be staged in, but AthenaMP needs it so we still need to stage it in

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

                    fdata.retries = _attempt - 1
                    if _attempt > 1: # if not first stage-in attempt, try to use different ddm protocols
                        try:
                            new_replica = sitemover.resolve_replica(fdata, dat)
                        except Exception, e:
                            self.log("Failed to resolve new replica for attempts=%s, error=%s" % (_attempt, e))
                            new_replica = None

                        if new_replica and new_replica.get('ddmendpoint') == fdata.ddmendpoint:
                            if new_replica.get('surl'):
                                fdata.surl = new_replica['surl'] # TO BE CLARIFIED if it's still used and need
                            if new_replica.get('pfn'):
                                fdata.turl = new_replica['pfn']

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

            if fdata.status == 'error' and not skip_transfer_failure:
                self.log('stage-in of file (%s/%s) with lfn=%s failed: code=%s .. skip transferring remaining files..' % (fnum, nfiles, fdata.lfn, fdata.status_code))
                dumpFileStates(self.workDir, self.job.jobId, ftype="input")
                status_code = fdata.status_code if fdata.status_code != PilotErrors.ERR_UNKNOWN else PilotErrors.ERR_STAGEINFAILED
                raise PilotException("STAGEIN FAILED: %s: lfn=%s, error=%s" % (PilotErrors.getErrorStr(status_code), fdata.lfn, getattr(fdata, 'status_message', '')), code=status_code, state='STAGEIN_FILE_FAILED')

            if bad_copytools:
                raise PilotException("STAGEIN FAILED: bad copytools: no supported copytools", code=PilotErrors.ERR_NOSTORAGE, state='STAGEIN_BAD_COPYTOOLS')

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

        if not self.job.usePrefetcher:
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

        activities = ['pl', 'pw', 'w']  ## look up order of astorages activities
        activities_cp = ['pl', 'pw']    ## look up order of copytools activities

        # apply pilot side decision about which destination should be used
        data = self._prepare_destinations(self.job.logData, activities)

        return self.stageout(activities_cp, data, skip_transfer_failure=True)


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
        :param activity: activity or resolution order of activities
        :param copytools: default copytools to be used
        :param skip_transfer_failure: if enabled then all errors with previous file transfers will be ignored and the logic will continue processing of all remaining files
        :return: list of entries (is_success, success_transfers, failed_transfers, exception) for each ddmendpoint
        :return: (transferred_files, failed_transfers)
        :raise: PilotException in case of error
        """

        if not files:
            raise PilotException("Failed to put files: empty file list to be transferred", code=PilotErrors.ERR_MISSINGOUTPUTFILE, state="NO_OUTFILES")

        activities = [activity] if isinstance(activity, (str, unicode)) else activity
        activity = activities[0] ## primary activity

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        protocols = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])
        if copytools:
            self.log("Mover.stageout() [new implementation] [%s]: default copytools=%s" % (activity, copytools))

        overwrite = dict([k,v] for k,v in self.job.overwriteAGISData.iteritems() if k in ['copytools', 'acopytools'])
        copytools = self.si.resolvePandaCopytools(pandaqueue, activities, copytools, masterdata={pandaqueue:overwrite})[pandaqueue]

        self.log("Mover.stageout() [new implementation] started for activity=%s, order of activities=%s, files=%s, protocols=%s, copytools=%s" % (activity, activities, files, protocols, copytools))

        # add the local checksum since it has already been calculated and is stored in the metadata-<jobId>.xml file
        xml_dictionary = get_metadata_from_xml(self.job.workdir, 'metadata-%s.xml' % self.job.jobId)
        self.log('xml_dictionary = %s' % str(xml_dictionary))

        # check if file exists before actual processing
        # populate filesize if need

        for fspec in files:
            pfn = fspec.pfn if fspec.pfn else os.path.join(self.job.workdir, fspec.lfn)
            if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                error = "Error: output pfn file does not exist: %s" % pfn
                self.log(error)
                raise PilotException(error, code=PilotErrors.ERR_MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            fspec.filesize = os.path.getsize(pfn)
            fspec.activity = activity

            try:
                fspec.checksum = "ad:%s" % xml_dictionary.get(fspec.lfn).get('adler32')
            except Exception as e:
                self.log('failed to read local checksum from metadata file: %s' % e)
            else:
                self.log('set checksum from xml for %s: %s' % (fspec.lfn, fspec.checksum))
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

        # remember the original tracing choise
        useTracingService = self.useTracingService

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

                    # switch off tracing if copytool=rucio, as this is handled internally by rucio
                    #if copytool == 'rucio':
                    #    self.useTracingService = False
                    #else:
                    #    # re-activate tracing in case rucio is not used for staging
                    #    self.useTracingService = useTracingService

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
                            self.log("[stage-out] [%s] protocol=%s of ddmendpoint=%s is skipped since copytool=%s is not in the list of allowed (local) destinations, accepted schemes=%s" % (activity, dat['se'], ddmendpoint, copytool, dat['scheme']))

                            continue

                    self.log("Copy command [stage-out][%s]: %s, sitemover=%s" % (activity, copytool, sitemover))
                    self.log("Copy setup   [stage-out][%s]: %s" % (activity, copysetup))

                    localSite = os.environ.get('DQ2_LOCAL_SITE_ID', None)
                    localSite = localSite if localSite else ddmendpoint
                    self.trace_report.update(protocol=copytool, localSite=localSite, remoteSite=ddmendpoint)

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
                                                       ddmEndpoint=fdata.ddmendpoint)

                    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="not_transferred", ftype="output")

                    # job is passing here for possible JOB specific processing
                    fdata.turl = sitemover.getSURL(se, se_path, fdata.scope, fdata.lfn, self.job, pathConvention=fdata.pathConvention, ddmEndpoint=fdata.ddmendpoint)

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
                            sleep_time = self.calc_stageout_sleeptime(_attempt)
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
                            if e.code == PilotErrors.ERR_FILEEXIST: ## skip further attempts
                                self.log('INFO: Error in copying file (fspec %s/%s) (protocol %s/%s) (attempt %s/%s): File already exist: skip further retries (if any)' % (fnum, nfiles, protnum, nprotocols, _attempt, self.stageoutretry))
                                break
                        except Exception, e:
                            result = PilotException("stageOut failed with error=%s" % e, code=PilotErrors.ERR_STAGEOUTFAILED, state="STAGEOUT_ATTEMPT_FAILED")
                            self.log(traceback.format_exc())
                            self.log('WARNING: Error in copying file (fspec %s/%s) (protocol %s/%s) (attempt %s/%s) (exception): skip further retries (if any)' % (fnum, nfiles, protnum, nprotocols, _attempt, self.stageoutretry))
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
                self.log('[stage-out] [%s] failed to transfer file (%s/%s) with lfn=%s: code=%s .. skip transferring of remaining data..' % (activity, fnum, nfiles, fdata.lfn, fdata.status_code))
                dumpFileStates(self.workDir, self.job.jobId, ftype="output")
                status_code = fdata.status_code if fdata.status_code != PilotErrors.ERR_UNKNOWN else PilotErrors.ERR_STAGEOUTFAILED
                raise PilotException("STAGEOUT FAILED: %s: lfn=%s, error=%s" % (PilotErrors.getErrorStr(status_code), fdata.lfn, getattr(fdata, 'status_message', '')), code=status_code, state='STAGEOUT_FILE_FAILED')

            if bad_copytools:
                raise PilotException("STAGEOUT FAILED: bad copytools: no supported copytools", code=PilotErrors.ERR_NOSTORAGE, state='STAGEOUT_BAD_COPYTOOLS')


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
            raise PilotException("Failed to put files: empty file list to be transferred", code=PilotErrors.ERR_STAGEOUTFAILED)

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

        localSite = os.environ.get('DQ2_LOCAL_SITE_ID', None)
        localSite = localSite if localSite else ddmendpoint
        self.trace_report.update(localSite=localSite, remoteSite=ddmendpoint)

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
                surl = sitemover.getSURL(surl_prot.get('se'), surl_prot.get('path'), scope, lfn, self.job, pathConvention=fdata.pathConvention, ddmEndpoint=fdata.ddmendpoint)
                # job is passing here for possible JOB specific processing
                turl = sitemover.getSURL(se, se_path, scope, lfn, self.job, pathConvention=fdata.pathConvention, ddmEndpoint=fdata.ddmendpoint)

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
                        sleep_time = self.calc_stageout_sleeptime(_attempt)
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
            return False

        # remove any escape characters that might be present in the stateReason field
        stateReason = report.get('stateReason', '')
        report.update(stateReason=stateReason.replace('\\', ''))

        url = 'https://rucio-lb-prod.cern.ch/traces/'
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
