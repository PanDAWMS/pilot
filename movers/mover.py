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

import os
import time
import traceback
from random import shuffle
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

    MAX_STAGEIN_RETRY = 3
    MAX_STAGEOUT_RETRY = 10

    _stageoutretry = 2 # default value
    _stageinretry = 2  # devault value

    stagein_sleeptime = 10*60   # seconds, sleep time in case of stagein failure
    stageout_sleeptime = 10*60  # seconds, sleep time in case of stageout failure


    def __init__(self, job, si, **kwargs):

        self.job = job
        self.si = si

        self.workDir = kwargs.get('workDir', '')

        self.stageoutretry = kwargs.get('stageoutretry', self._stageoutretry)
        self.stageinretry = kwargs.get('stageinretry', self._stageinretry)
        self.protocols = {}
        self.ddmconf = {}
        self.trace_report = {}

        self.useTracingService = kwargs.get('useTracingService', self.si.getExperimentObject().useTracingService())


    def log(self, value): # quick stub
        tolog(value)

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

    def resolve_replicas(self, files):
        """
            populates fdat.inputddms and fdat.replicas of each entry from `files` list
            fdat.replicas = [(ddmendpoint, replica, ddm_se)]
            ddm_se -- integration logic -- is used to manualy form TURL when ignore_rucio_replicas=True
            (quick stab until all protocols are properly populated in Rucio from AGIS)
        """

        # build list of local ddmendpoints grouped by site

        # load ALL ddmconf
        self.ddmconf.update(self.si.resolveDDMConf([]))
        ddms = {}
        for ddm, dat in self.ddmconf.iteritems():
            ddms.setdefault(dat['site'], []).append(dat)

        for fdat in files:

            # build and order list of local ddms
            ddmdat = self.ddmconf.get(fdat.ddmendpoint)
            if not ddmdat:
                raise Exception("Failed to resolve ddmendpoint by name=%s send by Panda job, please check configuration. fdat=%s" % (fdat.ddmendpoint, fdat))
            if not ddmdat['site']:
                raise Exception("Failed to resolve site name of ddmendpoint=%s. please check ddm declaration: ddmconf=%s ... fdat=%s" % (fdat.ddmendpoint, ddmconf, fdat))
            localddms = ddms.get(ddmdat['site'])
            # sort/filter ddms (as possible input source)
            fdat.inputddms = self._prepare_input_ddm(ddmdat, localddms)

        # load replicas from Rucio
        from rucio.client import Client
        c = Client()

        dids = [dict(scope=e.scope, name=e.lfn) for e in files]
        schemes = ['srm', 'root', 'https', 'gsiftp']

        # Get the replica list
        try:
            replicas = c.list_replicas(dids, schemes=schemes)
        except Exception, e:
            raise PilotException("Failed to get replicas from Rucio: %s" % e, code=PilotErrors.ERR_FAILEDLFCGETREPS)

        files_lfn = dict(((e.scope, e.lfn), e) for e in files)

        for r in replicas:
            k = r['scope'], r['name']
            fdat = files_lfn.get(k)
            if not fdat: # not requested replica returned?
                continue
            fdat.replicas = [] # reset replicas list
            for ddm in fdat.inputddms:
                if ddm not in r['rses']: # skip not interesting rse
                    continue
                ddm_se = self.ddmconf[ddm].get('se', '')
                fdat.replicas.append((ddm, r['rses'][ddm], ddm_se))
            if fdat.filesize != r['bytes']:
                self.log("WARNING: filesize value of input file=%s mismatched with info got from Rucio replica:  job.indata.filesize=%s, replica.filesize=%s, fdat=%s" % (fdat.lfn, fdat.filesize, r['bytes'], fdat))
            cc_ad = 'ad:%s' % r['adler32']
            cc_md = 'md:%s' % r['md5']
            if fdat.checksum not in [cc_ad, cc_md]:
                self.log("WARNING: checksum value of input file=%s mismatched with info got from Rucio replica:  job.indata.checksum=%s, replica.checksum=%s, fdat=%s" % (fdat.lfn, fdat.filesize, (cc_ad, cc_md), fdat))

            # update filesize & checksum info from Rucio?
            # TODO

        return files

    def is_directaccess(self):
        """
            check if direct access I/O is allowed
            quick workaround: should be properly implemented in SiteInformation
        """

        try:
            from FileHandling import useDirectAccessLAN
            return useDirectAccessLAN()
        except Exception, e:
            self.log("mover.is_directaccess(): Failed to resolve direct access settings: exception=%s" % e)
            return False

    def stagein(self):
        """
            :return: (transferred_files, failed_transfers)
        """

        activity = 'pr'

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        protocols = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])
        copytools = self.si.resolvePandaCopytools(pandaqueue, activity)[pandaqueue]

        self.log("stage-in: pq.aprotocols=%s, pq.copytools=%s" % (protocols, copytools))

        files = self.job.inData
        self.resolve_replicas(files) # populates also self.ddmconf = self.si.resolveDDMConf([])

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
            raise PilotException("Failed to get files: neither aprotocols nor allowed copytools defined for input. check copytools/acopytools/aprotocols schedconfig settings for activity=%s, pandaqueue=%s" % (activity, pandaqueue), code=PilotErrors.ERR_NOSTORAGE)

        sitemover_objects = {}

        for dat in protocols:

            remain_files = [e for e in files if e.status not in ['direct_access', 'transferred']]
            if not remain_files:
                self.log('INFO: all input files have been successfully processed')
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
            except Exception, e:
                self.log('WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                continue

            self.log("Copy command [stage-in]: %s, sitemover=%s" % (copytool, sitemover))
            self.log("Copy setup   [stage-in]: %s" % copysetup)

            self.trace_report.update(protocol=copytool)

            # verify file sizes and available space for stagein
            sitemover.check_availablespace(maxinputsize, remain_files)

            for fdata in remain_files:

                updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="not_transferred", ftype="input")

                self.log("[stage-in] Prepare to get_data: protocol=%s, fspec=%s" % (dat, fdata))

                # check if protocol and fdata.ddmendpoint belong to same site
                #
                if dat.get('ddm'):
                    protocol_site = self.ddmconf.get(dat.get('ddm'), {}).get('site')
                    replica_site = self.ddmconf.get(fdata.ddmendpoint, {}).get('site')

                    if protocol_site != replica_site:
                        self.log('INFO: cross-sites checks: protocol_site=%s and (fdata.ddmenpoint) replica_site=%s mismatched .. skip file processing for copytool=%s (protocol=%s)' % (protocol_site, replica_site, copytool, dat))
                        continue

                r = sitemover.resolve_replica(fdata, dat)

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
                        self.log('INFO: cross-sites checks: protocol_site=%s and replica_site=%s mismatched .. skip file processing for copytool=%s' % (protocol_site, replica_site, copytool))
                        continue

                # check direct access
                self.log("fdata.is_directaccess()=%s, job.accessmode=%s, mover.is_directaccess()=%s" % (fdata.is_directaccess(), self.job.accessmode, self.is_directaccess()))

                is_directaccess = self.is_directaccess()
                if self.job.accessmode == 'copy':
                    is_directaccess = False
                elif self.job.accessmode == 'direct':
                    is_directaccess = True
                if fdata.is_directaccess() and is_directaccess: # direct access mode, no transfer required
                    fdata.status = 'direct_access'
                    updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="transfer_mode", state="direct_access", ftype="input")

                    self.log("Direct access mode will be used for lfn=%s .. skip transfer the file" % fdata.lfn)
                    continue

                # apply site-mover custom job-specific checks for stage-in
                try:
                    is_stagein_allowed = sitemover.is_stagein_allowed(fdata, self.job)
                    if not is_stagein_allowed:
                        reason = 'SiteMover does not allowed stage-in operation for the job'
                except PilotException, e:
                    is_stagein_allowed = False
                    reason = e
                except Exception:
                    raise
                if not is_stagein_allowed:
                    self.log("WARNING: sitemover=%s does not allow stage-in transfer for this job, lfn=%s with reason=%s.. skip transfer the file" % (sitemover.getID(), fdata.lfn, reason))
                    failed_transfers.append(reason)

                    continue

                self.trace_report.update(localSite=fdata.ddmendpoint, remoteSite=fdata.ddmendpoint)
                self.trace_report.update(catStart=time.time(), filename=fdata.lfn, guid=fdata.guid.replace('-', ''))
                self.trace_report.update(scope=fdata.scope, dataset=fdata.prodDBlock)

                self.log("[stage-in] Preparing copy for lfn=%s using copytool=%s: mover=%s" % (fdata.lfn, copytool, sitemover))

                #dumpFileStates(self.workDir, self.job.jobId, ftype="input")

                # loop over multple stage-in attempts
                for _attempt in xrange(1, self.stageinretry + 1):

                    if _attempt > 1: # if not first stage-out attempt, take a nap before next attempt
                        self.log(" -- Waiting %s seconds before next stage-in attempt for file=%s --" % (self.stagein_sleeptime, fdata.lfn))
                        time.sleep(self.stagein_sleeptime)

                    self.log("Get attempt %s/%s for filename=%s" % (_attempt, self.stageinretry, fdata.lfn))

                    try:
                        result = sitemover.get_data(fdata)
                        fdata.status = 'transferred' # mark as successful
                        if result.get('ddmendpoint'):
                            fdata.ddmendpoint = result.get('ddmendpoint')
                        if result.get('surl'):
                            fdata.surl = result.get('surl')
                        if result.get('pfn'):
                            fdata.turl = result.get('pfn')

                        #self.trace_report.update(url=fdata.surl) ###
                        self.trace_report.update(url=fdata.turl) ###

                        break # transferred successfully
                    except PilotException, e:
                        result = e
                        self.log(traceback.format_exc())
                    except Exception, e:
                        result = PilotException("stageIn failed with error=%s" % e, code=PilotErrors.ERR_STAGEINFAILED)
                        self.log(traceback.format_exc())

                    self.log('WARNING: Error in copying file (attempt %s/%s): %s' % (_attempt, self.stageinretry, result))

                if not isinstance(result, Exception): # transferred successfully

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
                    failed_transfers.append(result)

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

        self.job.print_infiles()

        return transferred_files, failed_transfers


    def stageout_outfiles(self):
        return self.stageout("pw", self.job.outData)

    def stageout_logfiles(self):
        return self.stageout("pl", self.job.logData)


    def stageout(self, activity, files):
        """
            Copy files to dest SE:
            main control function, it should care about alternative stageout and retry-policy for diffrent ddmendpoints
        :return: list of entries (is_success, success_transfers, failed_transfers, exception) for each ddmendpoint
        :return: (transferred_files, failed_transfers)
        :raise: PilotException in case of error
        """

        if not files:
            raise PilotException("Failed to put files: empty file list to be transferred")

        pandaqueue = self.si.getQueueName() # FIX ME LATER
        protocols = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(pandaqueue, activity)[pandaqueue])
        copytools = self.si.resolvePandaCopytools(pandaqueue, activity)[pandaqueue]

        self.log("Mover.stageout() [new implementation] started for activity=%s, files=%s, protocols=%s, copytools=%s" % (activity, files, protocols, copytools))

        # check if file exists before actual processing
        # populate filesize if need

        for fspec in files:
            pfn = os.path.join(self.job.workdir, fspec.lfn)
            if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                error = "Erron: input pfn file is not exist: %s" % pfn
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
            raise PilotException("Failed to put files: no protocols defined for output ddmendpoints=%s .. check aprotocols schedconfig settings for activity=%s or default ddm.aprotocols entries" % (unknown_ddms, activity), code=PilotErrors.ERR_NOSTORAGE)

        self.log("[stage-out] [%s] filtered protocols to be used to transfer files: protocols=%s" % (activity, ddmprotocols))

        # get SURL endpoint for Panda callback registration
        # resolve from special protocol activity='SE' or fallback to activity='a', then to 'r'

        surl_protocols, no_surl_ddms = {}, set()

        for fspec in files:
            if not fspec.surl: # initialize only if not already set
                d = self.ddmconf.get(fspec.ddmendpoint, {}).get('aprotocols', {})
                xprot = d.get('SE', [])
                if not xprot:
                    xprot = [e for e in d.get('a', d.get('r', [])) if e[0] and e[0].startswith('srm')]
                surl_prot = [dict(se=e[0], path=e[2]) for e in sorted(xprot, key=lambda x: x[1])]
                if surl_prot:
                    surl_protocols.setdefault(fspec.ddmendpoint, surl_prot[0])
                else:
                    no_surl_ddms.add(fspec.ddmendpoint)

        if no_surl_ddms: # failed to resolve SURLs
            self.log('FAILED to resolve default SURL path for ddmendpoints=%s' % list(no_surl_ddms))
            raise PilotException("Failed to put files: no SE/SURL protocols defined for output ddmendpoints=%s .. check ddmendpoints aprotocols settings for activity=SE/a/r" % list(no_surl_ddms), code=PilotErrors.ERR_NOSTORAGE)

        sitemover_objects = {}

        # try to iterate over protocol of given ddmendpoint until successfull transfer
        for ddmendpoint, iprotocols in ddmprotocols.iteritems():

            for dat in iprotocols:

                remain_files = [e for e in ddmfiles.get(ddmendpoint) if e.status not in ['transferred']]
                if not remain_files:
                    self.log('INFO: all files to be transfered to ddm=%s have been successfully processed for activity=%s ..' % (ddmendpoint, activity))
                    # stop checking other protocols of ddmendpoint
                    break

                if not 'copytools' in dat:
                    # use allowed copytools
                    cdat = []
                    for cp, settings in copytools:
                        cdat.append({'copytool':cp, 'copysetup':settings.get('setup')})
                    dat['copytools'] = cdat

                if not dat['copytools']:
                    msg = 'FAILED to resolve final copytools settings for ddmendpoint=%s, please check schedconf.copytools settings: copytools=%s, iprotocols=' % list(ddmendpoint, copytools, iprotocols)
                    self.log(msg)
                    raise PilotException(msg, code=PilotErrors.ERR_NOSTORAGE)

                for cpsettings in dat.get('copytools', []):
                    copytool, copysetup = cpsettings.get('copytool'), cpsettings.get('copysetup')

                    try:
                        sitemover = sitemover_objects.get(copytool)
                        if not sitemover:
                            sitemover = getSiteMover(copytool)(copysetup, workDir=self.job.workdir)
                            sitemover_objects.setdefault(copytool, sitemover)

                            sitemover.trace_report = self.trace_report
                            sitemover.protocol = dat # ##
                            sitemover.ddmconf = self.ddmconf # quick workaround  ###
                            sitemover.setup()
                        if dat.get('resolve_scheme'):
                            dat['scheme'] = sitemover.schemes
                    except Exception, e:
                        self.log('WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                        continue

                    if dat.get('scheme'): # filter protocols by accepted scheme from copytool
                        should_skip = True
                        for scheme in dat.get('scheme'):
                            if dat['se'].startswith(scheme):
                                should_skip = False
                                break
                        if should_skip:
                            self.log("[stage-out] protocol=%s of ddmendpoint=%s is skipped since copytool=%s does not support it, accepted schemes=%s" % (dat['se'], ddmendpoint, copytool, dat['scheme']))

                            continue

                    self.log("Copy command [stage-out]: %s, sitemover=%s" % (copytool, sitemover))
                    self.log("Copy setup   [stage-out]: %s" % copysetup)

                    self.trace_report.update(protocol=copytool, localSite=ddmendpoint, remoteSite=ddmendpoint)

                    # validate se value?
                    se, se_path = dat.get('se', ''), dat.get('path', '')

                    for fdata in remain_files:

                        if not fdata.surl:
                            fdata.surl = sitemover.getSURL(surl_protocols[fdata.ddmendpoint].get('se'), surl_protocols[fdata.ddmendpoint].get('path'), fdata.scope, fdata.lfn, self.job) # job is passing here for possible JOB specific processing

                        updateFileState(fdata.lfn, self.workDir, self.job.jobId, mode="file_state", state="not_transferred", ftype="output")

                        fdata.turl = sitemover.getSURL(se, se_path, fdata.scope, fdata.lfn, self.job) # job is passing here for possible JOB specific processing

                        self.log("[stage-out] resolved SURL=%s to be used for lfn=%s, ddmendpoint=%s" % (fdata.surl, fdata.lfn, fdata.ddmendpoint))

                        self.log("[stage-out] resolved TURL=%s to be used for lfn=%s, ddmendpoint=%s" % (fdata.turl, fdata.lfn, fdata.ddmendpoint))

                        self.log("[stage-out] Prepare to put_data: ddmendpoint=%s, protocol=%s, fspec=%s" % (ddmendpoint, dat, fdata))

                        self.trace_report.update(catStart=time.time(), filename=fdata.lfn, guid=fdata.guid.replace('-', ''))
                        self.trace_report.update(scope=fdata.scope, dataset=fdata.destinationDblock, url=fdata.turl)

                        self.log("[stage-out] Preparing copy for lfn=%s using copytool=%s: mover=%s" % (fdata.lfn, copytool, sitemover))
                        #dumpFileStates(self.workDir, self.job.jobId, ftype="output")

                        # loop over multple stage-out attempts
                        for _attempt in xrange(1, self.stageoutretry + 1):

                            if _attempt > 1: # if not first stage-out attempt, take a nap before next attempt
                                self.log(" -- Waiting %s seconds before next stage-out attempt for file=%s --" % (self.stageout_sleeptime, fdata.lfn))
                                time.sleep(self.stageout_sleeptime)

                            self.log("Put attempt %s/%s for filename=%s" % (_attempt, self.stageoutretry, fdata.lfn))

                            try:
                                result = sitemover.put_data(fdata)
                                fdata.status = 'transferred' # mark as successful
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
                                result = PilotException("stageOut failed with error=%s" % e, code=PilotErrors.ERR_STAGEOUTFAILED)
                                self.log(traceback.format_exc())

                            self.log('WARNING: Error in copying file (attempt %s/%s): %s' % (_attempt, self.stageoutretry, result))

                        if isinstance(result, Exception): # failure transfer
                            failed_transfers.append(result)

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

                surl = sitemover.getSURL(surl_prot.get('se'), surl_prot.get('path'), scope, lfn, self.job) # job is passing here for possible JOB specific processing
                turl = sitemover.getSURL(se, se_path, scope, lfn, self.job) # job is passing here for possible JOB specific processing

                self.trace_report.update(scope=scope, dataset=fdata.get('dsname_report'), url=surl)
                self.trace_report.update(catStart=time.time(), filename=lfn, guid=guid.replace('-', ''))

                self.log("[do_put_files] Preparing copy for pfn=%s to ddmendpoint=%s using copytool=%s: mover=%s" % (pfn, ddmendpoint, copytool, sitemover))
                self.log("[do_put_files] lfn=%s: SURL=%s" % (lfn, surl))
                self.log("[do_put_files] TURL=%s" % turl)

                if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                    error = "Erron: input pfn file is not exist: %s" % pfn
                    self.log(error)
                    raise PilotException(error, code=PilotErrors.ERR_MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")

                filename = os.path.basename(pfn)

                # update the current file state
                updateFileState(filename, self.workDir, self.job.jobId, mode="file_state", state="not_transferred")
                dumpFileStates(self.workDir, self.job.jobId)

                # loop over multple stage-out attempts
                for _attempt in xrange(1, self.stageoutretry + 1):

                    if _attempt > 1: # if not first stage-out attempt, take a nap before next attempt
                        self.log(" -- Waiting %d seconds before next stage-out attempt for file=%s --" % (self.stageout_sleeptime, filename))
                        time.sleep(self.stageout_sleeptime)

                    self.log("[do_put_files] Put attempt %d/%d for filename=%s" % (_attempt, self.stageoutretry, filename))

                    try:
                        # quick work around
                        from Job import FileSpec
                        stub_fspec = FileSpec(ddmendpoint=ddmendpoint, guid=guid, scope=scope, lfn=lfn)
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
