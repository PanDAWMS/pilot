"""
  JobMover: top level mover to copy all Job files

  This functionality actually needs to be incorporated directly into RunJob,
  because it's just a workflow of RunJob
  but since RunJob at the moment is implemented as a Singleton
  it could be dangerous because of Job instance stored in the class object.
  :author: Alexey Anisenkov
"""

from subprocess import Popen, PIPE, STDOUT

from . import getSiteMover
from .trace_report import TraceReport

from FileStateClient import updateFileState, dumpFileStates
from PilotErrors import PilotException, PilotErrors

from pUtil import tolog

import time
import os


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

    protocols = {} #
    ddmconf = {}

    _stageoutretry = 2
    _stageinretry = 2

    sleeptime = 10*60  # sleep time in case of stageout failure


    def __init__(self, job, si, **kwargs):

        self.job = job
        self.si = si

        self.workDir = kwargs.get('workDir', '')

        self.stageoutretry = kwargs.get('stageoutretry', self._stageoutretry)
        self.stageinretry = kwargs.get('stageinretry', self._stageinretry)

        self.protocols = {}
        self.ddmconf = {}

        self.useTracingService = kwargs.get('useTracingService', self.si.getExperimentObject().useTracingService())
        self.trace_report = {}


    def log(self, value): # quick stub

        #print value
        tolog(value)


    @property
    def stageoutretry(self):
        return self._stageoutretry


    @stageoutretry.setter
    def stageoutretry(self, value):
        if value >= 10 or value < 1:
            ival = value
            value = JobMover._stageoutretry
            self.log("WARNING: Unreasonable number of stage-out tries: %d, reset to default value=%s" % (ival, value))
        self._stageoutretry = value


    @property
    def stageinretry(self):
        return self._stageinretry


    @stageoutretry.setter
    def stageinretry(self, value):
        if value >= 10:
            ival = value
            value = JobMover._stageinretry
            self.log("WARNING: Unreasonable number of stage-in tries: %d, reset to default value=%s" % (ival, value))
        self._stageinretry = value


    def put_outfiles(self, files):
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


    def put_logfiles(self, files):
        """
        Copy log files to dest SE
        :files: list of files to be moved
        """

        activity = 'pl'
        ddms = self.job.ddmEndPointLog

        if not ddms:
            raise PilotException("Output ddmendpoint list (job.ddmEndPointLog) is not set", code=PilotErrors.ERR_NOSTORAGE)

        return self.put_files(ddms, activity, files)


    def put_files(self, ddmendpoints, activity, files):
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

        ddmprot = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(ddmendpoints, activity))

        output = []

        for ddm in ddmendpoints:
            protocols = ddmprot.get(ddm)
            if not protocols:
                self.log('Failed to resolve protocols data for ddmendpoint=%s .. skipped processing..' % ddm)
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


    def do_put_files(self, ddmendpoint, protocols, files):
        """
        Copy files to dest SE
        :ddmendpoint: DDMEndpoint name used to store files
        :return: (list of transferred_files details, list of failed_transfers details)
        :raise: PilotException in case of error
        """

        self.log('Prepare to copy files=%s to ddmendpoint=%s using protocols data=%s' % (files, ddmendpoint, protocols))
        self.log("Number of stage-out tries: %s" % self.stageoutretry)

        # get SURL for Panda calback registration
        # resolve from special protocol activity=SE # fix me later to proper name of activitiy=SURL (panda SURL, at the moment only 2-letter name is allowed on AGIS side)

        surl_prot = [dict(se=e[0], se_path=e[2]) for e in sorted(self.ddmconf.get(ddmendpoint, {}).get('aprotocols', {}).get('SE', []), key=lambda x: x[1])]

        if not surl_prot:
            self.log('FAILED to resolve default SURL path for ddmendpoint=%s' % ddmendpoint)
            return [], []

        surl_prot = surl_prot[0] # take first

        self.trace_report.update(localSite=ddmendpoint, remoteSite=ddmendpoint)

        transferred_files = []
        failed_transfers = []

        for dat in protocols:

            copytool = dat.get('copytool') or 'xrdcp' # quick stub : to be implemented later
            copysetup = dat.get('copysetup') or '/cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh' # quick stub : to be implemented later

            try:
                sitemover = getSiteMover(copytool)(copysetup)
                sitemover.setup()
            except Exception, e:
                self.log('WARNING: Failed to get SiteMover: %s .. skipped .. try to check next available protocol, current protocol details=%s' % (e, dat))
                continue

            self.log("Copy command: %s, sitemover=%s" % (copytool, sitemover))
            self.log("Copy setup: %s" % copysetup)

            self.trace_report.update(protocol=copytool)
            sitemover.trace_report = self.trace_report

            se, se_path = dat.get('se', ''), dat.get('se_path', '')

            self.log("Found N=%s files to be transferred: %s" % (len(files), [e.get('pfn') for e in files]))

            for fdata in files:
                scope, lfn, pfn = fdata.get('scope', ''), fdata.get('lfn'), fdata.get('pfn')
                guid = fdata.get('guid', '')

                surl = sitemover.getSURL(surl_prot.get('se'), surl_prot.get('se_path'), scope, lfn, self.job) # job is passing here for possible JOB specific processing
                turl = sitemover.getSURL(se, se_path, scope, lfn, self.job) # job is passing here for possible JOB specific processing

                self.trace_report.update(scope=scope, dataset=fdata.get('dsname_report'), url=surl)
                self.trace_report.update(catStart=time.time(), filename=lfn, guid=guid.replace('-', ''))

                self.log("Preparing copy for pfn=%s to ddmendpoint=%s using copytool=%s: mover=%s" % (pfn, ddmendpoint, copytool, sitemover))
                self.log("lfn=%s: SURL=%s" % (lfn, surl))
                self.log("TURL=%s" % turl)

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
                        self.log(" -- Waiting %d seconds before next stage-out attempt for file=%s --" % (self.sleeptime, filename))
                        time.sleep(self.sleeptime)

                    self.log("Put attempt %d/%d for filename=%s" % (_attempt, self.stageoutretry, filename))

                    try:
                        result = sitemover.stageOut(pfn, turl)
                        break # transferred successfully
                    except PilotException, e:
                        result = e
                    except Exception, e:
                        result = PilotException("stageOut failed with error=%s" % e, code=PilotErrors.ERR_STAGEOUTFAILED)

                    self.log('WARNING: Error in copying file (attempt %s): %s' % (_attempt, result))

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
            data =data.replace('"','\\"') # escape quote-char

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
