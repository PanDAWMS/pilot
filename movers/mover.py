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

from ..FileStateClient import updateFileState, dumpFileStates
from ..PilotErrors import PilotException, PilotErrors

#from ..pUtil import tolog

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


    def log(self, value): # quick stub
        print value


    @property
    def stageoutretry(self):
        return self._stageoutretry


    @stageoutretry.setter
    def stageoutretry(self, value):
        if value >= 10:
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

        activity = 'pr'
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

        if not ddmednpoints:
            raise PilotException("Failed to put files: Output ddmendpoint list is not set", code=PilotErrors.ERR_NOSTORAGE)

        missing_ddms = set(self.ddmconf) - set(ddmendpoints)

        if missing_ddms:
            self.ddmconf.update(self.si.resolveDDMConf(missing_ddms))

        ddmprot = self.protocols.setdefault(activity, self.si.resolvePandaProtocols(ddmendpoints, activity))

        self.log("Number of stage-out tries: %s" % self.stageoutretry)
        ret = None

        for ddm in ddmendpoints:
            protocols = ddmprot.get(ddm)
            if not protocols:
                self.log('Failed to resolve protocols data for ddmendpoint=%s .. skipped' % ddm)
                continue

            ret = self.do_put_files(ddmendpoint, protocols, files)
            if ret: # success
                break

            self.log('put_files(): Failed to put files to ddmendpoint=%s .. with try next ddmendpoint from the list' % ddm)

        if not ret:  # failed to put file
            self.log('put_outfiles failed')

        return bool(ret)


    def do_put_files(self, ddmendpoint, protocols, files):
        """
        Copy files to dest SE
        :ddmendpoint: output DDMEndpoint
        :return: ---
        """

        ret = False

        self.log('Prepare to copy files=%s to ddmendpoint=%s using protocols data=%s' % (files, ddmendpoint, protocols))

        # get SURL for Panda calback registration
        # resolve from special protocol activity=SE # fix me later to proper name of activitiy=SURL (panda SURL, at the moment only 2-letter name is allowed on AGIS side)

        surl_prot = [dict(se=e[0], se_path=e[2]) for e in sorted(self.ddmconf.get(ddmendpoint, {}).get('aprotocols', {}).get('SE', []), key=lambda x: x[1])]

        if not surl_prot:
            self.log('FAILED to resolve default SURL path for ddmendpoint=%s' % ddmendpoint)
            return False

        for dat in protocols:

            copytool = dat.get('copytool') or 'xrdcp' # quick stub : to be implemented later
            copysetup = dat.get('copysetup') or '/cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh' # quick stub : to be implemented later

            try:
                sitemover = getSiteMover(copytool)(copysetup)
                sitemover.setup()
            except Exception, e:
                self.log('WARNING: Failed to get SiteMover: %s .. skipped ..' % e)
                continue

            self.log("Copy command: %s, sitemover=%s" % (copytool, sitemover))
            self.log("Copy setup: %s" % copysetup)


            se, se_path = dat.get('se', ''), dat.get('se_path', '')

            N_filesNormalStageOut = 0

            for fdata in files:
                scope, lfn = fdata.get('scope', ''), fdata.get('lfn')

                surl = sitemover.getSURL(surl_prot.get('se'), surl_prot.get('se_path'), scope, lfn, self.job) # job is passing here for possible JOB specific processing

                turl = sitemover.getSURL(se, se_path, scope, lfn, self.job) # job is passing here for possible JOB specific processing

                pfn = fdata['pfn']

                self.log("Preparing copy for pfn=%s to ddmednpoint=%s using copytool=%s: mover=%s" % (pfn, ddmendpoint, copytool, sitemover))
                self.log("lfn=%s: SURL=%s" % (lfn, surl))
                self.log("TURL=%s" % turl)

                if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                    self.log("Erron: input pfn file is not exist: %s" % pfn)
                    return PilotErrors.ERR_MISSINGOUTPUTFILE # !!!

                filename = os.path.basename(pfn)

                # update the current file state
                updateFileState(filename, self.workDir, self.job.jobId, mode="file_state", state="not_transferred")
                dumpFileStates(self.workDir, self.job.jobId)

                rcode = True

                # loop over put_data() to allow for multple stage-out attempts
                for _attempt in xrange(1, self.stageoutretry + 1):

                    if _attempt > 1 and not rcode: # no errors, break
                        break

                    if _attempt > 1: # if not first stage-out attempt, take a nap before next attempt
                        self.log(" -- Waiting %d seconds before next stage-out attempt  for file=%s --" % (self.sleeptime, filename))
                        time.sleep(self.sleeptime)

                    self.log("Put attempt %d/%d" % (_attempt, self.stageoutretry))

                    try:
                        rcode, outputRet = sitemover.stageOut(pfn, turl)

                    except Exception, e:
                        rcode = PilotErrors.ERR_STAGEOUTFAILED
                        outputRet = {'errorLog': 'Caught an exception: %s' % (_attempt, e)}

                    if not rcode: # transfered successfully
                        break

                    self.log('WARNING: Error in copying file (attempt %s): outputRet=%s' % (_attempt, outputRet))


                if not rcode: # transfered successfully
                    N_filesNormalStageOut += 1
                    updateFileState(filename, self.workDir, self.job.jobId, mode="file_state", state="transferred")
                    dumpFileStates(self.workDir, self.job.jobId)





        return ret

    def sendTrace(self, report):
        """
            Go straight to the tracing server and post the instrumentation dictionary
        """

        if not self.useTracingService:
            self.log("Experiment is not using Tracing service. skip sending tracing report")
            return

        url = 'https://rucio-lb-prod.cern.ch/traces/'

        self.log("Tracing server: %s" % url)
        self.log("Sending tracing report: %s" % report)

        try:
            # take care of the encoding
            #data = urlencode({'API':'0_3_0', 'operation':'addReport', 'report':report})

            data = json.dumps(report).replace('"','\\"')
            sslCertificate = self.si.getSSLCertificate()

            cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -v -k -d "%s" %s' % (sslCertificate, data, url)
            self.log("Executing command: %s" % cmd)

            c = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)
            output = c.communicate()[0]
            if c.returncode:
                raise Exception(output)
        except Exception, e:
            self.log('WARNING: FAILED to send tracing report: %s' % e)
            return

        self.log("Tracing report successfully sent to %s" % url)
