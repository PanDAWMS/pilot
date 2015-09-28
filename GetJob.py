
import commands
import json
import os
import traceback
import Job
import pUtil
from pUtil import tolog

class GetJob:
    def __init__(self, pilot_initdir, node, siteInfo, jobSite):
        self.__pilot_initdir = pilot_initdir
        self.__pilotWorkingDir = pilot_initdir
        self.__env = None
        self.__node = node
        self.__siteInfo = siteInfo
        self.__jobSite = jobSite
        self.__thisExperiment = None
        self.setup()

    def setup(self):
        with open(os.path.join(self.__pilot_initdir, 'env.json')) as inputFile:
           self.__env = json.load(inputFile)
        self.__env['si'] = self.__siteInfo
        self.__env['thisSite'] =  self.__jobSite
        self.__env['workerNode'] = self.__node
        self.__thisExperiment = self.__env['experiment']
        self.__env['pilot_initdir'] = self.__pilot_initdir

    def getProdSourceLabel(self):
        """ determine the job type """

        prodSourceLabel = None
 
        # not None value; can be user (user analysis job), ddm (panda mover job, sitename should contain DDM)
        # test will return a testEvgen/testReco job, ptest will return a job sent with prodSourceLabel ptest
        if self.__env['uflag']:
            if self.__env['uflag'] == 'self' or self.__env['uflag'] == 'ptest':
                if self.__env['uflag'] == 'ptest':
                    prodSourceLabel = self.__env['uflag']
                elif self.__env['uflag'] == 'self':
                    prodSourceLabel = 'user'
            else:
                prodSourceLabel = self.__env['uflag']

        # for PandaMover jobs the label must be ddm
        if "DDM" in self.__env['thisSite'].sitename or (self.__env['uflag'] == 'ddm' and self.__env['thisSite'].sitename == 'BNL_ATLAS_test'):
            prodSourceLabel = 'ddm'
        elif "Install" in self.__env['thisSite'].sitename:  # old, now replaced with prodSourceLabel=install
            prodSourceLabel = 'software'
        if pUtil.readpar('status').lower() == 'test' and self.__env['uflag'] != 'ptest' and self.__env['uflag'] != 'ddm':
            prodSourceLabel = 'test'

        # override for release candidate pilots
        if self.__env['pilot_version_tag'] == "RC":
            prodSourceLabel = "rc_test"
        if self.__env['pilot_version_tag'] == "DDM":
            prodSourceLabel = "ddm"

        return prodSourceLabel

    def getDispatcherDictionary(self, _diskSpace, tofile):
        """ Construct a dictionary for passing to jobDispatcher """

        pilotErrorDiag = ""

        # glExec proxy key
        _getProxyKey = "False"
        # Eddie - commented out
        # if pUtil.readpar('glexec').lower() in ['true', 'uid']:
        #     _getProxyKey = "True"

        nodename = self.__env['workerNode'].nodename
        pUtil.tolog("Node name: %s" % (nodename))

        jNode = {'siteName':         self.__env['thisSite'].sitename,
                 'cpu':              self.__env['workerNode'].cpu,
                 'mem':              self.__env['workerNode'].mem,
                 'diskSpace':        _diskSpace,
                 'node':             nodename,
                 'computingElement': self.__env['thisSite'].computingElement,
                 'getProxyKey':      _getProxyKey,
                 'workingGroup':     self.__env['workingGroup']}

        if self.__env['countryGroup'] == "":
            pUtil.tolog("No country group selected")
        else:
            jNode['countryGroup'] = self.__env['countryGroup']
            pUtil.tolog("Using country group: %s" % (self.__env['countryGroup']))

        if self.__env['workingGroup'] == "":
            pUtil.tolog("No working group selected")
        else:
            pUtil.tolog("Using working group: %s" % (jNode['workingGroup']))

        if self.__env['allowOtherCountry']:
            pUtil.tolog("allowOtherCountry is set to True (will be sent to dispatcher)")
            jNode['allowOtherCountry'] = self.__env['allowOtherCountry']

        # should the job be requested for a special DN?
        if self.__env['uflag'] == 'self':
            # get the pilot submittor DN, and only process this users jobs
            DN, pilotErrorDiag = self.getDN()
            if DN == "":
                return {}, "", pilotErrorDiag
            else:
                jNode['prodUserID'] = DN

            pUtil.tolog("prodUserID: %s" % (jNode['prodUserID']))

        # determine the job type
        prodSourceLabel = self.getProdSourceLabel()
        if prodSourceLabel:
            jNode['prodSourceLabel'] = prodSourceLabel
            pUtil.tolog("prodSourceLabel: %s" % (jNode['prodSourceLabel']), tofile=tofile)

        # send the pilot token
        # WARNING: do not print the jNode dictionary since that will expose the pilot token
        if self.__env['pilotToken']:
            jNode['token'] = self.__env['pilotToken']

        return jNode, prodSourceLabel, pilotErrorDiag

    def getDN(self):
        """ Return the DN for the pilot submitter """

        DN = ""
        pilotErrorDiag = ""

        # Try to use arcproxy first since voms-proxy-info behaves poorly under SL6
        # cmd = "arcproxy -I |grep 'subject'| sed 's/.*: //'"
        cmd = "arcproxy -i subject"
        pUtil.tolog("Executing command: %s" % (cmd))
        err, out = commands.getstatusoutput(cmd)
        if "command not found" in out:
            pUtil.tolog("!!WARNING!!1234!! arcproxy is not available")
            pUtil.tolog("!!WARNING!!1235!! Defaulting to voms-proxy-info (can lead to memory problems with the command in case of low schedconfig.memory setting)")

            # Default to voms-proxy-info
            cmd = "voms-proxy-info -subject"
            pUtil.tolog("Executing command: %s" % (cmd))
            err, out = commands.getstatusoutput(cmd)

        if err == 0:
            DN = out
            pUtil.tolog("Got DN = %s" % (DN))

            CN = "/CN=proxy"
            if not DN.endswith(CN):
                pUtil.tolog("!!WARNING!!1234!! DN does not end with %s (will be added)" % (CN))
                DN += CN
        else:
            pilotErrorDiag = "User=self set but cannot get proxy: %d, %s" % (err, out)

        return DN, pilotErrorDiag

    def writeDispatcherEC(self, EC):
        """ write the dispatcher exit code to file """    
        filename = os.path.join(self.__env['pilot_initdir'], "STATUSCODE")
        if os.path.exists(filename):
            try:
                os.remove(filename)
            except Exception, e:
                pUtil.tolog("Warning: Could not remove file: %s" % str(e))
            else:
                pUtil.tolog("Removed existing STATUSCODE file")
        pUtil.writeToFile(os.path.join(filename), str(EC))    

    def getStatusCode(self, data):
        """ get and write the dispatcher status code to file """

        pUtil.tolog("Parsed response: %s" % str(data))

        try:
            StatusCode = data['StatusCode']
        except Exception, e:
            pilotErrorDiag = "Can not receive any job from jobDispatcher: %s" % str(e)
            pUtil.tolog("!!WARNING!!1200!! %s" % (pilotErrorDiag))
            StatusCode = '45'

        # Put the StatusCode in a file (used by some pilot wrappers), erase if it already exists
        self.writeDispatcherEC(StatusCode)

        return StatusCode

    def backupDispatcherResponse(self, response, tofile):
        """ Backup response (will be copied to workdir later) """        
        try:
            fh = open(self.__env['pandaJobDataFileName'], "w")
            fh.write(response)
            fh.close()
        except Exception, e:
            pUtil.tolog("!!WARNING!!1999!! Could not store job definition: %s" % str(e), tofile=tofile)
        else:
            pUtil.tolog("Job definition stored (for later backup) in file %s" % (self.__env['pandaJobDataFileName']), tofile=tofile)


    def backupJobData(self, newJob, data):
        filename = os.path.join(self.__pilotWorkingDir, "Job_%s.json" % newJob.jobId)
        content = {'workdir': newJob.workdir, 'data': data, 'experiment': self.__thisExperiment}
        with open(filename, 'w') as outputFile:
            json.dump(content, outputFile)

    def getNewJob(self, tofile=True):
        try:
            _maxinputsize = pUtil.getMaxInputSize(MB=True)
            _disk = self.__node.disk
            pUtil.tolog("Available WN disk space: %d MB" % (_disk))
            _diskSpace = min(_disk, _maxinputsize)
            pUtil.tolog("Sending disk space %d MB to dispatcher" % (_diskSpace))

            # construct a dictionary for passing to jobDispatcher and get the prodSourceLabel
            jNode, prodSourceLabel, pilotErrorDiag = self.getDispatcherDictionary(_diskSpace, tofile)
            if jNode == {}:
                errorText = "!!FAILED!!1200!! %s" % (pilotErrorDiag)
                pUtil.tolog(errorText, tofile=tofile)
                # send to stderr
                print >> sys.stderr, errorText
                return None, None, pilotErrorDiag

            # get a random server
            url = '%s:%s/server/panda' % (self.__env['pshttpurl'], str(self.__env['psport']))
            pUtil.tolog("Looking for a primary job (contacting server at %s)" % (url), tofile=tofile)

            # make http connection to jobdispatcher
            # format: status, parsed response (data), response
            ret = pUtil.httpConnect(jNode, url, mode = "GETJOB", path = self.__pilotWorkingDir, experiment = self.__thisExperiment) # connection mode is GETJOB

            # get and write the dispatcher status code to file
            StatusCode = str(ret[0])

            # the original response will be put in a file in this function
            data = ret[1] # dictionary
            response = ret[2] # text

            # write the dispatcher exit code to file
            self.writeDispatcherEC(StatusCode)

            if ret[0]: # non-zero return
                return None, None, pUtil.getDispatcherErrorDiag(ret[0])

            if StatusCode != '0':
                pilotErrorDiag = "No job received from jobDispatcher, StatusCode: %s" % (StatusCode)
                pUtil.tolog("%s" % (pilotErrorDiag), tofile=tofile)
                return None, None, pilotErrorDiag

            # test if he attempt number was sent
            try:
                attemptNr = int(data['attemptNr'])
            except Exception,e:
                pUtil.tolog("!!WARNING!!1200!! Failed to get attempt number from server: %s" % str(e), tofile=tofile)
            else:
                pUtil.tolog("Attempt number from server: %d" % attemptNr)

            # should there be a delay before setting running state?
            try:
                nSent = int(data['nSent'])
            except Exception,e:
                nSent = 0
            else:
                pUtil.tolog("Received nSent: %d" % (nSent))

            # backup response (will be copied to workdir later)
            self.backupDispatcherResponse(response, tofile)

            if data.has_key('prodSourceLabel'):
                if data['prodSourceLabel'] == "":
                    pUtil.tolog("Setting prodSourceLabel in job def data: %s" % (prodSourceLabel))
                    data['prodSourceLabel'] = prodSourceLabel
                else:
                    pUtil.tolog("prodSourceLabel already set in job def data: %s" % (data['prodSourceLabel']))

                    # override ptest value if install job to allow testing using dev pilot
                    if prodSourceLabel == "ptest" and "atlpan/install/sw-mgr" in data['transformation']:
                        pUtil.tolog("Dev pilot will run test install job (job.prodSourceLabel set to \'install\')")
                        data['prodSourceLabel'] = "install"
            else:
                pUtil.tolog("Adding prodSourceLabel to job def data: %s" % (prodSourceLabel))
                data['prodSourceLabel'] = prodSourceLabel

            # look for special commands in the job parameters (can be set by HammerCloud jobs; --overwriteQueuedata, --disableFAX)
            # if present, queuedata needs to be updated (as well as jobParameters - special commands need to be removed from the string)
            data['jobPars'], transferType = self.__siteInfo.updateQueuedataFromJobParameters(data['jobPars'])
            if transferType != "":
                # we will overwrite whatever is in job.transferType using jobPars
                data['transferType'] = transferType

            # update the copytoolin if transferType is set to fax/xrd
            if data.has_key('transferType'):
                if data['transferType'] == 'fax' or data['transferType']== 'xrd':
                    if pUtil.readpar('faxredirector') != "":
                        pUtil.tolog("Encountered transferType=%s, will use FAX site mover for stage-in" % (data['transferType']))
                        ec = self.__siteInfo.replaceQueuedataField("copytoolin", "fax")
                        ec = self.__siteInfo.replaceQueuedataField("allowfax", "True")
                        ec = self.__siteInfo.replaceQueuedataField("timefloor", "")
                    else:
                        pilotErrorDiag = "Cannot switch to FAX site mover for transferType=%s since faxredirector is not set" % (data['transferType'])
                        pUtil.tolog("!!WARNING!!1234!! %s" % (pilotErrorDiag))
                        return None, None, pilotErrorDiag

            # convert the data into a file for child process to pick for running real job later
            try:
                f = open("Job_%s.py" % data['PandaID'], "w")
                print >>f, "job=", data
                f.close()
            except Exception,e:
                pilotErrorDiag = "[pilot] Exception caught: %s" % str(e)
                pUtil.tolog("!!WARNING!!1200!! %s" % (pilotErrorDiag), tofile=tofile)
                return None, None, pilotErrorDiag

            # create the new job
            newJob = Job.Job()
            newJob.setJobDef(data)  # fill up the fields with correct values now
            newJob.mkJobWorkdir(self.__pilotWorkingDir)
            self.backupJobData(newJob, data)
            newJob.datadir = self.__jobSite.workdir + "/PandaJob_%s_data" % (newJob.jobId)
            newJob.experiment = self.__thisExperiment

            if data.has_key('logGUID'):
                logGUID = data['logGUID']
                if logGUID != "NULL" and logGUID != "":
                    newJob.tarFileGuid = logGUID
                    pUtil.tolog("Got logGUID from server: %s" % (logGUID), tofile=tofile)
                else:
                    pUtil.tolog("!!WARNING!!1200!! Server returned NULL logGUID", tofile=tofile)
                    pUtil.tolog("Using generated logGUID: %s" % (newJob.tarFileGuid), tofile=tofile)
            else:
                pUtil.tolog("!!WARNING!!1200!! Server did not return logGUID", tofile=tofile)
                pUtil.tolog("Using generated logGUID: %s" % (newJob.tarFileGuid), tofile=tofile)

            if newJob.prodSourceLabel == "":
                pUtil.tolog("Giving new job prodSourceLabel=%s" % (prodSourceLabel))
                newJob.prodSourceLabel = prodSourceLabel
            else:
                pUtil.tolog("New job has prodSourceLabel=%s" % (newJob.prodSourceLabel))

            # should we use debug mode?
            if data.has_key('debug'):
                if data['debug'].lower() == "true":
                    self.__env['update_freq_server'] = 5*30
                    pUtil.tolog("Debug mode requested: Updating server update frequency to %d s" % (self.__env['update_freq_server']))
            return newJob, data, ""
        except:
            errLog = "Failed to get New job: %s" % (traceback.format_exc())
            tolog(errLog)
            return None, None, errLog

