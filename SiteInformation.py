# Class definition:
#   SiteInformation
#   This class is responsible for downloading, verifying and manipulating queuedata
#   Note: not compatible with Singleton Design Pattern due to the subclassing

import os
import re
import commands
from pUtil import tolog, getExtension, replace, readpar, getDirectAccessDic
from pUtil import getExperiment as getExperimentObject
from PilotErrors import PilotErrors

class SiteInformation(object):
    """

    Should this class ask the Experiment class which the current experiment is?
    Not efficient if every readpar() calls some Experiment method unless Experiment is a singleton class as well

    """

    # private data members
    __experiment = "generic"
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object
    __securityKeys = {}

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def readpar(self, par, alt=False):
        """ Read parameter variable from queuedata """

        value = ""
        fileName = self.getQueuedataFileName(alt=alt)
        try:
            fh = open(fileName)
        except:
            try:
                # try without the path
                fh = open(os.path.basename(fileName))
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not read queuedata file: %s" % str(e))
                fh = None
        if fh:
            queuedata = fh.read()
            fh.close()
            if queuedata != "":
                value = self.getpar(par, queuedata, containsJson=fileName.endswith("json"))

        # repair JSON issue
        if value == None:
            value = ""

        return value

    def getpar(self, par, s, containsJson=False):
        """ Extract par from s """

        parameter_value = ""
        if containsJson:
            # queuedata is a json string
            from json import loads
            pars = loads(s)
            if pars.has_key(par):
                parameter_value = pars[par]
                if type(parameter_value) == unicode: # avoid problem with unicode for strings
                    parameter_value = parameter_value.encode('ascii')
            else:
                tolog("WARNING: Could not find parameter %s in queuedata" % (par))
                parameter_value = ""
        else:
            # queuedata is a string on the form par1=value1|par2=value2|...
            matches = re.findall("(^|\|)([^\|=]+)=",s)
            for tmp,tmpPar in matches:
                if tmpPar == par:
                    patt = "%s=(.*)" % par
                    idx = matches.index((tmp,tmpPar))
                    if idx+1 == len(matches):
                        patt += '$'
                    else:
                        patt += "\|%s=" % matches[idx+1][1]
                    mat = re.search(patt,s)
                    if mat:
                        parameter_value = mat.group(1)
                    else:
                        parameter_value = ""

        return parameter_value

    def getQueuedataFileName(self, useExtension=None, check=True, alt=False):
        """ Define the queuedata filename """

        # use a forced extension if necessary
        if useExtension:
            extension = useExtension
        else:
            extension = getExtension(alternative='dat')

        # prepend alt. for alternative stage-out site queuedata
        if alt:
            extension = "alt." + extension

        path = "%s/queuedata.%s" % (os.environ['PilotHomeDir'], extension)

        # remove the json extension if the file cannot be found (complication due to wrapper)
        if not os.path.exists(path) and check:
            if extension == 'json':
                _path = path.replace('.json', '.dat')
                if os.path.exists(_path):
                    tolog("Updating queuedata file name to: %s" % (_path))
                    path = _path
                else:
                    tolog("!!WARNING!! Queuedata paths do not exist: %s, %s" % (path, _path))
            if extension == 'dat':
                _path = path.replace('.dat', '.json')
                if os.path.exists(_path):
                    tolog("Updating queuedata file name to: %s" % (_path))
                    path = _path
                else:
                    tolog("!!WARNING!! Queuedata paths do not exist: %s, %s" % (path, _path))
        return path

    def replaceQueuedataField(self, field, value, verbose=True):
        """ replace a given queuedata field with a new value """
        # copytool = <whatever> -> lcgcp
        # replaceQueuedataField("copytool", "lcgcp")

        status = False

        verbose = True
        queuedata_filename = self.getQueuedataFileName()
        if "json" in queuedata_filename.lower():
            if self.replaceJSON(queuedata_filename, field, value):
                if verbose:
                    tolog("Successfully changed %s to: %s" % (field, value))
                    status = True
        else:
            stext = field + "=" + self.readpar(field)
            rtext = field + "=" + value
            if replace(queuedata_filename, stext, rtext):
                if verbose:
                    tolog("Successfully changed %s to: %s" % (field, value))
                    status = True
            else:
                tolog("!!WARNING!!1999!! Failed to change %s to: %s" % (field, value))

        return status

    def replaceJSON(self, queuedata_filename, field, value):
        """ Replace/update queuedata field in JSON file """

        status = False
        from json import load, dump
        try:
            fp = open(queuedata_filename, "r")
        except Exception, e:
            tolog("!!WARNING!!4003!! Failed to open file: %s, %s" % (queuedata_filename, e))
        else:
            try:
                dic = load(fp)
            except Exception, e:
                tolog("!!WARNING!!4004!! Failed to load dictionary: %s" % (e))
            else:
                fp.close()
                if dic.has_key(field):
                    dic[field] = value
                    try:
                        fp = open(queuedata_filename, "w")
                    except Exception, e:
                        tolog("!!WARNING!!4005!! Failed to open file: %s, %s" % (queuedata_filename, e))
                    else:
                        try:
                            dump(dic, fp)
                        except Exception, e:
                            tolog("!!WARNING!!4005!! Failed to dump dictionary: %s" % (e))
                        else:
                            fp.close()
                            status = True
                else:
                    tolog("!!WARNING!!4005!! No such field in queuedata dictionary: %s" % (field))

        return status

    def evaluateQueuedata(self):
        """ Evaluate environmental variables if used and replace the value in the queuedata """

        tolog("Evaluating queuedata")

        # the following fields are allowed to contain environmental variables
        fields = ["appdir", "copysetup", "copysetupin", "recoverdir", "wntmpdir", "sepath", "seprodpath", "lfcpath", "lfcprodpath"]

        # process each field and evaluate the environment variables if present
        for field in fields:
            # grab the field value and split it since some fields can contain ^-separators
            old_values = self.readpar(field)
            new_values = []
            try:
                for value in old_values.split("^"):
                    pipe = ""
                    if value.startswith("$"):
                        # get rid of any |-signs (e.g. appdir containing nightlies bit)
                        if "|" in value:
                            pipe = value[value.find('|'):] # add this back later (e.g. pipe = "|nightlies")
                            value = value[:value.find('|')]
                        # evaluate the environmental variable
                        new_value = os.path.expandvars(value)
                        if new_value == "":
                            tolog("!!WARNING!!2999!! Environmental variable not set: %s" % (value))
                        value = new_value + pipe
                    new_values.append(value)

                # rebuild the string (^-separated if necessary)
                new_values_joined = '^'.join(new_values)

                # replace the field value in the queuedata with the new value
                if new_values_joined != old_values:
                    if self.replaceQueuedataField(field, new_values_joined, verbose=False):
                        tolog("Updated field %s in queuedata (replaced \'%s\' with \'%s\')" % (field, old_values, new_values_joined))
            except:
                # ignore None values
                continue

    def verifyQueuedata(self, queuename, filename, _i, _N, url):
        """ Verify the consistency of the queuedata """

        hasQueuedata = False
        try:
            f = open(filename, "r")
        except Exception, e:
            tolog("!!WARNING!!1999!! Open failed with %s" % (e))
        else:
            output = f.read()
            f.close()
            if not ('appdir' in output and 'copytool' in output):
                if len(output) == 0:
                    tolog("!!WARNING!!1999!! curl command returned empty queuedata (wrong queuename %s?)" % (queuename))
                else:
                    tolog("!!WARNING!!1999!! Attempt %d/%d: curl command did not return valid queuedata from config DB server %s" %\
                          (_i, _N, url))
                    output = output.replace('\n', '')
                    output = output.replace(' ', '')
                    tolog("!!WARNING!!1999!! Output begins with: %s" % (output[:64]))
                try:
                    os.remove(filename)
                except Exception, e:
                    tolog("!!WARNING!!1999!! Failed to remove file %s: %s" % (filename, e))
            else:
                # found valid queuedata info, break the for-loop
                tolog("schedconfigDB returned: %s" % (output))
                hasQueuedata = True

        return hasQueuedata

    def getQueuedata(self, queuename, forceDownload=False, alt=False, url=""):
        """ Download the queuedata if not already downloaded """

        # Queuedata means the dump of all geometrical data for a given site. This method downloads and stores queuedata in a JSON or pickle
        # file called queuedata.[json|dat]. JSON format is preferable and is used for python versions >= 2.6.
        #
        # Exeute the following command for a queuedata example:
        # curl --connect-timeout 20 --max-time 120 -sS "http://pandaserver.cern.ch:25085/cache/schedconfig/CERN-PROD-all-prod-CEs.pilot.json"

        # Input:
        #   queuename = name of the PanDA queue (e.g. CERN-PROD-all-prod-CEs)
        #   forceDownload = False (default), 
        #   alt = False (default), if alternative queuedata should be downloaded (if stage-out to an alternative SE, new queuedata is needed 
        #         but it will not overwrite the old queuedata)
        # Returns:
        #   error code (int), status for queuedata download (boolean)

        if url == "":
            exp = getExperimentObject(self.__experiment)
            url = exp.getSchedconfigURL()
            tolog("The schedconfig URL was not set by the wrapper - Will use default server url = %s (hardcoded)" % (url))

        if not os.environ.has_key('PilotHomeDir'):
            os.environ['PilotHomeDir'] = commands.getoutput('pwd')
        hasQueuedata = False

        # try the config servers one by one in case one of them is not responding

        # in case the wrapper has already downloaded the queuedata, it might have a .dat extension
        # otherwise, give it a .json extension if possible
        filename_dat = self.getQueuedataFileName(useExtension='dat', check=False, alt=alt)
        if os.path.exists(filename_dat):
            filename = filename_dat
        else:
            filename = self.getQueuedataFileName(check=False, alt=alt)

        if os.path.exists(filename) and not forceDownload:
            tolog("Queuedata has already been downloaded by pilot wrapper script (will confirm validity)")
            hasQueuedata = self.verifyQueuedata(queuename, filename, 1, 1, "(see batch log for url)")
            if hasQueuedata:
                tolog("Queuedata was successfully downloaded by pilot wrapper script")
            else:
                tolog("Queuedata was not downloaded successfully by pilot wrapper script, will try again")

        if not hasQueuedata:
            # loop over pandaserver round robin _N times until queuedata has been verified, or fail
            ret = -1
            if os.environ.has_key('X509_USER_PROXY'):
                sslCert = os.environ['X509_USER_PROXY']
            else:
                sslCert  = '/tmp/x509up_u%s' % str(os.getuid())
            cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -sS "%s:25085/cache/schedconfig/%s.all.%s" > %s' % \
                  (sslCert, url, queuename, getExtension(alternative='pilot'), filename)
            _N = 3
            for _i in range(_N):
                tolog("Executing command: %s" % (cmd))
                try:
                    # output will be empty since we pipe into a file
                    ret, output = commands.getstatusoutput(cmd)
                except Exception, e:
                    tolog("!!WARNING!!1999!! Failed with curl command: %s" % str(e))
                    return -1, False
                else:
                    if ret == 0:
                        # read back the queuedata to verify its validity
                        hasQueuedata = self.verifyQueuedata(queuename, filename, _i, _N, url)
                        if hasQueuedata:
                            break
                    else:
                        tolog("!!WARNING!!1999!! curl command exited with code %d" % (ret))

        return 0, hasQueuedata

    def postProcessQueuedata(self, queuename, pshttpurl, thisSite, _jobrec, force_devpilot):
        """ Update queuedata fields if necessary """

        if 'pandadev' in pshttpurl or force_devpilot or thisSite.sitename == "CERNVM":
            ec = self.replaceQueuedataField("status", "online")

        _status = self.readpar('status')
        if _status != None and _status != "":
            if _status.upper() == "OFFLINE":
                tolog("Site %s is currently in %s mode - aborting pilot" % (thisSite.sitename, _status.lower()))
                return -1, None, None
            else:
                tolog("Site %s is currently in %s mode" % (thisSite.sitename, _status.lower()))

        # override pilot run options
        temp_jobrec = self.readpar('retry')
        if temp_jobrec.upper() == "TRUE":
            tolog("Job recovery turned on")
            _jobrec = True
        elif temp_jobrec.upper() == "FALSE":
            tolog("Job recovery turned off")
            _jobrec = False
        else:
            tolog("Job recovery variable (retry) not set")

        # evaluate the queuedata if needed
        self.evaluateQueuedata()

        # set pilot variables in case they have not been set by the pilot launcher
        thisSite = self.setUnsetVars(thisSite)

        return 0, thisSite, _jobrec

    def extractQueuedataOverwrite(self, jobParameters):
        """ Extract the queuedata overwrite key=value pairs from the job parameters """
        # The dictionary will be used to overwrite existing queuedata values
        # --overwriteQueuedata={key1=value1,key2=value2}

        queuedataUpdateDictionary = {}

        # define regexp pattern for the full overwrite command
        pattern = re.compile(r'\ \-\-overwriteQueuedata\=\{.+}')
        fullCommand = re.findall(pattern, jobParameters)

        if fullCommand[0] != "":
            # tolog("Extracted the full command from the job parameters: %s" % (fullCommand[0]))
            # e.g. fullCommand[0] = '--overwriteQueuedata={key1=value1 key2=value2}'

            # remove the overwriteQueuedata command from the job parameters
            jobParameters = jobParameters.replace(fullCommand[0], "")
            tolog("Removed the queuedata overwrite command from job parameters: %s" % (jobParameters))

            # define regexp pattern for the full overwrite command
            pattern = re.compile(r'\-\-overwriteQueuedata\=\{(.+)\}')

            # extract the key value pairs string from the already extracted full command
            keyValuePairs = re.findall(pattern, fullCommand[0])
            # e.g. keyValuePairs[0] = 'key1=value1,key2=value2'

            if keyValuePairs[0] != "":
                # tolog("Extracted the key value pairs from the full command: %s" % (keyValuePairs[0]))

                # remove any extra spaces if present
                keyValuePairs[0] = keyValuePairs[0].replace(" ", "")

                commaDictionary = {}
                if "\'" in keyValuePairs[0] or '\"' in keyValuePairs[0]:
                    tolog("Detected quotation marks in the job parameters: %s" % (keyValuePairs[0]))
                    # e.g. key1=value1,key2=value2,key3='value3,value4'

                    # handle quoted key-values separately

                    # replace any simple qoutation marks with double quotation marks to simplify the regexp below
                    keyValuePairs[0] = keyValuePairs[0].replace("\'",'\"')
                    keyValuePairs[0] = keyValuePairs[0].replace('\\"','\"') # in case double backslashes are present

                    # extract all values containing commas
                    commaList = re.findall('"([^"]*)"', keyValuePairs[0])

                    # create a dictionary with key-values using format "key_%d" = value, where %d is the id of the found value
                    # e.g. { key_1: valueX,valueY,valueZ, key_2: valueA,valueB }
                    # replace the original comma-containing value with "key_%d", and replace it later
                    commaDictionary = {}
                    counter = 0
                    for commaValue in commaList:
                        counter += 1
                        key = 'key_%d' % (counter)
                        commaDictionary[key] = commaValue
                        keyValuePairs[0] = keyValuePairs[0].replace('\"'+commaValue+'\"', key)

                    tolog("keyValuePairs=%s" % (keyValuePairs[0]))
                    tolog("commaDictionary=%s" % str(commaDictionary))

                # define the regexp pattern for the actual key=value pairs
                # full backslash escape, see (adjusted for python):
                # http://stackoverflow.com/questions/168171/regular-expression-for-parsing-name-value-pairs
                pattern = re.compile( r'((?:\\.|[^=,]+)*)=("(?:\\.|[^"\\]+)*"|(?:\\.|[^,"\\]+)*)' )

                # finally extract the key=value parameters
                keyValueList = re.findall(pattern, keyValuePairs[0])
                # e.g. keyValueList = [('key1', 'value1'), ('key2', 'value_2')]

                # put the extracted pairs in a proper dictionary
                if keyValueList != []:
                    tolog("Extracted the following key value pairs from job parameters: %s" % str(keyValueList))

                    for keyValueTuple in keyValueList:
                        key = keyValueTuple[0]
                        value = keyValueTuple[1]

                        if key != "":
                            # extract the value from the commaDictionary if it exists
                            if commaDictionary.has_key(value):
                                value = commaDictionary[value]

                            queuedataUpdateDictionary[key] = value
                        else:
                            tolog("!!WARNING!!1223!! Bad key detected in key value tuple: %s" % str(keyValueTuple))
                else:
                    tolog("!!WARNING!!1223!! Failed to extract the key value pair list from: %s" % (keyValuePairs[0]))
            else:
                tolog("!!WARNING!!1223!! Failed to extract the key value pairs from: %s" % (keyValuePairs[0]))
        else:
            tolog("!!WARNING!!1223!! Failed to extract the full queuedata overwrite command from jobParameters=%s" % (jobParameters))

        return jobParameters, queuedataUpdateDictionary

    def updateQueuedataFromJobParameters(self, jobParameters):
        """ Extract queuedata overwrite command from job parameters and update queuedata """

        tolog("called updateQueuedataFromJobParameters with: %s" % (jobParameters))

        transferType = ""

        # extract and remove queuedata overwrite command from job parameters
        if "--overwriteQueuedata" in jobParameters:
            tolog("Encountered an --overwriteQueuedata command in the job parameters")

            # (jobParameters might be updated [queuedata overwrite command should be removed if present], so they needs to be returned)
            jobParameters, queuedataUpdateDictionary = self.extractQueuedataOverwrite(jobParameters)

            # update queuedata
            if queuedataUpdateDictionary != {}:
                tolog("Queuedata will be updated from job parameters")
                for field in queuedataUpdateDictionary.keys():
                    if field.lower() == "transfertype":
                        # transferType is not a schedconfig field and must be handled separately
                        transferType = queuedataUpdateDictionary[field]
                    else:
                        ec = self.replaceQueuedataField(field, queuedataUpdateDictionary[field])
                        tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))

        # disable FAX if set in schedconfig
        if "--disableFAX" in jobParameters:
            tolog("Encountered a --disableFAX command in the job parameters")

            # remove string from jobParameters
            jobParameters = jobParameters.replace(" --disableFAX", "")

            # update queuedata if necessary
            if readpar("allowfax").lower() == "true":
                field = "allowfax"
                ec = self.replaceQueuedataField(field, "False")
                tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))

            else:
                tolog("No need to update queuedata for --disableFAX (allowfax is not set to True)")

        return jobParameters, transferType

    def setUnsetVars(self, thisSite):
        """ Set pilot variables in case they have not been set by the pilot launcher """
        # thisSite will be updated and returned

        tolog('Setting unset pilot variables using queuedata')
        if thisSite.appdir == "":
            scappdir = self.readpar('appdir')
            if os.environ.has_key("OSG_APP") and not os.environ.has_key("VO_ATLAS_SW_DIR"):
                if scappdir == "":
                    scappdir = os.environ["OSG_APP"]
                    if scappdir == "":
                        scappdir = "/usatlas/projects/OSG"
                        tolog('!!WARNING!!4000!! appdir not set in queuedata or $OSG_APP: using default %s' % (scappdir))
                    else:
                        tolog('!!WARNING!!4000!! appdir not set in queuedata - using $OSG_APP: %s' % (scappdir))
            tolog('appdir: %s' % (scappdir))
            thisSite.appdir = scappdir

        if thisSite.dq2url == "":
            _dq2url = self.readpar('dq2url')
            if _dq2url == "":
                tolog('Note: dq2url not set')
            else:
                tolog('dq2url: %s' % (_dq2url))
            thisSite.dq2url = _dq2url

        if thisSite.wntmpdir == "":
            _wntmpdir = self.readpar('wntmpdir')
            if _wntmpdir == "":
                _wntmpdir = thisSite.workdir
                tolog('!!WARNING!!4000!! wntmpdir not set - using site workdir: %s' % (_wntmpdir))
            tolog('wntmpdir: %s' % (_wntmpdir))
            thisSite.wntmpdir = _wntmpdir

        return thisSite

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """

        return False

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """

        return False

    def isTier3(self):
        """ Is the given site a Tier-3? """
        # Note: defined by DB

        return False

    def updateCopysetup(self, jobParameters, field, _copysetup, transferType=None, useCT=None, directIn=None, useFileStager=None):
        """
        Update copysetup in the presence of directIn and/or useFileStager in jobParameters
        Possible copysetup's are:
        "setup^oldPrefix^newPrefix^useFileStager^directIn"
        "setup^oldPrefix1,oldPrefix2^newPrefix1,newPrefix2^useFileStager^directIn"
        "setup^useFileStager^directIn"
        "setup"
        "None"
        """

        # get copysetup from queuedata
        copysetup = _copysetup

        tolog("updateCopysetup: copysetup=%s" % (copysetup))

        if "^" in copysetup:
            fields = copysetup.split("^")
            n = len(fields)
            # fields[0] = setup
            # fields[1] = useFileStager
            # fields[2] = directIn
            # or
            # fields[0] = setup
            # fields[1] = oldPrefix or oldPrefix1, oldPrefix2
            # fields[2] = newPrefix or newPrefix1, newPrefix2
            # fields[3] = useFileStager
            # fields[4] = directIn
            if n == 3 or n == 5:
                # update the relevant fields if necessary
                if useCT:
                    tolog("Copy tool is enforced, turning off any set remote I/O or file stager options")
                    fields[n-1] = "False"
                    fields[n-2] = "False"
                else:
                    # in case directIn or useFileStager were set by accessmode via jobParameters
                    if directIn or useFileStager:
                        if useFileStager and directIn:
                            fields[n-1] = "True" # directIn
                            fields[n-2] = "True" # useFileStager
                        elif directIn and not useFileStager:
                            fields[n-1] = "True"  # directIn
                            fields[n-2] = "False" # useFileStager
                        if transferType == "direct":
                            fields[n-1] = "True"  # directIn 
                            fields[n-2] = "False" # make sure file stager is turned off
                    # in case directIn or useFileStager were set in jobParameters or with transferType
                    else:
                        if fields[n-1].lower() == "false" and ("--directIn" in jobParameters or transferType == "direct"):
                            fields[n-1] = "True"  # directIn
                            fields[n-2] = "False" # useFileStager
                        if fields[n-2].lower() == "false" and "--useFileStager" in jobParameters:
                            fields[n-1] = "True" # directIn
                            fields[n-2] = "True" # useFileStager

                if "," in copysetup:
                    tolog("Multiple old/new prefices, turning off any set remote I/O or file stager options")
                    fields[n-1] = "False"
                    fields[n-2] = "False"

                copysetup = "^".join(fields)
            else:
                tolog("!!WARNING!!2990!! This site is not setup properly for using direct access/file stager: copysetup=%s" % (copysetup))
        else:
            if transferType == "direct" or directIn and not useFileStager:
                copysetup += "^False^True"
            elif useFileStager:
                copysetup += "^True^True"

        # undo remote I/O copysetup modification if requested
        if transferType == "undodirect" and "^" in copysetup:
            tolog("Requested re-modification of copysetup due to previous error")
            fields = copysetup.split("^")
            copysetup = fields[0]
            copysetup += "^False^False"

        # update copysetup if updated
        if copysetup != _copysetup:
            ec = self.replaceQueuedataField(field, copysetup)
            tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))
        else:
            tolog("copysetup does not need to be updated")

    def getAppdirs(self, appdir):
        """ Create a list of all appdirs in appdir """

        # appdir = '/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies'
        # -> ['/cvmfs/atlas.cern.ch/repo/sw', '/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies']

        appdirs = []
        if "|" in appdir:
            for a in appdir.split("|"):
                # remove any processingType
                if "^" in a:
                    a = a.split("^")[1]
                appdirs.append(a)
        else:
            appdirs.append(appdir)

        return appdirs

    def extractAppdir(self, appdir, processingType, homePackage):
        """ extract and (re-)confirm appdir from possibly encoded schedconfig.appdir """
        # e.g. for CERN:
        # processingType = unvalid
        # schedconfig.appdir = /afs/cern.ch/atlas/software/releases|release^/afs/cern.ch/atlas/software/releases|unvalid^/afs/cern.ch/atlas/software/unvalidated/caches
        # -> appdir = /afs/cern.ch/atlas/software/unvalidated/caches
        # if processingType does not match anything, use the default first entry (/afs/cern.ch/atlas/software/releases)
        # NOTE: this function can only be called after a job has been downloaded since processType is unknown until then

        ec = 0

        tolog("Extracting appdir (current value=%s)" % (appdir))

        # override processingType for analysis jobs that use nightlies
        if "rel_" in homePackage:
            tolog("Temporarily modifying processingType from %s to nightlies" % (processingType))
            processingType = "nightlies"

        _appdir = appdir
        if "|" in _appdir and "^" in _appdir:
            # extract appdir by matching with processingType
            appdir_split = _appdir.split("|")
            appdir_default = appdir_split[0]
            # loop over all possible appdirs
            sub_appdir = ""
            for i in range(1, len(appdir_split)):
                # extract the processingType and sub appdir
                sub_appdir_split = appdir_split[i].split("^")
                if processingType == sub_appdir_split[0]:
                    # found match
                    sub_appdir = sub_appdir_split[1]
                    break
            if sub_appdir == "":
                _appdir = appdir_default
                tolog("Using default appdir: %s (processingType = \'%s\')" % (_appdir, processingType))
            else:
                _appdir = sub_appdir
                tolog("Matched processingType %s to appdir %s" % (processingType, _appdir))
        else:
            # check for empty appdir's on LCG
            if _appdir == "":
                if os.environ.has_key("VO_ATLAS_SW_DIR"):
                    _appdir = os.environ["VO_ATLAS_SW_DIR"]
                    tolog("Set site.appdir to %s" % (_appdir))
            else:
                tolog("Got plain appdir: %s" % (_appdir))

        # should the software directory be verified? (at the beginning of the pilot)
        if self.verifySoftwareDirectory():
            # verify the existence of appdir
            if os.path.exists(_appdir):
                tolog("Software directory %s exists" % (_appdir))

                # force queuedata update
                _ec = self.replaceQueuedataField("appdir", _appdir)
                del _ec
            else:
                if _appdir != "":
                    tolog("!!FAILED!!1999!! Software directory does not exist: %s" % (_appdir))
                else:
                    tolog("!!FAILED!!1999!! Software directory (appdir) is not set")
                ec = self.__error.ERR_NOSOFTWAREDIR
        else:
            tolog("WARNING: Software directory will not be verified")

        return ec, _appdir

    def verifySoftwareDirectory(self):
        """ Should the software directory (schedconfig.appdir) be verified? """

        return True

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def allowAlternativeStageOut(self, flag=None):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?
        # Argument 'flag' can be used for special conditions

        return False

    def forceAlternativeStageOut(self, flag=None):
        """ Force stage-out to use alternative SE """
        # Argument 'flag' can be used for special conditions
        # See allowAlternativeStageOut()

        return False

    def getSSLCertificate(self):
        """ Return the path to the SSL certificate """

        if os.environ.has_key('X509_USER_PROXY'):
            sslCertificate = os.environ['X509_USER_PROXY']
        else:
            sslCertificate  = '/tmp/x509up_u%s' % str(os.getuid())

        return sslCertificate

    def getSSLCertificatesDirectory(self):
        """ Return the path to the SSL certificates directory """

        sslCertificatesDirectory = ''
        if os.environ.has_key('X509_CERT_DIR'):
            sslCertificatesDirectory = os.environ['X509_CERT_DIR']
        else:
            _dir = '/etc/grid-security/certificates'
            if os.path.exists(_dir):
                sslCertificatesDirectory = _dir
            else:
                tolog("!!WARNING!!2999!! $X509_CERT_DIR is not set and default location %s does not exist" % (_dir))

        return sslCertificatesDirectory

    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, **pdict):
        """ Return proper paths for the storage element used during stage-out """

        # Implement in sub-class

        return ""

    def getTier1Queue(self, cloud):
        """ Download the queuedata for the Tier-1 in the corresponding cloud and get the queue name """

        # Implement in sub-class
        # This method is used during stage-out to alternative [Tier-1] site when primary stage-out on a Tier-2 fails
        # See methods in ATLASSiteInformation

        return None

    def getCopySetup(self, stageIn=False):
        """Get the setup string from queuedata"""
        copysetup = ""
        if stageIn:
            copysetup = readpar('copysetupin')

        if copysetup == "":
            copysetup = readpar('copysetup')
            tolog("Using copysetup = %s" % (copysetup))
        else:
            tolog("Using copysetupin = %s" % (copysetup))

        if copysetup != '':
            # discard the directAccess info also stored in this variable
            _count = copysetup.count('^')
            if _count > 0:
                # make sure the DB entry doesn't start with directAccess info
                if _count == 2 or _count == 4 or _count == 5:
                    copysetup = copysetup.split('^')[0]
                else:
                    tolog('!!WARNING!!2999!! Could not figure out copysetup: %s' % (copysetup))
                    tolog('!!WARNING!!2999!! Resetting copysetup to an empty string')
                    copysetup = ''
            # Check copysetup actually exists!
            if copysetup != '' and os.access(copysetup, os.R_OK) == False:
                tolog('WARNING: copysetup %s is not readable - resetting to empty string' % (copysetup))
                copysetup = ''
            else:
                tolog("copysetup is: %s (file access verified)" % (copysetup))
        else:
            tolog("No copysetup found in queuedata")

        return copysetup

    def getCopyTool(self, stageIn=False):
        """
        Selects the correct copy tool (SiteMover id) given a site name
        'mode' is used to distinguish between different copy commands
        """

        copytoolname = ''
        if stageIn:
            copytoolname = readpar('copytoolin')

        if copytoolname == "":
            # not set, use same copytool for stage-in as for stage-out
            copytoolname = readpar('copytool')

        if copytoolname.find('^') > -1:
            copytoolname, pstage = copytoolname.split('^')

        if copytoolname == '':
            tolog("!!WARNING!!2999!! copytool not found (using default cp)")
            copytoolname = 'cp'

        copysetup = self.getCopySetup(stageIn)

        return (copytoolname, copysetup)

    def getCopyPrefix(self, stageIn=False):
        """Get Copy Prefix"""
        copyprefix = ""
        if stageIn:
            copyprefix = readpar('copyprefixin')

        if copyprefix == "":
            copyprefix = readpar('copyprefix')
            tolog("Using copyprefix = %s" % (copyprefix))
        else:
            tolog("Using copyprefixin = %s" % (copyprefix))

        return copyprefix

    def getCopyPrefixList(self, copyprefix):
        """ extract from and to info from copyprefix """

        pfrom = ""
        pto = ""

        if copyprefix != "":
            if copyprefix.count("^") == 1:
                pfrom, pto = copyprefix.split("^")
            elif copyprefix.startswith("^") or copyprefix.count("^") > 1:
                tolog("!!WARNING!!2988!! copyprefix has wrong format (not pfrom^pto): %s" % (copyprefix))
            else:
                pfrom = copyprefix

        if pfrom == "":
            pfrom = "dummy"
        else:
            if pfrom.endswith('/'):
                pfrom = pfrom[:-1]
                tolog("Cut away trailing / from %s (see copyprefix[in])" % (pfrom))
        if pto == "":
            pto = "dummy"
        
        if "," in pfrom:
            pfroms = pfrom.split(",")
        else:
            pfroms = [pfrom]
        if "," in pto:
            ptos = pto.split(",")
        else:
            ptos = [pto]

        return pfroms, ptos

    def getCopyPrefixPath(self, path, stageIn=False):
        """convert path to copy prefix path """
        # figure out which copyprefix to use (use the PFN to figure out where the file is and then use the appropriate copyprefix)
        # e.g. copyprefix=srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/.. use copyprefix root://eosatlas.cern.ch/ to build the TURL src_loc_pfn
        # full example:
        # Using copyprefixin = srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1
        # TURL=root://eosatlas.cern.ch//eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1

        copyprefix = self.getCopyPrefix(stageIn=stageIn)
        if copyprefix == "":
            errorLog = "Empty copyprefix, cannot continue"
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            return path

        # handle copyprefix lists
        pfroms, ptos = self.getCopyPrefixList(copyprefix)
        if len(pfroms) != len(ptos):
            errorLog = "Copyprefix lists not of equal length: %s, %s" % (str(pfroms), str(ptos))
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            return path

        if "SFN" in path:
            local_path = path.split('SFN=')[1]
        else:
            local_path = '/' + path.split('/', 3)[3] # 0:method, 2:host+port, 3:abs-path

        ret_path = path
        for (pfrom, pto) in map(None, pfroms, ptos):
            if (pfrom != "" and pfrom != None and pfrom != "dummy") and (pto != "" and pto != None and pto != "dummy"):
                if path[:len(pfrom)] == pfrom or path[:len(pto)] == pto:
                    ret_path = pto + local_path
                    ret_path = ret_path.replace('///','//')
                    break

        return ret_path

    def getCopyFileAccessInfo(self, stageIn=True):
        """ return a tuple with all info about how the input files should be accessed """

        # default values
        oldPrefix = None
        newPrefix = None
        useFileStager = None
        directIn = None

        # move input files from local DDM area to workdir if needed using a copy tool (can be turned off below in case of remote I/O)
        useCT = True

        dInfo = None
        if stageIn:
            # remove all input root files for analysis job for xrootd sites
            # (they will be read by pAthena directly from xrootd)
            # create the direct access dictionary
            dInfo = getDirectAccessDic(readpar('copysetupin'))
        # if copysetupin did not contain direct access info, try the copysetup instead
        if not dInfo:
            dInfo = getDirectAccessDic(readpar('copysetup'))

        # check if we should use the copytool
        if dInfo:
            if not dInfo['useCopyTool']:
                useCT = False
            oldPrefix = dInfo['oldPrefix']
            newPrefix = dInfo['newPrefix']
            useFileStager = dInfo['useFileStager']
            directIn = dInfo['directIn']
        if useCT:
            tolog("Copy tool will be used for stage-in")
        else:
            if useFileStager:
                tolog("File stager mode: Copy tool will not be used for stage-in of root files")
            else:
                tolog("Direct access mode: Copy tool will not be used for stage-in of root files")
                if oldPrefix == "" and newPrefix == "":
                    tolog("Will attempt to create a TURL based PFC")

        return useCT, oldPrefix, newPrefix, useFileStager, directIn

    def getDirectInAccessMode(self, prodDBlockToken, isRootFileName):
        """Get Direct Access mode"""
        directIn = False
        useFileStager = False
        transfer_mode = None

        useCT, oldPrefix, newPrefix, useFileStager, directIn = self.getCopyFileAccessInfo(stageIn=True)

        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                #updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                transfer_mode = "copy_to_scratch"
            else:
                # determine if the file is a root file according to its name
                rootFile = isRootFileName

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    #updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                    transfer_mode = "copy_to_scratch"
                elif rootFile:
                    tolog("Found root file according to file name (will not be transferred in direct reading mode)")
                    if useFileStager:
                        #updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                        transfer_mode = "file_stager"
                    else:
                        #updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                        transfer_mode = "remote_io"
                else:
                    tolog("Normal file transfer")
        else:
            tolog("not directIn")

        return directIn, transfer_mode

    # Optional
    def getFileSystemRootPath(self):
        """ Return the root path of the local file system """

        # Can e.g. be used to return "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
        # where it usually is (e.g. on an HPC). See example implementation in ATLASSiteInformation
        # E.g. site movers that have setup paths on CVMFS use this method to locate the setup script. See e.g. objectstoreSiteMover

        return ""

    # Required if a local ROOT setup is necessary from e.g. a site mover (FAXSiteMover, objectstoreSiteMover, ..)
    def getLocalROOTSetup(self):
        """ Prepare the local ROOT setup script """
        # See example implementation in ATLASExperiment
        # See example usage in objectstoreSiteMover

        return ""

    # Required if a local EMI setup is necessary (used in GFAL2iteMover)
    def getLocalEMISetup(self):
        """ Return the path for the local EMI setup """

        return ""

    # Required if use S3 objectstore
    def getSecurityKey(self, privateKeyName, publicKeyName):
        """ Return the key pair """
        
        return {"publicKey": None, "privateKey": None}

    # Required if use S3 objectstore
    def setSecurityKey(self, privateKeyName, privateKey, publicKeyName, publicKey):
        """ Return the key pair """

        keyName=privateKeyName + "_" + publicKeyName
        self.__securityKeys[keyName] = {"publicKey": publicKey, "privateKey": privateKey}

        return {"publicKey": publicKey, "privateKey": privateKey}

if __name__ == "__main__":
    from SiteInformation import SiteInformation
    import os
    os.environ['PilotHomeDir'] = os.getcwd()
    s1 = SiteInformation()
    print "copytool=",s1.readpar('copytool')
