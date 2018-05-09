# Class definition:
#   SiteInformation
#   This class is responsible for downloading, verifying and manipulating queuedata
#   Note: not compatible with Singleton Design Pattern due to the subclassing

import os
import re
import commands
import random
import time
import urlparse
import urllib2
from datetime import datetime, timedelta
from pUtil import tolog, replace, getDirectAccessDic
from pUtil import getExperiment as getExperimentObject
from FileHandling import getExtension, readJSON, writeJSON, getJSONDictionary, getDirectAccess
from PilotErrors import PilotErrors

try:
    import json
except ImportError:
    import simplejson as json


class SiteInformation(object):
    """

    Should this class ask the Experiment class which the current experiment is?
    Not efficient if every readpar() calls some Experiment method unless Experiment is a singleton class as well

    """

    # private data members
    __experiment = "generic"
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object
    __securityKeys = {}                    # S3 secret keys (for object store)
    __queuename = ""                       # Name of the queue
    __benchmarks = None                    # The benchmarks dictionary
    ddmconf = {}                           # DDMEndpoints data dict with protocols definition
    ddmblacklistingconf = {}               # DDMBlacklisting Endpoints, dict

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def getQueueName(self):
        """ Getter for __queuename """

        return self.__queuename

    def setQueueName(self, queuename):
        """ Setter for __queuename """

        self.__queuename = queuename

    def readpar(self, par, alt=False, version=0, queuename=None):
        """ Read parameter variable from queuedata """

        value = ""

        # tolog("par: %s, alt: %s, version:%s, queuename: %s" % (par, alt, version, queuename))
        # Should we should use the new queuedata JSON version?
        if version == 1:
            return self.getField(par, queuename=queuename)

        # Use olf queuedata version
        fileName = self.getQueuedataFileName(alt=alt)
        try:
            fh = open(fileName)
        except:
            try:
                # Try without the path
                fh = open(os.path.basename(fileName))
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not read queuedata file: %s" % str(e))
                fh = None
        if fh:
            #
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

    def getQueuedataFileName(self, useExtension=None, check=True, alt=False, version=0, queuename=None, os_bucket_id=-1):
        """ Define the queuedata filename """
        # alt: alternative extension
        # version: 0 (default, old queuedata version), 1 (new AGIS JSON format)

        # use a forced extension if necessary
        if useExtension:
            extension = useExtension
        else:
            extension = getExtension(alternative='dat')

        # prepend alt. for alternative stage-out site queuedata
        if alt:
            extension = "alt." + extension

        if version == 1:
            if queuename and False: # skip this for now (in case of alt stage-out to OS, e.g. JobLog won't know which is the queuename)
                filename = "queuedata-%s.%s" % (queuename, extension)
            else:
                if os_bucket_id != -1:
                    filename = "new_queuedata_%d.%s" % (os_bucket_id, extension)
                else:
                    filename = "new_queuedata.%s" % (extension)
        else:
            filename = "queuedata.%s" % (extension)
        path = os.path.join(os.environ['PilotHomeDir'], filename)

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
            exp = getExperimentObject(self.__experiment)  # this is a bug and should not work as expected if getQueuedata() is called from child class (e.g. from ATLASSiteInformation) since self.__experiment is PRIVATE in this scope! (anisyonk)
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

    def removeContainerImage(self, jobParameters):
        """ Remove the container image option from the job parameters """
        # Note : --containerimage is a Pilot 2 feature and full support for it will only be implemented in Pilot 2
        # This function only removes the string from job parameters

        # define regexp pattern for the full container image option
        pattern = re.compile(r'(\ \-\-containerimage\=?\s?[\S]+)')
        fullCommand = re.findall(pattern, jobParameters)

        if fullCommand and fullCommand[0] != "":
            # remove the overwriteQueuedata command from the job parameters
            jobParameters = jobParameters.replace(fullCommand[0], "")
            tolog("Removed the --containerimage XYZ option from job parameters: %s" % (jobParameters))

        return jobParameters

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

    def get_key_value_for_queuedata(self, parameter):
        m = parameter.split('=', 1)
        key = m[0]
        value = True
        if len(m) > 1:
            try:
                value = json.loads(m[1])
            except ValueError:
                value = m[1]

        return key, value

    def fixQueuedataFromParams(self, key, value):
        if key.lower() == 'transfertype':
            self.transferTypeFix = value
        else:
            tolog("Overwriting queuedata parameter \"%s\" to %s" % (key, json.dumps(value)))
            self.replaceQueuedataField(key, value)
            tolog("Updated %s in queuedata: %s (read back from file)" % (key, self.readpar(key)))

    def updateQueuedataFromJobParameters(self, jobParameters):
        """ Extract queuedata overwrite command from job parameters and update queuedata """

        tolog("called updateQueuedataFromJobParameters with: %s" % (jobParameters))

        self.transferTypeFix = ""

        # remove any --containerimage XYZ option since that will only be supported by Pilot 2
        if "--containerimage" in jobParameters:
            jobParameters = self.removeContainerImage(jobParameters)

        # extract and remove queuedata overwrite command from job parameters (the old way)
        if "--overwriteQueuedata={" in jobParameters:
            tolog("Old --overwriteQueuedata parameter type, please check your HammerCloud configs!", label='WARNING')
            tolog("Encountered an --overwriteQueuedata command in the job parameters")

            # (jobParameters might be updated [queuedata overwrite command should be removed if present], so they needs to be returned)
            jobParameters, queuedataUpdateDictionary = self.extractQueuedataOverwrite(jobParameters)

            # update queuedata
            if queuedataUpdateDictionary != {}:
                tolog("Queuedata will be updated from job parameters")
                for field in queuedataUpdateDictionary.keys():
                    self.fixQueuedataFromParams(field, queuedataUpdateDictionary[field])

        import shlex, pipes
        """
        Splits command parameters and extracts queuedata modifications if present.

        Queuedata modification principles:
            Extraction is done from one of the parameter strings:
              1) ... --overwriteQueuedata key1=val1[ key2=val2[ ...]] -- ...
              2) ... --overwriteQueuedata key1=val1[ key2=val2[ ...]] -...
              3) ... --overwriteQueuedata key1=val1[ key2=val2[ ...]]
              3) ... --overwriteQueuedata '{"key1":"any valid JSON","key2":"..."}' ...

            Extraction starts from --overwriteQueuedata, then goes number of key=value pairs.
            Each value in pairs is either valid JSON or simple string.

            Example:
                'parameter list --overwriteQueuedata k1 k2=val k3=\'{"a":"b"}\' k4=false k5=23 -- will lead to'
                modification in queuedata:
                {
                    k1: True
                    k2: "val"
                    k3: {a: "b"}
                    k4: False
                    k5: (int)23
                }
                and parameter list:
                'parameter list will lead to'

            The end of the list is marked with:
              1) "--" (two dashes exactly), which is also stripped from parameter list;
              2) some parameter starting with "-" and is not just two dashes;
              3) EOL.
              4) If the arguments are presented as a solid valid JSON.

            If the next parameter (case 2) is --overwriteQueuedata, it is parsed all the same.
        """
        try:
            job_args = shlex.split(jobParameters)
        except ValueError as e:
            tolog("Unparsable job arguments. Shlex exception: " + e.message, label='WARNING')
            return jobParameters, self.transferTypeFix

        overwriting = False
        new_args = []

        if not hasattr(self, 'xrootd_test'):
            self.xrootd_test = False

        if not hasattr(self, 'ALRB_asetupVersion'):
            self.ALRB_asetupVersion = None

        for arg in job_args:
            if overwriting:
                if arg.startswith('-'):
                    overwriting = False
                    if arg == '--':
                        continue  # variant to end the parameter list
                elif arg.startswith('{'):  # the argument is a piece of JSON
                    array = json.loads(arg)
                    for k in array.keys():
                        self.fixQueuedataFromParams(k, array[k])
                    overwriting = False
                    continue
                else:
                    key, value = self.get_key_value_for_queuedata(arg)
                    self.fixQueuedataFromParams(key, value)

            if not overwriting:
                if arg == '--overwriteQueuedata':
                    tolog("overwriteQueuedata found")
                    overwriting = True
                elif arg == '--disableFAX':
                    tolog("Encountered a --disableFAX command in the job parameters")

                    # update queuedata if necessary
                    if self.readpar("allowfax").lower() == "true":
                        field = "allowfax"
                        ec = self.replaceQueuedataField(field, "False")
                        tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))

                    else:
                        tolog("No need to update queuedata for --disableFAX (allowfax is not set to True)")
                elif arg == '--useTestASetup':
                    os.environ['ALRB_asetupVersion'] = 'testing'
                    self.ALRB_asetupVersion = 'testing'
                elif arg == '--useTestXRootD':
                    os.environ['TestXRootD'] = 'True'
                    self.xrootd_test = True
                else:
                    new_args.append(arg)

        jobParameters = " ".join(pipes.quote(x) for x in new_args)
        tolog("Prepared parameters: %s" % jobParameters)
        return jobParameters, self.transferTypeFix

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

    def getExperimentObject(self): # quick stub: to be properly implemented later

        return getExperimentObject(self.getExperiment())


    def allowAlternativeStageOut(self, **pdict):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?
        # Argument 'pdict' can be used for special conditions

        return False

    def forceAlternativeStageOut(self, **pdict):
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
            copysetup = self.readpar('copysetupin')

        if copysetup == "":
            copysetup = self.readpar('copysetup')
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
            copytoolname = self.readpar('copytoolin')

        if copytoolname == "":
            # not set, use same copytool for stage-in as for stage-out
            copytoolname = self.readpar('copytool')

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
            copyprefix = self.readpar('copyprefixin')

        if copyprefix == "":
            copyprefix = self.readpar('copyprefix')
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

    def getCopyPrefixPathNew(self, path, stageIn=False):
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

        ret_path = path
        for (pfrom, pto) in map(None, pfroms, ptos):
            ret_path = re.sub(pfrom, pto, ret_path)
            ret_path = ret_path.replace('///','//')

        return ret_path

    def getFileAccessInfo(self, transferType, stageIn=True):
        """ return a tuple with all info about how the input files should be accessed """

        # default values
        oldPrefix = None
        newPrefix = None

        # move input files from local DDM area to workdir if needed using a copy tool (can be turned off below in case of remote I/O)
        useCT = True

        dInfo = None
        if stageIn:
            # remove all input root files for analysis job for xrootd sites
            # (they will be read by pAthena directly from xrootd)
            # create the direct access dictionary
            dInfo = getDirectAccessDic(self.readpar('copysetupin'))
        # if copysetupin did not contain direct access info, try the copysetup instead
        if not dInfo:
            dInfo = getDirectAccessDic(self.readpar('copysetup'))

        # check if we should use the copytool
        if dInfo:
            if not dInfo['useCopyTool']:
                useCT = False
            oldPrefix = dInfo['oldPrefix']
            newPrefix = dInfo['newPrefix']
        if transferType == 'direct' or (transferType == 'fax' and self.readpar('direct_access_wan').lower() == 'true'):
            useCT = False
            oldPrefix = ""
            newPrefix = ""
        elif useCT:
            tolog("Copy tool will be used for stage-in")
        else:
            tolog("Direct access mode: Copy tool will not be used for stage-in of root files")
            if oldPrefix == "" and newPrefix == "":
                tolog("Will attempt to create a TURL based PFC")

        return useCT, oldPrefix, newPrefix

    def getDirectInAccessMode(self, prodDBlockToken, isRootFileName, transferType):
        """ Get Direct Access mode """
        directIn = False
        useFileStager = False
        transfer_mode = None

        # get the file access info
        directIn, directInType = getDirectAccess()
        useCT, oldPrefix, newPrefix = self.getFileAccessInfo(transferType)

        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                transfer_mode = "copy_to_scratch"
            else:
                # determine if the file is a root file according to its name
                rootFile = isRootFileName

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    transfer_mode = "copy_to_scratch"
                elif rootFile:
                    tolog("Found root file according to file name (will not be transferred in direct reading mode)")
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

    # Optional
    def getFullQueuedataFilePath(self):
        """ Location of full schedconfig info """

        # E.g. the full queuedata can be located on cvmfs. Queuedata is assumed to be in json format.
        # The full queuedata should contain a complete list of schedconfig info for all sites
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

    def getObjectstoresInfo(self):
        """ Get the objectstores list from the normal queuedata """
        # Note: this corresponds to the default OS info for a given queue

        # Input:  None
        # Output: List containing all OS dictionaries (one per bucket) for the current queue

        objectstores = None
        _objectstores = self.readpar('objectstores')
        if _objectstores != "":
            # Try to parse the field (should be a JSON parsable object)
            from json import loads
            try:
                objectstores = loads(_objectstores)
            except Exception, e:
                tolog("!!WARNING!!3444!! JSON loads failed to parse objectstores field: %s, %s" % (e, _objectstores))
            else:
                from FileHandling import convert
                objectstores = convert(objectstores)
                if type(objectstores) != list:
                    tolog("!!WARNING!!3445!! Not a list: Failed to parse objectstores field: %s (type=%s)" % (str(objectstores), type(objectstores)))
                    objectstores = None
        else:
            pass
        return objectstores

    def getObjectstoresField(self, os_field_name, os_bucket_name='logs', os_bucket_id=-1, objectstoresInfo=[]):
        """ Get the objectstore field value for the given bucket (from normal queuedata) """

        # Input:  os_field_name (os_name, os_access_key, ..)
        #         bucket name (logs, eventservice, http)
        #         bucket id (optional; if set, bucket name will be ignored)
        #         objectstoresInfo (optional); if set, the objectstores field will not be re-parsed from file (useful during alt OS stage-outs)
        # Output: OS field value for the given bucket

        os_field_value = ""
        if objectstoresInfo != []:
            d = objectstoresInfo
        else:
            d = self.getObjectstoresInfo()

        if d != []:
            if os_bucket_id != -1:
                name = 'os_bucket_id'
                value = os_bucket_id
            else:
                name = 'os_bucket_name'
                value = os_bucket_name

            # Loop over the OS entries
            for l in d:
                if l.has_key(name) and l.has_key(os_field_name):
                    if value == l[name]:
                        os_field_value = l[os_field_name]
                        break
                    else:
                        pass
                else:
                    tolog("!!WARNING!!4554!! Either field name %s or %s is missing in OS info list" % (name, os_field_name))
        else:
            tolog("!!WARNING!!4555!! Cannot resolve os_field_value (empty OS info list)")

        return os_field_value

    def getObjectstoreDDMEndpointFromBucketID(self, os_bucket_id):
        """ Get the objectstore ddm endpoint corresponding to the bucket id (from the full OS queuedata) """

        # Input:  os_bucket_id (integer)
        # Output: ddmendpoint

        ddmendpoint = ""

        # Use the full objectstore queuedata file
        filename = self.getObjectstoreFilename()
        if not os.path.exists(filename):
            tolog("Cannot find %s - will download it" % (filename))
            self.getObjectstoreInfoFile()

        dictionary = readJSON(filename)
        if dictionary != {}:
            for os_ddmendpoint in dictionary.keys():
                # Check the os_bucket_id from the json
                resource = dictionary[os_ddmendpoint].get('resource', {})
                if resource != {}:
                    bucket_id = resource.get('bucket_id', -999)
                    if bucket_id == os_bucket_id:
                        # Found the right ddmendpoint
                        tolog("os_bucket_id=%d located at ddmendpoint=%s" % (os_bucket_id, os_ddmendpoint))
                        ddmendpoint = os_ddmendpoint
                        break
                    else:
                        # Go to the next ddmendpoint, wrong bucket_id
                        continue
                else:
                    # Ignore this ddmendpoint
                    continue
        else:
            tolog("!!WARNING!!5670!! Empty objectstore dictionary from file (cannot resolve ddm endpoint from bucket id)")

        return ddmendpoint

    def getObjectstoreEndpointID(self, ddmendpoint='BNL-ATLAS_LOGS', label='r', protocol='s3'):
        """ Get id for endpoint from arprotocols (from the full OS queuedata file) """

        # Input:  ddmendpoint (e.g. 'BNL-ATLAS_LOGS')
        #         label ('r', 'w', 'd')
        #         protocol="s3", "s3+rucio", "http"
        # Output: endpoint id (integer)

        endpoint_id = -1

        # Use the reduced objectstore queuedata file (i.e. for the given ddmendpoint)
        filename = self.getObjectstoreFilename(name=ddmendpoint)
        if not os.path.exists(filename):
            tolog("Cannot find %s - will download it" % (filename))
            self.getObjectstoreInfoFile()

        # Extract the endpoint id from the dictionary
        ddmendpoint_dictionary = readJSON(filename)
        if ddmendpoint_dictionary != {}:
            arprotocols = ddmendpoint_dictionary.get('arprotocols', {})
            if arprotocols != {}:
                arprotocols_label = arprotocols.get(label, [])
                if arprotocols_label != []:
                    # There can be a list for different protocols
                    for entry in arprotocols_label:
                        _endpoint = entry.get('endpoint', '')
                        if _endpoint.startswith(protocol):
                            endpoint_id = entry.get('id', -1)
                            break
                    if endpoint_id == -1:
                        tolog("!!WARNING!!4553!! Failed to extract endpoint id from arprotocols dictionary")
                else:
                    tolog("!!WARNING!!4553!! Empty arprotocols label list - cannot extract endpoint id")
            else:
                tolog("!!WARNING!!4552!! Empty arprotocols dictionary - cannot extract endpoint id")
        else:
            tolog("!!WARNING!!4550!! Empty objectstore ddm endpoint dictionary - cannot extract endpoint id")

        return endpoint_id

    def getObjectstoreKeyInfo(self, endpoint_id, ddmendpoint='BNL-ATLAS_LOGS'):
        """ Use endpoint id from arprotocols to get key info from rprotocols (from the full OS queuedata file) """

        # Input:  endpoint_id (integer), ddmendpoint (e.g. 'BNL-ATLAS_LOGS')
        # Output: access_key (e.g. 'BNL_ObjectStoreKey.pub'), secret_key (e.g. 'BNL_ObjectStoreKey'), is_secure (boolean)
        access_key = ""
        secret_key = ""
        is_secure = False

        # Use the reduced objectstore queuedata file (i.e. for the given ddmendpoint)
        filename = self.getObjectstoreFilename(name=ddmendpoint)
        if not os.path.exists(filename):
            tolog("Cannot find %s - will download it" % (filename))
            self.getObjectstoreInfoFile()

        # Extract the key info from the dictionary
        ddmendpoint_dictionary = readJSON(filename)
        if ddmendpoint_dictionary != {}:
            rprotocols = ddmendpoint_dictionary.get('rprotocols', {})
            if rprotocols != {}:
                rprotocols_id = rprotocols.get(str(endpoint_id), '')
                if rprotocols_id != {}:
                    settings = rprotocols_id.get('settings', {})
                    if settings != {}:
                        access_key = settings.get('access_key', '')
                        secret_key = settings.get('secret_key', '')
                        is_secure = settings.get('is_secure', False)
                    else:
                        tolog("!!WARNING!!4553!! Empty rprotocols id settings dictionary - cannot extract key info")
                else:
                    tolog("!!WARNING!!4552!! Empty rprotocols id dictionary - cannot extract key info")
            else:
                tolog("!!WARNING!!4551!! Empty rprotocols dictionary - cannot extract key info")
        else:
            tolog("!!WARNING!!4550!! Empty objectstore ddm endpoint dictionary - cannot extract key info")

        return access_key, secret_key, is_secure

    def getObjectstoreDDMEndpoint(self, os_bucket_name='logs', os_bucket_id=-1, objectstoresInfo=[]):
        """ Return the default OS DDM endpoint corresponding to the given bucket (from the normal queuedata file) """
        # Note: value is read from queuedata and is thus the default value

        # Input:  os_bucket_name (logs, eventservice, http)
        #         os_bucket_id (optional)
        #         objectstoresInfo (optional); if set, the objectstores field will not be re-parsed from file
        # Output: DDM endpoint, e.g. CERN-PROD_LOGS

        ddmendpoint = ""

        # Read the objectstores field from file
        if objectstoresInfo == []:
            objectstoresInfo = self.getObjectstoresInfo()

        tolog("objectstoresInfo: %s" % objectstoresInfo)
        if objectstoresInfo:
            ddmendpoint = self.getObjectstoresField('ddmendpoint', os_bucket_name=os_bucket_name, os_bucket_id=os_bucket_id, objectstoresInfo=objectstoresInfo)
            if ddmendpoint == "":
                tolog("!!WARNING!!4555!! Encountered an unset ddmendpoint in AGIS OS info")

        return ddmendpoint

    def findAllObjectstoreDdmendpoints(self, os_bucket_name):
        """ Find all Object Store ddm endpoints corresponding to a particular bucket name from the full OS queuedata file """

        # Input:  os_bucket_name, e.g. 'logs', 'eventservice' - only return Object Stores that have the logs bucket defined
        # Output: list of ddmendpoints corresponding to the input bucket name (same as the 'resource' 'name' field in the json)
        # Note: never return excluded ddmendpoints (AMAZON)
        ddmendpoints = []

        # Do not include the following as alternative ddm endpoints
        exclusion_list = ['AMAZON']

        # Load the dictionary
        dictionary = self.getFullQueuedataDictionary()
        if dictionary != {}:

            for ddmendpoint in dictionary.keys():  # keys are DDM endpoints
                resource = dictionary[ddmendpoint].get('resource', {})
                if resource:
                    name = resource.get('name', '')
                    if name == os_bucket_name:
                        found_not_valid = False
                        for not_valid in exclusion_list:
                            if not_valid in ddmendpoint:
                                found_not_valid = True
                        if not found_not_valid:
                            ddmendpoints.append(ddmendpoint)

        return ddmendpoints

    def getAlternativeObjectstoreDDMEndpoint(self, default_ddmendpoint, os_bucket_name):
        """ Get an alternative objectstore ddmendpoint corresponding to the input bucket name """

        # Input:  ddmendpoint for the default objectstore (as listed in the 'normal' queuedata)
        #         os_bucket_name, e.g. 'logs', 'eventservice'
        # Output: ddmendpoint for an alternative objectstore corresponding to the input bucket name (as listed in the OS queuedata)

        alternative_ddmendpoint = ""

        # Use the full objectstore queuedata file (containing all ddm endpoints)
        filename = self.getObjectstoreFilename()
        if not os.path.exists(filename):
            tolog("Cannot find %s - will download it" % (filename))
            self.getObjectstoreInfoFile()

        # Get all ddm endpoints corresponding to the input bucket name
        all_ddmendpoints = self.findAllObjectstoreDdmendpoints(os_bucket_name)

        # Get an alternative ddm endpoint from the list
        # (All objectstores fail over to BNL OS, BNL fails over to CERN)

        for ddmendpoint in all_ddmendpoints:
            if not "BNL" in default_ddmendpoint:
                if "BNL" in ddmendpoint:
                    alternative_ddmendpoint = ddmendpoint
                    break
                else:
                    continue
            else:
                if "CERN" in ddmendpoint:
                    alternative_ddmendpoint = ddmendpoint
                    break
                else:
                    continue

        return alternative_ddmendpoint

    def getObjectstorePathFromARProtocols(self, dictionary_ddmendpoint, label, protocol, os_bucket_id):
        """ Get the os_path from the arprotocol dictinoary """

        # Input: dictionary_ddmendpoint (for given ddmendpoint), label (d,r,w), protocol (s3, s3+rucio, http)
        # Output: os_path

        os_path = ""

        arprotocols = dictionary_ddmendpoint['arprotocols']
        if arprotocols != {}:
            if arprotocols.has_key(label):
                d = arprotocols[label]
            else:
                tolog("!!WARNING!!2323!! No such arprotocols label: %s (using default label (\'r\'))" % (label))
                label = "r"
                d = arprotocols[label]
            if d != []:
                endpoint = ""
                path = ""
                for entry in d:
                    _endpoint = entry.get('endpoint', '')
                    _path = entry.get('path', '')
                    if _endpoint != "" and _path != "":
                        if _endpoint.startswith(protocol):
                            endpoint = _endpoint
                            path = _path
                            break
                if endpoint == "" or path == "":
                    tolog("!!WARNING!!2325!! Empty endpoint/path in %s (for protocol %s)" % (str(d), protocol))
                else:
                    os_path = endpoint + path

            else:
                tolog("!!WARNING!!2328!! Empty arprotocols dictionary for os_bucket_id=%d - Cannot locate path" % (
                os_bucket_id))

        return os_path

    def getObjectstorePath(self, ddmendpoint="", os_bucket_id=-99, label="r", protocol="s3"):
        """ Return the path to the objectstore (using the full OS queuedata file) """

        # Input: either ddmendpoint or os_bucket_id ("id" in arprotocols "r"/"w" dictionary) must be set
        #        label="w" means get an OS path for writing ("w" dictionary), "r" is used for reading ("r" dictionary) ["d" means delete]
        #        protocol="s3", "s3+rucio", "http"
        # Output: os_path, full path to OS bucket, e.g. 's3://s3.echo.stfc.ac.uk:80//atlas_logs'

        os_path = ""

        try:
            # First collect all the queuedata
            if not self.getObjectstoreInfoFile():
                tolog("!!WARNING!!3333!! No access to AGIS OS info file, forced to abort")
                return os_path
        except:
            rand = random.randrange(20, 50)
            tolog("!!WARNING!!3333!! Failed to get AGIS OS info file, sleep %s seconds" % rand)
            time.sleep(rand)
            if not self.getObjectstoreInfoFile():
                tolog("!!WARNING!!3333!! No access to AGIS OS info file, forced to abort")
                return os_path

        # If ddmendpoint is not specified, os_bucket_id must be set
        # In that case, the full objectstore info file will be searched for the corresponding os_bucket_id

        filename = self.getObjectstoreFilename()
        if not os.path.exists(filename):
            tolog("Cannot find %s - will download it" % (filename))
            self.getObjectstoreInfoFile()

        dictionary = readJSON(filename)
        if dictionary != {}:
            # Simplest case first, known ddmendpoint
            if ddmendpoint != "":

                dictionary_ddmendpoint = dictionary.get(ddmendpoint, {})
                if dictionary_ddmendpoint != {}:
                    os_path = self.getObjectstorePathFromARProtocols(dictionary[ddmendpoint], label, protocol, os_bucket_id)
                else:
                    tolog("!!WARNING!!5656!! No such ddm endpoint: %s (cannot resolve path)" % (ddmendpoint))

            elif os_bucket_id != -1:
                # ddmendpoint not known, rely on the os_bucket_id
                for ddmendpoint in dictionary.keys():

                    # Check the os_bucket_id from the json
                    resource = dictionary[ddmendpoint].get('resource', {})
                    if resource != {}:
                        bucket_id = resource.get('bucket_id', -999)
                        if bucket_id == os_bucket_id:
                            # Found the right ddmendpoint
                            tolog("os_bucket_id=%d located at ddmendpoint=%s" % (os_bucket_id, ddmendpoint))
                            os_path = self.getObjectstorePathFromARProtocols(dictionary[ddmendpoint], label, protocol,
                                                                        os_bucket_id)
                        else:
                            # Go to the next ddmendpoint, wrong bucket_id
                            continue
                    else:
                        # Ignore this ddmendpoint
                        continue
            else:
                tolog("!!WARNING!!2326!! Either ddmendpoint or os_bucket_id must be set! Cannot resolve OS path")

        return os_path

    def getObjectstoreFilename(self, name=""):
        """ Return the name of the objectstore info file """

        # Input:  name (e.g. the name of the ddm endpoint or the objectstore itself - this is used for caching
        #               potentially large files - see example implementation in ATLASSiteInformation)
        # Output: filename

        return ""

    def getObjectstoreBucketID(self, ddm_endpoint):
        """ Return the os_bucket_id corresponding to the given ddm endpoint (from the objectstore queuedata file) """

        # Input:  ddm_endpoint
        # Output: os_bucket_id

        os_bucket_id = -1

        # First collect all the queuedata
        if not self.getObjectstoreInfoFile():
            tolog("!!WARNING!!3333!! No access to AGIS OS info file, forced to abort")
            return os_bucket_id

        filename = self.getObjectstoreFilename(name=ddm_endpoint)
        dictionary = readJSON(filename)
        if dictionary != {}:
            resource = dictionary.get('resource', {})
            if resource != {}:
                os_bucket_id = resource.get('bucket_id', -1)
                if os_bucket_id == -1:
                    tolog("!!WARNING!!5652!! Objectstore bucket id not set (-1)")
            else:
                tolog("!!WARNING!!5651!! Resource dictionary not present in info file, cannot resolve bucket_id for objectstore")
        else:
            tolog("!!WARNING!!5650!! Failed to read objectstore dictionary from file: %s" % (filename))

        return os_bucket_id

    def getObjectstoreInfoFile(self):
        try:
            return self.getObjectstoreInfoFileReal()
        except:
            rand = random.randrange(5, 20)
            tolog("!!WARNING!!3333!! Failed to get AGIS OS info file, sleep %s seconds" % rand)
            time.sleep(rand)
            return self.getObjectstoreInfoFileReal()

    def getObjectstoreInfoFileReal(self):
        """ Download the Objectstore info primarily from CVMFS and secondarily from the AGIS server """

        status = False

        filename = self.getObjectstoreFilename()

        # Don't bother if the file already exists
        if os.path.exists(filename):
            # tolog("AGIS objectstore info file already exist")
            status = True
            return status

        # Download queuedata from CVMFS, full version which needs to be trimmed
        tolog("Copying master objectstore queuedata file from primary location (CVMFS)")
        from shutil import copy2
        try:
            _filename = "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints_objectstores.json"
            tolog("%s -> %s" % (_filename, filename))
            copy2(_filename, filename)
        except Exception, e:
            tolog("!!WARNING!!3434!! Failed to copy AGIS queuedata from CVMFS: %s" % (e))
        else:
            status = True

        # CVMFS download failed, default to AGIS
        if not status:
            # Get the queuedata from AGIS
            tries = 2
            for trial in range(tries):
                tolog("Downloading queuedata (attempt #%d)" % (trial + 1))
                # cmd = "curl --connect-timeout 20 --max-time 120 -sS \"http://atlas-agis-api.cern.ch/request/service/query/get_os_services/?json\" >%s" % (filename)
                # tolog("Executing command: %s" % (cmd))
                # ret, output = commands.getstatusoutput(cmd)
                # if ret == 0:
                #    status = True
                #    break
                url = 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&state=ACTIVE&site_state=ACTIVE&preset=dict&json_pretty=1&type[]=OS_LOGS&type[]=OS_ES'
                from urllib2 import urlopen
                f = urlopen(url)
                if f:
                    # Import the dictionary
                    from json import loads
                    json_dictionary = loads(f.read())
                    # Write the downloaded dictionary to file
                    if type(json_dictionary) == dict:
                        if writeJSON(filename, json_dictionary):
                            tolog("Wrote AGIS OS dictionary to file %s" % (filename))
                            status = True
                            f.close()
                            break
                        else:
                            tolog("!!WARNING!!5656!! Failed to write AGIS JSON dictionary to file")
                    else:
                        tolog("!!WARNING!!5657!! Not a dictionary: %s" % str(type(json_dictionary)))
                else:
                    tolog("!!WARNING!!4545!! Failed to open %s" % (url))
        else:
            tolog("Opening %s" % (filename))
            try:
                f = open(filename, 'r')
            except IOError, e:
                tolog("!!WARNING!!5659!! Failed to open %s" % (filename))
                status = False
            else:
                # Import the dictionary
                from json import loads
                json_dictionary = loads(f.read())
                f.close()
        if status:
            tolog("Will create trimmed json files for each objectstore")
            if json_dictionary != {}:
                for os_name in json_dictionary.keys():
                    trimmed_dictionary = json_dictionary[os_name]
                    filename_trimmed = self.getObjectstoreFilename(name=os_name)
                    if writeJSON(filename_trimmed, trimmed_dictionary):
                        tolog("Stored trimmed AGIS dictionary from CVMFS in: %s" % (filename_trimmed))
                        status = True
                    else:
                        tolog("!!WARNING!!2122!! Failed to write trimmed AGIS dictionary to file: %s" % (
                        filename_trimmed))
                        status = False
            else:
                tolog("!!WARNING!!2120!! Failed to read JSON from file %s" % (filename))

        return status

    # WARNING: deprecated - remove
    def getNewQueuedata(self, queuename, overwrite=True, version=1, os_bucket_id=-1):
        """ Download the queuedata primarily from CVMFS and secondarily from the AGIS server """

        filename = self.getQueuedataFileName(version=version, check=False, queuename=queuename, os_bucket_id=os_bucket_id)
        status = False

        # If overwrite is not required, return True if the queuedata already exists
        if os.path.exists(filename):
            tolog("AGIS queuedata already exist")
            status = True
            return status

        # Download queuenadata from CVMFS, full version which needs to be trimmed
        tolog("Copying queuedata from primary location (CVMFS)")
        from shutil import copy2
        try:
            _filename = filename #+ "-ALL"
            copy2("/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json", _filename)
        except Exception, e:
            tolog("!!WARNING!!3434!! Failed to copy AGIS queuedata from CVMFS: %s" % (e))
        else:
            # Trim the JSON since it contains too much info
            # Load the dictionary
            dictionary = readJSON(_filename)
            if dictionary != {}:
                # in case os_bucket_id is set, we are only interested in objectstore info so find the corresponding queuename
                if os_bucket_id != -1:
                    tolog("os_bucket_id=%d - will find a corresponding queuename (only interested in OS info)" % (os_bucket_id))
                    queuename = self.getQueuenameFromOSBucketID(os_bucket_id)
                else:
                    # queuename is expected to be set in case os_bucket_id = -1
                    pass

                if queuename:
                    # Get the entry for queuename
                    try:
                        _d = dictionary[queuename]
                    except Exception, e:
                        tolog("No entry for queue %s in JSON: %s" % (queuename, e))
                    else:
                        # Create a new queuedata dictionary
                        trimmed_dictionary = { queuename: _d }

                        # Store it
                        if writeJSON(filename, trimmed_dictionary):
                            tolog("Stored trimmed AGIS dictionary from CVMFS in: %s" % (filename))
                            status = True
                        else:
                            tolog("!!WARNING!!4545!! Failed to write trimmed AGIS dictionary to file: %s" % (filename))
                else:
                    tolog("!!WARNING!!2122!! Can not proceed with unset os_bucket_id or queuename  (need it to find the queue)")
            else:
                tolog("!!WARNING!!2120!! Failed to read dictionary from file %s" % (filename))

        # CVMFS download failed, default to AGIS
        if not status:
            # Get the queuedata from AGIS
            tries = 2
            for trial in range(tries):
                tolog("Downloading queuedata (attempt #%d)" % (trial+1))
                cmd = 'curl --connect-timeout 20 --max-time 120 -sS "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&panda_queue=%s" >%s' % (queuename, filename)
                tolog("Executing command: %s" % (cmd))
                ret, output = commands.getstatusoutput(cmd)

                # Verify queuedata
                value = self.getField('objectstores', queuename=queuename)
                if value:
                    status = True
                    tolog("Downloaded queuedata")
                    break

        return status

    def getField(self, field, version=1, queuename=None, os_bucket_id=-1):
        """ Get the value for entry 'field' in the full AGIS queuedata """

        value = None
        filename = self.getQueuedataFileName(version=version, check=False, queuename=queuename, os_bucket_id=os_bucket_id)
        tolog("queuedata file: %s" % filename)
        if os.path.exists(filename):

            # Load the dictionary
            dictionary = readJSON(filename)
            if dictionary != {}:
                # Get the entry for queuename
                try:
                    _queuename = dictionary.keys()[0]
                    _d = dictionary[_queuename]
                except Exception, e:
                    tolog("!!WARNING!!2323!! Caught exception: %s" % (e))
                else:
                    # Get the field value
                    try:
                        value = _d[field]
                    except Exception, e:
                        tolog("!!WARNING!!2112!! Queuedata problem: %s" % (e))
            else:
                tolog("!!WARNING!!2120!! Failed to read dictionary from file %s" % (filename))
        else:
            tolog("!!WARNING!!3434!! File does not exist: %s" % (filename))

        return value

    @classmethod
    def isFileExpired(self, fname, cache_time=0): ## should be isolated later
        """ check if file fname older then cache_time seconds from its last_update_time """
        if cache_time:
            lastupdate = self.getFileLastupdateTime(fname)
            return not (lastupdate and datetime.now() - lastupdate < timedelta(seconds=cache_time))

        return True

    @classmethod
    def getFileLastupdateTime(self, fname): ## should be isolated later
        try:
            lastupdate = datetime.fromtimestamp(os.stat(fname).st_mtime)
        except OSError, e:
            lastupdate = None
        return lastupdate

    @classmethod
    def loadURLData(self, url, fname=None, cache_time=0, nretry=3, sleeptime=60): # should be unified and isolated later
        """
        Download data from url/file resource and optionally save it into cachefile fname,
        The file will not be (re-)loaded again if cache age from last file modification does not exceed "cache_time" seconds
        :return: data loaded from the url or file content if url passed is a filename
        """

        content = None
        if url and self.isFileExpired(fname, cache_time): # load data into temporary cache file
            for trial in range(nretry):
                if content:
                    break
                try:
                    if os.path.isfile(url):
                        tolog('[attempt=%s] Loading data from file=%s' % (trial, url))
                        f = open(url, "r") # python 2.5 .. replace by 'with' statement (min python2.6??)
                        content = f.read()
                        f.close()
                    else:
                        tolog('[attempt=%s] Loading data from url=%s' % (trial, url))
                        content = urllib2.urlopen(url, timeout=20).read() # python2.6
                        #content = urllib2.urlopen(url).read() # python 2.5

                    if fname: # save to cache
                        f = open(fname, "w+")
                        f.write(content)
                        f.close()
                        tolog('Saved data from "%s" resource into file=%s, length=%.1fKb' % (url, fname, len(content)/1024.))
                    return content
                except Exception, e: # ignore errors, try to use old cache if any
                    tolog("Failed to load data from url=%s, error: %s .. trying to use data from cache=%s" % (url, e, fname))
                    # will try to use old cache below
                    if trial < nretry-1:
                        tolog("Will try again after %ss.." % sleeptime)
                        from time import sleep
                        sleep(sleeptime)

        if content is not None: # just loaded
            return content

        try:
            with open(fname, 'r') as f:
                content = f.read()
        except Exception, e:
            tolog("loadURLData: Caught exception: %s" % e)
            return None

        return content

    def loadDDMConfData(self, ddmendpoints=[], cache_time=60):

        # try to get data from CVMFS first
        # then AGIS or Panda JSON sources
        # passing cache time is a quick hack to avoid overloading
        # normally ddmconf data should be loaded only once in the init function and saved as dict like self.ddmconf = loadDDMConfData

        # list of sources to fetch ddmconf data from
        base_dir = os.environ.get('PilotHomeDir', '')
        ddmconf_sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json',
                                     'nretry': 1,
                                     'fname': os.path.join(base_dir, 'agis_ddmendpoints.cvmfs.json')},
                           'AGIS':  {'url':'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&state=ACTIVE&preset=dict&ddmendpoint=%s' % ','.join(ddmendpoints),
                                     'nretry':3,
                                     'fname': os.path.join(base_dir, 'agis_ddmendpoints.agis.%s.json' % ('_'.join(sorted(ddmendpoints)) or 'ALL'))},
                           'LOCAL': {'url':None,
                                     'nretry':1,
                                     'fname': os.path.join(base_dir, 'agis_ddmendpoints.json')},
                           'PANDA' : None
        }

        ddmconf_sources_order = ['LOCAL', 'CVMFS', 'AGIS'] # can be moved into the schedconfig in order to configure workflow in AGIS on fly: TODO

        for key in ddmconf_sources_order:
            tolog("Loading DDMConfData from source %s" % key)
            dat = ddmconf_sources.get(key)
            if not dat:
                continue

            content = self.loadURLData(cache_time=cache_time, **dat)
            if not content:
                continue
            try:
                data = json.loads(content)
            except Exception, e:
                tolog("!!WARNING: loadDDMConfData(): Failed to parse JSON content from source=%s .. skipped, error=%s" % (dat.get('url'), e))
                data = None

            if data and isinstance(data, dict):
                return data

        return None

    def resolveDDMConf(self, ddmendpoints):

        #if not ddmendpoints:
        #    return {}

        self.ddmconf = self.loadDDMConfData(ddmendpoints, cache_time=6000) or {} # quick stub: fix me later: ddmconf should be loaded only once in any init function from top level, cache_time is used as a workaround here

        return self.ddmconf

    def resolveDDMProtocols(self, ddmendpoints, activity):
        """
            Resolve [SE endpoint, SE path] protocol entry for requested ddmendpoint by given pilot activity ("pr" means pilot_read, "pw" for pilot_write)
            Return the list of possible protocols ordered by priority
            :return: dict('ddmendpoint_name':[(SE_1, path2), (SE_2, path2)])
        """

        if not ddmendpoints:
            return {}

        self.ddmconf = self.loadDDMConfData(ddmendpoints, cache_time=6000) or {} # quick stub: fix me later: ddmconf should be loaded only once in any init function from top level, cache_time is used as a workaround here

        ret = {}
        for ddm in set(ddmendpoints):
            protocols = [dict(se=e[0], path=e[2]) for e in sorted(self.ddmconf.get(ddm, {}).get('aprotocols', {}).get(activity, []), key=lambda x: x[1])]
            ret.setdefault(ddm, protocols)

        return ret


    def loadDDMBlacklistingConfData(self, cache_time=60):

        # try to get data from CVMFS first
        # then AGIS or Panda JSON sources
        # passing cache time is a quick hack to avoid overloading

        # list of sources to fetch ddmconf data from
        base_dir = os.environ.get('PilotHomeDir', '')
        ddmconf_sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmblacklisting.json',
                                     'nretry': 1,
                                     'fname': os.path.join(base_dir, 'agis_ddmblacklisting.cvmfs.json')},
                           'AGIS':  {'url':'http://atlas-agis-api.cern.ch/request/ddmendpointstatus/query/list/?json&fstate=OFF',
                                     'nretry':3,
                                     'fname': os.path.join(base_dir, 'agis_ddmblacklisting.agis.json')},
                           'LOCAL': {'url':None,
                                     'nretry':1,
                                     'fname': os.path.join(base_dir, 'agis_ddmblacklisting.json')},
                           'PANDA' : None
        }

        ddmconf_sources_order = ['LOCAL', 'CVMFS', 'AGIS'] # can be moved into the schedconfig in order to configure workflow in AGIS on fly: TODO

        for key in ddmconf_sources_order:
            tolog("Loading DDMBlacklistingConfData from source %s" % key)
            dat = ddmconf_sources.get(key)
            if not dat:
                continue

            content = self.loadURLData(cache_time=cache_time, **dat)
            if not content:
                continue
            try:
                data = json.loads(content)
            except Exception, e:
                tolog("!!WARNING: loadDDMConfData(): Failed to parse JSON content from source=%s .. skipped, error=%s" % (dat.get('url'), e))
                data = None

            if data and isinstance(data, dict):
                return data

        return None

    def resolveDDMBlacklistingConf(self):
        """
        Reload DDM Blaclisting conf every hour.
        """
        self.ddmblacklistingconf = self.loadDDMBlacklistingConfData(cache_time=3600) or {}

        return self.ddmblacklistingconf


    def loadSchedConfData(self, pandaqueues=[], cache_time=60):
        """
            Download the queuedata from various sources (prioritized)
            this function should replace getNewQueuedata() later.
        """

        # try to get data from CVMFS first
        # then AGIS or Panda JSON sources
        # passing cache time is a quick hack to avoid overloading
        # normally data should be loaded only once in the init function and saved as dict like self.queueconf = loadQueueData

        # list of sources to fetch ddmconf data from
        base_dir = os.environ.get('PilotHomeDir', '')
        pandaqueues = set(pandaqueues)

        schedcond_sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json',
                                     'nretry': 1,
                                     'fname': os.path.join(base_dir, 'agis_schedconf.cvmfs.json')},
                            'AGIS':  {'url':'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&panda_queue=%s' % ','.join(pandaqueues),
                                     'nretry':3,
                                     'fname': os.path.join(base_dir, 'agis_schedconf.agis.%s.json' % ('_'.join(sorted(pandaqueues)) or 'ALL'))},
                            'PANDA' : None
        }

        schedcond_sources_order = ['CVMFS', 'AGIS'] # can be moved into the schedconfig in order to configure workflow in AGIS on fly: TODO

        for key in schedcond_sources_order:
            dat = schedcond_sources.get(key)
            if not dat:
                continue

            content = self.loadURLData(cache_time=cache_time, **dat)
            if not content:
                continue
            try:
                data = json.loads(content)
            except Exception, e:
                tolog("!!WARNING: loadSchedConfData(): Failed to parse JSON content from source=%s .. skipped, error=%s" % (dat.get('url'), e))
                data = None

            if data and isinstance(data, dict):
                if 'error' in data:
                    tolog("!!WARNING: loadSchedConfData(): skipped source=%s since response contains error: data=%s" % (dat.get('url'), data))
                else: # valid response
                    return data

        return None


    def resolvePandaProtocols(self, pandaqueues, activity):
        """
            Resolve (SE endpoint, path, copytool, copyprefix) protocol entry for requested ddmendpoint by given pilot activity ("pr" means pilot_read, "pw" for pilot_write)
            Return the list of possible protocols ordered by priority
            :return: dict('ddmendpoint_name':[(SE_1, path2, copytool, copyprefix), )
        """

        if not pandaqueues:
            return {}
        if isinstance(pandaqueues, (str, unicode)):
            pandaqueues = [pandaqueues]

        self.schedconf = self.loadSchedConfData(pandaqueues, cache_time=6000) or {} # quick stub: fix me later: schedconf should be loaded only once in any init function from top level, cache_time is used as a workaround here

        ret = {}
        for pandaqueue in set(pandaqueues):
            qdata = self.schedconf.get(pandaqueue, {})
            protocols = qdata.get('aprotocols', {}).get(activity, [])
            copytools = qdata.get('copytools', {})
            for p in protocols:
                p.setdefault('copysetup', copytools.get(p.get('copytool'), {}).get('setup'))
            ret.setdefault(pandaqueue, protocols)

        return ret


    def resolvePandaCopytools(self, pandaqueues, activity, defval=[], masterdata={}):
        """
            Resolve supported copytools by given pandaqueues
            Check first settings for requested activity (pr, pw, pl, pls), then defval values,
            if not set then return copytools explicitly defined for all activities (not restricted to specific activity)
            Return ordered list of accepted copytools
            :param activity: activity of prioritized list of activities to resolve data
            :param defval: default copytools values which will be used if no copytools defined for requested activity
            :param masterdata: custom job specific data (overwritten by job) schedconfig data
            :return: dict('pandaqueue':[(copytool, {settings}), ('copytool_name', {'setup':''}), ])
        """

        if isinstance(pandaqueues, (str, unicode)):
            pandaqueues = [pandaqueues]

        if isinstance(activity, (str, unicode)):
            activity = [activity]

        r = self.loadSchedConfData(pandaqueues, cache_time=6000) or {} # quick stub: fix me later: schedconf should be loaded only once in any init function from top level, cache_time is used as a workaround here
        self.schedconf = r

        ret = {}
        for pandaqueue in set(pandaqueues):
            copytools = masterdata.get(pandaqueue, {}).get('copytools', {}) or r.get(pandaqueue, {}).get('copytools', {})
            pq_acopytools = r.get(pandaqueue, {}).get('acopytools', {})
            if 'acopytools' in masterdata.get(pandaqueue, {}):
                pq_acopytools = masterdata.get(pandaqueue, {}).get('acopytools', {})
            cptools, acopytools = [], None
            for a in activity:
                acopytools = pq_acopytools.get(a, [])
                if acopytools:
                    break
            if acopytools:
                cptools = [(cp, copytools[cp]) for cp in acopytools if cp in copytools]
            elif defval:
                cptools = defval[:]
            else:
                explicit_copytools = set()
                for v in pq_acopytools.itervalues():
                    explicit_copytools.update(v or [])

                cptools = [(cp,v) for cp,v in copytools.iteritems() if cp not in explicit_copytools]

            ret.setdefault(pandaqueue, cptools)

        return ret


    def resolvePandaOSDDMs(self, pandaqueues):
        """
            Resolve OS ddmendpoint associated to requested pandaqueues
            :return: list of accepted ddmendpoints
        """

        if isinstance(pandaqueues, (str, unicode)):
            pandaqueues = [pandaqueues]

        r = self.loadSchedConfData(pandaqueues, cache_time=6000) or {} # quick stub: fix me later: schedconf should be loaded only once in any init function from top level, cache_time is used as a workaround here
        #self.schedconf = r

        ret = {}
        for pandaqueue in set(pandaqueues):
            ret[pandaqueue] = r.get(pandaqueue, {}).get('ddmendpoints', [])

        return ret


    def resolvePandaAssociatedStorages(self, pandaqueues):
        """
            Resolve DDM storages associated to requested pandaqueues
            :return: list of accepted ddmendpoints
        """

        if isinstance(pandaqueues, (str, unicode)):
            pandaqueues = [pandaqueues]

        r = self.loadSchedConfData(pandaqueues, cache_time=6000) or {} # quick stub: fix me later: schedconf should be loaded only once in any init function from top level, cache_time is used as a workaround here
        #self.schedconf = r

        ret = {}
        for pandaqueue in set(pandaqueues):
            ret[pandaqueue] = r.get(pandaqueue, {}).get('astorages', {})

        return ret

    def resolveItems(self, pandaqueues, itemName):
        """
            Resolve DDM storages associated to requested pandaqueues
            :return: list of accepted ddmendpoints
        """

        if isinstance(pandaqueues, (str, unicode)):
            pandaqueues = [pandaqueues]

        r = self.loadSchedConfData(pandaqueues, cache_time=6000) or {} # quick stub: fix me later: schedconf should be loaded only once in any init function from top level, cache_time is used as a workaround here
        #self.schedconf = r

        ret = {}
        for pandaqueue in set(pandaqueues):
            ret[pandaqueue] = r.get(pandaqueue, {}).get(itemName, {})

        return ret


    # Optional
    def getBenchmarkFileName(self, workdir):
        """ Return the filename of the benchmark dictionary """

        return os.path.join(workdir, "benchmark.json")

    # Optional
    def shouldExecuteBenchmark(self):
        """ Should the pilot execute a benchmark test before asking server for a job? """

        return False

    # Optional
    def getBenchmarkDictionary(self, workdir):
        """ Return the benchmarks dictionary """

        return self.__benchmarks

    def copyFullQueuedata(self, destination=None):
        """ Copy the full queuedata file from a location specified in SiteInformation """

        # Get the path to the full queuedata json file
        path = self.getFullQueuedataFilePath()
        if destination:
            dest = destination
        else:
            dest = os.getcwd()
        try:
            if os.path.exists(path):
                tolog("File already exists, will not copy again: %s" % (path))
            else:
                from shutil import copy2
                copy2(path, dest)
                tolog("Copied %s to %s" % (path, dest))
        except IOError, e:
            tolog("!!WARNING!!4444!! Failed to copy file %s: %s" % (path, e))

    def getFullQueuedataDictionary(self):
        """ Return the full queuedata JSON dictionary """
        # Note: this dictionary is very heavy since it contains all the info for all the queues

        return getJSONDictionary(self.getFullQueuedataFilePath())

    def hasOSBucketIDs(self, prodDBlockToken):
        """ Does the prodDBlockToken contain OS bucket IDs? """
        # The prodDBlockToken is considered to contain bucket IDs if it's a list of string integers

        status = False
        prodDBlockTokenInts = []

        try:
            # Can the list of string integers be converted to a list of integers?
            prodDBlockTokenInts = [int(x) if x.isdigit() else None for x in prodDBlockToken] # map(int, prodDBlockToken)
        except:
            # Will throw a ValueError in case of present non-integers
            tolog("prodDBlockToken does not contain OS bucket IDs (prodDBlockToken=%s)" % str(prodDBlockToken))
        else:
            tolog("prodDBlockToken contains OS bucket IDs (prodDBlockToken=%s)" % str(prodDBlockToken))
            status = True

        return status, prodDBlockTokenInts

    # WARNING: deprecated - can be removed when getNewQueuedata() has been removed
    def getQueuenameFromOSBucketID(self, os_bucket_id):
        """ Return the name of a queue that has the given os_bucket_id """

        _queuename = ""

        # First copy all the queuedata
        self.copyFullQueuedata()

        # Load the dictionary
        dictionary = self.getFullQueuedataDictionary()
        if dictionary != {}:
            for queuename in dictionary.keys():
                if dictionary[queuename]["objectstores"] != []:
                    l = dictionary[queuename]["objectstores"]
                    for i in l:
                        if i['os_bucket_id'] == os_bucket_id:
                            tolog("Queuename %s has os_bucket_id=%d (note: queuename not important for OS transfers - only its OS info)" % (queuename, os_bucket_id))
                            _queuename = queuename
                            break
                if _queuename != "":
                    break
        else:
            tolog("Full queuedata not available")

        return _queuename

    def updateDirectAccess(self, transferType):
        """ Update the direct_access_lan/wan depending on the transferType """

        if transferType == "direct":
            if self.readpar('direct_access_lan').lower() == 'false':
                ec = self.replaceQueuedataField("direct_access_lan", "True")
            else:
                tolog("No need to update direct_access_lan (already set to True)")
        elif transferType == "fax":
            if self.readpar('direct_access_wan').lower() == 'false':
                ec = self.replaceQueuedataField("direct_access_wan", "True")
            else:
                tolog("No need to update direct_access_wan (already set to True)")
        else:
            tolog("Unknown transferType=%s" % (transferType))

if __name__ == "__main__":
    from SiteInformation import SiteInformation
    import os
    os.environ['PilotHomeDir'] = os.getcwd()
    s1 = SiteInformation()
    p = "--maxEvents=2 --inputHITSFile HITS.06828093._000096.pool.root.1 --outputRDOFile RDO_20c37551-abfd-4ce1-b0ae-09a0a5b72484.root --useTestASetup --useTestXRootD"
    p, t = s1.updateQueuedataFromJobParameters(p)
    print p, t
    print os.environ.get('ALRB_asetupVersion')
    print os.environ.get('TestXRootD')
    #print "copytool=",s1.readpar('copytool')
    #path = 'srm://srm-eosatlas.cern.ch/eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/f4/NTUP_SMWZ.00836697._000601.root.1'
    #print path
    #ret = s1.getCopyPrefixPath(path, stageIn=True)
    #print "ret:" + ret
    #print
    #path = 'root://atlas-xrd-eos-rucio.cern.ch:1094//atlas/rucio/mc12_8TeV:NTUP_SMWZ.00836697._000601.root.1'
    #print path
    #ret = s1.getCopyPrefixPath(path, stageIn=True)
    #print "ret:" + ret
    #print

    ##bnl
    #s1.replaceQueuedataField("copyprefixin", "srm://dcsrm.usatlas.bnl.gov.*/pnfs/^root://dcgftp.usatlas.bnl.gov:1096/pnfs")
    #path = 'srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/atlasuserdisk/rucio/panda/a7/bf/panda.0317011154.376400.lib._5118143.1962296626.lib.tgz'
    #print path
    #ret = s1.getCopyPrefixPath(path, stageIn=True)
    #print "ret:" + ret
    #print
    #path = 'root://dcxrd.usatlas.bnl.gov:1096///atlas/rucio/panda:panda.0317011154.376400.lib._5118143.1962296626.lib.tgz'
    #print path
    #ret = s1.getCopyPrefixPath(path, stageIn=True)
    #print "ret:" + ret
    #print

    ##EC2
    #s1.replaceQueuedataField("copyprefixin", "srm://aws01.racf.bnl.gov.*/mnt/atlasdatadisk,srm://aws01.racf.bnl.gov.*/mnt/atlasuserdisk,srm://aws01.racf.bnl.gov.*/mnt/atlasproddisk^s3://s3.amazonaws.com:80//s3-atlasdatadisk-racf,s3://s3.amazonaws.com:80//s3-atlasuserdisk-racf,s3://s3.amazonaws.com:80//s3-atlasproddisk-racf")
    #s1.replaceQueuedataField("copyprefix", "srm://aws01.racf.bnl.gov.*/mnt/atlasdatadisk,srm://aws01.racf.bnl.gov.*/mnt/atlasuserdisk,srm://aws01.racf.bnl.gov.*/mnt/atlasproddisk^s3://s3.amazonaws.com:80//s3-atlasdatadisk-racf,s3://s3.amazonaws.com:80//s3-atlasuserdisk-racf,s3://s3.amazonaws.com:80//s3-atlasproddisk-racf")
    #path = 'srm://aws01.racf.bnl.gov:8443/srm/managerv2?SFN=/mnt/atlasproddisk/rucio/panda/7b/c4/86c7b8a5-d955-41a5-9f0f-36d067b9931b_0.job.log.tgz'
    #print path
    #ret = s1.getCopyPrefixPathNew(path, stageIn=True)
    #print "ret:" + ret
    #print

    #s1.getNewQueuedata("BNL_PROD_MCORE-condor")
