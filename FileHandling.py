# This module contains functions related to file handling.

import os
import time

from pUtil import tolog, convert

def openFile(filename, mode):
    """ Open and return a file pointer for the given mode """
    # Note: caller needs to close the file

    f = None
    if os.path.exists(filename):
        try:
            f = open(filename, mode)
        except IOError, e:
            tolog("!!WARNING!!2997!! Caught exception: %s" % (e))
    else:
        tolog("!!WARNING!!2998!! File does not exist: %s" % (filename))

    return f

def getJSONDictionary(filename):
    """ Read a dictionary with unicode to utf-8 conversion """

    dictionary = None
    from json import load
    f = openFile(filename, 'r')
    if f:
        try:
            dictionary = load(f)
        except Exception, e:
            tolog("!!WARNING!!2222!! Failed to load json dictionary: %s" % (e))
        else:
            f.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = convert(dictionary)
                except Exception, e:
                    tolog("!!WARNING!!2996!! Failed to convert dictionary from unicode to utf-8: %s, %s" % (dictionary, e))
            else:
                tolog("!!WARNING!!2995!! Load function returned empty JSON dictionary: %s" % (filename))
 
    return dictionary

def writeJSON(file_name, dictionary):
    """ Write the dictionary to a JSON file """

    status = False

    from json import dump
    try:
        fp = open(file_name, "w")
    except Exception, e:
        tolog("!!WARNING!!2323!! Failed to open file %s: %s" % (file_name, e))
    else:
        # Write the dictionary
        try:
            dump(dictionary, fp)
        except Exception, e:
            tolog("!!WARNING!!2324!! Failed to write dictionary to file %s: %s" % (file_name, e))
        else:
            tolog("Wrote dictionary to file %s" % (file_name))
            status = True
        fp.close()

    return status

def readJSON(file_name):
    """ Read a dictionary from a JSON file """

    dictionary = {}
    from json import load
    f = openFile(file_name, 'r')
    if f:
        # Read the dictionary
        try:
            dictionary = load(f)
        except Exception, e:
            tolog("!!WARNING!!2332!! Failed to read dictionary from file %s: %s" % (file_name, e))
        else:
            f.close()
            tolog("Read dictionary from file %s" % (file_name))

    return dictionary

def findLatestTRFLogFile(workdir):
    """ Find out which is the latest log.* file """

    last_log_file = ""

    # Assume the log files begin with 'log.'
    pattern = "log."
    file_list = sortedLs(workdir, pattern)
    if file_list != []:
        last_log_file = os.path.join(workdir, file_list[-1])
        tolog("Found payload log files: %s" % str(file_list))
        tolog("File %s was the last log file that was updated" % (last_log_file))
    else:
        tolog("Did not find any log.* files")

    return last_log_file

def sortedLs(path, pattern):
    """ Sort the contents of directory 'path' using 'pattern' """

    # Note: pattern is only a string, e.g. pattern = 'log.' will return a
    # list with all files starting with 'log.' in time order
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    file_list = []
    try:
        file_list = list(sorted(os.listdir(path), key=mtime))
    except Exception, e:
        tolog("!!WARNING!!3232!! Failed to obtain sorted file list: %s" % (e))

    final_file_list = []
    if file_list != []:
        for f in file_list:
            if f.startswith(pattern):
                final_file_list.append(f)
    return final_file_list

def readFile(filename):
    """ Read the contents of a file """

    contents = ""
    if os.path.exists(filename):
        try:
            f = open(filename, 'r')
        except IOError, e:
            tolog("!!WARNING!!2121!! Failed to open file %s: %s" % (filename, e))
        else:
            try:
                contents = f.read()
            except Exception, e:
                tolog("!!WARNING!!2122!! Failed to read file %s: %s" % (filename, e))
            f.close()
    else:
        tolog("!!WARNING!!2121!! File does not exist: %s" % (filename))

    return contents

def writeFile(filename, contents):
    """ Write the contents to filename """

    status = False
    try:
        f = open(filename, 'w')
    except IOError, e:
        tolog("!!WARNING!!2123!! Failed to open file %s: %s" % (filename, e))
    else:
        try:
            f.write(contents)
        except IOError, e:
            tolog("!!WARNING!!2123!! Failed to write to file %s: %s" % (filename, e))
        else:
            status = True
        f.close()

    return status

def tail(f, lines=10):
    """ Get the n last lines from file f """

    if lines == 0:
        return ""

    BUFSIZ = 1024
    f.seek(0, 2)
    bytes = f.tell()
    size = lines + 1
    block = -1
    data = []
    while size > 0 and bytes > 0:
        if bytes - BUFSIZ > 0:
            # Seek back one whole BUFSIZ
            f.seek(block * BUFSIZ, 2)
            # read BUFFER
            data.insert(0, f.read(BUFSIZ))
        else:
            # file too small, start from begining
            f.seek(0,0)
            # only read what was not read
            data.insert(0, f.read(bytes))
        linesFound = data[0].count('\n')
        size -= linesFound
        bytes -= BUFSIZ
        block -= 1

    tail_list = ''.join(data).splitlines()[-lines:]
    return '\n'.join(tail_list)

def getTracingReportFilename():
    """ Return the name of the tracing report JSON file """

    return "tracing_report.json"

def getOSTransferDictionaryFilename():
    """ Return the name of the objectstore transfer dictionary file """

    return "os_transfer_dictionary.json"

def getExtension(alternative='pickle'):
    """ get the file extension (json or whatever 'alternative' is set to, pickle by default) """

    try:
        from json import load
    except:
        extension = alternative
    else:
        extension = "json"

    return extension

def getNewQueuedataFilename():
    """ Return the name of the queuedata file """

    return "new_queuedata.json"

def getNewQueuedataXXX(queuename):
    """ Download the queuedata primarily from the AGIS server and secondarily from CVMFS """

    filename = getNewQueuedataFilename()

    status = False
    tries = 2
    for trial in range(tries):
        tolog("Downloading queuedata (attempt #%d)" % (trial+1))
        cmd = "curl --connect-timeout 20 --max-time 120 -sS \"http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas&panda_queue=%s\" >%s" % (queuename, filename)
        tolog("Executing command: %s" % (cmd))
        import commands
        ret, output = commands.getstatusoutput(cmd)

        # Verify queuedata
        value = getField('copysetup')
        if value:
            status = True
            break

    return status

def getField(field):
    """ Get the value for entry 'field' in the queuedata """

    value = None
    filename = getNewQueuedataFilename()    
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

def getObjectstoresFieldXXX(field, mode, queuename):
    """ Return the objectorestores field from the objectstores list for the relevant mode """
    # mode: eventservice, logs, http

    value = None

    # Get the objectstores list
    objectstores_list = getObjectstoresList(queuename)

    if objectstores_list:
        for d in objectstores_list:
            try:
                os_bucket_name = d['os_bucket_name']
                if os_bucket_name == mode:
                    value = d[field]
                    break
            except Exception, e:
                tolog("!!WARNING!!2222!! Failed to read field %s from objectstores list: %s" % (field, e))
    return value

def getObjectstorePathXXX(filename, mode, queuename):
    """ Get the proper path to a file in the OS """

    path = ""

    # Get the objectstores list
    objectstores_list = getObjectstoresList(queuename)

    if objectstores_list:
        # Get the relevant fields
        for d in objectstores_list:
            try:
                os_bucket_name = d['os_bucket_name']
                if os_bucket_name == mode:
                    os_endpoint = d['os_endpoint']
                    os_bucket_endpoint = d['os_bucket_endpoint']

                    if os_endpoint.endswith('/'):
                        os_endpoint = os_endpoint[:-1]
                    if not os_bucket_endpoint.startswith('//'):
                        if os_bucket_endpoint.startswith('/'):
                            os_bucket_endpoint = "/" + os_bucket_endpoint
                        else:
                            os_bucket_endpoint = "//" + os_bucket_endpoint

                    path = os.path.join(os_endpoint + os_bucket_endpoint, filename)
                    break
            except Exception, e:
                tolog("!!WARNING!!2222!! Failed to read field %s from objectstores list: %s" % (field, e))

    return path

def getObjectstoresListXXX(queuename):
    """ Get the objectstores list from the proper queuedata for the relevant queue """
    # queuename is needed as long as objectstores field is not available in normal queuedata (temporary)

    objectstores = None

    # First try to get the objectstores field from the normal queuedata
    try:
        from pUtil import readpar
        _objectstores = readpar('objectstores')
    except:
        #tolog("Field \'objectstores\' not yet available in queuedata")
        _objectstores = None

    # Get the field from AGIS
    if not _objectstores:
        s = True
        # Download the new queuedata in case it has not been downloaded already
        if not os.path.exists(getNewQueuedataFilename()):
            s = getNewQueuedata(queuename)
        if s:
            _objectstores = getField('objectstores')

    if _objectstores:
        objectstores = _objectstores

    return objectstores

def getHash(s, length):
    """ Return a hash from string s """

    import hashlib
    _hash = hashlib.md5()
    _hash.update(s)
    return  _hash.hexdigest()[:length]

def getHashedBucketEndpoint(endpoint, file_name):
    """ Return a hashed bucket endpoint """
    # Example:
    # endpoint = "atlas_logs", file_name = "log.tgz"
    # -> hash = "07" and hashed_endpoint = "atlas_logs_07"

#    return endpoint + "_" + getHash(file_name, 2)
    return endpoint

def addToOSTransferDictionary(file_name, workdir, os_name, os_bucket_endpoint):
    """ Add the transferred file to the OS transfer file """
    # Note: we don't want to store the file name since potentially there can be a large number of files
    # We only store a file number count
    # We still need to know the file name in order to figure out which bucket it belongs to (using a hash of the file name)
    # The has will be added to the os_bucket_endpoint (e.g. 'atlas_logs' -> 'atlas_logs_E2')

    # Only proceed if os_name and os_bucket_endpoint have values
    if os_name and os_name != "" and os_bucket_endpoint and os_bucket_endpoint != "":

        # Get the name and path of the objectstore transfer dictionary file 
        os_tr_path = os.path.join(workdir, getOSTransferDictionaryFilename())

        # Create a hash of the file name, two char long, and then the final bucket endpoint
        _endpoint = getHashedBucketEndpoint(os_bucket_endpoint, file_name)

        # Does the transfer file exist already? If not, create it
        if os.path.exists(os_tr_path):
            # Read back the existing dictionary
            dictionary = readJSON(os_tr_path)
            if not dictionary:
                tolog("Failed to open OS transfer dictionary - will recreate it")
                dictionary = {}
        else:
            # Create a new dictionary
            dictionary = {}
            tolog("New OS transfer dictionary created: %s" % (os_tr_path))

        # Populate the dictionary
        if dictionary.has_key(os_name):
            if dictionary[os_name].has_key(_endpoint):
                # Increase the file count
                dictionary[os_name][_endpoint] += 1
            else:
                # One file has been stored in this endpoint
                dictionary[os_name][_endpoint] = 1
        else:
            # One file has been stored in this endpoint
            dictionary[os_name] = {_endpoint: 1}

        # Store the dictionary
        if writeJSON(os_tr_path, dictionary):
            tolog("Stored updated OS transfer dictionary: %s" % (os_tr_path))
        else:
            tolog("!!WARNING!!2211!! Failed to store OS transfer dictionary")

    else:
        tolog("Cannot add to OS transfer dictionary due to unset values (os_name/os_bucket_endpoint)")

def getOSNames(filename):
    """ Get the dictionary of objectstore os_names with populated buckets from the OS transfer dictionary """
    # Note: will return a dictionary of os_names identifiers to which files were actually transferred, along
    # with the names of the buckets where the files were transferred to
    # This function is used to generate the jobMetrics OS message (only the OS and bucket endpoints are of interest)

    # os_names_dictionary
    # FORMAT: { 'os_name': ['os_bucket_endpoint', ''], .. }
    # OS transfer dictionary
    # FORMAT: { 'os_name': { 'os_bucket_endpoint': <number of transferred files>, .. }, .. }

    os_names_dictionary = {}

    if os.path.exists(filename):
        # Get the OS transfer dictionary
        dictionary = readJSON(filename)
        if dictionary != {}:
            tmp_os_names_list = dictionary.keys()

            # Only report the os_name if there were files transferred to it
            for os_name in tmp_os_names_list:

                # Get the os_bucket_endpoint list and populate the final dictionary
                os_bucket_endpoint_list = dictionary[os_name].keys()
                for os_bucket_endpoint in os_bucket_endpoint_list:
                    n = dictionary[os_name][os_bucket_endpoint]

                    tolog("%s: %d file(s) transferred to bucket %s" % (os_name, n, os_bucket_endpoint))
                    if n > 0:
                        if os_names_dictionary.has_key(os_name):
                            os_names_dictionary[os_name].append(os_bucket_endpoint)
                        else:
                            os_names_dictionary[os_name] = [os_bucket_endpoint]
        else:
            tolog("!!WARNING!!3334!! OS transfer dictionary is empty")
    else:
        tolog("!!WARNING!!3333!! OS transfer dictionary does not exist at: %s" % (filename))

    return os_names_dictionary

def getPilotErrorReportFilename(workdir):
    """ Return the filename for the pilot error report """
    # This file should be placed in the pilot init dir

    return os.path.join(workdir, "pilot_error_report.json")

def updatePilotErrorReport(pilotErrorCode, pilotErrorDiag, priority, jobID, workdir):
    """ Temporary wrapper during testing """

    try:
        updatePilotErrorReportTMP(pilotErrorCode, pilotErrorDiag, priority, jobID, workdir)
    except Exception, e:
        tolog("!!WARNING!!4545!! Exception caught: %s" % (e))

def updatePilotErrorReportTMP(pilotErrorCode, pilotErrorDiag, priority, jobID, workdir):
    """ Write pilot error info to file """
    # Report format:
    # { jobID1: { priority1: [{ pilotErrorCode1:<nr>, pilotErrorDiag1:<str> }, .. ], .. }, .. }
    # The pilot will report only the first of the highest priority error when it reports the error at the end of the job
    # Use the following priority convention:
    # "0": errors that originate from the main pilot module (unless otherwise necessary)
    # "1": errors that originate from the Monitor module (-||-)
    # "2": errors that originate from other modules (-||-)

    # Convert to string if integer is sent for priority
    if type(priority) != str:
        priority = str(priority)

    filename = getPilotErrorReportFilename(workdir)
    if os.path.exists(filename):
        # The file already exists, read it back
        dictionary = getJSONDictionary(filename)
    else:
        dictionary = {}

    # Sort the new error
    if dictionary.has_key(jobID):
        jobID_dictionary = dictionary[jobID]

        # Update with the latest error info
        if not jobID_dictionary.has_key(priority):
            dictionary[jobID][priority] = []

        new_dictionary = { 'pilotErrorCode':pilotErrorCode, 'pilotErrorDiag':pilotErrorDiag }

        # Update the dictionary with the new info
        dictionary[jobID][priority].append(new_dictionary)
        print dictionary

    else:
        # Create a first entry into the error report
        dictionary[jobID] = {}
        dictionary[jobID][priority] = []
        dictionary[jobID][priority].append({})
        dictionary[jobID][priority][0] = { 'pilotErrorCode':pilotErrorCode, 'pilotErrorDiag':pilotErrorDiag }

    # Finally update the file
    status = writeJSON(filename, dictionary)

def getHighestPriorityError(jobId, workdir):
    """ Return the highest priority error for jobId from the pilot error report file """
    # Return: {'pilotErrorCode': <nr>, 'pilotErrorDiag': '..'}
    # Note: only the first of the highest priority errors will be returned
    errorInfo = {}

    filename = getPilotErrorReportFilename(workdir)
    if os.path.exists(filename):
        # The file already exists, read it back
        dictionary = getJSONDictionary(filename)
        if dictionary.has_key(jobId):

            # Find the highest priority error for this job
            highestPriority = 999
            for priority in dictionary[jobId].keys():
                try:
                    p = int(priority)
                except Exception, e:
                    tolog("!!WARNING!!2321!! Unexpected key in pilot error report: %s" % (e))
                else:
                    if p < highestPriority:
                        highestPriority = p

                if highestPriority < 999:
                    # Get the first reported error
                    errorInfo = dictionary[jobId][str(highestPriority)][0]
                else:
                    tolog("!!WARNING!!2322!! Could not locate the highest priority error")
        else:
            tolog("Pilot error report does not contain any error info for job %s" % (jobId))
    else:
        tolog("Pilot error report does not exist: %s (should only exist if there actually was an error)" % (filename))

    return errorInfo

# print findLatestTRFLogFile(os.getcwd())

