# This module contains functions related to file handling.

import os
import time

import pUtil

def openFile(filename, mode):
    """ Open and return a file pointer for the given mode """
    # Note: caller needs to close the file

    f = None
    if os.path.exists(filename):
        try:
            f = open(filename, mode)
        except IOError, e:
            pUtil.tolog("!!WARNING!!2997!! Caught exception: %s" % (e))
    else:
        pUtil.tolog("!!WARNING!!2998!! File does not exist: %s" % (filename))

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
            pUtil.tolog("!!WARNING!!2222!! Failed to load json dictionary: %s" % (e))
        else:
            f.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = pUtil.convert(dictionary)
                except Exception, e:
                    pUtil.tolog("!!WARNING!!2996!! Failed to convert dictionary from unicode to utf-8: %s, %s" % (dictionary, e))
            else:
                pUtil.tolog("!!WARNING!!2995!! Load function returned empty JSON dictionary: %s" % (filename))
 
   return dictionary

def writeJSON(file_name, dictionary):
    """ Write the dictionary to a JSON file """

    status = False

    from json import dump
    try:
        fp = open(file_name, "w")
    except Exception, e:
        pUtil.tolog("!!WARNING!!2323!! Failed to open file %s: %s" % (file_name, e))
    else:
        # Write the dictionary
        try:
            dump(dictionary, fp)
        except Exception, e:
            pUtil.tolog("!!WARNING!!2324!! Failed to write dictionary to file %s: %s" % (file_name, e))
        else:
            pUtil.tolog("Wrote dictionary to file %s" % (file_name))
            status = True
        fp.close()

    return status

def readJSON(file_name):
    """ Read a dictionary from a JSON file """

    dictionary = {}
    from json import load
    f = openFile(filename, 'r')
    if f:
        # Read the dictionary
        try:
            dictionary = load(fp)
        except Exception, e:
            pUtil.tolog("!!WARNING!!2332!! Failed to read dictionary from file %s: %s" % (file_name, e))
        else:
            fp.close()
            pUtil.tolog("Read dictionary from file %s" % (file_name))            

    return dictionary

def findLatestTRFLogFile(workdir):
    """ Find out which is the latest log.* file """

    last_log_file = ""

    # Assume the log files begin with 'log.'
    pattern = "log."
    file_list = sortedLs(workdir, pattern)
    if file_list != []:
        last_log_file = os.path.join(workdir, file_list[-1])
        pUtil.tolog("Found payload log files: %s" % str(file_list))
        pUtil.tolog("File %s was the last log file that was updated" % (last_log_file))
    else:
        pUtil.tolog("Did not find any log.* files")

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
        pUtil.tolog("!!WARNING!!3232!! Failed to obtain sorted file list: %s" % (e))

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
            pUtil.tolog("!!WARNING!!2121!! Failed to open file %s: %s" % (filename, e))
        else:
            try:
                contents = f.read()
            except Exception, e:
                pUtil.tolog("!!WARNING!!2122!! Failed to read file %s: %s" % (filename, e))
            f.close()
    else:
        pUtil.tolog("!!WARNING!!2121!! File does not exist: %s" % (filename))

    return contents

def writeFile(filename, contents):
    """ Write the contents to filename """

    status = False
    try:
        f = open(filename, 'w')
    except IOError, e:
        pUtil.tolog("!!WARNING!!2123!! Failed to open file %s: %s" % (filename, e))
    else:
        try:
            f.write(contents)
        except IOError, e:
            pUtil.tolog("!!WARNING!!2123!! Failed to write to file %s: %s" % (filename, e))
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

def dumpFile(filename, topilotlog=False):
    """ dump a given file to stdout or to pilotlog """

    if os.path.exists(filename):
        pUtil.tolog("Dumping file: %s (size: %d)" % (filename, os.path.getsize(filename)))
        try:
            f = open(filename, "r")
        except IOError, e:
            pUtil.tolog("!!WARNING!!4000!! Exception caught: %s" % (e))
        else:
            i = 0
            for line in f.readlines():
                i += 1
                line = line.rstrip()
                if topilotlog:
                    pUtil.tolog("%s" % (line))
                else:
                    print "%s" % (line)
            f.close()
            pUtil.tolog("Dumped %d lines from file %s" % (i, filename))
    else:
        pUtil.tolog("!!WARNING!!4000!! %s does not exist" % (filename))

def addToOSTransferDictionary(path, workdir, queuename, mode, si):
    """ Add the transferred file to the OS transfer file """

    # Get the OS name identifier
    os_name = si.getObjectstoreName(mode, queuename)
    pUtil.tolog("xx. os_name=%s"%str(os_name))

    # Get the name and path of the objectstore transfer dictionary file 
    file_name = getOSTransferDictionaryFilename()
    os_tr_path = os.path.join(workdir, file_name)
    pUtil.tolog("xx. os_tr_path=%s"%os_tr_path)

    # Does the transfer file exist already? If not, create it
    if not os.path.exists(os_tr_path):
        # Read back the existing dictionary
        dictionary = readJSON(os_tr_path)
        if not dictionary:
            pUtil.tolog("Failed to open OS transfer dictionary - will recreate it")
            dictionary = {}
    else:
        # Create a new dictionary
        dictionary = {}

    # Populate the dictionary
    if dictionary.has_key(os_name):
        l = dictionary[os_name]
        l.append(path)
    else:
        dictionary[os_name] = path

    pUtil.tolog("xx. dictionary=%s"%str(dictionary))

    # Store the dictionary
    if writeJSON(os_tr_path, dictionary):
        pUtil.tolog("Stored updated OS transfer dictionary: %s" % (os_tr_path))
    else:
        pUtil.tolog("!!WARNING!!2211!! Failed to store OS transfer dictionary")

def getObjectstoresList(queuename):
    """ Get the objectstores list from the proper queuedata for the relevant queue """
    # queuename is needed as long as objectstores field is not available in normal queuedata (temporary)

    objectstores = []

    # First try to get the objectstores field from the normal queuedata
    try:
        from pUtil import readpar
        _objectstores = readpar('objectstores')
    except:
        pUtil.tolog("Field \'objectstores\' not yet available in queuedata")
        _objectstores = None

    # Get the full info from AGIS
    if not _objectstores:

        filename = "q.json"
        # Has the file been downloaded already?
        if os.path.exists(filename):
            pUtil.tolog("File exists: %s" % (filename))
            ret = 0
            output = ""
        else:
            cmd = "curl --connect-timeout 20 --max-time 120 -sS \"http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all\" >%s" % (filename)
            pUtil.tolog("Executing command: %s" % (cmd))
            import commands
            ret, output = commands.getstatusoutput(cmd)

        if ret == 0:
            # Make sure the JSON file exists
            if os.path.exists(filename):
                # Load the dictionary
                dictionary = readJSON(filename)
                if dictionary != {}:
                    # Get the entry for queuename
                    try:
                        _d = dictionary[queuename]
                    except Exception, e:
                        pUtil.tolog("No entry for queue %s in JSON: %s" % (queuename, e))
                    else:
                        # Read the objectstores field
                        try:
                            _objectstores = _d['objectstores']
                        except Exception, e:
                            pUtil.tolog("!!WARNING!!2112!! %s" % (e))
                        else:
                            objectstores = _objectstores
                else:
                    pUtil.tolog("!!WARNING!!2120!! Failed to read dictionary from file %s" % (filename))
            else:
                pUtil.tolog("!!WARNING!!2122!! File does not exist: %s" % (filename))
        else:
            pUtil.tolog("!!WARNING!!2121!! Failed to download schedconfig JSON: %d, %s" % (ret, output))

    return objectstores

def getObjectstoresField(field, mode, queuename):
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
                pUtil.tolog("!!WARNING!!2222!! Failed to read field %s from objectstores list: %s" % (field, e))
    return value

def getObjectstorePath(mode, queuename):
    """ Return the path to the objectstore """
    # mode: https, eventservice, logs

    # Read the endpoint info from the queuedata
    os_endpoint = getObjectstoresField('os_endpoint', mode, queuename)
    os_bucket_endpoint = getObjectstoresField('os_bucket_endpoint', mode, queuename)
    if os_endpoint and os_bucket_endpoint and os_endpoint != "" and os_bucket_endpoint != "":
        path = os_endpoint + os_bucket_endpoint
    else:
        path = ""

    return path

def getObjectstoreName(mode, queuename):
    """ Return the objectstore name identifier """
    # E.g. CERN_OS_0

    return getObjectstoresField('os_name', mode, queuename)

def getOSNames(filename):
    """ Get the dictionary of objectstore os_names with populated buckets from the OS transfer dictionary """
    # Note: will return a dictionary of os_names identifiers to which files were actually transferred, along
    # with the names of the buckets where the files were transferred to
    # This function is used to generate the jobMetrics OS message (only the OS and bucket names are of interest)

    # os_names_dictionary
    # FORMAT: { 'os_name_id': ['os_bucket_name', ''], .. }
    # OS transfer dictionary
    # FORMAT: { 'os_name_id': { 'os_bucket_name': ['path', .. ], .. }, .. }
    # os_bucket_name: 'eventservice', 'logs' or 'http'

    os_names_dictionary = {}

    if os.path.exists(filename):
        # Get the OS transfer dictionary
        dictionary = readJSON(filename)
        if dictionary != {}:
            tmp_os_names_list = dictionary.keys()

            # Only report the os_name if there were files transferred to it
            for os_name in tmp_os_names_list:

                # Get the os_bucket_name list and populate the final dictionary
                os_bucket_name_list = dictionary[os_name].keys()
                for os_bucket_name in os_bucket_name_list:
                    n = len(dictionary[os_name][os_bucket_name])

                    pUtil.tolog("%s: %d file(s) transferred to bucket %s" % (os_name, n, os_bucket_name))
                    if n > 0:
                        if os_names_dictionary.has_key(os_name):
                            os_names_dictionary[os_name].append(os_bucket_name)
                        else:
                            os_names_dictionary[os_name] = [os_bucket_name]
        else:
            pUtil.tolog("!!WARNING!!3334!! OS transfer dictionary is empty")
    else:
        pUtil.tolog("!!WARNING!!3333!! OS transfer dictionary does not exist at: %s" % (filename))

    return os_names_dictionary

# print findLatestTRFLogFile(os.getcwd())

