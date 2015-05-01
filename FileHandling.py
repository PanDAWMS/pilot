# This module contains functions related to file handling.

import os
import time
import json

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
    """ Open json file and return its dictionary with unicode to utf-8 conversion """

    d = None
    f = openFile(filename, 'r')
    if f:
        try:
            d = json.load(f)
        except Exception, e:
            tolog("!!WARNING!!2222!! Failed to load json dictionary: %s" % (e))
        else:
            f.close()

            # Try to convert the dictionary from unicode to utf-8
            if d != {}:
                try:
                    d = convert(d)
                except Exception, e:
                    tolog("!!WARNING!!2996!! Failed to convert dictionary from unicode to utf-8: %s, %s" % (d, e))
            else:
                tolog("!!WARNING!!2995!! Load function returned empty JSON dictionary: %s" % (filename))
    return d

    # MOVE TO FileHandling
    def writeJSON(self, file_name, dictionary):
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

    def readJSON(self, file_name):
        """ Read a dictionary from a JSON file """

        dictionary = {}
        from json import load
        try:
            fp = open(file_name, 'r')
        except Exception, e:
            tolog("!!WARNING!!2334!! Failed to open file %s: %s" % (file_name, e))
        else:
            # Read the dictionary
            try:
                dictionary = load(fp)
            except Exception, e:
                tolog("!!WARNING!!2332!! Failed to read dictionary from file %s: %s" % (file_name, e))
            else:
                tolog("Read dictionary from file %s" % (file_name))            
            fp.close()

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
        tolog("Dumping file: %s (size: %d)" % (filename, os.path.getsize(filename)))
        try:
            f = open(filename, "r")
        except IOError, e:
            tolog("!!WARNING!!4000!! Exception caught: %s" % (e))
        else:
            i = 0
            for line in f.readlines():
                i += 1
                line = line.rstrip()
                if topilotlog:
                    tolog("%s" % (line))
                else:
                    print "%s" % (line)
            f.close()
            tolog("Dumped %d lines from file %s" % (i, filename))
    else:
        tolog("!!WARNING!!4000!! %s does not exist" % (filename))

# print findLatestTRFLogFile(os.getcwd())

