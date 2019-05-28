# This module contains functions related to file handling.

import os
from time import time
from commands import getoutput

from pUtil import tolog, convert, readpar

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
            dump(dictionary, fp, sort_keys=True, indent=4, separators=(',', ': '))
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

def writeFile(filename, contents, mode='w'):
    """ Write the contents to filename """

    status = False
    try:
        f = open(filename, mode)
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

def addToOSTransferDictionary(file_name, workdir, os_bucket_id, os_ddmendpoint):
    """ Add the transferred file to the OS transfer file """
    # Note: we don't want to store the file name since potentially there can be a large number of files
    # We only store a file number count
    # We still need to know the file name in order to figure out which bucket it belongs to (using a hash of the file name)
    # The hash will be added to the os_ddmendpoint (e.g. 'atlas_logs' -> 'atlas_logs_E2')

    # Only proceed if os_bucket_id and os_ddmendpoint have values
    if os_bucket_id and os_bucket_id != "" and os_ddmendpoint and os_ddmendpoint != "":

        # Get the name and path of the objectstore transfer dictionary file
        os_tr_path = os.path.join(workdir, getOSTransferDictionaryFilename())

        # Create a hash of the file name, two char long, and then the final bucket endpoint
        _endpoint = getHashedBucketEndpoint(os_ddmendpoint, file_name)

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
        if dictionary.has_key(os_bucket_id):
            if dictionary[os_bucket_id].has_key(_endpoint):
                # Increase the file count
                dictionary[os_bucket_id][_endpoint] += 1
            else:
                # One file has been stored in this endpoint
                dictionary[os_bucket_id][_endpoint] = 1
        else:
            # One file has been stored in this endpoint
            dictionary[os_bucket_id] = {_endpoint: 1}

        # Store the dictionary
        if writeJSON(os_tr_path, dictionary):
            tolog("Stored updated OS transfer dictionary: %s" % (os_tr_path))
        else:
            tolog("!!WARNING!!2211!! Failed to store OS transfer dictionary")

    else:
        tolog("Cannot add to OS transfer dictionary due to unset values (os_name/os_ddmendpoint)")

def getOSTransferDictionary(filename):
    """ Get the dictionary of objectstore os_bucket_ids with populated buckets from the OS transfer dictionary """
    # Note: will return a dictionary of os_bucket_ids identifiers to which files were actually transferred, along
    # with the names of the buckets where the files were transferred to
    # This function is used to generate the jobMetrics OS message (only the OS and bucket endpoints are of interest)

    # os_bucket_ids_dictionary
    # FORMAT: { 'os_bucket_id': ['os_bucket_endpoint', ''], .. }
    # OS transfer dictionary
    # FORMAT: { 'os_bucket_id': { 'os_bucket_endpoint': <number of transferred files>, .. }, .. }

    os_bucket_ids_dictionary = {}

    if os.path.exists(filename):
        # Get the OS transfer dictionary
        dictionary = readJSON(filename)
        if dictionary != {}:
            tmp_os_bucket_ids_list = dictionary.keys()

            # Only report the os_bucket_id if there were files transferred to it
            for os_bucket_id in tmp_os_bucket_ids_list:

                # Get the os_bucket_endpoint list and populate the final dictionary
                os_bucket_endpoint_list = dictionary[os_bucket_id].keys()
                for os_bucket_endpoint in os_bucket_endpoint_list:
                    n = dictionary[os_bucket_id][os_bucket_endpoint]

                    tolog("OS bucket id %s: %d file(s) transferred to bucket %s" % (os_bucket_id, n, os_bucket_endpoint))
                    if n > 0:
                        if os_bucket_ids_dictionary.has_key(os_bucket_id):
                            os_bucket_ids_dictionary[os_bucket_id].append(os_bucket_endpoint)
                        else:
                            os_bucket_ids_dictionary[os_bucket_id] = [os_bucket_endpoint]
        else:
            tolog("!!WARNING!!3334!! OS transfer dictionary is empty")
    else:
        tolog("!!WARNING!!3333!! OS transfer dictionary does not exist at: %s" % (filename))

    return os_bucket_ids_dictionary

def getPilotErrorReportFilename(workdir):
    """ Return the filename for the pilot error report """
    # This file should be placed in the pilot init dir

    return os.path.join(workdir, "pilot_error_report.json")

def updatePilotErrorReport(pilotErrorCode, pilotErrorDiag, priority, jobID, workdir):
    """ Write pilot error info to file """
    # Report format:
    # { jobID1: { priority1: [{ pilotErrorCode1:<nr>, pilotErrorDiag1:<str> }, .. ], .. }, .. }
    # The pilot will report only the first of the highest priority error when it reports the error at the end of the job
    # Use the following priority convention:
    # "0": highest priority [e.g. errors that originate from the main pilot module (unless otherwise necessary)]
    # "1": high priority [e.g. errors that originate from the Monitor module (-||-)]
    # "2": normal priority [errors that originate from other modules (-||-)]
    # etc

    # Convert to string if integer is sent for priority
    if type(priority) != str:
        priority = str(priority)

    filename = getPilotErrorReportFilename(workdir)
    if os.path.exists(filename):
        # The file already exists, read it back (with unicode to utf-8 conversion)
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
        # The file already exists, read it back (with unicode to utf-8 conversion)
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

def discoverAdditionalOutputFiles(output_file_list, workdir, datasets_list, scope_list):
    """ Have any additional output files been produced by the trf? If so, add them to the output file list """

    # In case an output file has reached the max output size, the payload can spill over the remaining events to
    # a new file following the naming scheme: original_output_filename.extension_N, where N >= 1
    # Any additional output file will have the same dataset as the original file

    from glob import glob
    from re import compile, findall
    new_output_file_list = []
    new_datasets_list = []
    new_scope_list = []
    found_new_files = False

    # Create a lookup dictionaries
    dataset_dict = dict(zip(output_file_list, datasets_list))
    scope_dict = dict(zip(output_file_list, scope_list))

    # Loop over all output files
    for output_file in output_file_list:

        # Add the original file and dataset
        new_output_file_list.append(output_file)
        new_datasets_list.append(dataset_dict[output_file])
        new_scope_list.append(scope_dict[output_file])

        # Get a list of all files whose names begin with <output_file>
        files = glob(os.path.join(workdir, "%s*" % (output_file)))
        for _file in files:

            # Exclude the original file
            output_file_full_path = os.path.join(workdir, output_file)
            if _file != output_file_full_path:

                # Create the search pattern
                pattern = compile(r'(%s\.?\_\d+)' % (output_file_full_path))
                found = findall(pattern, _file)

                # Add the file name (not full path) of the found file, if found
                if found:
                    found_new_files = True
                    new_file = os.path.basename(found[0])
                    new_output_file_list.append(new_file)
                    dataset = dataset_dict[output_file]
                    new_datasets_list.append(dataset)
                    scope = scope_dict[output_file]
                    new_scope_list.append(scope)
                    tolog("Discovered additional output file: %s (dataset = %s, scope = %s)" % (new_file, dataset, scope))

    return new_output_file_list, new_datasets_list, new_scope_list

def getJobReportFileName(workDir):
    """ Return the name of the jobReport, full path """

    return os.path.join(workDir, "jobReport.json")

# WARNING: EXPERIMENT SPECIFIC AND ALSO DEFINED IN ERRORDIAGNOSIS
def getJobReport(workDir):
    """ Get the jobReport.json dictionary """
    # Note: always return at least an empty dictionary

    dictionary = {}
    filename = getJobReportFileName(workDir)
    if os.path.exists(filename):
        # the jobReport file exists, read it back (with unicode to utf-8 conversion)
        dictionary = getJSONDictionary(filename)
        if not dictionary: # getJSONDictionary() can return None
            dictionary = {}
    else:
        tolog("!!WARNING!!1111!! File %s does not exist" % (filename))

    return dictionary

def removeNoOutputFiles(workdir, outFiles, allowNoOutput, outFilesGuids):
    """ Remove files from output file list if they are listed in allowNoOutput and do not exist """

    _outFiles = []
    _guids = []
    i = 0
    for filename in outFiles:
        path = os.path.join(workdir, filename)

        if filename in allowNoOutput:
            if os.path.exists(path):
                tolog("File %s is listed in allowNoOutput but exists (will not be removed from list of files to be staged-out)" % (filename))
                _outFiles.append(filename)
                _guids.append(outFilesGuids[i])
            else:
                tolog("File %s is listed in allowNoOutput and does not exist (will be removed from list of files to be staged-out)" % (filename))
        else:
            if os.path.exists(path):
                tolog("File %s is not listed in allowNoOutput (will be staged-out)" % (filename))
            else:
                tolog("!!WARNING!!4343!! File %s is not listed in allowNoOutput and does not exist (job will fail)" % (filename))
            _outFiles.append(filename) # Append here, fail later
            _guids.append(outFilesGuids[i])
        i += 1

    return _outFiles

def extractOutputFiles(analysisJob, workdir, allowNoOutput, outFiles, outFilesGuids):
    """ Extract the output files from the JSON if possible """
    try:
        if not analysisJob:
            extracted_output_files, extracted_guids = extractOutputFilesFromJSON(workdir, allowNoOutput)
        else:
            if allowNoOutput == []:
                tolog("Will not extract output files from jobReport for user job (and allowNoOut list is empty)")
                extracted_output_files = []
                extracted_guids = []
            else:
                # Remove the files listed in allowNoOutput if they don't exist
                extracted_output_files, extracted_guids = removeNoOutputFiles(workdir, outFiles, allowNoOutput, outFilesGuids)
    except Exception, e:
        tolog("!!WARNING!!2327!! Exception caught: %s" % (e))
        extracted_output_files = []
        extracted_guids = []
    return extracted_output_files, extracted_guids

def addToJobReport(workDir, key, value, section=None, subsection=None):
    """ Add the key with value to the jobReport """
    # Add the key and value to the corresponding section in set

    # Note: the function reads the jobReport, adds the new key (or overwrites it) then saves the updated jobReport again (overwrite)

    try:
        jobReport_dictionary = getJobReport(workDir)
        if jobReport_dictionary != {}:
            # Add the new key and value
            if section:
                if jobReport_dictionary.has_key(section):
                    if subsection:
                        if jobReport_dictionary[section].has_key(subsection):
                            jobReport_dictionary[section][subsection][key] = value
                        else:
                            tolog("!!WARNING!!2325!! jobReport does not have subsection=%s in the expected location; will not add key=%s" % (subsection, key))
                    else:
                        jobReport_dictionary[section][key] = value
                else:
                    tolog("!!WARNING!!2324!! jobReport does not have section=%s in the expected location; will not add key=%s" % (section, key))
            else:
                jobReport_dictionary[key] = value

            # Overwrite the jobReport with the updated dictionary
            filename = getJobReportFileName(workDir)
            if not writeJSON(filename, jobReport_dictionary):
                tolog("!!WARNING!!2323!! Failed to write updated jobReport")
        else:
            tolog("jobReport not available, will not add new key: %s" % (key))
    except Exception, e:
        tolog("!!WARNING!!2321!! Exception caught: %s" % (e))

def extractOutputFilesFromJSON(workDir, allowNoOutput):
    """ In case the trf has produced additional output files (spill-over), extract all output files from the jobReport """
    # Note: ignore files with nentries = 0

    output_files = []
    guids = []
    tolog("Extracting output files from jobReport")

    jobReport_dictionary = getJobReport(workDir)
    if jobReport_dictionary != {}:

        if jobReport_dictionary.has_key('files'):
            file_dictionary = jobReport_dictionary['files']
            if file_dictionary.has_key('output'):
                output_file_list = file_dictionary['output']
                for f_dictionary in output_file_list:
                    if f_dictionary.has_key('subFiles'):
                        subFiles_list = f_dictionary['subFiles']
                        for f_names_dictionary in subFiles_list:
                            if f_names_dictionary.has_key('name'):# and f_names_dictionary.has_key('nentries'):
                                # Only add the file is nentries > 0
                                nentries = f_names_dictionary.get("nentries", "UNDEFINED")
                                if type(nentries) == int and nentries > 0:
                                    output_files.append(f_names_dictionary['name'])

                                    # Also get the file guid
                                    if f_names_dictionary.has_key('file_guid'):
                                        guids.append(f_names_dictionary['file_guid'])
                                    else:
                                        tolog("!!WARNING!!1212!! Did not find any guid for this file: %s (will be generated)" % (f_names_dictionary['name']))
                                        guids.append(None)
                                else:
                                    # Only ignore the file if it is allowed to be ignored
                                    if not type(nentries) == int:
                                        tolog("!!WARNING!!4542!! nentries is not a number: %s" % str(nentries))

                                    # Special handling for origName._NNN
                                    # origName._NNN are unmerged files dynamically produced by AthenaMP. Job definition doesn't
                                    # explicitly specify those names but only the base names, thus allowNoOutput contains only base names
                                    # in this case. We want to ignore origName._NNN when allowNoOutput=['origName']
                                    from re import compile
                                    allowNoOutputEx = [compile(s+'\.?_\d+$') for s in allowNoOutput]
                                    if f_names_dictionary['name'] in allowNoOutput or any(patt.match(f_names_dictionary['name']) for patt in allowNoOutputEx):
                                        tolog("Ignoring file %s since nentries=%s" % (f_names_dictionary['name'], str(nentries)))
                                    else:
                                        tolog("Will not ignore empty file %s since file is not in allowNoOutput list" % (f_names_dictionary['name']))
                                        output_files.append(f_names_dictionary['name'])

                                        # Also get the file guid
                                        if f_names_dictionary.has_key('file_guid'):
                                            guids.append(f_names_dictionary['file_guid'])
                                        else:
                                            tolog("!!WARNING!!1212!! Did not find any guid for this file: %s (will be generated)" % (f_names_dictionary['name']))
                                            guids.append(None)

                            else:
                                tolog("No such key: name/nentries")
                    else:
                        tolog("No such key: subFiles")
            else:
                tolog("No such key: output")
        else:
            tolog("No such key: files")

        if len(output_files) == 0:
            tolog("No output files found in jobReport")
        else:
            tolog("Output files found in jobReport: %s" % (output_files))

    return output_files, guids

def getDestinationDBlockItems(filename, original_output_files, destinationDBlockToken, destinationDblock, scopeOut):
    """ Return destinationDBlock items (destinationDBlockToken, destinationDblock, scope) for given file """

    # Note: in case of spill-over file, the file name will end with _NNN or ._NNN. This will be removed from the file name
    # so that the destinationDBlockToken of the original output file will be used
    filename = filterSpilloverFilename(filename)

    # Which is the corresponding destinationDBlockToken for this file?
    _destinationDBlockToken = getOutputFileItem(filename, destinationDBlockToken, original_output_files)

    # Which is the corresponding destinationDblock for this file?
    _destinationDblock = getOutputFileItem(filename, destinationDblock, original_output_files)

    # Which is the corresponding scopeOut for this file?
    _scopeOut = getOutputFileItem(filename, scopeOut, original_output_files)

    return _destinationDBlockToken, _destinationDblock, _scopeOut

def getOutputFileItem(filename, outputFileItem, original_output_files):
    """ Which is the corresponding destinationDBlock item for this file? """

    # Find the file number (all lists are ordered)
    i = 0
    if filename in original_output_files:
        for f in original_output_files:
            if f == filename:
                break
            i += 1

        _outputFileItem = outputFileItem[i]
    else:
        tolog("!!WARNING!!4545!! File %s not found in original output file list (will use outputFileItem[0])" % (filename))
        _outputFileItem = outputFileItem[0]

    return _outputFileItem

def filterSpilloverFilename(filename):
    """ Remove any unwanted spill-over filename endings (i.e. _NNN or ._NNN) """

    # Create the search pattern
    from re import compile, findall
    pattern = compile(r'(\.?\_\d+)')
    found = findall(pattern, filename)
    if found:
        # Make sure that the _NNN substring is at the end of the string
        for f in found:
            if filename.endswith(f):
                # Do not use replace here since it might cut away something from inside the filename and not only at the end
                filename = filename[:-len(f)]

    return filename

def getDirSize(d):
    """ Return the size of directory d using du -sk """

    tolog("Checking size of work dir: %s" % (d))
    from commands import getoutput
    size_str = getoutput("du -sk %s" % (d))
    size = 0

    # E.g., size_str = "900\t/scratch-local/nilsson/pilot3z"
    try:
        # Remove tab and path, and convert to int (and B)
        size = int(size_str.split("\t")[0])*1024
    except Exception, e:
        tolog("!!WARNING!!4343!! Failed to convert to int: %s" % (e))
    else:
        tolog("Size of directory %s: %d B" % (d, size))

    return size

def addToTotalSize(path, total_size):
    """ Add the size of file with 'path' to the total size of all in/output files """

    if os.path.exists(path):
        from SiteMover import SiteMover
        sitemover = SiteMover()

        # Get the file size
        fsize = sitemover.getLocalFileSize(path)
        tolog("Size of file %s: %s B" % (path, fsize))
        if fsize != "":
            total_size += long(fsize)
    else:
        tolog("Skipping file %s in work dir size check since it is not present" % (path))

    return total_size

def storeWorkDirSize(workdir_size, pilot_initdir, job, correction=True):
    """ Store the measured remaining disk space """
    # If correction=True, then input and output file sizes will be deducated

    filename = os.path.join(pilot_initdir, getWorkDirSizeFilename(job.jobId))
    dictionary = {} # FORMAT: { 'workdir_size': [value1, value2, ..] }
    workdir_size_list = []

    if os.path.exists(filename):
        # Read back the dictionary
        dictionary = readJSON(filename)
        if dictionary != {}:
            workdir_size_list = dictionary['workdir_size']
        else:
            tolog("!!WARNING!!4555!! Failed to read back remaining disk space from file: %s" % (filename))

    # Correct for any input and output files
    if correction:

        total_size = 0L # B

        if os.path.exists(job.workdir):
            # Find out which input and output files have been transferred and add their sizes to the total size
            # (Note: output files should also be removed from the total size since outputfilesize is added in the task def)

            # First remove the log file from the output file list
            outFiles = []
            for f in job.outFiles:
                if not job.logFile in f:
                    outFiles.append(f)

            # Then update the file list in case additional output files have been produced
            # Note: don't do this deduction since it is not known by the task definition
            #outFiles, dummy, dummy = discoverAdditionalOutputFiles(outFiles, job.workdir, job.destinationDblock, job.scopeOut)

            file_list = job.inFiles + outFiles
            for f in file_list:
                if f != "":
                    total_size = addToTotalSize(os.path.join(job.workdir, f), total_size)

            tolog("Total size of present input+output files: %d B (work dir size: %d B)" % (total_size, workdir_size))
            workdir_size -= total_size
        else:
            tolog("WARNING: Can not correct for input/output files since workdir does not exist: %s" % (job.workdir))

    # Append the new value to the list and store it
    workdir_size_list.append(workdir_size)
    dictionary = {'workdir_size': workdir_size_list}
    status = writeJSON(filename, dictionary)
    if status:
        tolog("Stored %d B in file %s" % (workdir_size, filename))

    return status

def getWorkDirSizeFilename(jobId):
    """ Return the name of the workdir_size.json file """

    return "workdir_size-%s.json" % (jobId)

def getMaxWorkDirSize(path, jobId):
    """ Return the maximum disk space used by a payload """

    filename = os.path.join(path, getWorkDirSizeFilename(jobId))
    maxdirsize = 0

    if os.path.exists(filename):
        # Read back the workdir space dictionary
        dictionary = readJSON(filename)
        if dictionary != {}:
            # Get the workdir space list
            try:
                workdir_size_list = dictionary['workdir_size']
            except Exception, e:
                tolog("!!WARNING!!4557!! Could not read back work dir space list: %s" % (e))
            else:
                # Get the maximum value from the list
                maxdirsize = max(workdir_size_list)
        else:
            tolog("!!WARNING!!4555!! Failed to read back work dir space from file: %s" % (filename))
    else:
        tolog("!!WARNING!!4556!! No such file: %s" % (filename))

    return maxdirsize

# ATLAS specific
def getNumberOfEvents(workDir):
    """ Extract the number of events from the job report """

    Nevents = {} # FORMAT: { format : total_events, .. }

    jobReport_dictionary = getJobReport(workDir)
    if jobReport_dictionary != {}:

        if jobReport_dictionary.has_key('resource'):
            resource_dictionary = jobReport_dictionary['resource']
            if resource_dictionary.has_key('executor'):
                executor_dictionary = resource_dictionary['executor']
                for format in executor_dictionary.keys(): # "RAWtoESD", ..
                    if executor_dictionary[format].has_key('nevents'):
                        if Nevents.has_key(format):
                            print executor_dictionary[format]['nevents']
                            Nevents[format] += executor_dictionary[format]['nevents']
                        else:
                            Nevents[format] = executor_dictionary[format]['nevents']
                    else:
                        tolog("Format %s has no such key: nevents" % (format))
            else:
                tolog("No such key: executor")
        else:
            tolog("No such key: resource")

    # Now find the largest number of events among the different formats
    if Nevents != {}:
        try:
            Nmax = max(Nevents.values())
        except Exception, e:
            tolog("!!WARNING!!2323!! Exception caught: %s" % (e))
            Nmax = 0
    else:
        tolog("Did not find the number of events in the job report")
        Nmax = 0

    return Nmax

# ATLAS specific
def getDBInfo(workDir):
    """ Extract and add up the DB info from the job report """

    # Input:  workDir (location of jobReport.json
    # Output: dbTime, dbData [converted strings, e.g. "dbData=105077960 dbTime=251.42"]

    dbTime = 0
    dbData = 0L

    jobReport_dictionary = getJobReport(workDir)
    if jobReport_dictionary != {}:

        if jobReport_dictionary.has_key('resource'):
            resource_dictionary = jobReport_dictionary['resource']
            if resource_dictionary.has_key('executor'):
                executor_dictionary = resource_dictionary['executor']
                for format in executor_dictionary.keys(): # "RAWtoESD", ..
                    if executor_dictionary[format].has_key('dbData'):
                        try:
                            dbData += executor_dictionary[format]['dbData']
                        except:
                            pass
                    else:
                        tolog("Format %s has no such key: dbData" % (format))
                    if executor_dictionary[format].has_key('dbTime'):
                        try:
                            dbTime += executor_dictionary[format]['dbTime']
                        except:
                            pass
                    else:
                        tolog("Format %s has no such key: dbTime" % (format))
            else:
                tolog("No such key: executor")
        else:
            tolog("No such key: resource")

    if dbData != 0L:
        dbDataS = "%s" % (dbData)
    else:
        dbDataS = ""
    if dbTime != 0:
        dbTimeS = "%.2f" % (dbTime)
    else:
        dbTimeS = ""
    return dbTimeS, dbDataS

# ATLAS specific
def getCPUTimes(workDir):
    """ Extract and add up the total CPU times from the job report """
    # Note: this is used with Event Service jobs

    # Input:  workDir (location of jobReport.json)
    # Output: cpuCU (unit), totalCPUTime, conversionFactor

    totalCPUTime = 0L

    jobReport_dictionary = getJobReport(workDir)
    if jobReport_dictionary != {}:
        if jobReport_dictionary.has_key('resource'):
            resource_dictionary = jobReport_dictionary['resource']
            if resource_dictionary.has_key('executor'):
                executor_dictionary = resource_dictionary['executor']
                for format in executor_dictionary.keys(): # "RAWtoESD", ..
                    if executor_dictionary[format].has_key('cpuTime'):
                        try:
                            totalCPUTime += executor_dictionary[format]['cpuTime']
                        except:
                            pass
                    else:
                        tolog("Format %s has no such key: cpuTime" % (format))
            else:
                tolog("No such key: executor")
        else:
            tolog("No such key: resource")

    conversionFactor = 1.0
    cpuCU = "s"

    return cpuCU, totalCPUTime, conversionFactor

def getDirectAccess(analyjob=False):
    """ Should direct i/o be used, and which type of direct i/o """

    directInLAN = useDirectAccessLAN()
    directInWAN = useDirectAccessWAN()
    directInType = 'None'

    directIn = False
    if directInLAN:
        directInType = 'LAN'
        directIn = True
    if directInWAN:
    # if (directInWAN and not analyjob) or (directInWAN and directInLAN and analyjob):
        directInType = 'WAN' # Overrides LAN if both booleans are set to True
        directIn = True

    return directIn, directInType

def _useDirectAccess(LAN=True, WAN=False):
    """ Should direct i/o be used over LAN or WAN? """

    useDA = False

    if LAN:
        par = 'direct_access_lan'
    elif WAN:
        par = 'direct_access_wan'
    else:
        tolog("!!WARNING!!3443!! Bad LAN/WAN combination: LAN=%s, WAN=%s" % (str(LAN), str(WAN)))
        par = ''

    if par != '':
        da = readpar(par)
        if da:
            da = da.lower()
            if da == "true":
                useDA = True

    return useDA

def useDirectAccessLAN():
    """ Should direct i/o be used over LAN? """

    return _useDirectAccess(LAN=True, WAN=False)

def useDirectAccessWAN():
    """ Should direct i/o be used over WAN? """

    return _useDirectAccess(LAN=False, WAN=True)

def getReplicaDictionaryFromXML(workdir, pfc_name="PoolFileCatalog.xml"):
    """ Return the replica information from a PFC """
    # NOTE: Currently this function only returns LFNs and PFNs

    ec = 0
    pilotErrorDiag = ""
    replica_dictionary = {} # FORMAT: { <lfn1>:{'pfn':<pfn1>, ..}, .. }
    pfcFile = os.path.join(workdir, pfc_name)

    if not os.path.exists(pfcFile):
        tolog("!!WARNING!!3332!! No such file: %s" % pfcFile)
        return None

    # make sure the PFC exists
    from xml.dom import minidom
    xmldoc = minidom.parse(pfcFile)
    fileList = xmldoc.getElementsByTagName("File")
    for thisfile in fileList:
        #lfn = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
        pfn = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
        lfn = os.path.basename(pfn)
        replica_dictionary[lfn] = {}
        replica_dictionary[lfn]['pfn'] = pfn

    return replica_dictionary

def touch(path):
    """
    Touch a file and update mtime in case the file exists.

    :param path:
    :return:
    """

    with open(path, 'a'):
        os.utime(path, None)

def getOsTimesTuple(workdir):
    """ Read os.times() from a txt file and convert it back to a proper os.times() tuple again """
    # This function is used to calculate the cpu consumption time. The t0_times.txt file is created just before the
    # payload is executed in

    times = []
    failed = False
    path = os.path.join(workdir, 't0_times.txt')

    if os.path.exists(path):
        with open(path, 'r') as f:
            for t in f.read().split():
                # remove any initial (, trailing ) or ,
                a = t.strip('(').strip(')').strip(',')
                try:
                    times.append(float(a))
                except ValueError as e:
                    tolog("!!WARNING!!1212!! Exception caught: offending value=%s (cannot convert to float)" % (e))
                    failed = True
                    break

            if not failed:
                # consistency check
                if len(times) == 5:
                    return tuple(times)
                else:
                    tolog("!!WARNING!!1222!! os.times() tuple has wrong length (not 5): %s" % str(times))
            else:
                tolog("!!WARNING!!1222!! Failed to convert os.times() txt file to tuple - CPU consumption meausurement cannot be done")
                return None
    else:
        tolog("t0 file does not exist - probably the payload was not executed")

    return None

def get_files(pattern="*.log"):

    files = []
    stdout = getoutput("find . -name %s" % pattern)
    if stdout:
        # remove last \n if present
        if stdout.endswith('\n'):
            stdout = stdout[:-1]
        files = stdout.split('\n')

    # make sure all files exist
    return verified_files(files)

def tail(filename, lines=10):

    return getoutput('tail -%d %s' % (lines, filename))

def find_latest_modified_file(list_of_files):

    # make sure all files exist
    list_of_files = verified_files(list_of_files)

    latest_file = max(list_of_files, key=os.path.getmtime)
    try:
        mtime = int(os.path.getmtime(latest_file))
    except Exception as e:
        tolog("!!WARNING!!2323!! Int conversion failed for mod time: %s" % e)
        mtime = None

    return latest_file, mtime

def verified_files(file_list):

    # return a list of files that actually exist
    return [f for f in file_list if os.path.exists(f)]
