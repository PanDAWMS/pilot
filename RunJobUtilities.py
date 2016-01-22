import commands
import os
import socket
import time
import re
import sys

from pUtil import timeStamp, debugInfo, tolog, readpar, verifyReleaseString,\
     isAnalysisJob, dumpOrderedItems, grep, getExperiment, getGUID,\
     getCmtconfig, timedCommand, getProperTimeout, removePattern, encode_string
from PilotErrors import PilotErrors
from FileStateClient import dumpFileStates, hasOnlyCopyToScratch
from SiteInformation import SiteInformation

# global variables
#siteroot = ""

def filterTCPString(TCPMessage):
    """ Remove any unwanted characters from the TCP message string and truncate if necessary """

    # sometimes a failed command will return html which end up in (e.g.) pilotErrorDiag, remove it
    if TCPMessage.upper().find("<HTML>") >= 0:
        # reset here. the error diag will be set in pilot.updatePandaServer()
        tolog("Found html in TCP message string (will be reset): %s" % (TCPMessage))
        TCPMessage = ""

    # remove any ;-signs which will cause the TCP message to become corrupt (see use of ; below)
    # and cause an exception in the TCP server (job will fail with no subprocesses error)
    TCPMessage = TCPMessage.replace(";"," ")

    # also remove any =-signs since they are interpreted as well
    TCPMessage = TCPMessage.replace("!=","ne")
    TCPMessage = TCPMessage.replace("="," ")

    # also remove any "-signs
    TCPMessage = TCPMessage.replace('"','')

    # truncate if necessary
    if len(TCPMessage) > 250:
        tolog("TCP message string will be truncated to size 250")
        tolog("Original TCP message string: %s" % (TCPMessage))
        TCPMessage = TCPMessage[:250]

    return TCPMessage

def updateJobInfo(job, server, port, logfile=None, final=False, latereg=False):
    """ send job status updates to local pilot TCP server, in the format
    of status=running;pid=2343; logfile is the file that contains
    some debug information, usually used in failure case """

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(120)
        s.connect((server, port))
    except Exception, e:
        tolog("!!WARNING!!2999!! updateJobInfo caught a socket/connect exception: %s" % str(e))
        return "NOTOK"

    msgdic = {}
    msgdic["pid"] = os.getpid()
    msgdic["pgrp"] = os.getpgrp()
    msgdic["jobid"] = job.jobId
    msgdic["status"] = job.result[0]
    msgdic["jobState"] = job.jobState
    msgdic["transecode"] = job.result[1]
    msgdic["pilotecode"] = job.result[2]
    msgdic["timeStageIn"] = job.timeStageIn
    msgdic["timeStageOut"] = job.timeStageOut
    msgdic["timeSetup"] = job.timeSetup
    msgdic["timeExe"] = job.timeExe
    msgdic["cpuTime"] = job.cpuConsumptionTime
    msgdic["cpuUnit"] = job.cpuConsumptionUnit
    msgdic["cpuConversionFactor"] = job.cpuConversionFactor
    msgdic["nEvents"] = job.nEvents
    msgdic["nEventsW"] = job.nEventsW
    msgdic["vmPeakMax"] = job.vmPeakMax
    msgdic["vmPeakMean"] = job.vmPeakMean
    msgdic["RSSMean"] = job.RSSMean
    msgdic["JEM"] = job.JEM
    msgdic["cmtconfig"] = getCmtconfig(job.cmtconfig)

    # hpc job status
    if job.mode:
        msgdic["mode"] = job.mode
    if job.hpcStatus:
        msgdic['hpcStatus'] = job.hpcStatus
    if job.refreshNow:
        msgdic['refreshNow'] = job.refreshNow
    if job.coreCount or job.coreCount == 0:
        msgdic['coreCount'] = job.coreCount

    # report FAX usage if at least one successful FAX transfer
    if job.filesWithFAX > 0:
        msgdic["filesWithFAX"] = job.filesWithFAX
    if job.filesWithoutFAX > 0:
        msgdic["filesWithoutFAX"] = job.filesWithoutFAX
    if job.bytesWithFAX > 0:
        msgdic["bytesWithFAX"] = job.bytesWithFAX
    if job.bytesWithoutFAX > 0:
        msgdic["bytesWithoutFAX"] = job.bytesWithoutFAX

    # report alternative stage-out usage if at least one successful stage-out to an alternative SE
    if job.filesAltStageOut > 0:
        msgdic["filesAltStageOut"] = job.filesAltStageOut
        tolog("filesAltStageOut=%d" % (job.filesAltStageOut))
    else:
        tolog("filesAltStageOut not set")
    if job.filesNormalStageOut > 0:
        msgdic["filesNormalStageOut"] = job.filesNormalStageOut
        tolog("filesNormalStageOut=%d" % (job.filesNormalStageOut))
    else:
        tolog("filesNormalStageOut not set")

    # truncate already now if necesary so not too much junk is sent back to the local pilot TCP server
    if job.pilotErrorDiag != None:
        # remove any unwanted characters from the string
        job.pilotErrorDiag = encode_string(job.pilotErrorDiag)
    msgdic["pilotErrorDiag"] = job.pilotErrorDiag

    # report trf error message if set
    if job.exeErrorDiag != "":
        # remove any unwanted characters from the string
        msgdic["exeErrorDiag"] = encode_string(job.exeErrorDiag)
        msgdic["exeErrorCode"] = job.exeErrorCode

    if logfile:
        msgdic["logfile"] = logfile

    # send the special setup string for the log transfer (on xrdcp systems)
    if job.spsetup:
        # temporarily remove = and ;-signs not to disrupt the TCP message (see ;-handling below)
        msgdic["spsetup"] = job.spsetup.replace(";", "^").replace("=", "!")
        tolog("Updated spsetup: %s" % (msgdic["spsetup"]))

    # set final job state (will be propagated to the job state file)
    if final:
        job.finalstate = getFinalState(job.result)
        tolog("Final payload state set to: %s" % (job.finalstate))
        msgdic["finalstate"] = job.finalstate
        if job.result[0] == "holding" and job.finalstate == "finished":
            if readpar('retry').upper() == "TRUE":
                tolog("This job is recoverable")
            else:
                tolog("This job is not recoverable since job recovery is switched off")

        # variables needed for later registration of output files
        # (log will be handled by the pilot)
        if latereg:
            latereg_str = "True"
        else:
            latereg_str = "False"
        msgdic["output_latereg"] = latereg_str

    msg = ''
    for k in msgdic.keys():
        msg += "%s=%s;" % (k, msgdic[k])

    tolog("About to send TCP message to main pilot thread of length = %d" % len(msg))
    if len(msg) > 4096:
        tolog("!!WARNING!!1234!! TCP message too long (cannot truncate easily without harming encoded message)")
    try:
        s.send(msg)
        tolog("(Sent)")
        tm = s.recv(1024)
        tolog("(Received)")
    except Exception, e:
        tolog("!!WARNING!!2999!! updateJobInfo caught a send/receive exception: %s" % str(e))
        return "NOTOK"
    else:
        s.settimeout(None)
        s.close()
        tolog("Successfully sent and received TCP message")

    return tm  # =OK or NOTOK

def getFinalState(result):
    """
    Figure out the final job state (finished or failed)
    Simplies job recovery
    """

    state = "failed"

    # job has failed if transExitCode != 0
    if result[1] != 0:
        state = "failed"
    else:
        error = PilotErrors()
        # job has finished if pilotErrorCode is in the allowed list or recoverable jobs
        if ((error.isRecoverableErrorCode(result[2])) or (result[2] == error.ERR_KILLSIGNAL and result[0] == "holding")):
            state = "finished"

    return state

def updatePilotServer(job, server, port, logfile=None, final=False, latereg=False):
    """ handle the local pilot TCP server updates """

    if not final:
        max_trials = 2
    else:
        max_trials = 10
    status = False

    trial = 1
    while trial <= max_trials:
        rt = updateJobInfo(job, server, port, logfile=logfile, final=final, latereg=latereg)
        if rt == "OK":
            tolog("Successfully updated local pilot TCP server at %s (Trial %d/%d)" % (timeStamp(), trial, max_trials))
            status = True
            break
        else:
            tolog("[Trial %d/%d] Failed to communicate with local pilot TCP server: %s" % (trial, max_trials, rt))
            time.sleep(5)
            trial += 1

    if not status:
        tolog("updatePilotServer failed")
        if final:
            tolog("!!FAILED!!3000!! Local pilot TCP server down, expecting pilot to fail job (cannot communicate final update back to main pilot thread)")

    return status

def getFileNamesFromString(s=""):
    """
    Return a list from the input string
    Ex. s = "a+b+c+", return: [a,b,c]
    """

    list = []
    if len(s) > 0:
        # remove any trailing + sign
        if s[-1] == "+":
            s = s[:-1]

        # create the list
        list = s.split("+")

        # remove the path from the file names
        for i in range(len(list)):
            list[i] = os.path.basename(list[i])

    return list

def getRemainingFiles(movedFileList, allFiles):
    """ Make a diff between the moved files and all files """

    # loop over all entries and remove those that are in the movedFileList
    for file in movedFileList:
        for i in range(len(allFiles)):
            if file == allFiles[i]:
                del allFiles[i]
                break

    return allFiles

def dumpOutput(filename):
    """ dump an extract from an ascii file """

    ret = ""
    if os.path.exists(filename):
        fsize = os.path.getsize(filename)
        tolog("Filename : %s" % (filename))
        tolog("File size: %s" % str(fsize))
        if fsize > 2**14: # 16k
            tolog("Begin output (first and last 8k)............................................")
            ret = commands.getoutput("head --bytes=8192 %s" % filename)
            if ret == "":
                tolog("[no output?]")
            else:
                # protect against corrupted files containing illegal chars
                try:
                    tolog(str(ret))
                except Exception, e:
                    tolog("!!WARNING!!3000!! Could not dump file: %s" % str(e))
            tolog("\n.... [snip] ....\n")
            ret = commands.getoutput("tail --bytes=8192 %s" % filename)
            if ret == "":
                tolog("[no output?]")
            else:
                # protect against corrupted files containing illegal chars
                try:
                    tolog(str(ret))
                except Exception, e:
                    tolog("!!WARNING!!3000!! Could not dump file: %s" % str(e))
        else:
            tolog("Begin output (all)..........................................................")
            ret = commands.getoutput("cat %s" % filename)
            if ret == "":
                tolog("[no output?]")
            else:
                # protect against corrupted files containing illegal chars
                try:
                    tolog(ret)
                except Exception, e:
                    tolog("!!WARNING!!3000!! Could not dump file: %s" % str(e))
        tolog("End output..................................................................")
    else:
        tolog("!!WARNING!!3000!! file %s does not exist" % (filename))

    return ret

def isCommandOk(cmd):
    """ return True if the command is returning exit code 0 """

    status = True
    tolog("Executing command: %s" % (cmd))
    a, b = commands.getstatusoutput(cmd)
    if a != 0:
        tolog("!!WARNING!!3000!! Command test failed with exit code %d: %s" % (a, b))
        status = False

    return status

def prepareInFiles(inFiles, filesizeIn, checksumIn):
    """ prepare the input files (remove non-valid names) """

    # if the file name list is modified, make sure that the file size and checksum lists are modified as well
    ins = []
    fIn = []
    cIn = []
    file_nr = -1
    if inFiles: # non empty list
        for inf in inFiles:
            file_nr += 1
            if inf and inf != 'NULL': # non-empty string and not NULL
                ins.append(inf)
                fIn.append(filesizeIn[file_nr])
                cIn.append(checksumIn[file_nr])
    if inFiles[0] != '':
        tolog("Input file(s): (%d in total)" % (len(inFiles)))
        dumpOrderedItems(inFiles)
    else:
        tolog("No input files for this job")

    return ins, fIn, cIn

def prepareOutFiles(outFiles, logFile, workdir, fullpath=False):
    """ verify and prepare and the output files for transfer """

    # fullpath = True means that the file in outFiles already has a full path, adding it to workdir is then not needed
    ec = 0
    pilotErrorDiag = ""
    outs = []
    modt = []

    from SiteMover import SiteMover
    for outf in outFiles:
        if outf and outf != 'NULL': # non-empty string and not NULL
            if (not os.path.isfile("%s/%s" % (workdir, outf)) and not fullpath) or (not os.path.isfile(outf) and fullpath): # expected output file is missing
                pilotErrorDiag = "Expected output file %s does not exist" % (outf)
                tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
                error = PilotErrors()
                ec = error.ERR_MISSINGOUTPUTFILE
                break
            else:
                tolog("outf = %s" % (outf))
                if fullpath:
                    # remove the full path here from outf
                    workdir = os.path.dirname(outf)
                    outf = os.path.basename(outf)

                outs.append(outf)

                # get the modification time for the file (needed by NG)
                modt.append(SiteMover.getModTime(workdir, outf))

                tolog("Output file(s):")
                try:
                    _ec, _rs = commands.getstatusoutput("ls -l %s/%s" % (workdir, outf))
                except Exception, e:
                    tolog(str(e))
                else:
                    tolog(_rs)

    if ec == 0:
        # create a dictionary of the output files with matched modification times (needed to create the NG OutputFiles.xml)
        outsDict = dict(zip(outs, modt))
        # add the log file with a fictious date since it has not been created yet
        outsDict[logFile] = ''
    else:
        outsDict = {}

    return ec, pilotErrorDiag, outs, outsDict

def convertMetadata4NG(filenameOUT, filenameIN, outsDict, dataset, datasetDict):
    """ convert the metadata-<jobId>.xml to NG format """
    # note: 'dataset' will only be used if datasetDict is None

    status = True

    # xml tags and conversion dictionaries
    _header = '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n<!-- ATLAS file meta-data catalog -->\n<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n'
    _tagsBEGIN_END = '<%s>%s</%s>\n'
    _tagBEGIN = '<%s>\n'
    _tagEND = '</%s>\n'
    _tagDICT = { 'ID':'guid', 'fsize':'size', 'md5sum':'md5sum', 'adler32':'ad32', 'name':'lfn', 'csumtypetobeset':'ad32', 'surl':'surl' }
    dic = {}
    dic['md5sum'] = '' # to avoid KeyError's with older python
    dic['adler32'] = ''
    dic['fsize'] = ''

    if os.path.exists(filenameIN):
        try:
            f = open(filenameIN, 'r')
        except Exception, e:
            tolog("!!WARNING!!1999!! Could not open file: %s, %s" % (filenameIN, str(e)))
            status = False
        else:
            # get the metadata
            xmlIN = f.read()
            f.close()

            xmlOUT = _header
            xmlOUT += _tagBEGIN % 'outputfiles'

            from xml.dom import minidom
            xmldoc = minidom.parseString(xmlIN)
            fileList = xmldoc.getElementsByTagName("File")

            # convert the metadata to NG format
            for _file in fileList:
                xmlOUT += _tagBEGIN % 'file'
                lfn = str(_file.getElementsByTagName("lfn")[0].getAttribute("name"))
                guid = str(_file.getAttribute("ID"))
                lrc_metadata_dom = _file.getElementsByTagName("metadata")
                for i in range(len(lrc_metadata_dom)):
                    _key = str(_file.getElementsByTagName("metadata")[i].getAttribute("att_name"))
                    _value = str(_file.getElementsByTagName("metadata")[i].getAttribute("att_value"))
                    # e.g. key = 'fsize', get the corresponding NG name ('size')
                    _keyNG = _tagDICT[_key]
                    xmlOUT += ' ' + _tagsBEGIN_END % (_keyNG, _value, _keyNG)
                xmlOUT += ' ' + _tagsBEGIN_END % ('guid', guid, 'guid')
                xmlOUT += ' ' + _tagsBEGIN_END % ('lfn', lfn, 'lfn')
                if datasetDict:
                    try:
                        xmlOUT += ' ' + _tagsBEGIN_END % ('dataset', datasetDict[lfn], 'dataset')
                    except Exception, e:
                        tolog("!!WARNING!!2999!! datasetDict could not be used: %s (using default dataset instead)" % str(e))
                        xmlOUT += ' ' + _tagsBEGIN_END % ('dataset', dataset, 'dataset')
                else:
                    xmlOUT += ' ' + _tagsBEGIN_END % ('dataset', dataset, 'dataset')
                xmlOUT += ' ' + _tagsBEGIN_END % ('date', outsDict[lfn], 'date')
                xmlOUT += _tagEND % 'file'

            xmlOUT += _tagEND % 'outputfiles'
            tolog("Converted xml for NorduGrid / CERNVM")

            # write the new metadata to the OutputFiles.xml
            try:
                f = open(filenameOUT, 'w')
            except Exception, e:
                tolog("!!WARNING!!1999!! Could not create output file: %s, %s" % (filenameOUT, str(e)))
                status = False
            else:
                f.write(xmlOUT)
                f.close()
    else:
        status = False

    return status

def getOutFilesGuids(outFiles, workdir, experiment, TURL=False):
    """ get the outFilesGuids from the PFC """

    ec = 0
    pilotErrorDiag = ""
    outFilesGuids = []

    # Get the experiment object and the GUID source filename
    thisExperiment = getExperiment(experiment)
    filename = thisExperiment.getGUIDSourceFilename()

    # If a source file should not be used (ie empty filename string), then generate the GUIDs here
    if filename == "":
        tolog("Pilot will generate GUIDs for the output files")
        for i in range (0, len(outFiles)):
            guid = getGUID()
            if guid == "":
                guid = "- GUID generation failed -"
            outFilesGuids.append(guid)

        return ec, pilotErrorDiag, outFilesGuids
    else:
        tolog("Pilot will get GUIDs for the output files from source: %s" % (filename))
        pfcFile = os.path.join(workdir, filename) #"%s/PoolFileCatalog.xml" % (workdir)

    # The PFC used for Event Service will be TURL based, use the corresponding file
    if TURL:
        pfcFile = pfcFile.replace(".xml", "TURL.xml")

    # Initialization: make sure the guid list has the same length as the file list
    for i in range (0, len(outFiles)):
        outFilesGuids.append(None)

    # make sure the PFC exists
    if os.path.isfile(pfcFile):
        from xml.dom import minidom
        xmldoc = minidom.parse(pfcFile)
        fileList = xmldoc.getElementsByTagName("File")
        for thisfile in fileList:
            gpfn = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
            guid = str(thisfile.getAttribute("ID"))
            for i in range(0, len(outFiles)):
                if outFiles[i] == gpfn:
                    outFilesGuids[i] = guid
    else:
        pilotErrorDiag = "PFC file does not exist: %s" % (pfcFile)
        tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
        error = PilotErrors()
        ec = error.ERR_MISSINGPFC

    return ec, pilotErrorDiag, outFilesGuids

def verifyMultiTrf(jobParameterList, jobHomePackageList, jobTrfList, jobAtlasRelease):
    """ make sure that a multi-trf (or single trf) job is properly setup """

    error = PilotErrors()

    ec = 0
    pilotErrorDiag = ""
    N_jobParameterList = len(jobParameterList)
    N_jobHomePackageList = len(jobHomePackageList)
    N_jobTrfList = len(jobTrfList)
    N_jobAtlasRelease = len(jobAtlasRelease)

    # test jobs have multiple atlas releases defined, but not real tasks
    if N_jobTrfList > N_jobAtlasRelease and N_jobAtlasRelease == 1:
        # jobAtlasRelease = ['14.0.0'] -> ['14.0.0', '14.0.0']
        jobAtlasRelease = jobAtlasRelease*N_jobTrfList
        N_jobAtlasRelease = len(jobAtlasRelease)

    if (N_jobParameterList == N_jobHomePackageList) and \
       (N_jobHomePackageList == N_jobTrfList) and \
       (N_jobTrfList == N_jobAtlasRelease):
        if N_jobAtlasRelease == 1:
            tolog("Multi-trf verification succeeded (single job)")
        else:
            tolog("Multi-trf verification succeeded")
    else:
        pilotErrorDiag = "Multi-trf verification failed: N(jobPars) eq %d, but N(homepackage,transformation,AtlasRelease) eq (%d,%d,%d)" %\
                         (N_jobParameterList, N_jobHomePackageList, N_jobTrfList, N_jobAtlasRelease)
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        ec = error.ERR_SETUPFAILURE

    return ec, pilotErrorDiag, jobAtlasRelease

def updateCopysetups(cmd3, transferType=None, useCT=None, directIn=None, useFileStager=None):
    """ Update the relevant copysetup fields for remote I/O or file stager """

    si = SiteInformation()

    _copysetupin = readpar('copysetupin')
    _copysetup = readpar('copysetup')

    if _copysetupin != "":
        si.updateCopysetup(cmd3, 'copysetupin', _copysetupin, transferType=transferType, useCT=useCT, directIn=directIn, useFileStager=useFileStager)
    else:
        si.updateCopysetup(cmd3, 'copysetup', _copysetup, transferType=transferType, useCT=useCT, directIn=directIn, useFileStager=useFileStager)

def addSPSetupToCmd(special_setup_cmd, cmd):
    """ Add the special command setup if it exists to the main run command """

    if special_setup_cmd != "":
        if not special_setup_cmd.endswith(";"):
            special_setup_cmd += ";"

        # the special command must be squeezed in before the trf but after the setup scripts
        # find the last ; and add the special setup there

        # in case of -f options, there's a possibility that the command can contain extra ;-signs, remove that part
        pos_f = cmd.rfind("-f ")
        if pos_f != -1:
            _cmd0 = cmd[:pos_f]
            _cmd1 = cmd[pos_f:]
        else:
            _cmd0 = cmd
            _cmd1 = ""

        last_semicolon = _cmd0.rfind(";")
        cmd = _cmd0[:last_semicolon] + ";" + special_setup_cmd[:-1] + _cmd0[last_semicolon:] + _cmd1
        tolog("Special setup command added to main run command")

    return cmd

def removeSkippedFromJobPars(fname, jobPars):
    """ remove skipped input files from jobPars """

    # get the skipped file names from the xml
    skipped = getLFNsFromSkippedXML(fname)

    if skipped == []:
        tolog("Did not find any skipped LFNs in: %s" % (fname))
    else:
        tolog("Removing skipped input files from jobPars")
        tolog("..skipped: %s" % str(skipped))
        tolog("..jobPars:\n%s" % (jobPars))
        for skip in skipped:
            tolog("..Removing: %s" % (skip))
            # try difference styles
            _skip = "\'%s\'," % (skip)
            if _skip in jobPars:
                jobPars = jobPars.replace(_skip,'')
                tolog('..Removed %s from jobPars' % (_skip))
            else:
                _skip = "\'%s\'" % (skip)
                if _skip in jobPars:
                    jobPars = jobPars.replace(_skip,'')
                    tolog('..Removed %s from jobPars' % (_skip))
                else:
                    _skip = "%s," % (skip)
                    if _skip in jobPars:
                        jobPars = jobPars.replace(skip,'')
                        tolog('..Removed %s from jobPars' % (skip))
                    else:
                        if skip in jobPars:
                            jobPars = jobPars.replace(skip,'')
                            print '..Removed %s from jobPars' % (skip)
                        else:
                            # nothing to remove
                            tolog("..Found nothing to remove from jobPars: %s" % (jobPars))
    return jobPars

def getLFNsFromSkippedXML(fname):
    """ extract the list of skipped files from the skipped xml """

    lfns = []
    try:
        f = open(fname, "r")
    except Exception, e:
        tolog("Warning: could not open skipped xml file: %s" % str(e))
    else:
        pre_xml = f.read()
        f.close()

        # add an XML header etc since the skipped xml is just an XML fragment
        # so that it can be processed
        xmlstr =  '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n'
        xmlstr += "<!-- Edited By POOL -->\n"
        xmlstr += '<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n'
        xmlstr += "<POOLFILECATALOG>\n"
        xmlstr += pre_xml
        xmlstr += "</POOLFILECATALOG>\n"

        from xml.dom import minidom
        xmldoc = minidom.parseString(xmlstr)
        fileList = xmldoc.getElementsByTagName("File")
        for thisfile in fileList:
            lfns.append(str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name")))

    return lfns

def setEnvVars(sitename):
    """ Set ATLAS_CONDDB if necessary """

    if not os.environ.has_key('ATLAS_CONDDB'):
        atlas_conddb = readpar('gatekeeper')
        if atlas_conddb != "to.be.set":
            os.environ["ATLAS_CONDDB"] = atlas_conddb
            tolog("Note: ATLAS_CONDDB was not set by the pilot wrapper script")
            tolog("The pilot has set ATLAS_CONDDB to: %s" % (atlas_conddb))
        else:
            tolog("Warning: ATLAS_CONDDB was not set by the pilot wrapper and schedconfig.gatekeeper value is to.be.set (pilot will take no action)")

    # set specially requested env vars
    os.environ["PANDA_SITE_NAME"] = sitename
    tolog("Set PANDA_SITE_NAME = %s" % (sitename))
    copytool = readpar("copytoolin")
    if copytool == "":
        copytool = readpar("copytool")
    if "^" in copytool:
        copytool = copytool.split("^")[0]
    os.environ["COPY_TOOL"] = copytool
    tolog("Set COPY_TOOL = %s" % (copytool))

def updateRunCommandList(runCommandList, pworkdir, jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO, hasInput):
    """ update the run command list if --directIn is no longer needed """
    # the method is using the file state dictionary

    # remove later
    dumpFileStates(pworkdir, jobId, ftype="input")

    # remove any instruction regarding tag file creation for event service jobs
    _runCommandList = []
    for cmd in runCommandList:
        if "--createTAGFileForES" in cmd:
            cmd = cmd.replace("--createTAGFileForES","")
        _runCommandList.append(cmd)
    runCommandList = _runCommandList

    # no need to continue if no input files
    if not hasInput:
        return runCommandList

    # are there only copy_to_scratch transfer modes in the file state dictionary?
    # if so, remove any lingering --directIn instruction
    only_copy_to_scratch = hasOnlyCopyToScratch(pworkdir, jobId)
    if only_copy_to_scratch:
#    if hasOnlyCopyToScratch(pworkdir, jobId): # python bug? does not work, have to use previous two lines?
        _runCommandList = []

        tolog("There are only copy_to_scratch transfer modes in file state dictionary")
        for cmd in runCommandList:
            # remove the --directIn string if present
            if "--directIn" in cmd:
                tolog("(Removing --directIn instruction from run command since it is not needed)")
                cmd = cmd.replace("--directIn", "")
            # remove the --useFileStager string if present
            if "--useFileStager" in cmd:
                tolog("(Removing --useFileStager instruction from run command since it is not needed)")
                cmd = cmd.replace("--useFileStager", "")
            # remove additional run options if creation of TURL based PFC failed
            if statusPFCTurl == False: # (note: can also be None, so do not use 'if not statusPFCTurl')
                if "--usePFCTurl" in cmd:
                    tolog("(Removing --usePFCTurl instruction from run command since it is not needed)")
                    cmd = cmd.replace(" --usePFCTurl", "")
                if not "--lfcHost" in cmd and analysisJob:
                    tolog("Adding lfcHost to run command")
                    cmd += ' --lfcHost %s' % (readpar('lfchost'))

            tolog("Updated run command: %s" % (cmd))
            _runCommandList.append(cmd)
    else:
        tolog("Nothing to update in run command list related to copy-to-scratch")
        _runCommandList = runCommandList

    # was FAX used as primary site mover in combination with direct I/O?
    if usedFAXandDirectIO == True:
        tolog("Since FAX was used as primary site mover in combination with direct I/O, the run command list need to be updated")
        _runCommandList2 = []

        for cmd in _runCommandList:
            # remove the --lfcHost
            if "--lfcHost" in cmd:
                _lfcHost = ' --lfcHost %s' % (readpar('lfchost'))
                cmd = cmd.replace(_lfcHost, '')
                tolog("(Removed the LFC host:%s)" % (_lfcHost))

            # remove the --oldPrefix
            if "--oldPrefix" in cmd:
                pattern = "(\-\-oldPrefix\ \S+)"
                cmd = removePattern(cmd, pattern)
                tolog("(Removed --oldPrefix pattern)")

            # remove the --newPrefix
            if "--newPrefix" in cmd:
                pattern = "(\-\-newPrefix\ \S+)"
                cmd = removePattern(cmd, pattern)
                tolog("(Removed --newPrefix pattern)")

            # add the --usePFCTurl if not there already
            if not "--usePFCTurl" in cmd and analysisJob:
                cmd += " --usePFCTurl"
                tolog("(Added --usePFCTurl)")

            tolog("Updated run command: %s" % (cmd))
            _runCommandList2.append(cmd)

        _runCommandList = _runCommandList2


    ### new movers quick integration: reuse usedFAXandDirectIO variable with special meaning
    ### to avoid any LFC and prefixes lookups in transformation scripts
    ### since new movers already form proper pfn values
    ### proper workflow is required: to be reimplemented later
    if usedFAXandDirectIO == 'newmover' or usedFAXandDirectIO == 'newmover-directaccess':
        tolog("updateRunCommandList(): use new movers logic")
        tolog("updateRunCommandList(): remove to be deprecated options (--lfcHost, --oldPrefix, --newPrefix) from command list")
        tolog("updateRunCommandList(): force to set --usePFCTurl")
        tolog("updateRunCommandList(): check directaccess mode if need (--directIn)")
        tolog("current runCommandList=%s" % _runCommandList)

        _runCommandList2 = []

        for cmd in _runCommandList:

            # remove the --lfcHost, --oldPrefix, --newPrefix
            # add --usePFCTurl

            if "--lfcHost" in cmd:
                cmd = removePattern(cmd, "(\-\-lfcHost\ \S+)")
                tolog("(Removed the --lfcHost)")

            if "--oldPrefix" in cmd:
                pattern = "(\-\-oldPrefix\ \S+)"
                cmd = removePattern(cmd, pattern)
                tolog("(Removed --oldPrefix pattern)")

            if "--newPrefix" in cmd:
                pattern = "(\-\-newPrefix\ \S+)"
                cmd = removePattern(cmd, pattern)
                tolog("(Removed --newPrefix pattern)")

            if "--usePFCTurl" not in cmd and analysisJob:
                cmd += " --usePFCTurl"
                tolog("(Added --usePFCTurl)")

            # add --directIn if need
            if usedFAXandDirectIO == 'newmover-directaccess':
                if "--directIn" not in cmd and analysisJob:
                    cmd += " --directIn"
                    tolog("(Added --directIn)")

            tolog("Updated run command: %s" % cmd)

            _runCommandList2.append(cmd)

        _runCommandList = _runCommandList2

    tolog("Dumping final input file states")
    dumpFileStates(pworkdir, jobId, ftype="input")

    return _runCommandList

def getStdoutFilename(workdir, _stdout, current_job_number, number_of_jobs):
    """ Return a proper stdout filename """

    if number_of_jobs > 1:
        _stdout = _stdout.replace(".txt", "_%d.txt" % (current_job_number))
    return os.path.join(workdir, _stdout)

def findVmPeaks(setup):
    """ Find the VmPeak values """

    vmPeakMax = 0
    vmPeakMean = 0
    RSSMean = 0

#    matched_lines = grep(["Py\:PerfMonSvc\s*INFO\s*VmPeak:\s*[0-9]"], stdout_filename)
#    pattern = "([0-9]+)"

#    # now extract the digits from the found lines
#    N = 0
#    vmPeaks = 0
#    for line in matched_lines:
#        _vmPeak = re.search(pattern, line)
#        if _vmPeak:
#            N += 1
#            vmPeak = _vmPeak.group(1)
#            if vmPeak > vmPeakMax:
#                vmPeakMax = vmPeak
#            vmPeaks += vmPeak

    # use the VmPeak script to get all values
    cmd = "%s python VmPeak.py >Pilot_VmPeak.txt" % (setup)
    try:
        ec, output = timedCommand(cmd, timeout=getProperTimeout(setup))
    except Exception, e:
        tolog("!!WARNING!!1111!! Failed to execute VmPeak script: %s" % (e))
    else:
        # now get the values from the deault file
        file_name = os.path.join(os.getcwd(), "VmPeak_values.txt")
        if ec == 0:
            if os.path.exists(file_name):
                try:
                    f = open(file_name, "r")
                except Exception, e:
                    tolog("!!WARNING!!1111!! Failed to open VmPeak values file: %s" % (e))
                else:
                    _values = f.read()
                    f.close()
                    values = _values.split(",")
                    try:
                        vmPeakMax = int(values[0])
                        vmPeakMean = int(values[1])
                        RSSMean = int(values[2])
                    except Exception, e:
                        tolog("!!WARNING!!1111!! VmPeak exception: %s" % (e))
            else:
                tolog("Note: File %s does not exist" % (file_name))
        else:
            tolog("!!WARNING!!1111!! VmPeak script returned: %d, %s" % (ec, output))

    tolog("[VmPeak] vmPeakMax=%d" % (vmPeakMax))
    tolog("[VmPeak] vmPeakMean=%d" % (vmPeakMean))
    tolog("[VmPeak] RSSMean=%d" % (RSSMean))

    return vmPeakMax, vmPeakMean, RSSMean

def getSourceSetup(runCommand):
    """ Extract the source setup command from the run command """

    if type(runCommand) is dict:
            to_str = " ".join(runCommand['environment'])
            to_str = "%s %s %s %s" % (to_str,  runCommand["interpreter"], runCommand["payload"], runCommand["parameters"])
            runCommand = to_str

    setup = ""
    pattern = re.compile(r"(source /.+?;)")
    s = re.findall(pattern, runCommand)
    if s != []:
        setup = s[0]

    return setup
