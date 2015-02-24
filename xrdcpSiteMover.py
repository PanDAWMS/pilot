#!/usr/bin/env python

# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wguan@cern.ch>, 2014

import os, re
import commands
from time import time

from TimerCommand import TimerCommand

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, getSiteInformation, extractFilePaths, getExperiment
from FileStateClient import updateFileState
from SiteInformation import SiteInformation

# placing the import lfc here breaks compilation on non-lcg sites
# import lfc

class xrdcpSiteMover(SiteMover.SiteMover):
    """ SiteMover that uses xrdcp for both get and put """
    # no registration is done
    copyCommand = "xrdcp"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
    has_chmod = False
    timeout = 3600

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path.strip()
        self.__isSetuped = False
        self._defaultSetup = None
        self.__experiment = None

    def get_timeout(self):
        return self.timeout

    def log(self, errorLog):
        tolog(errorLog)

    def getLocalROOTSetup(self, si):
        """ Build command to prepend the xrdcp command [xrdcp will in general not be known in a given site] """
        return si.getLocalROOTSetup()

    def getSetup(self):
        """ Return the setup string (pacman setup os setup script) for the copy command used by the mover """
        _setup_str = ""
        self._setup = self._setup.strip()
        tolog("self setup: %s" % self._setup)
        if self._setup and self._setup != "" and self._setup.strip() != "":
            if not self._setup.endswith(";"):
                self._setup += ";"
            if not "alias" in self._setup:
                if "atlasLocalSetup.sh" in self._setup and "--quiet" not in self._setup:
                    self._setup = self._setup.replace("atlasLocalSetup.sh", "atlasLocalSetup.sh --quiet")
                if self._setup.startswith("export") or self._setup.startswith("source"):
                    _setup_str = "%s" % self._setup
                else:
                    _setup_str = "source %s" % self._setup
            else:
                _setup_str = self._setup

        if _setup_str != "":
            tolog("Using setup: %s" % (_setup_str))

        return _setup_str

    def verifySetupCommand(self, _setupStr):
        """ Make sure the setup command exists """

        statusRet = 0
        outputRet={}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None

        # remove any '-signs
        _setupStr = _setupStr.replace("'", "")
        self.log("Will verify: %s" % (_setupStr))

        if _setupStr != "" and "source " in _setupStr:
            # first extract the file paths from the source command(s)
            setupPaths = extractFilePaths(_setupStr)

            # only run test if string begins with an "/"
            if setupPaths:
                # verify that the file paths actually exists
                for setupPath in setupPaths:
                    if "-" in setupPath:
                        continue
                    if os.path.exists(setupPath):
                        self.log("File %s has been verified" % (setupPath))
                    else:
                        outputRet["errorLog"] = errorLog = "No such file or directory: %s" % (setupPath)
                        self.log('!!WARNING!!2991!! %s' % (errorLog))
                        statusRet = PilotErrors.ERR_NOSUCHFILE
                        break
            else:
                # nothing left to test
                pass
        else:
            self.log("Nothing to verify in setup: %s (either empty string or no source command)" % (_setupStr))

        return statusRet, outputRet

    def verifySetupProxy(self, _setupStr, experiment):
        #check do we have a valid proxy

        # get the experiment object
        thisExperiment = getExperiment(experiment)

        status, output = thisExperiment.verifyProxy(envsetup=_setupStr)
        return status, output

    def verifySetup(self, _setupStr, experiment, proxycheck=False):
        statusRet, outputRet = self.verifySetupCommand(_setupStr)
        if statusRet != 0:
            #self.__sendReport('RFCP_FAIL', self._variables['report'])
            outputRet["report"]["clientState"] = "RFCP_FAIL"
            return statusRet, outputRet

        command = _setupStr
        if command != "" and not command.endswith(';'):
            command = command + ";"
        command += " which " + self.copyCommand
        status, output = commands.getstatusoutput(command)
        self.log("Execute command:  %s" % command)
        self.log("Status: %s, Output: %s" % (status, output))
        if status != 0:
            self.log(self.copyCommand +" is not found in envsetup: " + _setupStr)
            #self.__sendReport('RFCP_FAIL', self._variables['report'])
            outputRet["report"]["clientState"] = "RFCP_FAIL"
            outputRet["errorLog"] = output
            return status, outputRet

        if proxycheck:
            status, outputLog = self.verifySetupProxy(_setupStr, experiment)
            if status != 0:
                outputRet["errorLog"] = outputLog
                outputRet["report"]["clientState"] = 'PROXYFAIL'
                return status, outputRet

        return status, outputRet

    def setup(self, experiment):
        """ setup env """
        if self.__isSetuped:
            return 0, None
        self.__experiment = experiment
        thisExperiment = getExperiment(experiment)
        self.useTracingService = thisExperiment.useTracingService()
        si = getSiteInformation(experiment)
        self._defaultSetup = self.getLocalROOTSetup(si)

        _setupStr = self.getSetup()

        # get the user proxy if available
        envsetupTest = _setupStr.strip()
        if envsetupTest != "" and not envsetupTest.endswith(';'):
            envsetupTest += ";"
        if os.environ.has_key('X509_USER_PROXY'):
            envsetupTest += " export X509_USER_PROXY=%s;" % (os.environ['X509_USER_PROXY'])

        self.log("to verify site setup: %s " % envsetupTest)
        status, output = self.verifySetup(envsetupTest, experiment)
        self.log("site setup verifying: status: %s, output: %s" % (status, output["errorLog"]))
        if status == 0:
            self._setup = envsetupTest
            self.__isSetuped = True
            return status, output
        else:
            if self._defaultSetup:
                #try to use default setup
                self.log("Try to use default envsetup")
                envsetupTest = self._defaultSetup.strip()
                if envsetupTest != "" and not envsetupTest.endswith(';'):
                     envsetupTest += ";"
                if os.environ.has_key('X509_USER_PROXY'):
                     envsetupTest += " export X509_USER_PROXY=%s;" % (os.environ['X509_USER_PROXY'])

                self.log("verify default setup: %s " % envsetupTest)
                status, output = self.verifySetup(envsetupTest, experiment)
                self.log("default setup verifying: status: %s, output: %s" % (status, output["errorLog"]))
                if status == 0:
                    self._setup = envsetupTest
                    self.__isSetuped = True
                    return status, output

        return status, output

    def fixStageInPath(self, path):
        """Fix the path"""
        statusRet = 0
        outputRet={}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None

        siteInformation = SiteInformation()

        cpt = siteInformation.getCopyTool(stageIn=True)
        tolog("Site mover will use get command: %s, %s" % (cpt))

        # figure out which copyprefix to use (use the PFN to figure out where the file is and then use the appropriate copyprefix)
        # e.g. copyprefix=srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/.. use copyprefix root://eosatlas.cern.ch/ to build the TURL src_loc_pfn
        # full example:
        # Using copyprefixin = srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1
        # TURL=root://eosatlas.cern.ch//eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1

        ret_path = siteInformation.getCopyPrefixPath(path, stageIn=True)
        if not ret_path.startswith("root:"):
            errorLog = "Failed to use copyprefix to convert the current path to local path."
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            outputRet["errorLog"] = errorLog
            outputRet["report"]["clientState"] = 'PSTAGE_FAIL'
            statusRet = PilotErrors.ERR_STAGEINFAILED

        tolog("PFN=%s" % (path))
        tolog("TURL=%s" % (ret_path))
        outputRet['path'] = ret_path

        return statusRet, outputRet

    def getStageInMode(self, lfn, prodDBlockToken):
        # should the root file be copied or read directly by athena?
        status = 0
        output={}
        output["errorLog"] = None
        output["report"] = {}
        output["report"]["clientState"] = None

        output["transfer_mode"] = None

        isRootFileName = self.isRootFileName(lfn)

        siteInformation = SiteInformation()
        directIn, transfer_mode = siteInformation.getDirectInAccessMode(prodDBlockToken, isRootFileName)
        if transfer_mode:
            #updateFileState(lfn, workDir, jobId, mode="transfer_mode", state=transfer_mode, type="input")
            output["transfer_mode"] = transfer_mode
        if directIn:
            output["report"]["clientState"] = 'FOUND_ROOT'
            output["report"]['relativeStart'] = None
            output["report"]['transferStart'] = None

            return PilotErrors.ERR_DIRECTIOFILE, output

        return 0, output

    def stageInFile(self, source, destination):
        """StageIn the file. should be implementated by different site mover."""
        statusRet = 0
        outputRet = {}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None

        self.log("StageIn files started.")
        _cmd_str = '%s xrdcp -np %s %s' % (self._setup, source, destination)

        # update job setup script
        thisExperiment = getExperiment(self.__experiment)
        # add the full stage-out command to the job setup script
        to_script = _cmd_str.replace(destination, "`pwd`/%s" % os.path.basename(destination))
        to_script = to_script.lstrip(' ') # remove any initial spaces
        if to_script.startswith('/'):
            to_script = 'source ' + to_script
        thisExperiment.updateJobSetupScript(os.path.dirname(destination), to_script=to_script)

        self.log('Executing command: %s' % (_cmd_str))
        s = -1
        o = '(not defined)'
        t0 = os.times()
        outputRet["report"]['relativeStart'] = time()
        outputRet["report"]['transferStart'] = time()
        try:
            timerCommand = TimerCommand(_cmd_str)
            s, o = timerCommand.run(timeout=self.timeout)
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught by stageInFile(): %s" % (str(e)))
            o = str(e)
        t1 = os.times()
        t = t1[4] - t0[4]
        self.log("Command finished after %f s: %s" % (t, o.replace('\n', ' ')))

        if s == 0:
            self.log("Stagein succeeded")
        else:
            self.log("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            o = o.replace('\n', ' ')
            #check_syserr(s, o)
            self.log("!!WARNING!!2990!! get_data failed. Status=%s Output=%s" % (s, str(o)))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(destination)
            if not _status:
                self.log("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            statusRet = PilotErrors.ERR_STAGEINFAILED
            outputRet["report"]["clientState"] = 'COPY_FAIL'

        return statusRet, outputRet

    def verifyStageIN(self, sourceFile, sourceSize, sourceChecksum, destFile):
        """Verify file stagin successfull"""

        statusRet = 0
        outputRet={}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None
        outputRet["report"]['validateStart'] = time()

        self.log("Verify file Staging: source: %s, sourceSize: %s, sourceChecksum: %s, destFile: %s" % (sourceFile, sourceSize, sourceChecksum, destFile))
        if sourceChecksum == 0 and sourceSize ==0:
            return statusRet, outputRet

        # get the checksum type (md5sum or adler32)
        if sourceChecksum != 0 and sourceChecksum != "":
            csumtype = self.getChecksumType(sourceChecksum)
        else:
            csumtype = "default"

        self.log("Getting destination file(%s) information." % destFile)
        status, output = self.getLocalFileInfo(destFile, checksumType=csumtype)
        if status != 0:
            self.log("Failed to get local file information")
            outputRet["report"]["clientState"] = "FILE_INFO_FAIL"
            outputRet["errorLog"] = output["errorLog"]

            _status = self.removeLocal(destFile)
            self.log("Remove local file.")
            if not _status:
                self.log("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return status, outputRet

        destSize = output["size"]
        destChecksum = output["checksum"]

        self.log("Destination file information: file: %s, size: %s, checksum: %s" % (destFile, destSize, destChecksum))
        # compare remote and local file size
        if long(sourceSize) != 0 and long(destSize) != long(sourceSize):
            errorLog = "Remote and local file sizes do not match for %s (%s != %s)" %\
                      (os.path.basename(sourceFile), str(destSize), str(sourceSize))
            self.log("!!WARNING!!2990!! %s" % (errorLog))

            outputRet["errorLog"] = errorLog
            outputRet["report"]["clientState"] = "WRONG_SIZE"
            status = self.removeLocal(destFile)
            if not status:
                self.log("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return PilotErrors.ERR_GETWRONGSIZE, outputRet

        # compare remote and local file checksum
        if sourceChecksum and str(destChecksum) != str(sourceChecksum) and not self.isDummyChecksum(sourceChecksum):
            outputRet["errorLog"] = errorLog = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                     (csumtype, os.path.basename(sourceFile),  destChecksum, sourceChecksum)
            self.log("!!WARNING!!2990!! %s" % (errorLog))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(destFile)
            if not _status:
                self.log("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            if csumtype == "adler32":
                outputRet["report"]["clientState"] = "AD_MISMATCH"
                return PilotErrors.ERR_GETADMISMATCH, outputRet
            else:
                outputRet["report"]["clientState"] = "MD5_MISMATCH"
                return PilotErrors.ERR_GETMD5MISMATCH, outputRet

        self.log("Verify staging done.")
        outputRet["report"]["clientState"] = "DONE"
        return statusRet, outputRet

    def stageIn(self, source, destination, sourceSize, sourceChecksum, experiment):
        """Stage in the source file"""

        statusRet = 0
        outputRet ={}
        outputRet["errorLog"] = None
        outputRet["report"] = None

        status, output = self.setup(experiment)
        if status !=0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        status, output = self.fixStageInPath(source)
        if status != 0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        source = output['path']

        status, output = self.stageInFile(source, destination)
        if status !=0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        status, output = self.verifyStageIN(source, sourceSize, sourceChecksum, destination)
        statusRet = status
        outputRet["errorLog"] = output["errorLog"]
        outputRet["report"] = output["report"]
        return statusRet, outputRet

    def getLocalFileInfo(self, fileName, checksumType="default", date=None):
        """ Return exit code (0 if OK), file size and checksum of a local file, as well as as date string if requested """
        # note that date is mutable
        statusRet = 0
        outputRet = {}
        outputRet["errorLog"] = ""
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None

        outputRet["size"] = 0
        outputRet["checksum"] = ""
        outputRet["checksumType"] = checksumType

        self.log("Getting local File(%s) info." % fileName)
        # does the file exist?
        if not os.path.isfile(fileName):
            if fileName.find("DBRelease") >= 0 and os.path.exists(os.path.dirname(fileName)):
                outputRet["errorLog"] = errorLog = "DBRelease file missing: %s" % (fileNameame)
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_MISSDBREL, outputRet
            else:
                outputRet["errorLog"] = errorLog = "No such file or directory: %s" % (fileName)
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_MISSINGLOCALFILE, outputRet

            # get the modification time if needed and store it in the mutable object
            if date:
                date = SiteMover.getModTime(os.path.dirname(fileName), os.path.basename(fileName))

        # get the file size
        try:
            self.log("Executing getsize() for file: %s" % (fileName))
            outputRet["size"] = fsize = str(os.path.getsize(fileName))
        except OSError, e:
            outputRet["errorLog"] = errorLog = "Could not get file size: %s" % str(e)
            tolog("!!WARNING!!2999!! %s" % (errorLog))
            return PilotErrors.ERR_FAILEDSIZELOCAL, outputRet
        else:
            if fsize == "0":
                outputRet["errorLog"] = errorLog = "Encountered zero file size for file %s" % (fileName)
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_ZEROFILESIZE, outputRet
            else:
                self.log("Got file size: %s" % (fsize))

        # get the checksum
        if checksumType == "adler32" or checksumType == "default":
            self.log("Executing adler32() for file: %s" % (fileName))
            outputRet["checksum"] = fchecksum = SiteMover.SiteMover.adler32(fileName)
            if fchecksum == '00000001': # "%08x" % 1L
                outputRet["errorLog"] = errorLog = "Adler32 failed (returned 1)"
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_FAILEDADLOCAL, outputRet
            else:
                self.log("Got adler32 checksum: %s" % (fchecksum))
        else:
            _cmd = '%s %s' % (CMD_CHECKSUM, fileName)
            self.log("Executing command: %s" % (_cmd))
            try:
                s, o = commands.getstatusoutput(_cmd)
            except Exception, e:
                s = -1
                o = str(e)
                self.log("!!WARNING!!2999!! Exception caught in getstatusoutput: %s" % (o))
            if s != 0:
                o = o.replace('\n', ' ')
                check_syserr(s, o)
                outputRet["errorLog"] = errorLog = "Error running checksum command (%s): %s" % (CMD_CHECKSUM, o)
                self.log("!!WARNING!!2999!! %s" % (errorLog))
                return PilotErrors.ERR_FAILEDMD5LOCAL, outputRet
            outputRet["checksum"] = fchecksum = o.split()[0]
            self.log("Got checksum: %s" % (fchecksum))

        return 0, outputRet

    def fixStageOutPath(self, path):
        """Fix the path"""
        statusRet = 0
        outputRet={}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None

        siteInformation = SiteInformation()

        cpt = siteInformation.getCopyTool(stageIn=False)
        tolog("Site mover will use get command: %s, %s" % (cpt))

        # figure out which copyprefix to use (use the PFN to figure out where the file is and then use the appropriate copyprefix)
        # e.g. copyprefix=srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/.. use copyprefix root://eosatlas.cern.ch/ to build the TURL src_loc_pfn
        # full example:
        # Using copyprefixin = srm://srm-eosatlas.cern.ch,srm://srm-atlas.cern.ch^root://eosatlas.cern.ch/,root://castoratlas-xrdssl/
        # PFN=srm://srm-eosatlas.cern.ch/eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1
        # TURL=root://eosatlas.cern.ch//eos/atlas/atlasdatadisk/rucio/mc12_8TeV/8d/c0/EVNT.01212395._000004.pool.root.1

        ret_path = siteInformation.getCopyPrefixPath(path, stageIn=False)
        if not ret_path.startswith("root:"):
            errorLog = "Failed to use copyprefix to convert the current path to local path."
            tolog("!!WARNING!!1777!! %s" % (errorLog))
            outputRet["errorLog"] = errorLog
            outputRet["report"]["clientState"] = 'PSTAGE_FAIL'
            statusRet = PilotErrors.ERR_STAGEINFAILED

        tolog("PFN=%s" % (path))
        tolog("TURL=%s" % (ret_path))
        outputRet['path'] = ret_path

        return statusRet, outputRet

    def stageOutFile(self, source, destination, token=None):
        """Stage out the file. Should be implementated by different site mover"""
        statusRet = 0
        outputRet = {}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None
        outputRet["output"] = None


        command = "%s xrdcp -h" % (self._setup)
        status_local, output_local = commands.getstatusoutput(command)
        tolog("Execute command(%s) to decide whether -adler or --cksum adler32 to be used." % command)
        tolog("status: %s, output: %s" % (status_local, output_local))
        checksum_option = ""
        if "-adler" in output_local:
            checksum_option = " -adler "
        elif "--cksum" in output_local:
            checksum_option = " --cksum adler32 "
        #checksum_option = " -adler " # currently use this one. --cksum will fail on some sites
        if checksum_option != "":
            tolog("Use (%s) to get the checksum" % checksum_option)
        else:
            tolog("Cannot find -adler nor --cksum. will not use checksum")
        #checksum_option = " -adler " # currently use this one. --cksum will fail on some sites

        # surl is the same as putfile
        _cmd_str = '%s xrdcp -np -f %s %s %s' % (self._setup, checksum_option, source, destination)


        tolog("Executing command: %s" % (_cmd_str))
        ec = -1
        t0 = os.times()
        o = '(not defined)'
        outputRet["report"]['relativeStart'] = time()
        outputRet["report"]['transferStart'] =  time()
        try:
            timerCommand = TimerCommand(_cmd_str)
            ec, o = timerCommand.run(timeout=self.timeout)
        except Exception, e:
            tolog("!!WARNING!!2999!! xrdcp threw an exception: %s" % (o))
            o = str(e)
        outputRet["report"]['validateStart'] = time()
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        tolog("ec = %d, output = %s" % (ec, o.replace("\n"," ")))

        if ec != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            #check_syserr(ec, o)
            tolog('!!WARNING!!2990!! Stage Out failed: Status=%d Output=%s' % (ec, str(o.replace("\n"," "))))

            status, output = self.errorToReport(o, t, source, stageMethod="stageOut")
            if status == PilotErrors.ERR_FILEEXIST:
                return status, output

            # check if file was partially transferred, if so, remove it
            _ec, removeOutput = self.removeRemoteFile(destination)
            if not _ec :
                self.log("Failed to remove file ") # i.e. do not retry stage-out
           
            return status, output
        else:
            outputRet["output"] = o
        return statusRet, outputRet

    def getRemoteFileChecksum(self, full_surl, checksumType):
        """ get checksum with xrdadler32 command """
        remote_checksum = None
        output = None

        cmd = "%s xrdadler32 %s" % (self._setup, full_surl)
        tolog("Executing command: %s" % (cmd))
        try:
            ec, output = commands.getstatusoutput(cmd)
        except Exception, e:
            tolog("Warning: (Exception caught) xrdadler32 failed: %s" % (e))
            output = None
        else:
            if ec != 0 or "[fail]" in output:
                tolog("Warning: xrdadler32 failed: %d, %s" % (ec, output))
            else:
                tolog("output: %s" % output)
                try:
                    remote_checksum = output.split()[-2]
                except:
                    tolog("!!WARNING!!1998!! Cannot extract checksum from output: %s" % (output))
                if not remote_checksum.isalnum():
                    tolog("!!WARNING!!1998!! Failed to extract alphanumeric checksum string from output: %s" % (output))
                    remote_checksum = None
        return remote_checksum

    def getRemoteFileChecksumFromOutput(self, output):
        """ get checksum from xrdcp --chksum command output"""
        remote_checksum = None
        # get remote checksum from the command output
        if "xrootd" in output or "XRootD" in output:
            status = False
            # define the search patterns
            if "md5:" in output:
                checksum_pstr = r"md5: ([a-zA-Z0-9]+)"
                checksum_pattern = re.compile(checksum_pstr)
                status = True
            elif "adler32:" in output:
                checksum_pstr = r"adler32: ([a-zA-Z0-9]+)"
                checksum_pattern = re.compile(checksum_pstr)
                status = True
            else:
                tolog("!!WARNING!!2999!! Checksum info not found in xrdcp output: %s" % (output))

            if status:
                # grab the checksum from the output
                _checksum = re.findall(checksum_pattern, output)
                if len(_checksum) > 0:
                    remote_checksum = _checksum[0]

                    # note: there's a bug in xrdcp which will generate non-fixed length adler checksums; checksums can be
                    # of length 7. In that case add a "0" to the beginning of the string
                    if "adler32:" in output:
                        # verify string size length
                        if len(remote_checksum) == 7:
                            tolog("!!WARNING!!1111!! Adding 0 to beginning of checksum (xrdcp returned a length 7 checksum): %s" % (remote_checksum))
                            remote_checksum = "0" + remote_checksum
                        elif len(remote_checksum) == 6:
                            tolog("!!WARNING!!1111!! Adding 00 to beginning of checksum (xrdcp returned a length 6 checksum): %s" % (remote_checksum))
                            remote_checksum = "00" + remote_checksum
                        elif len(remote_checksum) == 5:
                            tolog("!!WARNING!!1111!! Adding 000 to beginning of checksum (xrdcp returned a length 5 checksum): %s" % (remote_checksum))
                            remote_checksum = "000" + remote_checksum
                        elif len(remote_checksum) == 4:
                            tolog("!!WARNING!!1111!! Adding 0000 to beginning of checksum (xrdcp returned a length 4 checksum): %s" % (remote_checksum))
                            remote_checksum = "0000" + remote_checksum

                    tolog("Copy command returned checksum: %s" % (remote_checksum))
                else:
                    tolog("!!WARNING!!2999!! checksum search failed: pattern (%s) not found in: %s" % (checksum_pstr, output))
                    remote_checksum = None
        else:
            tolog("!!WARNING!!2999!! Unexpected xrdcp output: %s" % (output))

        return remote_checksum


    def getRemoteFileSize(self, full_surl):
        """ extract checksum and file size from xrd ls output """

        remote_fsize = None
        # For xrdcp site mover, not implementation.

        return remote_fsize

    def verifyStageOut(self, sourceFile, sourceSize, sourceChecksum, checksumType, destFile, destChecksum=None, destSize=None):
        """Verify file stagout successfull"""
        statusRet = 0
        outputRet = {}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None
        outputRet["verified"] = False

        self.log("verifying stageout")
        status = 0
        if destChecksum == None:
            status, output = self.getRemoteFileInfo(destFile, checksumType)
            errorLog = output["errorLog"]
            destSize = output["size"]
            destChecksum = output["checksum"]
            destChecksumType = output["checksumType"]

        self.log("Remote checksum: %s" % str(destChecksum))
        self.log("Local checksum: %s" % str(sourceChecksum))

        if status == 0:
            if destChecksum:
                if str(sourceChecksum) != str(destChecksum):
                    outputRet["errorLog"] = errorLog = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                     (checksumType, os.path.basename(destFile), destChecksum, sourceChecksum)
                    self.log("!!WARNING!!1800!! %s" % (errorLog))
                    if checksumType == "adler32":
                        outputRet["report"]["clientState"] = 'AD_MISMATCH'
                        return PilotErrors.ERR_PUTADMISMATCH, outputRet
                    else:
                        outputRet["report"]["clientState"] = 'MD5_MISMATCH'
                        return PilotErrors.ERR_PUTMD5MISMATCH, outputRet
                else:
                    self.log("Remote and local checksums verified")
                    outputRet["verified"] = verified = True
            else:
                    # if the checksum could not be verified (as is the case for non-dCache sites) test the file size instead
                    if destSize:
                        self.log("Local file size: %s" % (sourceSize))

                        if destSize and destSize != "" and sourceSize != "" and sourceSize:
                            if sourceSize != destSize:
                                outputRet["errorLog"] = errorLog = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                                 (sourceFile, str(destSize), str(sourceSize))
                                self.log('!!WARNING!!2999!! %s' % (errorLog))
                                outputRet['report']["clientState"] = 'FS_MISMATCH'
                                return PilotErrors.ERR_PUTWRONGSIZE, outputRet
                            else:
                                 self.log("Remote and local file sizes verified")
                                 outputRet['verified'] = True
                        else:
                             self.log("Skipped file size test")
        else:
            self.log("Failed to get Remote file information: %s" % ())

        if outputRet['verified'] != True:
            # fail at this point
            outputRet["errorLog"] = errorLog = "Neither checksum nor file size could be verified (failing job)"
            self.log('!!WARNING!!2999!! %s' % (errorLog))
            outputRet['report']["clientState"] = 'NOFILEVERIFICATION'
            return PilotErrors.ERR_NOFILEVERIFICATION, outputRet

        self.log("verifying stageout done.")
        outputRet["report"]["clientState"] = "DONE"
        return statusRet, outputRet

    def removeRemoteFile(self, full_surl):
        """ Remove remote file"""
        # No function to remove the remote file
        ec = -2
        rs = "No function to remote the remote file"
        return ec, rs


    def getRemoteFileInfo(self, destFile, checksumType):
        """ Get Remote file info. Should be implementated by different site mover"""
        status = 0
        outputRet = {}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None
        outputRet["size"] = None
        outputRet["checksum"] = None
        outputRet["checksumType"] = checksumType

        checksum = None
        fileSize = None

        checksum = self.getRemoteFileChecksum(destFile, checksumType)
        if checksum == None:
            fileSize = self.getRemoteFileSize(destFile)

        outputRet["size"] = fileSize
        outputRet["checksum"] = checksum

        return status, outputRet

    def stageOut(self, source, destination, token, experiment):
        """Stage in the source file"""
        statusRet = 0
        outputRet ={}
        outputRet["errorLog"] = None
        outputRet["report"] = None

        status, output = self.setup(experiment)
        if status !=0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        status, output = self.getLocalFileInfo(source)
        if status !=0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        sourceSize = output["size"]
        sourceChecksum = output["checksum"]
        checksumType = output["checksumType"]
        if checksumType == "default":
            checksumType = "adler32"

        status, output = self.fixStageOutPath(destination)
        if status != 0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        destination = output['path']

        status, output = self.stageOutFile(source, destination, token)
        if status !=0:
            statusRet = status
            outputRet["errorLog"] = output["errorLog"]
            outputRet["report"] = output["report"]
            return statusRet, outputRet

        destChecksum = self.getRemoteFileChecksumFromOutput(output["output"])
        status, output = self.verifyStageOut(source, sourceSize, sourceChecksum, checksumType, destination, destChecksum=destChecksum, destSize=None)
        statusRet = status
        outputRet["errorLog"] = output["errorLog"]
        outputRet["report"] = output["report"]
        outputRet["size"] = sourceSize
        outputRet["checksum"] = sourceChecksum
        return statusRet, outputRet

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'xrdcp', lfn, guid)


        status, output = self.getStageInMode(lfn, prodDBlockToken)
        if output["transfer_mode"]:
            updateFileState(lfn, workDir, jobId, mode="transfer_mode", state=output["transfer_mode"], type="input")
        if status !=0:
            self.__sendReport(output["report"], report)
            return status, output["errorLog"]

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        status, output = self.stageIn(gpfn, fullname, fsize, fchecksum, experiment)

        if status == 0:
            updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")

        self.__sendReport(output["report"], report)
        return status, output["errorLog"]

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        # function is based on dCacheSiteMover put function

        error = PilotErrors()
        pilotErrorDiag = ""


        # Get input parameters from pdict
        alt = pdict.get('alt', False)
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        scope = pdict.get('scope', '')
        dsname = pdict.get('dsname', '')
        analysisJob = pdict.get('analJob', False)
        testLevel = pdict.get('testLevel', '0')
        extradirs = pdict.get('extradirs', '')
        experiment = pdict.get('experiment', '')
        proxycheck = pdict.get('proxycheck', False)
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        # get the site information object
        si = getSiteInformation(experiment)

        tolog("put_data received prodSourceLabel=%s" % (prodSourceLabel))
        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'xrdcp', lfn, guid)


        filename = os.path.basename(source)

        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = si.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename, scope=scope, alt=alt)
        if ec != 0:
            reportState = {}
            reportState["clientState"] = tracer_error
            self.__sendReport(reportState, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=surl)
        except Exception, e: 
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        if testLevel == "1":
            source = "thisisjustatest"

        status, output = self.stageOut(source, surl, token, experiment)
        if status !=0:
            self.__sendReport(output["report"], report)
            return self.put_data_retfail(status, output["errorLog"], surl)

        reportState = {}
        reportState["clientState"] = "DONE"
        self.__sendReport(reportState, report)
        return 0, pilotErrorDiag, surl, output["size"], output["checksum"], self.arch_type

    def errorToReport(self, errorOutput, timeUsed, fileName, stageMethod='stageIN'):
        status = 0
        outputRet = {}
        outputRet["errorLog"] = None
        outputRet["report"] = {}
        outputRet["report"]["clientState"] = None

        if "File exists" in errorOutput or "SRM_FILE_BUSY" in errorOutput or "file already exists" in errorOutput:
            pilotErrorDiag = "File already exist in the destination."
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            #self.__sendReport('FILE_EXIST', report)
            outputRet["report"]["clientState"] = 'FILE_EXIST'
            outputRet["errorLog"] = pilotErrorDiag
            return PilotErrors.ERR_FILEEXIST, outputRet
        elif "Could not establish context" in errorOutput:
            pilotErrorDiag = "Could not establish context: Proxy / VO extension of proxy has probably expired"
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            #self.__sendReport('CONTEXT_FAIL', report)
            outputRet["report"]["clientState"] = 'CONTEXT_FAIL'
            outputRet["errorLog"] = pilotErrorDiag
            return PilotErrors.ERR_NOPROXY, outputRet
        elif "globus_xio:" in errorOutput:
            pilotErrorDiag = "Globus system error: %s" % (errorOuput)
            self.log("Globus system error encountered")
            #self.__sendReport('GLOBUS_FAIL', report)
            outputRet["report"]["clientState"] = 'GLOBUS_FAIL'
            outputRet["errorLog"] = pilotErrorDiag
            return PilotErrors.ERR_GETGLOBUSSYSERR, outputRet
        elif "No space left on device" in errorOutput:
            pilotErrorDiag = "No available space left on local disk: %s" % (errorOutput)
            tolog("No available space left on local disk")
            #self.__sendReport('NO_SPACE', report)
            outputRet["report"]["clientState"] = 'NO_SPACE'
            outputRet["errorLog"] = pilotErrorDiag
            return PilotErrors.ERR_NOLOCALSPACE, outputRet
        elif "No such file or directory" in errorOutput:
            if "DBRelease" in fileName:
                pilotErrorDiag = "Missing DBRelease file: %s" % (fileName)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                #self.__sendReport('NO_DBREL', report)
                outputRet["report"]["clientState"] = 'NO_DBREL'
                outputRet["errorLog"] = pilotErrorDiag
                return PilotErrors.ERR_MISSDBREL, outputRet
            else:
                pilotErrorDiag = "No such file or directory: %s" % (fileName)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                #self.__sendReport('NO_FILE_DIR', report)
                outputRet["report"]["clientState"] = 'NO_FILE'
                outputRet["errorLog"] = pilotErrorDiag
                return PilotErrors.ERR_NOSUCHFILE, outputRet
        else:
            if timeUsed >= self.timeout:
                pilotErrorDiag = "Copy command self timed out after %d s" % (timeUsed)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                if stageMethod == "stageIN":
                    #self.__sendReport('GET_TIMEOUT', report)
                    outputRet["report"]["clientState"] = 'GET_TIMEOUT'
                    outputRet["errorLog"] = pilotErrorDiag
                    return PilotErrors.ERR_GETTIMEOUT, pilotErrorDiag
                else:
                    #self.__sendReport('CP_TIMEOUT', report)
                    outputRet["report"]["clientState"] = 'CP_TIMEOUT'
                    outputRet["errorLog"] = pilotErrorDiag
                    return PilotErrors.ERR_PUTTIMEOUT, outputRet
            else:
                if len(errorOutput) == 0:
                    pilotErrorDiag = "Copy command returned error code %d but no output" % (s)
                else:
                    pilotErrorDiag = errorOutput
                #self.__sendReport('COPY_ERROR', report)
                outputRet["report"]["clientState"] = 'COPY_ERROR'
                outputRet["errorLog"] = pilotErrorDiag
                if stageMethod == "stageIN":
                    return PilotErrors.ERR_STAGEINFAILED, outputRet
                else:
                    return PilotErrors.ERR_STAGEOUTFAILED, outputRet


    def __sendReport(self, reportState, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            for key in reportState.keys():
                report[key] = reportState[key]
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)
