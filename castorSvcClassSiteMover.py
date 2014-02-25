import os
import commands
import re

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand
from time import time
from FileStateClient import updateFileState
from timed_command import timed_command

class castorSvcClassSiteMover(SiteMover.SiteMover):
    """
    SiteMover for CASTOR, which finds the correct service class from which to stage in
    files via rfcp.
    """
    copyCommand = "rfcpsvcclass"
    checksum_command = "adler32"
    has_mkdir = True
    has_df = False
    has_getsize = True
    has_md5sum = False
    has_chmod = True
    timeout = 5*3600

    def __init__(self, setup_path='', *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def _check_space(self, ub):
        """CASTOR specific space verification.
        There is no simple way at the moment to verify CASTOR space availability - check info system instead"""
        return 999999
        
    def addMD5sum(self, lfn, md5sum):
        """ add md5sum to lfn """
        if os.environ.has_key('LD_LIBRARY_PATH'):
            tolog("LD_LIBRARY_PATH prior to lfc import: %s" % os.environ['LD_LIBRARY_PATH'])
        else:
            tolog("!!WARNING!!2999!! LD_LIBRARY_PATH not set prior to lfc import")
        import lfc
        os.environ['LFC_HOST'] = readpar('lfchost')
        stat = lfc.lfc_filestatg()
        exitcode = lfc.lfc_statg(lfn, "", stat)
        if exitcode != 0:
            #    print "error:",buffer
            err_num = lfc.cvar.serrno
            tolog("!!WARNING!!2999!! lfc.lfc_statg: %d %s" % (err_num, lfn))
            return exitcode
        exitcode = lfc.lfc_setfsizeg(stat.guid, stat.filesize, 'MD', md5sum)
        if exitcode != 0:
            #    print "error:",buffer
            err_num = lfc.cvar.serrno
            tolog("[Non-fatal] ERROR: lfc.lfc_setfsizeg: %d %s %s" % (err_num, lfn, md5sum))
            return exitcode
        tolog("Successfully set md5sum for %s" % (lfn))
        return exitcode

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ The local file is assubed to have a relative path that is the same of the relative path in the 'gpfn'
        loc_... are the variables used to access the file in the locally exported file system
        TODO: document GPFN format (SURL from catalog srm://host/path)
        TODO: document better constraint
        """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'castorSVC', lfn, guid)

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        # Hard code the configuration dictionary for now, but eventually this should be
        # set dynamically.
        #
        # There are the following configuration sections:
        #  setup - base environment veriables to be set
        #  svcClassMap - dictionary of string matches vs. service class names
        #  svcClassList - list of all service classes in case the svcClassMap matching fails
        #  svcClassDefault - the service class to set if the file appears to be staged no where
        #
        # Information from RAL:
        # [root@srm0661 ~]# listStorageArea -v atlas
        # <Space Token>                            <Description>   <service class>    <type>          <status>
        # 4948ef55-0000-1000-b7dd-9b38bdd87201    "ATLASGROUP"    "atlasStripDeg"     "DURABLE"       "ALLOCATED"
        # 4948ef38-0000-1000-8606-973e4e998e02    "ATLASMCDISK"   "atlasSimStrip"     "DURABLE"       "ALLOCATED"
        # 4948eec6-0000-1000-8ca2-aba0529b4806    "ATLASDATADISK" "atlasStripInput"   "DURABLE"       "ALLOCATED"
        # 4948ee8e-0000-1000-9ac5-81bb9b34ba7b    "ATLASMCTAPE"   "atlasSimRaw"       "PERMANENT"     "ALLOCATED"
        # 4948ee71-0000-1000-b611-a0afad31f6c8    "ATLASDATATAPE" "atlasT0Raw"        "PERMANENT"     "ALLOCATED"
        #                                         "ATLASHOTDISK"  "atlasHotDisk"      
        # In addition there is the "atlasFarm" class, which is used when data is staged back from tape
        castorConfig = {
            'setup' : {
                'STAGE_HOST' : 'catlasstager.ads.rl.ac.uk',
                'STAGER_HOST' : 'catlasstager.ads.rl.ac.uk',
                'RFIO_USE_CASTOR_V2' : 'YES',
                },
            'svcClassList' :  ('atlasHotDisk', 'atlasSimStrip', 'atlasStripInput', 'atlasFarm', 'atlasStripDeg', 'atlasT0Raw', 'atlasSimRaw', 'atlasScratchDisk', ),
            'svcClassMap' : {
                '/atlashotdisk/' : 'atlasHotDisk',
                '/atlasmcdisk/' : 'atlasStripInput',
                '/atlasdatadisk/' : 'atlasStripInput',
                '/atlasgroupdisk/' : 'atlasStripDeg',
                '/atlasdatatape/' : 'atlasFarm',
                '/atlasmctape/' : 'atlasFarm',
                '/atlasscratchdisk/' : 'atlasScratchDisk',
                '/atlasProdDisk/' : 'atlasScratchDisk',
                },
            'svcClassDefault' : 'atlasFarm',
            }


        # Set all environment variables for castor setup
        for envVar, value in castorConfig['setup'].iteritems():
            os.environ[envVar] = value

        # Strip the gpfn (SURL) back to its bare castor component
        tolog("gpfn is %s" % gpfn)

        if self._setup:
            _setup_str = "source %s; " % self._setup
        else:
            _setup_str = envsetup

        ec, pilotErrorDiag = verifySetupCommand(error, _setup_str)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        loc_pfn = ''
        if( gpfn.find('SFN') != -1 ):
            s = gpfn.split('SFN=')
            loc_pfn = s[1]
            tolog("Found SFN string. Local file name %s" % loc_pfn)
        else:
            _tmp = gpfn.split('/', 3)
            loc_pfn = '/'+_tmp[3]
            tolog("Splitting SURL on slashes. Got local file name %s" % loc_pfn)

        if not loc_pfn.startswith('/castor/'):
            tolog("WARNING: Problem with local filename: Does not start with '/castor/'.")

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.__sendReport('FOUND_ROOT', report)
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                    return error.ERR_DIRECTIOFILE, pilotErrorDiag
                else:
                    tolog("Normal file transfer")

        # Now need to find the service class associated with the file.
        # If we find a clear indication of a space token in the file path 
        # then this is easy. However, if we don't, then use stager_qry to 
        # interrogate each possible service class. If this fails then use
        # atlasFarm in desperation.
        serviceClass = None
        for pathMatch, svcClass in castorConfig['svcClassMap'].iteritems():
            if loc_pfn.find(pathMatch) >= 0:
                tolog('Matched path element %s - service class is %s' % (pathMatch, svcClass))
                serviceClass = svcClass
                break
            else:
                tolog('Path element %s for service class %s - no match' % (pathMatch, svcClass))

        # For testing the fallback, then we need to hobble ourselves by unsetting serviceClass:
        #tolog('Automatic service class was: %s' % serviceClass)
        #tolog('Unsetting service class for fallback testing')
        #serviceClass = None        
        if serviceClass == None:
            tolog("Warning: Failed to find service class hint in SURL.")

            for tryMe in castorConfig['svcClassList']:
                os.environ['STAGE_SVCCLASS'] = tryMe
                tolog('Trying service class %s for file' % tryMe)
                err, output = commands.getstatusoutput('stager_qry -M %s' % loc_pfn)
                if err != 0:
                    tolog('WARNING: Unexpected status from stager_qry: %d\n%s' % (err, output))
                else:
                    if output.find('STAGED') >= 0:
                        tolog('Found file in service class %s' % tryMe)
                        serviceClass = tryMe
                        break
                    else:
                        tolog('File not found in service class %s' % tryMe)
            if serviceClass == None:
                tolog('WARNING: Failed to find file in any expected service class - will set STAGE_SVCCLASS to %s' % castorConfig['svcClassDefault'])
                serviceClass = castorConfig['svcClassDefault']

        tolog('Setting STAGE_SVCCLASS to %s' % serviceClass)
        os.environ['STAGE_SVCCLASS'] = serviceClass

        dest_path = os.path.join(path, lfn)
        _cmd_str = '%s/usr/bin/rfcp %s %s' % (_setup_str, loc_pfn, dest_path)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()

        # execute
        timeout = 3600
        try:
            s, telapsed, cout, cerr = timed_command(_cmd_str, timeout)
        except Exception, e:
            pilotErrorDiag = 'timed_command() threw an exception: %s' % (e)
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            s = 1
            o = str(e)
            telapsed = timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            o = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))

        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            pilotErrorDiag = "rfcp failed: %d, %s"  % (s, o)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            check_syserr(s, o)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            ec = error.ERR_STAGEINFAILED
            if o.find("No such file or directory") >= 0:
                if loc_pfn.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (loc_pfn)
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    ec = error.ERR_MISSDBREL
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (loc_pfn)
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    ec = error.ERR_NOSUCHFILE
                self.__sendReport('RFCP_FAIL', report)
            elif is_timeout(s):
                pilotErrorDiag = "rfcp get was timed out after %d seconds" % (telapsed)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('GET_TIMEOUT', report)
                ec = error.ERR_GETTIMEOUT
            return ec, pilotErrorDiag
        else:
            tolog("Copy command finished")
        if fsize == 0:
            try:
                fsize = str(os.path.getsize(loc_pfn))
            except OSError, e:
                pilotErrorDiag = "Could not get file size: %s" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                self.__sendReport('FS_FAIL', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(dest_path)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return error.ERR_FAILEDSIZELOCAL, pilotErrorDiag
        loc_filename = lfn
        dest_file = os.path.join(path, loc_filename)

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # get remote file size and checksum 
        ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
        if ec != 0:
            self.__sendReport('LOCAL_FILE_INFO_FAIL', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return ec, pilotErrorDiag

        # get remote file size and checksum 
        if dstfsize != fsize:
            pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                             (os.path.basename(gpfn), str(dstfsize), str(fsize))
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('FS_MISMATCH', report)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            return error.ERR_GETWRONGSIZE, pilotErrorDiag

        # compare remote and local file checksum
        if fchecksum != 0 and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
            pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                             (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            if csumtype == "adler32":
                self.__sendReport('AD_MISMATCH', report)
                return error.ERR_GETADMISMATCH, pilotErrorDiag
            else:
                self.__sendReport('MD5_MISMATCH', report)
                return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, ddm_storage, fsize=0, fchecksum=0, dsname='', **pdict):
        """ Data transfer using rfcp - generic version
        It's not advisable to use this right now because there's no
        easy way to register the srm space token if the file is 
        copied with rfcp """

        error = PilotErrors()

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'castorSVC', lfn, guid)

        pilotErrorDiag = "put_data does not work for this mover"
        tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
        self.__sendReport('NOT_IMPL', report)
        return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)


    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)

