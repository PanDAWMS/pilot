import os, re
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, verifySetupCommand
from time import time
from FileStateClient import updateFileState

class CastorSiteMover(SiteMover.SiteMover):
    """
    SiteMover for Castor (CERN; not tried elsewhere)
    """

    copyCommand = "rfcp"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = False
    has_chmod = False
    timeout = 5*3600

    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path

    def get_timeout(self):
        return self.timeout

    def check_space(self, ub):
        """ For when space availability is not verifiable """
        return 999999
    
    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ stage-in function """

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        useCT = pdict.get('usect', True)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'castor', lfn, guid)

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        s, pilotErrorDiag = self.verifyProxy(envsetup=envsetup)
        if s != 0:
            self.__sendReport('PROXYFAIL', report)
            return s, pilotErrorDiag

        # Strip off prefix in order to use rfcp directly
        tolog("gpfn: %s" % (gpfn))
        pat = re.compile('^.*(/castor/.*)$')
        mat = pat.match(gpfn)
        if mat:
            getfile = mat.group(1)
        else:
            pilotErrorDiag = "Get file not in castor: %s" % (gpfn)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('NO_FILE', report)
            return error.ERR_STAGEINFAILED, pilotErrorDiag

        # when the file has been copied we will rename it to the lfn (to remove the __DQ2-string on some files)
        dest_path = os.path.join(path, lfn)

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

        # transfer the input file with rfcp
        _cmd_str = '%srfcp %s %s' % (envsetup, getfile, dest_path)
        tolog("Executing command: %s" % (_cmd_str))
        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['validateStart'] = time()
        if s != 0:
            o = o.replace('\n', ' ')
            check_syserr(s, o)

            # remove the local file before any get retry is attempted
            _status = self.removeLocal(dest_path)
            if not _status:
                tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

            if o.find("No such file or directory") >= 0:
                if getfile.find("DBRelease") >= 0:
                    pilotErrorDiag = "Missing DBRelease file: %s" % (getfile)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    ec = error.ERR_MISSDBREL
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (getfile)
                    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                    ec = error.ERR_NOSUCHFILE
            else:
                pilotErrorDiag = "rfcp failed: %d, %s" % (s, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                ec = error.ERR_STAGEINFAILED
            self.__sendReport('RFCP_FAIL', report)
            return ec, pilotErrorDiag

        # check file size and checksum
        if fsize != 0 or fchecksum != 0:
            # which checksum type are we using?
            if fchecksum != 0 and fchecksum != "":
                csumtype = self.getChecksumType(fchecksum)
            else:
                csumtype = "default"

            # get remote file size and checksum 
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_path, csumtype=csumtype)
            tolog("File info: %d, %s, %s" % (ec, dstfsize, dstfchecksum))
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)

                # remove the local file before any get retry is attempted
                _status = self.removeLocal(dest_path)
                if not _status:
                    tolog("!!WARNING!!1112!! Failed to remove local file, get retry will fail")

                return ec, pilotErrorDiag

            # compare remote and local file size
            if fsize != 0 and dstfsize != fsize:
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

    def put_data(self, pfn, ddm_storage, fsize=0, fchecksum=0, dsname='', extradirs='', **pdict):
        """ Copy all output file to the local SE """

        error = PilotErrors()
        pilotErrorDiag = ""

        tolog("put_data() got ddm_storage=%s" % (ddm_storage))

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        analJob = pdict.get('analJob', False)

        # get the DQ2 tracing report
        report = self.getStubTracingReport(pdict['report'], 'castor', lfn, guid)

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        ec, pilotErrorDiag = verifySetupCommand(error, envsetup)
        if ec != 0:
            self.__sendReport('RFCP_FAIL', report)
            return self.put_data_retfail(ec, pilotErrorDiag) 

        s, pilotErrorDiag = self.verifyProxy(envsetup=envsetup, limit=2)
        if s != 0:
            self.__sendReport('PROXYFAIL', report)
            return self.put_data_retfail(s, pilotErrorDiag)
        filename = pfn.split('/')[-1]

        # the current file
        report['filename'] = lfn

        # guid
        report['guid'] = guid.replace('-','')

        # Destination is the top level Castor store area. Append a subdirectory which is first two fields of dsname, or 'other'
        destination = ""
        if not analJob:
            # seprodpath can have a complex structure in case of space tokens
            # although currently not supported in this site mover, prepare the code anyway
            # (use the first list item only)
            destination = self.getDirList(readpar('seprodpath'))[0]
            if destination == "":
                tolog("!!WARNING!!2999!! seprodpath not defined, using sepath")
                destination = readpar('sepath')
            tolog("Going to store production job output")
        else:
            destination = readpar('sepath')
            tolog("Going to store analysis job output")

        if destination == '':
            pilotErrorDiag = "put_data destination path in SE not defined"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('DEST_PATH_UNDEF', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        else:
            tolog("destination: %s" % (destination))

        if dsname == '':
            pilotErrorDiag = "Dataset name not specified to put_data"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('NO_DSN', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
#        else:
#            dsname = self.remove_sub(dsname)
#            tolog("dsname: %s" % (dsname))

        lfcpath, pilotErrorDiag = self.getLFCPath(analJob)
        if lfcpath == "":
            self.__sendReport('LFC_PATH_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        tolog("LFC path: %s" % (lfcpath))

        pat = re.compile('([^\.]+\.[^\.]+)\..*')
        mat = pat.match(dsname)
        if mat:
            prefixdir = mat.group(1)
            castor_destination = os.path.join(destination,prefixdir)
        else:
            pilotErrorDiag = "Unexpected dataset name format: %s" % (dsname)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('DSN_FORMAT_FAIL', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
        tolog("SE destination: %s" % (castor_destination))

        # set up paths differently for analysis and production jobs
        # use conventional LFC paths or production jobs
        # use OSG style for analysis jobs (for the time being)
        if not analJob:
            # return full lfc file path (beginning lfcpath might need to be replaced)
            native_lfc_path = self.to_native_lfn(dsname, filename)
            # /grid/atlas/dq2/testpanda/testpanda.destDB.b7cd4b56-1b5e-465a-a5d7-38d5e2609724_sub01000457/
            #58f836d5-ff4b-441a-979b-c37094257b72_0.job.log.tgz
            tolog("Native_lfc_path: %s" % (native_lfc_path))

            # replace the default path /grid/atlas/dq2 with lfcpath if different
            # (to_native_lfn returns a path begining with /grid/atlas/dq2)
            default_lfcpath = '/grid/atlas/dq2' # to_native_lfn always returns this at the beginning of the string
            if default_lfcpath != lfcpath:
                final_lfc_path = native_lfc_path.replace(default_lfcpath, lfcpath)
            else:
                final_lfc_path = native_lfc_path

            # name of dir to be created in LFC
            lfcdir = os.path.dirname(final_lfc_path)
            # /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492
            tolog("LFC dir: %s" % (lfcdir))

            # dst_gpfn = destination
            # dst_gpfn = os.path.join(destination, os.path.join(dsname, filename))
            # /pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492
            # tolog("dst_gpfn: %s" % (dst_gpfn))

        else: # for analysis jobs

            lfcdir = '%s/%s/%s' % ( lfcpath, prefixdir, dsname )
            tolog("lfcdir: %s" % (lfcdir))

        report['relativeStart'] = time()

        # name of dir to be created on Castor
        dirname = os.path.join(castor_destination, dsname)

        dst_gpfn = os.path.join(castor_destination, os.path.join(dsname, filename))
        tolog("dst_gpfn: %s" % (dst_gpfn))
        fppfn = os.path.abspath(pfn)

        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=dst_gpfn)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        tolog("Getting local file size")
        try:
            fsize = str(os.path.getsize(fppfn))
        except OSError, e:
            pilotErrorDiag = "Could not get file size: %s" % str(e)
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('NO_FILESIZE', report)
            return self.put_data_retfail(error.ERR_FAILEDSIZELOCAL, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # Strip off prefix in order to use rfcp directly
        pat = re.compile('^.*(/castor/.*)$')
        mat = pat.match(dst_gpfn)
        if mat:
            putfile = mat.group(1)
        else:
            pilotErrorDiag = "Put file not in castor: %s" % (dst_gpfn)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('FILE_NOT_IN', report)
            return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        _cmd_str = '%srfmkdir -p %s; rfcp %s %s' % (envsetup, dirname, fppfn, putfile)
        tolog("Castor put (%s->%s) cmd: %s" % (fppfn, putfile, _cmd_str))
        s = 0

        report['transferStart'] = time()
        s, o = commands.getstatusoutput(_cmd_str)
        report['catStart'] = time()
        
        if s == 0:
            # register the file in LFC
            # Maybe be a comma list but take first always
            se = readpar('se').split(",")[0]
            tolog("Using SE: %s" % (se))
            try:
                tolog("Using LFC_HOST: %s" % (os.environ['LFC_HOST']))
            except:
                os.environ['LFC_HOST'] = readpar('lfchost')
                tolog("Using LFC_HOST: %s" % (os.environ['LFC_HOST']))

            turl = '%s%s' % (se, putfile)

            # LFC LFN = /grid/atlas/dq2/testpanda/testpanda.destDB.dfb45803-1251-43bb-8e7a-6ad2b6f205be_sub01000492/
            #364aeb74-8b62-4c8f-af43-47b447192ced_0.job.log.tgz
            lfclfn = '%s/%s' % (lfcdir, lfn)
            tolog('LFC LFN = %s   GUID = %s'% (lfclfn, guid))

            cmd = "%slfc-mkdir -p %s" % (envsetup, lfcdir)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                check_syserr(s, o)
                pilotErrorDiag = "Error creating the dir: %d, %s" % (s, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('MKDIR_FAIL', report)
                return self.put_data_retfail(error.ERR_FAILEDLFCREG, pilotErrorDiag)

            cmd = "%swhich lcg-rf" % (envsetup)
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            tolog("status, output: %s, %s" % (s, o))
            cmd = "ls -lF %s" % o
            tolog("Executing command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            tolog("status, output: %s, %s" % (s, o))
            cmd = "%slcg-rf --vo atlas --verbose -g %s -l %s %s" % (envsetup, guid, lfclfn, turl)
            tolog("Registration command: %s" % (cmd))
            s, o = commands.getstatusoutput(cmd)
            tolog("Registration command status, output: %s, %s" % (s, o))
            if s != 0:
                check_syserr(s, o)
                pilotErrorDiag = "Error registering the file: %d, %s" % (s, o)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                self.__sendReport('REGISTER_FAIL', report)
                return self.put_data_retfail(error.ERR_FAILEDLCGREG, pilotErrorDiag)
        else:
            o = o.replace('\n', ' ')
            check_syserr(s, o)
            pilotErrorDiag = "Error copying the file: %d, %s" % (s, o)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            self.__sendReport('COPY_FAIL', report)
            return self.put_data_retfail(s, pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, dst_gpfn, 0, 0, self.arch_type


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
