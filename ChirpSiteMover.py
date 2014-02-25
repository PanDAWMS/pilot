import os, re, sys
import commands

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, getDirectAccessDic
from timed_command import timed_command
from time import time

class ChirpSiteMover(SiteMover.SiteMover):
    """ SiteMover for CHIRP copy commands etc """
    
    copyCommand = "chirp"
    checksum_command = "adler32"
    __warningStr = '!!WARNING!!2995!! %s'
    __chirp = 'chirp -t 300 %s %s < %s' # options,server, command file
    __timeout = 300 # seconds
    __error = PilotErrors()
    __pilotErrorDiag = ''
    __MAX_FILE_SIZE = 200*1024**2

    def get_timeout(self):
        return self.__timeout

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'local'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        if not path:
            tolog('path is empty, using current directory')            
            path = os.getcwd()

        # build setup string
        envsetup = self.getEnvsetup(get=True)

        # should the root file be copied or read directly by athena?
        directIn = False
        dInfo = getDirectAccessDic(readpar('copysetupin'))
        # if copysetupin did not contain direct access info, try the copysetup instead
        if not dInfo:
            dInfo = getDirectAccessDic(readpar('copysetup'))

        tolog("dInfo: %s" % str(dInfo))
        # check if we should use the copytool
        if dInfo:
            directIn = dInfo['directIn']

        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.__sendReport('FOUND_ROOT', report)
                    return 0, self.__pilotErrorDiag
                else:
                    tolog("Normal file transfer")
        else:
            tolog("not directIn")

        # build the get command
        _params = ""
        if fchecksum and fchecksum != 'None' and fchecksum != 0 and fchecksum != "0" and not self.isDummyChecksum(fchecksum):
            csumtype = self.getChecksumType(fchecksum)
            # special case for md5sum (command only understands 'md5' and 'adler32', and not 'ad' and 'md5sum')
            if csumtype == 'md5sum':
                csumtype = 'md5'

        execStr = self.__localget % (envsetup, _params, gpfn, os.path.join(path, lfn))
        tolog("Executing command: %s" % (execStr))
        
        report['transferStart'] = time()
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            # improve output parsing, keep stderr and stdout separate
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))
        tolog("Command output:\n%s" % (output))
        report['validateStart'] = time()

        if status:
            # did the copy command time out?
            if is_timeout(status):
                self.__pilotErrorDiag = "lsm-get failed: time out after %d seconds" % (telapsed)
                tolog(self.__warningStr % self.__pilotErrorDiag)            
                self.__sendReport('GET_TIMEOUT', report)
                return self.__error.ERR_GETTIMEOUT, self.__pilotErrorDiag

            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'lsm-get failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)            
            self.__sendReport('COPY_FAIL', report)
            return self.__error.ERR_STAGEINFAILED, self.__pilotErrorDiag

        # the lsm-get command will compare the file size and checksum with the catalog values
        self.__sendReport('DONE', report)
        return 0, self.__pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from local dir to SE and register into dataset and catalogues """

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        dsname = pdict.get('dsname', '')
        analJob = pdict.get('analJob', False)
        sitename = pdict.get('sitename', '')
        extradirs = pdict.get('extradirs', '')
        prodSourceLabel = pdict.get('prodSourceLabel', '')
        dispatchDBlockTokenForOut = pdict.get('dispatchDBlockTokenForOut', '')

        if sitename == "CERNVM" and dispatchDBlockTokenForOut == "":
            dispatchDBlockTokenForOut = "chirp^cvmappi50.cern.ch^/panda_test^-d chirp"

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'local'
            # mark the relative start
            report['relativeStart'] = time()
            # the current file
            report['filename'] = lfn
            report['guid'] = guid.replace('-','')
#            report['dataset'] = dsname

        filename = os.path.basename(source)

        # get the local file size and checksum
        csumtype = self.checksum_command
        if fsize == 0 or fchecksum == 0:
            ec, self.__pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, self.__pilotErrorDiag)

        # do not transfer files larger than 50 MB except for CERNVM
        if sitename != "CERNVM" and int(fsize) > self.__MAX_FILE_SIZE:
            self.__pilotErrorDiag = "File sizes larger than %d B can currently not be tranferred with this site mover: size=%s" % (self.__MAX_FILE_SIZE, fsize)
            tolog("!!WARNING!!2997!! %s" % (self.__pilotErrorDiag))
            return self.put_data_retfail(self.__error.ERR_OUTPUTFILETOOLARGE, self.__pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # build the command
        _params = ""
        if fchecksum != 0:
            # special case for md5sum (command only understands 'md5' and 'adler32', and not 'ad' and 'md5sum')
            if csumtype == 'md5sum':
                _csumtype = 'md5'
            else:
                _csumtype = csumtype

       # This contains the user configuration for chirp server, path, debug
       # Format should be like
       # 'chirp^etpgrid01.garching.physik.uni-muenchen.de^/tanyasandoval^-d chirp'
        dispatchDBlockTokenForOut = pdict.get('dispatchDBlockTokenForOut', '')
        
        csplit = dispatchDBlockTokenForOut.split('^')
        if len(csplit) != 4:
          tolog("Wrong number of fields in chirp string: %s" %
                (dispatchDBlockTokenForOut))
          self.__pilotErrorDiag = "Wrong number of fields in chirp string: %s" % (dispatchDBlockTokenForOut)
          return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)        

       # Remove _sub part from dataset name
        re_sub=re.compile('(.*)_sub\d+')
        resub =re_sub.search(dsname)
        if resub:
            dsname_strip=resub.group(1)
        else:
            dsname_strip=dsname
        
        chirp_server = csplit[1]
        chirp_base = csplit[2]
        chirp_options = csplit[3]
        chirp_path = chirp_base+'/'+dsname_strip+'/'+filename

        tolog("Chirp path: %s" % (chirp_path))

       # Make compound command file to run in chirp
        chirpcom=open('chirp.com','w')
       # Create directories.
        dirs=chirp_path.split('/')
        dir_path=''
        for i in range(1,len(dirs)-1):
          dir_path=dir_path+'/'+dirs[i]
          chirpcom.write('mkdir '+dir_path+'\n')
       # and the cop command too
        chirpcom.write('put %s %s\n'%(source,chirp_path))
        chirpcom.close()  
        execStr = self.__chirp % (chirp_options, chirp_server, 'chirp.com')
        tolog("Executing command: %s" % (execStr))
        try:
            status, telapsed, cout, cerr = timed_command(execStr, self.__timeout)
        except Exception, e:
            self.__pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            status = 1
            output = str(e)
            telapsed = self.__timeout
        else:
            output = cout + cerr

        tolog("Elapsed time: %d" % (telapsed))
        tolog("Command output:\n%s" % (output))

        # validate
        if status:
            # did the copy command time out?
            if is_timeout(status):
                self.__pilotErrorDiag = "chirp_put failed: time out after %d seconds" % (telapsed)
                tolog(self.__warningStr % self.__pilotErrorDiag)            
                self.__sendReport('PUT_TIMEOUT', report)
                return self.put_data_retfail(self.__error.ERR_PUTTIMEOUT, self.__pilotErrorDiag)

            status = os.WEXITSTATUS(status)
            self.__pilotErrorDiag = 'chirp_put failed (%s): %s' % (status, output)
            tolog(self.__warningStr % self.__pilotErrorDiag)
            self.__sendReport('COPY_FAIL', report)
            return self.put_data_retfail(self.__error.ERR_STAGEOUTFAILED, self.__pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, self.__pilotErrorDiag, chirp_path, fsize, fchecksum, self.arch_type

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


if __name__ == '__main__':
  sitemover = ChirpSiteMover()
  pfn='README'
  ddm_storage=''
  dsname='user.tanyasandoval.0630133553.583378.lib._000181_sub012345'
  sitename=''
  analJob=True
  testLevel=0
  pinitdir=''
  dest=''
  proxycheck=''
  _token_file=''
  DEFAULT_TIMEOUT=10
  lfn=''
  guid=''
  spsetup=''
  userid=''
  report={}
  prodSourceLabel=''
  outputDir=''
  DN=''
  s, pilotErrorDiag, r_gpfn, r_fsize, r_fchecksum, r_farch = sitemover.put_data(pfn, ddm_storage, dsname=dsname, sitename=sitename,analJob=analJob, testLevel=testLevel, pinitdir=pinitdir, dest=dest,proxycheck=proxycheck, token=_token_file, timeout=DEFAULT_TIMEOUT, lfn=lfn, guid=guid, spsetup=spsetup, userid=userid, report=report,prodSourceLabel=prodSourceLabel, outputDir=outputDir, DN=DN, dispatchDBlockTokenForOut='chirp^2^3^4')
  tolog("Site mover put function returned: s=%s, r_gpfn=%s, r_fsize=%s, r_fchecksum=%s, r_farch=%s, pilotErrorDiag=%s" % (s, r_gpfn, r_fsize, r_fchecksum, r_farch, pilotErrorDiag))
